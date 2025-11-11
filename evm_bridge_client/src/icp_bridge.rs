// ICP CkDeposit bridge integration

use crate::{client::EvmClient, erc20::Erc20Client, errors::EvmError, types::*};
use alloy::{providers::DynProvider, sol};

sol! {
    #[sol(rpc)]
    contract CkDeposit {
        event ReceivedEth(
            uint256 indexed value,
            bytes32 indexed principal,
            bytes32 subaccount,
            address indexed from
        );

        event ReceivedErc20(
            uint256 indexed value,
            bytes32 indexed principal,
            address indexed erc20,
            bytes32 subaccount,
            address from
        );

        function getMinterAddress() external view returns (address);
        function depositEth(bytes32 principal, bytes32 subaccount) external payable;
        function depositErc20(address token, uint256 amount, bytes32 principal, bytes32 subaccount) external;
    }
}

#[derive(Debug, Clone)]
pub struct DepositEvent {
    pub value: U256,
    pub principal: [u8; 32],
    pub subaccount: [u8; 32],
    pub from: Address,
    pub erc20: Option<Address>,
}

#[derive(Clone)]
pub struct IcpBridge {
    pub bridge: Address,
    pub client: EvmClient,
}

impl IcpBridge {
    pub fn new(bridge: Address, client: EvmClient) -> Self {
        Self { bridge, client }
    }

    fn contract(&self) -> CkDeposit::CkDepositInstance<DynProvider> {
        CkDeposit::new(self.bridge, self.client.provider.clone())
    }

    pub async fn get_minter_address(&self) -> Result<Address, EvmError> {
        self.contract()
            .getMinterAddress()
            .call()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))
    }

    // Deposit ETH to ICP
    pub async fn deposit_eth(
        &self,
        principal: [u8; 32],
        subaccount: [u8; 32],
        wei: U256,
    ) -> Result<(TransactionReceipt, Option<DepositEvent>), EvmError> {
        let res = self
            .contract()
            .depositEth(principal.into(), subaccount.into())
            .value(wei)
            .send()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))?;

        let receipt = res.get_receipt().await.map_err(|e| EvmError::Other(e.to_string()))?;

        // Parse event
        let event = self.find_deposit_event_by_tx(receipt.transaction_hash).await?;

        Ok((receipt, event))
    }

    // Deposit ERC-20 to ICP
    pub async fn deposit_erc20(
        &self,
        token: Address,
        amount: U256,
        principal: [u8; 32],
        subaccount: [u8; 32],
        ensure_allowance: bool,
        spender_override: Option<Address>,
    ) -> Result<(TransactionReceipt, Option<DepositEvent>), EvmError> {
        // Ensure allowance if requested
        if ensure_allowance {
            let erc20 = Erc20Client::new(token, self.client.clone());
            let spender = spender_override.unwrap_or(self.bridge);
            erc20.ensure_allowance(spender, amount).await?;
        }

        let res = self
            .contract()
            .depositErc20(token, amount, principal.into(), subaccount.into())
            .send()
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))?;

        let receipt = res.get_receipt().await.map_err(|e| EvmError::Other(e.to_string()))?;

        // Parse event
        let event = self.find_deposit_event_by_tx(receipt.transaction_hash).await?;

        Ok((receipt, event))
    }

    // Find deposit event by transaction hash
    pub async fn find_deposit_event_by_tx(&self, tx_hash: B256) -> Result<Option<DepositEvent>, EvmError> {
        let receipt = self
            .client
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))?
            .ok_or(EvmError::Other("receipt not found".into()))?;

        // Parse logs for ReceivedEth or ReceivedErc20
        for log in receipt.inner.logs() {
            if log.address() != self.bridge {
                continue;
            }

            // Try parsing as ReceivedEth
            if let Ok(eth_event) = log.log_decode::<CkDeposit::ReceivedEth>() {
                return Ok(Some(DepositEvent {
                    value: eth_event.data().value,
                    principal: eth_event.data().principal.0,
                    subaccount: eth_event.data().subaccount.0,
                    from: eth_event.data().from,
                    erc20: None,
                }));
            }

            // Try parsing as ReceivedErc20
            if let Ok(erc20_event) = log.log_decode::<CkDeposit::ReceivedErc20>() {
                return Ok(Some(DepositEvent {
                    value: erc20_event.data().value,
                    principal: erc20_event.data().principal.0,
                    subaccount: erc20_event.data().subaccount.0,
                    from: erc20_event.data().from,
                    erc20: Some(erc20_event.data().erc20),
                }));
            }
        }

        Ok(None)
    }

    // Find deposits in a block range with optional filters
    pub async fn find_deposits(
        &self,
        from_block: u64,
        to_block: u64,
        principal_filter: Option<[u8; 32]>,
    ) -> Result<Vec<DepositEvent>, EvmError> {
        use alloy::{rpc::types::Filter, sol_types::SolEvent};

        let mut filter = Filter::new().address(self.bridge).from_block(from_block).to_block(to_block);

        // Add event signatures
        filter = filter.event_signature(vec![
            CkDeposit::ReceivedEth::SIGNATURE_HASH,
            CkDeposit::ReceivedErc20::SIGNATURE_HASH,
        ]);

        // Add principal filter if specified
        if let Some(principal) = principal_filter {
            filter = filter.topic1(B256::from(principal));
        }

        let logs = self
            .client
            .provider
            .get_logs(&filter)
            .await
            .map_err(|e| EvmError::Rpc(e.to_string()))?;

        let mut events = Vec::new();

        for log in logs {
            // Try ReceivedEth
            if let Ok(eth_event) = log.log_decode::<CkDeposit::ReceivedEth>() {
                events.push(DepositEvent {
                    value: eth_event.data().value,
                    principal: eth_event.data().principal.0,
                    subaccount: eth_event.data().subaccount.0,
                    from: eth_event.data().from,
                    erc20: None,
                });
                continue;
            }

            // Try ReceivedErc20
            if let Ok(erc20_event) = log.log_decode::<CkDeposit::ReceivedErc20>() {
                events.push(DepositEvent {
                    value: erc20_event.data().value,
                    principal: erc20_event.data().principal.0,
                    subaccount: erc20_event.data().subaccount.0,
                    from: erc20_event.data().from,
                    erc20: Some(erc20_event.data().erc20),
                });
            }
        }

        Ok(events)
    }
}
