use alloy::{
    network::{AnyNetwork, ReceiptResponse},
    primitives::{Address, FixedBytes, TxHash, U256},
    providers::{Provider, WalletProvider},
    sol,
};
use async_trait::async_trait;
use candid::{CandidType, Encode, Nat, Principal};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::approve::ApproveArgs,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::{
    backend::{
        amount_utils::{
            amount_to_base_units_strict, amount_to_nat_units_strict, base_units_to_amount_via_core,
            nat_units_to_amount_via_core,
        },
        bridge_backend::{
            BridgeBackend, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus, BridgeSubmission,
            resolve_cketh_forward_route_by_source, resolve_cketh_reverse_route_by_source,
        },
        evm_backend::EvmBackendImpl,
        icp_backend::IcpBackend,
        icp_backend_helpers::{icrc1_balance_with_context, icrc1_decimals_with_context, icrc2_approve_with_context},
    },
    pipeline_agent::PipelineAgent,
};
use super::ckerc20_bridge_utils::{
    destination_to_bytes32, ensure_source_matches_bridge_owner, ensure_source_matches_signer, expect_evm_destination,
    expect_icp_destination, parse_ckerc20_ledger_id, parse_evm_token_address, parse_source_icp_account,
    resolve_cketh_route_for_request,
};

sol! {
    #[sol(rpc)]
    interface ICkErc20HelperNative {
        function deposit(address token, uint256 amount, bytes32 principal) external;
    }
}

sol! {
    #[sol(rpc)]
    interface ICkErc20HelperWithSubaccount {
        function depositErc20(address erc20Address, uint256 amount, bytes32 principal, bytes32 subaccount) external;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvmReceiptStatus {
    pub success: bool,
    pub block_number: Option<u64>,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BridgeEvmBackend: Send + Sync {
    fn signer_address(&self) -> Address;

    async fn erc20_balance_of(&self, token: Address, owner: Address) -> Result<U256, String>;
    async fn erc20_decimals_of(&self, token: Address) -> Result<u8, String>;
    async fn erc20_approve_and_wait(&self, token: Address, spender: Address, amount: U256) -> Result<TxHash, String>;

    async fn helper_deposit_native(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn helper_deposit_with_subaccount(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn receipt_status(&self, tx_hash: TxHash) -> Result<Option<EvmReceiptStatus>, String>;
}

#[async_trait]
impl<P> BridgeEvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    fn signer_address(&self) -> Address {
        self.provider.default_signer_address()
    }

    async fn erc20_balance_of(&self, token: Address, owner: Address) -> Result<U256, String> {
        self.erc20_balance_of_raw(token, owner).await
    }

    async fn erc20_decimals_of(&self, token: Address) -> Result<u8, String> {
        self.erc20_decimals_raw(token).await
    }

    async fn erc20_approve_and_wait(&self, token: Address, spender: Address, amount: U256) -> Result<TxHash, String> {
        self.erc20_approve_and_wait_raw(token, spender, amount).await
    }

    async fn helper_deposit_native(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkErc20HelperNative::new(helper, self.provider.clone());
        let pending = helper_contract
            .deposit(token, amount, recipient_principal)
            .send()
            .await
            .map_err(|e| format!("native helper deposit failed (helper={helper}, token={token}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn helper_deposit_with_subaccount(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkErc20HelperWithSubaccount::new(helper, self.provider.clone());
        let pending = helper_contract
            .depositErc20(token, amount, recipient_principal, recipient_subaccount)
            .send()
            .await
            .map_err(|e| format!("helper depositErc20 failed (helper={helper}, token={token}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn receipt_status(&self, tx_hash: TxHash) -> Result<Option<EvmReceiptStatus>, String> {
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| format!("transaction receipt fetch failed for {tx_hash:#x}: {e}"))?;

        Ok(receipt.map(|receipt| EvmReceiptStatus {
            success: receipt.status(),
            block_number: receipt.block_number,
        }))
    }
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkEthMinterInfo {
    #[serde(default)]
    deposit_with_subaccount_helper_contract_address: Option<String>,
    #[serde(default)]
    erc20_helper_contract_address: Option<String>,
    #[serde(default)]
    cketh_ledger_id: Option<Principal>,
}

#[derive(Clone, Copy, Debug)]
enum HelperContract {
    WithSubaccount(Address),
    Native(Address),
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct Eip1559TransactionPriceArg {
    ckerc20_ledger_id: Principal,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct Eip1559TransactionPrice {
    max_priority_fee_per_gas: Nat,
    max_fee_per_gas: Nat,
    max_transaction_fee: Nat,
    timestamp: Option<u64>,
    gas_limit: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct WithdrawErc20Arg {
    ckerc20_ledger_id: Principal,
    recipient: String,
    from_cketh_subaccount: Option<Vec<u8>>,
    from_ckerc20_subaccount: Option<Vec<u8>>,
    amount: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct RetrieveErc20Request {
    ckerc20_block_index: Nat,
    cketh_block_index: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkErc20Token {
    erc20_contract_address: String,
    ledger_canister_id: Principal,
    ckerc20_token_symbol: String,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum LedgerError {
    TemporarilyUnavailable(String),
    InsufficientAllowance {
        token_symbol: String,
        ledger_id: Principal,
        allowance: Nat,
        failed_burn_amount: Nat,
    },
    AmountTooLow {
        minimum_burn_amount: Nat,
        token_symbol: String,
        ledger_id: Principal,
        failed_burn_amount: Nat,
    },
    InsufficientFunds {
        balance: Nat,
        token_symbol: String,
        ledger_id: Principal,
        failed_burn_amount: Nat,
    },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum WithdrawErc20Error {
    TokenNotSupported { supported_tokens: Vec<CkErc20Token> },
    TemporarilyUnavailable(String),
    CkErc20LedgerError { error: LedgerError, cketh_block_index: Nat },
    CkEthLedgerError { error: LedgerError },
    RecipientAddressBlocked { address: String },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum WithdrawErc20Ret {
    Ok(RetrieveErc20Request),
    Err(WithdrawErc20Error),
}

/// Bridge backend for `ERC20@ETH -> ckERC20` forward routes via ckETH minter helper contracts.
///
/// Token and route selection are resolved from bridge route metadata.
pub struct CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    pub agent: Arc<A>,
    pub icp_backend: Arc<B>,
    pub evm_backend: Arc<E>,
    pub cketh_minter_canister: Principal,
    pub bridge_ic_owner_principal: Principal,
}

impl<A, B, E> CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    pub fn new(
        agent: Arc<A>,
        icp_backend: Arc<B>,
        evm_backend: Arc<E>,
        cketh_minter_canister: Principal,
        bridge_ic_owner_principal: Principal,
    ) -> Self {
        Self {
            agent,
            icp_backend,
            evm_backend,
            cketh_minter_canister,
            bridge_ic_owner_principal,
        }
    }

    async fn minter_info(&self) -> Result<CkEthMinterInfo, String> {
        let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
        match self
            .agent
            .call_query::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args.clone())
            .await
        {
            Ok(v) => Ok(v),
            Err(query_err) => self
                .agent
                .call_update::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args)
                .await
                .map_err(|update_err| {
                    format!(
                        "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                        self.cketh_minter_canister
                    )
                }),
        }
    }

    async fn helper_contract(&self) -> Result<HelperContract, String> {
        let info = self.minter_info().await?;

        if let Some(addr) = info.deposit_with_subaccount_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid deposit_with_subaccount helper address '{addr}': {e}"))?;
            return Ok(HelperContract::WithSubaccount(parsed));
        }

        if let Some(addr) = info.erc20_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid erc20 helper address '{addr}': {e}"))?;
            return Ok(HelperContract::Native(parsed));
        }

        Err(format!(
            "minter {} returned no helper contract address in get_minter_info",
            self.cketh_minter_canister
        ))
    }

    async fn eip_1559_transaction_price(
        &self,
        ckerc20_ledger_id: Principal,
    ) -> Result<Eip1559TransactionPrice, String> {
        let args = Encode!(&Some(Eip1559TransactionPriceArg { ckerc20_ledger_id }))
            .map_err(|e| format!("encode eip_1559_transaction_price args failed: {e}"))?;
        self.agent
            .call_query::<Eip1559TransactionPrice>(&self.cketh_minter_canister, "eip_1559_transaction_price", args)
            .await
            .map_err(|e| {
                format!(
                    "eip_1559_transaction_price failed for minter {}: {}",
                    self.cketh_minter_canister, e
                )
            })
    }

    async fn token_decimals(&self, token: Address) -> Result<u8, String> {
        self.evm_backend.erc20_decimals_of(token).await
    }

    async fn approve_minter_spend(&self, ledger: Principal, amount: Nat) -> Result<(), String> {
        let approve_args = ApproveArgs {
            from_subaccount: None,
            spender: Account {
                owner: self.cketh_minter_canister,
                subaccount: None,
            },
            amount,
            expected_allowance: None,
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };
        icrc2_approve_with_context(self.icp_backend.as_ref(), ledger, approve_args, "ckerc20 bridge")
            .await
            .map(|_| ())?;
        Ok(())
    }

    fn with_fee_headroom(amount: &Nat) -> Nat {
        // Keep a modest buffer for quote drift between preflight and withdraw burn.
        amount.clone() + (amount.clone() / Nat::from(5u8))
    }

    async fn withdraw_erc20_call(&self, args: &WithdrawErc20Arg) -> Result<WithdrawErc20Ret, String> {
        let arg_blob = Encode!(args).map_err(|e| format!("encode withdraw_erc20 args failed: {e}"))?;
        self.agent
            .call_update::<WithdrawErc20Ret>(&self.cketh_minter_canister, "withdraw_erc20", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "withdraw_erc20 call failed for minter {}: {}",
                    self.cketh_minter_canister, e
                )
            })
    }

    async fn submit_forward_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let token_address = parse_evm_token_address(route)?;

        let signer = self.evm_backend.signer_address();
        // Forward bridging spends from the connected EVM wallet only.
        ensure_source_matches_signer(&request.source_address, signer)?;

        let destination_account = expect_icp_destination(route, &request.destination)?;
        // ckETH helper contracts accept principal/subaccount as bytes32 values.
        let (recipient_principal_bytes32, recipient_subaccount_bytes32) = destination_to_bytes32(destination_account);

        // Prefer helper with subaccount support when minter exposes it; fall back to native helper otherwise.
        let helper = self.helper_contract().await?;
        let helper_address = match helper {
            HelperContract::WithSubaccount(address) | HelperContract::Native(address) => address,
        };

        let decimals = self.token_decimals(token_address).await?;
        let amount_base_units = amount_to_base_units_strict(request.amount, decimals)?;

        // Helper must be approved before deposit call can transfer ERC20 from signer.
        self.evm_backend
            .erc20_approve_and_wait(token_address, helper_address, amount_base_units)
            .await
            .map_err(|e| format!("ERC20 approve(helper={helper_address}) failed: {e}"))?;

        let tx_hash = match helper {
            HelperContract::WithSubaccount(address) => self
                .evm_backend
                .helper_deposit_with_subaccount(
                    address,
                    token_address,
                    amount_base_units,
                    recipient_principal_bytes32,
                    recipient_subaccount_bytes32,
                )
                .await
                .map_err(|e| format!("helper depositErc20 failed: {e}"))?,
            HelperContract::Native(address) => self
                .evm_backend
                .helper_deposit_native(address, token_address, amount_base_units, recipient_principal_bytes32)
                .await
                .map_err(|e| format!("native helper deposit failed: {e}"))?,
        };

        // Return the submitted tx hash; completion is tracked asynchronously via get_bridge_status.
        Ok(BridgeSubmission {
            bridge_id: format!("{:#x}", tx_hash),
        })
    }

    async fn submit_reverse_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let source_account = parse_source_icp_account(&request.source_address)?;
        // Reverse flow is restricted to the configured bridge owner principal.
        ensure_source_matches_bridge_owner(&source_account, self.bridge_ic_owner_principal)?;

        let destination = expect_evm_destination(route, &request.destination)?;

        let ckerc20_ledger_id = parse_ckerc20_ledger_id(route)?;
        let ckusdc_decimals =
            icrc1_decimals_with_context(self.icp_backend.as_ref(), ckerc20_ledger_id, "ckerc20 bridge").await?;
        let amount_native = amount_to_nat_units_strict(request.amount, ckusdc_decimals)?;

        let minter_info = self.minter_info().await?;
        let cketh_ledger_id = minter_info.cketh_ledger_id.ok_or_else(|| {
            format!(
                "minter {} returned no cketh_ledger_id in get_minter_info",
                self.cketh_minter_canister
            )
        })?;
        let fee_quote = self.eip_1559_transaction_price(ckerc20_ledger_id).await?;
        let required_fee = fee_quote.max_transaction_fee;
        let required_fee_budget = Self::with_fee_headroom(&required_fee);

        // Minter withdraw burns ckERC20 and consumes ckETH for the EVM execution fee.
        let available_cketh = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            cketh_ledger_id,
            &source_account,
            "ckerc20 bridge",
        )
        .await?;
        if available_cketh < required_fee_budget {
            let cketh_decimals =
                icrc1_decimals_with_context(self.icp_backend.as_ref(), cketh_ledger_id, "ckerc20 bridge")
                    .await
                    .unwrap_or(18);
            let available_formatted = nat_units_to_amount_via_core(&available_cketh, cketh_decimals)?;
            let required_formatted = nat_units_to_amount_via_core(&required_fee_budget, cketh_decimals)?;
            return Err(format!(
                "bridge fee preflight failed: ckETH balance is below required transaction fee budget (available={} required={} source={})",
                available_formatted, required_formatted, request.source_address
            ));
        }

        // Approve ckERC20 amount to burn for withdrawal.
        self.approve_minter_spend(ckerc20_ledger_id, amount_native.clone())
            .await?;
        // Approve ckETH fee budget quoted by minter.
        self.approve_minter_spend(cketh_ledger_id, required_fee_budget).await?;

        let withdraw_args = WithdrawErc20Arg {
            ckerc20_ledger_id,
            recipient: destination.to_string(),
            from_cketh_subaccount: None,
            from_ckerc20_subaccount: None,
            amount: amount_native,
        };

        let first_try = self.withdraw_erc20_call(&withdraw_args).await?;
        let result = match first_try {
            ok @ WithdrawErc20Ret::Ok(_) => ok,
            WithdrawErc20Ret::Err(WithdrawErc20Error::CkEthLedgerError {
                error: LedgerError::InsufficientAllowance { failed_burn_amount, .. },
            }) => {
                // If fee drift exceeded pre-approved allowance, re-approve based on the
                // failed burn amount and retry once.
                let retry_fee_budget = Self::with_fee_headroom(&failed_burn_amount);
                self.approve_minter_spend(cketh_ledger_id, retry_fee_budget).await?;
                self.withdraw_erc20_call(&withdraw_args).await?
            }
            err @ WithdrawErc20Ret::Err(_) => err,
        };

        match result {
            // Both block indexes identify the ICP-side withdrawal request.
            WithdrawErc20Ret::Ok(request_id) => Ok(BridgeSubmission {
                bridge_id: format!(
                    "ic-withdraw:{}:{}",
                    request_id.ckerc20_block_index, request_id.cketh_block_index
                ),
            }),
            WithdrawErc20Ret::Err(err) => Err(format!("withdraw_erc20 error: {:?}", err)),
        }
    }
}

#[async_trait]
impl<A, B, E> BridgeBackend for CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        if let Some(route) = resolve_cketh_forward_route_by_source(asset, chain) {
            let token_address = parse_evm_token_address(route)?;
            let owner = address
                .parse::<Address>()
                .map_err(|e| format!("invalid source address '{address}': {e}"))?;

            let base_units = self
                .evm_backend
                .erc20_balance_of(token_address, owner)
                .await
                .map_err(|e| format!("ERC20 balanceOf failed for token {token_address}: {e}"))?;
            let decimals = self.token_decimals(token_address).await?;
            return base_units_to_amount_via_core(base_units, decimals);
        }

        if let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) {
            let ledger_id = parse_ckerc20_ledger_id(route)?;
            let source_account = parse_source_icp_account(address)?;
            let decimals =
                icrc1_decimals_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge").await?;
            let balance =
                icrc1_balance_with_context(self.icp_backend.as_ref(), ledger_id, &source_account, "ckerc20 bridge")
                    .await?;
            return nat_units_to_amount_via_core(&balance, decimals);
        }

        Err(format!(
            "unsupported source route {}@{} for ckETH ERC20 bridge backend",
            asset, chain
        ))
    }

    /// Executes a ckETH minter bridge submission for forward (`USDC@ETH -> ckUSDC@ICP`)
    /// or reverse (`ckUSDC@ICP -> USDC@ETH`) routes.
    ///
    /// Returns a [`BridgeSubmission`] with:
    /// - EVM tx hash for forward helper deposits.
    /// - `ic-withdraw:<ckerc20_block>:<cketh_block>` handle for reverse minter withdrawals.
    ///
    /// The method performs route validation and preflight checks before sending any
    /// on-chain/canister state-changing calls.
    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        // Resolve route metadata once; it validates asset/chain pair and destination kind.
        let route = resolve_cketh_route_for_request(&request)?;
        match route.route_kind {
            BridgeRouteKind::CkEthErc20Forward => self.submit_forward_bridge(route, &request).await,
            BridgeRouteKind::CkEthErc20Reverse => self.submit_reverse_bridge(route, &request).await,
            _ => Err(format!(
                "route {}@{} -> {} is not supported by CkErc20BridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        if bridge_id.starts_with("ic-withdraw:") {
            return Ok(BridgeStatus::Completed);
        }

        let tx_hash = bridge_id
            .parse::<TxHash>()
            .map_err(|e| format!("invalid bridge id '{}': expected EVM tx hash: {e}", bridge_id))?;

        let receipt = self
            .evm_backend
            .receipt_status(tx_hash)
            .await
            .map_err(|e| format!("failed to fetch transaction receipt for bridge id '{}': {e}", bridge_id))?;

        let Some(receipt) = receipt else {
            return Ok(BridgeStatus::Pending);
        };

        if receipt.success {
            return Ok(BridgeStatus::Completed);
        }

        Ok(BridgeStatus::Failed {
            reason: Some(format!(
                "bridge transaction {} reverted in block {}",
                bridge_id,
                receipt.block_number.unwrap_or_default()
            )),
        })
    }
}

#[cfg(test)]
#[path = "ckerc20_bridge_tests.rs"]
mod ckerc20_bridge_tests;
