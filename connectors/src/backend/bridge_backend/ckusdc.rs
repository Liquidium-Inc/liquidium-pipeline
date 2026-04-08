use alloy::{
    network::AnyNetwork,
    primitives::{Address, FixedBytes, U256},
    providers::{Provider, WalletProvider},
    sol,
};
use async_trait::async_trait;
use candid::{CandidType, Encode, Principal};
use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};
use serde::Deserialize;

use crate::{
    backend::bridge_backend::{BridgeBackend, BridgeRequest, BridgeStatus, BridgeSubmission},
    pipeline_agent::PipelineAgent,
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}

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

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkEthMinterInfo {
    #[serde(default)]
    deposit_with_subaccount_helper_contract_address: Option<String>,
    #[serde(default)]
    erc20_helper_contract_address: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum HelperContract {
    WithSubaccount(Address),
    Native(Address),
}

fn destination_to_bytes32(destination_account: &str) -> Result<(FixedBytes<32>, FixedBytes<32>), String> {
    let account = destination_account
        .parse::<Account>()
        .map_err(|e| format!("invalid destination account '{destination_account}': {e}"))?;
    let principal_bytes32: FixedBytes<32> = principal_to_subaccount(account.owner).into();
    let subaccount_bytes32: FixedBytes<32> = account.subaccount.unwrap_or([0u8; 32]).into();
    Ok((principal_bytes32, subaccount_bytes32))
}

/// Bridge backend for the `USDC@ETH -> ckUSDC` route via ckETH minter helper contracts.
///
/// TODO(liquidium): Introduce a shared `BridgeRouter` that dispatches by
/// `(asset, source_chain, target_asset)` and keeps route selection outside
/// concrete backends like this one.
pub struct CkUsdcBridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    pub agent: std::sync::Arc<A>,
    pub provider: P,
    pub cketh_minter_canister: Principal,
    pub usdc_eth_token: Address,
}

impl<A, P> CkUsdcBridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    pub fn new(
        agent: std::sync::Arc<A>,
        provider: P,
        cketh_minter_canister: Principal,
        usdc_eth_token: &str,
    ) -> Result<Self, String> {
        let usdc_eth_token = usdc_eth_token
            .parse::<Address>()
            .map_err(|e| format!("invalid BRIDGE_USDC_ETH_TOKEN_ADDRESS: {e}"))?;
        Ok(Self {
            agent,
            provider,
            cketh_minter_canister,
            usdc_eth_token,
        })
    }

    async fn helper_contract(&self) -> Result<HelperContract, String> {
        let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
        let info = match self
            .agent
            .call_query::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args.clone())
            .await
        {
            Ok(v) => v,
            Err(query_err) => self
                .agent
                .call_update::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args)
                .await
                .map_err(|update_err| {
                    format!(
                        "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                        self.cketh_minter_canister
                    )
                })?,
        };

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

    async fn token_decimals(&self) -> Result<u8, String> {
        let contract = IERC20::new(self.usdc_eth_token, self.provider.clone());
        contract
            .decimals()
            .call()
            .await
            .map_err(|e| format!("USDC decimals() failed: {e}"))
    }

    fn amount_to_base_units(amount: f64, decimals: u8) -> Result<U256, String> {
        if !amount.is_finite() || amount <= 0.0 {
            return Err("amount must be positive and finite".to_string());
        }
        let scale = 10f64.powi(decimals as i32);
        let raw = (amount * scale).floor();
        if !raw.is_finite() || raw <= 0.0 {
            return Err("amount rounds to zero base units".to_string());
        }
        if raw > (u128::MAX as f64) {
            return Err("amount too large".to_string());
        }
        Ok(U256::from(raw as u128))
    }

    fn base_units_to_amount(base_units: U256, decimals: u8) -> Result<f64, String> {
        let raw: f64 = base_units
            .to_string()
            .parse()
            .map_err(|e| format!("failed to convert base units to f64: {e}"))?;
        Ok(raw / 10f64.powi(decimals as i32))
    }
}

#[async_trait]
impl<A, P> BridgeBackend for CkUsdcBridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    /// Returns source wallet balance for USDC on Ethereum in human units.
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        if asset.to_ascii_uppercase() != "USDC" {
            return Err(format!("unsupported source asset '{asset}'; only USDC is supported"));
        }
        if !chain.eq_ignore_ascii_case("ETH") {
            return Err(format!("unsupported source chain '{chain}'; only ETH is supported"));
        }
        let owner = address
            .parse::<Address>()
            .map_err(|e| format!("invalid source address '{address}': {e}"))?;

        let contract = IERC20::new(self.usdc_eth_token, self.provider.clone());
        let base_units = contract
            .balanceOf(owner)
            .call()
            .await
            .map_err(|e| format!("USDC balanceOf failed: {e}"))?;
        let decimals = self.token_decimals().await?;
        Self::base_units_to_amount(base_units, decimals)
    }

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        // Until a dedicated BridgeRouter exists, this backend enforces a single route.
        if !request.asset.eq_ignore_ascii_case("USDC") || !request.source_chain.eq_ignore_ascii_case("ETH") {
            return Err(format!(
                "unsupported bridge route {}@{}; only USDC@ETH is supported",
                request.asset, request.source_chain
            ));
        }
        if request.target_asset != "ckUSDC" {
            return Err(format!(
                "unsupported target asset '{}'; only ckUSDC is supported",
                request.target_asset
            ));
        }

        let signer = self.provider.default_signer_address();
        let source = request
            .source_address
            .parse::<Address>()
            .map_err(|e| format!("invalid source address '{}': {e}", request.source_address))?;
        if source != signer {
            return Err(format!(
                "source_address {} does not match signer {}",
                request.source_address, signer
            ));
        }

        let (recipient_principal_bytes32, recipient_subaccount_bytes32) =
            destination_to_bytes32(&request.destination_account)?;

        let helper = self.helper_contract().await?;
        let helper_address = match helper {
            HelperContract::WithSubaccount(address) | HelperContract::Native(address) => address,
        };
        let decimals = self.token_decimals().await?;
        let amount_base_units = Self::amount_to_base_units(request.amount, decimals)?;

        let token = IERC20::new(self.usdc_eth_token, self.provider.clone());
        let approve_pending = token
            .approve(helper_address, amount_base_units)
            .send()
            .await
            .map_err(|e| format!("USDC approve(helper={helper_address}) failed: {e}"))?;
        approve_pending
            .watch()
            .await
            .map_err(|e| format!("USDC approve confirmation failed: {e}"))?;

        let pending = match helper {
            HelperContract::WithSubaccount(address) => {
                let helper_contract = ICkErc20HelperWithSubaccount::new(address, self.provider.clone());
                helper_contract
                    .depositErc20(
                        self.usdc_eth_token,
                        amount_base_units,
                        recipient_principal_bytes32,
                        recipient_subaccount_bytes32,
                    )
                    .send()
                    .await
                    .map_err(|e| format!("helper depositErc20 failed: {e}"))?
            }
            HelperContract::Native(address) => {
                let helper_contract = ICkErc20HelperNative::new(address, self.provider.clone());
                helper_contract
                    .deposit(self.usdc_eth_token, amount_base_units, recipient_principal_bytes32)
                    .send()
                    .await
                    .map_err(|e| format!("native helper deposit failed: {e}"))?
            }
        };

        Ok(BridgeSubmission {
            bridge_id: format!("{:#x}", pending.tx_hash()),
        })
    }

    async fn get_bridge_status(&self, _bridge_id: &str) -> Result<BridgeStatus, String> {
        Ok(BridgeStatus::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use candid::Principal;
    use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};

    use super::destination_to_bytes32;

    #[test]
    fn destination_account_encodes_principal_and_default_subaccount() {
        let (principal, subaccount) = destination_to_bytes32("aaaaa-aa").expect("principal must encode");
        let expected_principal = principal_to_subaccount(Principal::management_canister());
        assert_eq!(principal.as_slice(), expected_principal);
        assert_eq!(subaccount.as_slice(), [0u8; 32]);
    }

    #[test]
    fn destination_account_encodes_explicit_subaccount() {
        let account = Account {
            owner: Principal::management_canister(),
            subaccount: Some([7u8; 32]),
        };
        let account_text = account.to_string();

        let (principal, subaccount) = destination_to_bytes32(&account_text).expect("account must encode");
        let expected_principal = principal_to_subaccount(account.owner);
        assert_eq!(principal.as_slice(), expected_principal);
        assert_eq!(subaccount.as_slice(), [7u8; 32]);
    }

    #[test]
    fn destination_account_rejects_invalid_text() {
        let err = destination_to_bytes32("not-a-valid-account").expect_err("invalid text must fail");
        assert!(err.contains("invalid destination account"));
    }
}
