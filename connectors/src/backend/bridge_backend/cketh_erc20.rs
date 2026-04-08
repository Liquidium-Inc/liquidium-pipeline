use alloy::{
    network::{AnyNetwork, ReceiptResponse},
    primitives::{Address, FixedBytes, TxHash, U256},
    providers::{Provider, WalletProvider},
    sol,
};
use async_trait::async_trait;
use candid::{CandidType, Encode, Principal};
use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};
use serde::Deserialize;

use crate::{
    backend::bridge_backend::{
        BridgeBackend, BridgeDestination, BridgeDestinationKind, BridgeRequest, BridgeRouteKind, BridgeRouteSpec,
        BridgeStatus, BridgeSubmission, resolve_cketh_forward_route_by_source, resolve_route,
        validate_destination_for_route,
    },
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

fn destination_to_bytes32(account: &Account) -> (FixedBytes<32>, FixedBytes<32>) {
    let principal_bytes32: FixedBytes<32> = principal_to_subaccount(account.owner).into();
    let subaccount_bytes32: FixedBytes<32> = account.subaccount.unwrap_or([0u8; 32]).into();
    (principal_bytes32, subaccount_bytes32)
}

fn ensure_source_matches_signer(source_address: &str, signer: Address) -> Result<(), String> {
    let source = source_address
        .parse::<Address>()
        .map_err(|e| format!("invalid source address '{source_address}': {e}"))?;
    if source != signer {
        return Err(format!(
            "source_address {} does not match signer {}",
            source_address, signer
        ));
    }
    Ok(())
}

fn parse_evm_token_address(route: &BridgeRouteSpec) -> Result<Address, String> {
    let token = route.evm_token_address.ok_or_else(|| {
        format!(
            "route {}@{} -> {} has no EVM token address",
            route.source_asset, route.source_chain, route.target_asset
        )
    })?;
    token.parse::<Address>().map_err(|e| {
        format!(
            "invalid EVM token address '{}' for route {}@{} -> {}: {e}",
            token, route.source_asset, route.source_chain, route.target_asset
        )
    })
}

fn resolve_cketh_forward_route_for_request(request: &BridgeRequest) -> Result<&'static BridgeRouteSpec, String> {
    let Some(route) = resolve_route(&request.asset, &request.source_chain, &request.target_asset) else {
        return Err(format!(
            "unsupported bridge route {}@{} -> {}; no route metadata found",
            request.asset, request.source_chain, request.target_asset
        ));
    };
    if route.route_kind != BridgeRouteKind::CkEthErc20Forward {
        return Err(format!(
            "route {}@{} -> {} is not supported by CkEthErc20BridgeBackend",
            request.asset, request.source_chain, request.target_asset
        ));
    }
    validate_destination_for_route(route, &request.destination)?;
    let _ = parse_evm_token_address(route)?;
    Ok(route)
}

/// Bridge backend for `ERC20@ETH -> ckERC20` forward routes via ckETH minter helper contracts.
///
/// Token and route selection are resolved from bridge route metadata.
pub struct CkEthErc20BridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    pub agent: std::sync::Arc<A>,
    pub provider: P,
    pub cketh_minter_canister: Principal,
}

impl<A, P> CkEthErc20BridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    pub fn new(agent: std::sync::Arc<A>, provider: P, cketh_minter_canister: Principal) -> Self {
        Self {
            agent,
            provider,
            cketh_minter_canister,
        }
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

    async fn token_decimals(&self, token: Address) -> Result<u8, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .decimals()
            .call()
            .await
            .map_err(|e| format!("ERC20 decimals() failed for token {token}: {e}"))
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
impl<A, P> BridgeBackend for CkEthErc20BridgeBackend<A, P>
where
    A: PipelineAgent,
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + Send + Sync + 'static,
{
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        let route = resolve_cketh_forward_route_by_source(asset, chain).ok_or_else(|| {
            format!(
                "unsupported source route {}@{} for ckETH ERC20 bridge backend",
                asset, chain
            )
        })?;
        let token_address = parse_evm_token_address(route)?;
        let owner = address
            .parse::<Address>()
            .map_err(|e| format!("invalid source address '{address}': {e}"))?;

        let contract = IERC20::new(token_address, self.provider.clone());
        let base_units = contract
            .balanceOf(owner)
            .call()
            .await
            .map_err(|e| format!("ERC20 balanceOf failed for token {token_address}: {e}"))?;
        let decimals = self.token_decimals(token_address).await?;
        Self::base_units_to_amount(base_units, decimals)
    }

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        let route = resolve_cketh_forward_route_for_request(&request)?;
        let token_address = parse_evm_token_address(route)?;

        let signer = self.provider.default_signer_address();
        ensure_source_matches_signer(&request.source_address, signer)?;

        let destination_account = match &request.destination {
            BridgeDestination::IcpAccount(account) => account,
            _ => {
                return Err(format!(
                    "unsupported destination for {}@{} -> {}; expected {:?}",
                    route.source_asset,
                    route.source_chain,
                    route.target_asset,
                    BridgeDestinationKind::IcpAccount
                ));
            }
        };
        let (recipient_principal_bytes32, recipient_subaccount_bytes32) = destination_to_bytes32(destination_account);

        let helper = self.helper_contract().await?;
        let helper_address = match helper {
            HelperContract::WithSubaccount(address) | HelperContract::Native(address) => address,
        };
        let decimals = self.token_decimals(token_address).await?;
        let amount_base_units = Self::amount_to_base_units(request.amount, decimals)?;

        let token = IERC20::new(token_address, self.provider.clone());
        let approve_pending = token
            .approve(helper_address, amount_base_units)
            .send()
            .await
            .map_err(|e| format!("ERC20 approve(helper={helper_address}) failed: {e}"))?;
        approve_pending
            .watch()
            .await
            .map_err(|e| format!("ERC20 approve confirmation failed: {e}"))?;

        let pending = match helper {
            HelperContract::WithSubaccount(address) => {
                let helper_contract = ICkErc20HelperWithSubaccount::new(address, self.provider.clone());
                helper_contract
                    .depositErc20(
                        token_address,
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
                    .deposit(token_address, amount_base_units, recipient_principal_bytes32)
                    .send()
                    .await
                    .map_err(|e| format!("native helper deposit failed: {e}"))?
            }
        };

        Ok(BridgeSubmission {
            bridge_id: format!("{:#x}", pending.tx_hash()),
        })
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        let tx_hash = bridge_id
            .parse::<TxHash>()
            .map_err(|e| format!("invalid bridge id '{}': expected EVM tx hash: {e}", bridge_id))?;

        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| format!("failed to fetch transaction receipt for bridge id '{}': {e}", bridge_id))?;

        let Some(receipt) = receipt else {
            return Ok(BridgeStatus::Pending);
        };

        if receipt.status() {
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
mod tests {
    use alloy::primitives::Address;
    use candid::Principal;
    use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};

    use crate::backend::bridge_backend::BridgeDestination;

    use super::{destination_to_bytes32, ensure_source_matches_signer, resolve_cketh_forward_route_for_request};

    #[test]
    fn destination_account_encodes_principal_and_default_subaccount() {
        let account = Account {
            owner: Principal::management_canister(),
            subaccount: None,
        };
        let (principal, subaccount) = destination_to_bytes32(&account);
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
        let (principal, subaccount) = destination_to_bytes32(&account);
        let expected_principal = principal_to_subaccount(account.owner);
        assert_eq!(principal.as_slice(), expected_principal);
        assert_eq!(subaccount.as_slice(), [7u8; 32]);
    }

    #[test]
    fn validate_source_matches_signer() {
        let signer = "0x1111111111111111111111111111111111111111"
            .parse::<Address>()
            .expect("address");
        ensure_source_matches_signer("0x1111111111111111111111111111111111111111", signer).expect("must pass");
        let err =
            ensure_source_matches_signer("0x2222222222222222222222222222222222222222", signer).expect_err("must fail");
        assert!(err.contains("does not match signer"));
    }

    #[test]
    fn route_validation_enforces_icp_destination_for_forward_route() {
        let request = crate::backend::bridge_backend::BridgeRequest {
            asset: "USDC".to_string(),
            source_chain: "ETH".to_string(),
            source_address: "0x1111111111111111111111111111111111111111".to_string(),
            target_asset: "ckUSDC".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: Principal::management_canister(),
                subaccount: None,
            }),
            amount: 1.0,
        };
        resolve_cketh_forward_route_for_request(&request).expect("route must validate");

        let mut bad_destination = request;
        bad_destination.destination = BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        );
        let err = resolve_cketh_forward_route_for_request(&bad_destination).expect_err("must fail");
        assert!(err.contains("invalid destination type"));
    }

    #[test]
    fn route_validation_rejects_non_forward_route_kind() {
        let request = crate::backend::bridge_backend::BridgeRequest {
            asset: "ckBTC".to_string(),
            source_chain: "ICP".to_string(),
            source_address: "0x1111111111111111111111111111111111111111".to_string(),
            target_asset: "BTC".to_string(),
            destination: BridgeDestination::BtcAddress("1BoatSLRHtKNngkdXEeobR76b53LETtpyT".to_string()),
            amount: 1.0,
        };
        let err = resolve_cketh_forward_route_for_request(&request).expect_err("must fail");
        assert!(err.contains("not supported by CkEthErc20BridgeBackend"));
    }
}
