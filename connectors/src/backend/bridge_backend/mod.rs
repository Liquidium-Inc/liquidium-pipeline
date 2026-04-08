use alloy::primitives::Address;
use async_trait::async_trait;
use bitcoin::{Address as BitcoinAddress, Network};
use icrc_ledger_types::icrc1::account::Account;
use std::str::FromStr;

pub mod cketh_erc20;
pub use cketh_erc20::CkEthErc20BridgeBackend;

const USDC_ETH_TOKEN_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeDestination {
    IcpAccount(Account),
    EvmAddress(Address),
    BtcAddress(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDestinationKind {
    IcpAccount,
    EvmAddress,
    BtcAddress,
}

impl BridgeDestination {
    pub fn kind(&self) -> BridgeDestinationKind {
        match self {
            Self::IcpAccount(_) => BridgeDestinationKind::IcpAccount,
            Self::EvmAddress(_) => BridgeDestinationKind::EvmAddress,
            Self::BtcAddress(_) => BridgeDestinationKind::BtcAddress,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeRouteKind {
    CkEthErc20Forward,
    BtcToCkBtc,
    CkBtcToBtc,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BridgeRouteSpec {
    pub source_asset: &'static str,
    pub source_chain: &'static str,
    pub target_asset: &'static str,
    pub destination_kind: BridgeDestinationKind,
    pub route_kind: BridgeRouteKind,
    pub evm_token_address: Option<&'static str>,
    pub min_sweep_amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeSweepRoute {
    pub source_asset: String,
    pub source_chain: String,
    pub target_asset: String,
    pub min_sweep_amount: f64,
}

const BRIDGE_ROUTE_CATALOG: [BridgeRouteSpec; 3] = [
    BridgeRouteSpec {
        source_asset: "USDC",
        source_chain: "ETH",
        target_asset: "ckUSDC",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::CkEthErc20Forward,
        evm_token_address: Some(USDC_ETH_TOKEN_ADDRESS),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "BTC",
        source_chain: "BTC",
        target_asset: "ckBTC",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::BtcToCkBtc,
        evm_token_address: None,
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckBTC",
        source_chain: "ICP",
        target_asset: "BTC",
        destination_kind: BridgeDestinationKind::BtcAddress,
        route_kind: BridgeRouteKind::CkBtcToBtc,
        evm_token_address: None,
        min_sweep_amount: 0.0,
    },
];

pub fn resolve_route(source_asset: &str, source_chain: &str, target_asset: &str) -> Option<&'static BridgeRouteSpec> {
    BRIDGE_ROUTE_CATALOG.iter().find(|route| {
        source_asset.eq_ignore_ascii_case(route.source_asset)
            && source_chain.eq_ignore_ascii_case(route.source_chain)
            && target_asset.eq_ignore_ascii_case(route.target_asset)
    })
}

pub fn resolve_cketh_forward_route_by_source(
    source_asset: &str,
    source_chain: &str,
) -> Option<&'static BridgeRouteSpec> {
    BRIDGE_ROUTE_CATALOG.iter().find(|route| {
        route.route_kind == BridgeRouteKind::CkEthErc20Forward
            && source_asset.eq_ignore_ascii_case(route.source_asset)
            && source_chain.eq_ignore_ascii_case(route.source_chain)
    })
}

pub fn cketh_forward_routes() -> Vec<BridgeSweepRoute> {
    BRIDGE_ROUTE_CATALOG
        .iter()
        .filter(|route| route.route_kind == BridgeRouteKind::CkEthErc20Forward)
        .map(|route| BridgeSweepRoute {
            source_asset: route.source_asset.to_string(),
            source_chain: route.source_chain.to_string(),
            target_asset: route.target_asset.to_string(),
            min_sweep_amount: route.min_sweep_amount,
        })
        .collect()
}

pub fn validate_destination_for_route(route: &BridgeRouteSpec, destination: &BridgeDestination) -> Result<(), String> {
    if destination.kind() != route.destination_kind {
        return Err(format!(
            "invalid destination type for route {}@{} -> {}; expected {:?}, got {:?}",
            route.source_asset,
            route.source_chain,
            route.target_asset,
            route.destination_kind,
            destination.kind()
        ));
    }

    if let BridgeDestination::BtcAddress(address) = destination {
        let trimmed = address.trim();
        if trimmed.is_empty() {
            return Err("destination BTC address must not be empty".to_string());
        }
        let parsed = BitcoinAddress::from_str(trimmed)
            .map_err(|e| format!("invalid BTC destination address '{trimmed}': {e}"))?;
        parsed
            .require_network(Network::Bitcoin)
            .map_err(|_| format!("BTC destination address '{trimmed}' is not mainnet"))?;
    }

    Ok(())
}

/// A normalized request for moving assets between chains/assets.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeRequest {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub destination: BridgeDestination,
    pub amount: f64,
}

/// Provider submission handle returned after a bridge transaction is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSubmission {
    pub bridge_id: String,
}

/// High-level bridge lifecycle state from the provider/backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeStatus {
    Pending,
    Completed,
    Failed { reason: Option<String> },
    Canceled { reason: Option<String> },
    Unknown,
}

/// Backend contract for reading balances and executing bridge routes.
#[mockall::automock]
#[async_trait]
pub trait BridgeBackend: Send + Sync {
    /// Returns a human-readable source balance for a route input asset/account.
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String>;

    /// Submits a bridge transfer and returns a provider-level submission id.
    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String>;

    /// Polls current status for a previously submitted bridge operation.
    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String>;
}

#[cfg(test)]
mod tests {
    use super::{
        BridgeDestination, BridgeDestinationKind, BridgeRouteKind, cketh_forward_routes, resolve_route,
        validate_destination_for_route,
    };
    use icrc_ledger_types::icrc1::account::Account;

    #[test]
    fn route_catalog_covers_expected_pairs() {
        let usdc = resolve_route("USDC", "ETH", "ckUSDC").expect("USDC route");
        assert_eq!(usdc.destination_kind, BridgeDestinationKind::IcpAccount);
        assert_eq!(usdc.route_kind, BridgeRouteKind::CkEthErc20Forward);
        assert_eq!(
            usdc.evm_token_address,
            Some("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        );
        assert_eq!(usdc.min_sweep_amount, 0.0);

        let btc = resolve_route("BTC", "BTC", "ckBTC").expect("BTC route");
        assert_eq!(btc.route_kind, BridgeRouteKind::BtcToCkBtc);
        assert_eq!(btc.evm_token_address, None);

        let ckbtc = resolve_route("ckBTC", "ICP", "BTC").expect("ckBTC route");
        assert_eq!(ckbtc.route_kind, BridgeRouteKind::CkBtcToBtc);
        assert_eq!(ckbtc.destination_kind, BridgeDestinationKind::BtcAddress);
    }

    #[test]
    fn route_resolution_is_case_insensitive() {
        assert!(resolve_route("usdc", "eth", "ckusdc").is_some());
        assert!(resolve_route("CKBTC", "icp", "btc").is_some());
        assert!(resolve_route("USDT", "ETH", "ckUSDT").is_none());
    }

    #[test]
    fn cketh_forward_routes_contains_usdc_entry() {
        let routes = cketh_forward_routes();
        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.source_asset, "USDC");
        assert_eq!(route.source_chain, "ETH");
        assert_eq!(route.target_asset, "ckUSDC");
        assert_eq!(route.min_sweep_amount, 0.0);
    }

    #[test]
    fn btc_destination_validation_checks_format_and_network() {
        let route = resolve_route("ckBTC", "ICP", "BTC").expect("route");
        validate_destination_for_route(
            route,
            &BridgeDestination::BtcAddress("1BoatSLRHtKNngkdXEeobR76b53LETtpyT".into()),
        )
        .expect("valid mainnet address must pass");

        let testnet = BridgeDestination::BtcAddress("mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn".into());
        let err = validate_destination_for_route(route, &testnet).expect_err("testnet must fail");
        assert!(err.contains("not mainnet"));

        let invalid = BridgeDestination::BtcAddress("not-an-address".into());
        let err = validate_destination_for_route(route, &invalid).expect_err("invalid must fail");
        assert!(err.contains("invalid BTC destination address"));
    }

    #[test]
    fn destination_kind_mismatch_fails_validation() {
        let route = resolve_route("USDC", "ETH", "ckUSDC").expect("route");
        let err = validate_destination_for_route(
            route,
            &BridgeDestination::EvmAddress("0x1111111111111111111111111111111111111111".parse().expect("address")),
        )
        .expect_err("kind mismatch must fail");
        assert!(err.contains("invalid destination type"));

        validate_destination_for_route(
            route,
            &BridgeDestination::IcpAccount(Account {
                owner: candid::Principal::management_canister(),
                subaccount: None,
            }),
        )
        .expect("matching kind must pass");
    }
}
