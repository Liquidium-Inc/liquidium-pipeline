use bitcoin::{Address as BitcoinAddress, Network};
use std::str::FromStr;

use super::{BridgeDestination, BridgeRouteKind, BridgeRouteSpec, BridgeSweepRoute, catalog::BRIDGE_ROUTE_CATALOG};

fn validate_solana_address(address: &str, field_name: &str) -> Result<(), String> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }

    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|e| format!("invalid {field_name} '{trimmed}': {e}"))?;
    if decoded.len() != 32 {
        return Err(format!(
            "invalid {field_name} '{trimmed}': expected 32-byte pubkey, got {} bytes",
            decoded.len()
        ));
    }

    Ok(())
}

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

pub fn resolve_cketh_reverse_route_by_source(
    source_asset: &str,
    source_chain: &str,
) -> Option<&'static BridgeRouteSpec> {
    BRIDGE_ROUTE_CATALOG.iter().find(|route| {
        route.route_kind == BridgeRouteKind::CkEthErc20Reverse
            && source_asset.eq_ignore_ascii_case(route.source_asset)
            && source_chain.eq_ignore_ascii_case(route.source_chain)
    })
}

pub fn resolve_cketh_forward_route_by_target(target_asset: &str) -> Option<&'static BridgeRouteSpec> {
    BRIDGE_ROUTE_CATALOG.iter().find(|route| {
        route.route_kind == BridgeRouteKind::CkEthErc20Forward && target_asset.eq_ignore_ascii_case(route.target_asset)
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

pub fn cketh_reverse_routes() -> Vec<BridgeSweepRoute> {
    BRIDGE_ROUTE_CATALOG
        .iter()
        .filter(|route| route.route_kind == BridgeRouteKind::CkEthErc20Reverse)
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

    match destination {
        BridgeDestination::BtcAddress(address) => {
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
        BridgeDestination::SolanaAddress(address) => {
            validate_solana_address(address, "destination Solana address")?;
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::BridgeDestinationKind;
    use super::{
        BridgeDestination, BridgeRouteKind, BridgeRouteSpec, cketh_forward_routes, cketh_reverse_routes,
        resolve_cketh_forward_route_by_target, resolve_route, validate_destination_for_route,
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
        assert_eq!(usdc.ckerc20_ledger_id, Some("xevnm-gaaaa-aaaar-qafnq-cai"));
        assert_eq!(usdc.min_sweep_amount, 0.0);

        let ckusdc = resolve_route("ckUSDC", "ICP", "USDC").expect("ckUSDC reverse route");
        assert_eq!(ckusdc.destination_kind, BridgeDestinationKind::EvmAddress);
        assert_eq!(ckusdc.route_kind, BridgeRouteKind::CkEthErc20Reverse);
        assert_eq!(ckusdc.ckerc20_ledger_id, Some("xevnm-gaaaa-aaaar-qafnq-cai"));

        let btc = resolve_route("BTC", "BTC", "ckBTC").expect("BTC route");
        assert_eq!(btc.route_kind, BridgeRouteKind::BtcToCkBtc);
        assert_eq!(btc.evm_token_address, None);
        assert_eq!(btc.ckerc20_ledger_id, None);

        let ckbtc = resolve_route("ckBTC", "ICP", "BTC").expect("ckBTC route");
        assert_eq!(ckbtc.route_kind, BridgeRouteKind::CkBtcToBtc);
        assert_eq!(ckbtc.destination_kind, BridgeDestinationKind::BtcAddress);
        assert_eq!(ckbtc.ckerc20_ledger_id, None);
    }

    #[test]
    fn route_resolution_is_case_insensitive() {
        assert!(resolve_route("usdc", "eth", "ckusdc").is_some());
        assert!(resolve_route("ckusdc", "icp", "usdc").is_some());
        assert!(resolve_route("CKBTC", "icp", "btc").is_some());
        assert!(resolve_route("USDT", "ETH", "ckUSDT").is_none());
    }

    #[test]
    fn resolve_cketh_forward_route_by_target_is_case_insensitive() {
        let route = resolve_cketh_forward_route_by_target("ckusdc").expect("expected ckUSDC forward route");
        assert_eq!(route.source_asset, "USDC");
        assert_eq!(route.source_chain, "ETH");
        assert_eq!(route.target_asset, "ckUSDC");
        assert_eq!(route.route_kind, BridgeRouteKind::CkEthErc20Forward);
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
    fn cketh_reverse_routes_contains_ckusdc_entry() {
        let routes = cketh_reverse_routes();
        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.source_asset, "ckUSDC");
        assert_eq!(route.source_chain, "ICP");
        assert_eq!(route.target_asset, "USDC");
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

    #[test]
    fn solana_destination_validation_checks_base58_shape() {
        let route = BridgeRouteSpec {
            source_asset: "ckSOL",
            source_chain: "ICP",
            target_asset: "SOL",
            destination_kind: BridgeDestinationKind::SolanaAddress,
            route_kind: BridgeRouteKind::IcpToSolana,
            evm_token_address: None,
            ckerc20_ledger_id: None,
            min_sweep_amount: 0.0,
        };

        validate_destination_for_route(
            &route,
            &BridgeDestination::SolanaAddress("So11111111111111111111111111111111111111112".to_string()),
        )
        .expect("valid Solana pubkey must pass");

        let malformed = BridgeDestination::SolanaAddress("not-a-base58-@@".to_string());
        let err = validate_destination_for_route(&route, &malformed).expect_err("malformed address must fail");
        assert!(err.contains("invalid destination Solana address"));

        let short = BridgeDestination::SolanaAddress(bs58::encode([1u8, 2u8, 3u8]).into_string());
        let err = validate_destination_for_route(&route, &short).expect_err("short pubkey must fail");
        assert!(err.contains("expected 32-byte pubkey"));
    }
}
