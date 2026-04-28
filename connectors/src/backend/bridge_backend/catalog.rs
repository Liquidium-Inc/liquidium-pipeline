use super::{BridgeDestinationKind, BridgeRouteKind, BridgeRouteSpec};

const USDC_ETH_TOKEN_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
const CKUSDC_ICP_LEDGER_ID: &str = "xevnm-gaaaa-aaaar-qafnq-cai";

pub(super) const BRIDGE_ROUTE_CATALOG: [BridgeRouteSpec; 6] = [
    BridgeRouteSpec {
        source_asset: "USDC",
        source_chain: "ETH",
        target_asset: "ckUSDC",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::CkEthErc20Forward,
        evm_token_address: Some(USDC_ETH_TOKEN_ADDRESS),
        ckerc20_ledger_id: Some(CKUSDC_ICP_LEDGER_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckUSDC",
        source_chain: "ICP",
        target_asset: "USDC",
        destination_kind: BridgeDestinationKind::EvmAddress,
        route_kind: BridgeRouteKind::CkEthErc20Reverse,
        evm_token_address: Some(USDC_ETH_TOKEN_ADDRESS),
        ckerc20_ledger_id: Some(CKUSDC_ICP_LEDGER_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "BTC",
        source_chain: "BTC",
        target_asset: "ckBTC",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::BtcToCkBtc,
        evm_token_address: None,
        ckerc20_ledger_id: None,
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckBTC",
        source_chain: "ICP",
        target_asset: "BTC",
        destination_kind: BridgeDestinationKind::BtcAddress,
        route_kind: BridgeRouteKind::CkBtcToBtc,
        evm_token_address: None,
        ckerc20_ledger_id: None,
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "SOL",
        source_chain: "SOL",
        target_asset: "ckSOL",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::SolanaToIcp,
        evm_token_address: None,
        ckerc20_ledger_id: None,
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckSOL",
        source_chain: "ICP",
        target_asset: "SOL",
        destination_kind: BridgeDestinationKind::SolanaAddress,
        route_kind: BridgeRouteKind::IcpToSolana,
        evm_token_address: None,
        ckerc20_ledger_id: None,
        min_sweep_amount: 0.0,
    },
];
