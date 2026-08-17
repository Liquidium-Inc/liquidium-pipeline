use super::{BridgeDestinationKind, BridgeRouteKind, BridgeRouteSpec};

const USDC_ETH_TOKEN_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
const CKUSDC_ICP_LEDGER_ID: &str = "xevnm-gaaaa-aaaar-qafnq-cai";
const CKUSDC_ICP_INDEX_ID: &str = "xrs4b-hiaaa-aaaar-qafoa-cai";
const USDT_ETH_TOKEN_ADDRESS: &str = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
const CKUSDT_ICP_LEDGER_ID: &str = "cngnf-vqaaa-aaaar-qag4q-cai";
const CKUSDT_ICP_INDEX_ID: &str = "cefgz-dyaaa-aaaar-qag5a-cai";
const CKETH_ICP_LEDGER_ID: &str = "ss2fx-dyaaa-aaaar-qacoq-cai";
const CKETH_ICP_INDEX_ID: &str = "s3zol-vqaaa-aaaar-qacpa-cai";

pub(super) const BRIDGE_ROUTE_CATALOG: [BridgeRouteSpec; 8] = [
    BridgeRouteSpec {
        source_asset: "ETH",
        source_chain: "ETH",
        target_asset: "ckETH",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::EthToCkEth,
        evm_token_address: None,
        ckerc20_ledger_id: Some(CKETH_ICP_LEDGER_ID),
        ckerc20_index_id: Some(CKETH_ICP_INDEX_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckETH",
        source_chain: "ICP",
        target_asset: "ETH",
        destination_kind: BridgeDestinationKind::EvmAddress,
        route_kind: BridgeRouteKind::CkEthToEth,
        evm_token_address: None,
        ckerc20_ledger_id: Some(CKETH_ICP_LEDGER_ID),
        ckerc20_index_id: Some(CKETH_ICP_INDEX_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "USDC",
        source_chain: "ETH",
        target_asset: "ckUSDC",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::CkEthErc20Forward,
        evm_token_address: Some(USDC_ETH_TOKEN_ADDRESS),
        ckerc20_ledger_id: Some(CKUSDC_ICP_LEDGER_ID),
        ckerc20_index_id: Some(CKUSDC_ICP_INDEX_ID),
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
        ckerc20_index_id: Some(CKUSDC_ICP_INDEX_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "USDT",
        source_chain: "ETH",
        target_asset: "ckUSDT",
        destination_kind: BridgeDestinationKind::IcpAccount,
        route_kind: BridgeRouteKind::CkEthErc20Forward,
        evm_token_address: Some(USDT_ETH_TOKEN_ADDRESS),
        ckerc20_ledger_id: Some(CKUSDT_ICP_LEDGER_ID),
        ckerc20_index_id: Some(CKUSDT_ICP_INDEX_ID),
        min_sweep_amount: 0.0,
    },
    BridgeRouteSpec {
        source_asset: "ckUSDT",
        source_chain: "ICP",
        target_asset: "USDT",
        destination_kind: BridgeDestinationKind::EvmAddress,
        route_kind: BridgeRouteKind::CkEthErc20Reverse,
        evm_token_address: Some(USDT_ETH_TOKEN_ADDRESS),
        ckerc20_ledger_id: Some(CKUSDT_ICP_LEDGER_ID),
        ckerc20_index_id: Some(CKUSDT_ICP_INDEX_ID),
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
        ckerc20_index_id: None,
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
        ckerc20_index_id: None,
        min_sweep_amount: 0.0,
    },
];
