mod catalog;
pub mod ckerc20_bridge;
mod ckerc20_bridge_utils;
pub mod solana_bridge;
mod types;
mod utils;
pub use ckerc20_bridge::{BridgeEvmBackend, CkErc20BridgeBackend};
pub use solana_bridge::SolanaBridgeBackend;
pub use types::{
    BridgeBackend, BridgeDestination, BridgeDestinationKind, BridgeRequest, BridgeRouteKind, BridgeRouteSpec,
    BridgeStatus, BridgeSubmission, BridgeSweepRoute, EvmReceiptStatus, MockBridgeBackend,
};
pub use utils::{
    cketh_forward_routes, cketh_reverse_routes, resolve_cketh_forward_route_by_source,
    resolve_cketh_forward_route_by_target, resolve_cketh_reverse_route_by_source, resolve_route,
    validate_destination_for_route,
};
