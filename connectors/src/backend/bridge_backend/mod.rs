use async_trait::async_trait;
pub mod ckusdc;
pub use ckusdc::CkUsdcBridgeBackend;

/// A normalized request for moving assets between chains/assets.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeRequest {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub destination_account: String,
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
///
/// TODO(liquidium): Add a `BridgeRouter` that dispatches requests by
/// `(asset, source_chain, target_asset)` so we can plug in multiple route-specific
/// backends (e.g. USDC@ETH->ckUSDC, USDT@ETH->ckUSDT) without hardcoding route checks
/// inside each backend implementation.
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
