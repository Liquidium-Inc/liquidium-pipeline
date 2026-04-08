use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeBackend, BridgeRequest, BridgeSubmission};
use tokio::time::sleep;
use tracing::{info, warn};

const SOURCE_ASSET: &str = "USDC";
const SOURCE_CHAIN: &str = "ETH";
const TARGET_ASSET: &str = "ckUSDC";

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeBalanceSnapshot {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeAction {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub destination_account: String,
    pub amount: f64,
}

#[async_trait]
pub trait BridgeService: Send + Sync {
    async fn tick(&self) -> Result<(), String>;
    fn plan(&self, balance_snapshot: BridgeBalanceSnapshot) -> Option<BridgeAction>;
    async fn submit(&self, action: BridgeAction) -> Result<BridgeSubmission, String>;
}

pub struct BridgeSweeper<B>
where
    B: BridgeBackend,
{
    pub backend: Arc<B>,
    pub source_address: String,
    pub destination_account: String,
    pub poll_interval: Duration,
}

impl<B> BridgeSweeper<B>
where
    B: BridgeBackend + Send + Sync,
{
    pub fn new(backend: Arc<B>, source_address: String, destination_account: String, poll_interval: Duration) -> Self {
        Self {
            backend,
            source_address,
            destination_account,
            poll_interval,
        }
    }

    pub async fn run(self) {
        loop {
            if let Err(err) = self.tick().await {
                warn!("[bridge] tick error: {}", err);
            }
            sleep(self.poll_interval).await;
        }
    }
}

#[async_trait]
impl<B> BridgeService for BridgeSweeper<B>
where
    B: BridgeBackend + Send + Sync,
{
    async fn tick(&self) -> Result<(), String> {
        if self.source_address.trim().is_empty() {
            return Err("bridge sweeper source address is empty".to_string());
        }
        if self.destination_account.trim().is_empty() {
            return Err("bridge sweeper destination account is empty".to_string());
        }

        let balance = self
            .backend
            .get_source_balance(SOURCE_ASSET, SOURCE_CHAIN, &self.source_address)
            .await
            .map_err(|e| format!("bridge balance read failed: {e}"))?;

        let action = self.plan(BridgeBalanceSnapshot {
            asset: SOURCE_ASSET.to_string(),
            source_chain: SOURCE_CHAIN.to_string(),
            source_address: self.source_address.clone(),
            amount: balance,
        });

        if let Some(action) = action {
            let submission = self.submit(action.clone()).await?;
            info!(
                "[bridge] submitted full sweep for {}@{} amount={} provider_bridge_id={}",
                action.asset, action.source_chain, action.amount, submission.bridge_id
            );
        }

        Ok(())
    }

    fn plan(&self, balance_snapshot: BridgeBalanceSnapshot) -> Option<BridgeAction> {
        if !balance_snapshot.amount.is_finite() || balance_snapshot.amount <= 0.0 {
            return None;
        }
        if balance_snapshot.asset != SOURCE_ASSET {
            return None;
        }
        if balance_snapshot.source_chain != SOURCE_CHAIN {
            return None;
        }

        Some(BridgeAction {
            asset: balance_snapshot.asset,
            source_chain: balance_snapshot.source_chain,
            source_address: balance_snapshot.source_address,
            target_asset: TARGET_ASSET.to_string(),
            destination_account: self.destination_account.clone(),
            amount: balance_snapshot.amount,
        })
    }

    async fn submit(&self, action: BridgeAction) -> Result<BridgeSubmission, String> {
        let request = BridgeRequest {
            asset: action.asset,
            source_chain: action.source_chain,
            source_address: action.source_address,
            target_asset: action.target_asset,
            destination_account: action.destination_account,
            amount: action.amount,
        };

        self.backend
            .submit_bridge(request)
            .await
            .map_err(|e| format!("bridge submission failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use liquidium_pipeline_connectors::backend::bridge_backend::{BridgeSubmission, MockBridgeBackend};

    use super::{BridgeBalanceSnapshot, BridgeService, BridgeSweeper};

    fn make_sweeper(backend: Arc<MockBridgeBackend>) -> BridgeSweeper<MockBridgeBackend> {
        BridgeSweeper::new(
            backend,
            "0x1111111111111111111111111111111111111111".to_string(),
            "aaaaa-aa".to_string(),
            Duration::from_secs(5),
        )
    }

    #[test]
    fn policy_plans_only_eth_usdc_and_sweeps_full_balance() {
        let backend = Arc::new(MockBridgeBackend::new());
        let sweeper = make_sweeper(backend);

        let action = sweeper.plan(BridgeBalanceSnapshot {
            asset: "USDC".to_string(),
            source_chain: "ETH".to_string(),
            source_address: "0xsource".to_string(),
            amount: 42.0,
        });
        let action = action.expect("expected action for ETH-USDC");
        assert_eq!(action.amount, 42.0);
        assert_eq!(action.target_asset, "ckUSDC");
        assert_eq!(action.destination_account, "aaaaa-aa");

        assert!(
            sweeper
                .plan(BridgeBalanceSnapshot {
                    asset: "USDT".to_string(),
                    source_chain: "ETH".to_string(),
                    source_address: "0xsource".to_string(),
                    amount: 1.0,
                })
                .is_none()
        );
        assert!(
            sweeper
                .plan(BridgeBalanceSnapshot {
                    asset: "USDC".to_string(),
                    source_chain: "ARB".to_string(),
                    source_address: "0xsource".to_string(),
                    amount: 1.0,
                })
                .is_none()
        );
    }

    #[tokio::test]
    async fn tick_submits_bridge_when_balance_positive() {
        let mut backend = MockBridgeBackend::new();
        backend
            .expect_get_source_balance()
            .times(1)
            .returning(|asset, chain, address| {
                assert_eq!(asset, "USDC");
                assert_eq!(chain, "ETH");
                assert_eq!(address, "0x1111111111111111111111111111111111111111");
                Ok(3.5)
            });
        backend.expect_submit_bridge().times(1).returning(|request| {
            assert_eq!(request.asset, "USDC");
            assert_eq!(request.source_chain, "ETH");
            assert_eq!(request.target_asset, "ckUSDC");
            assert_eq!(request.amount, 3.5);
            Ok(BridgeSubmission {
                bridge_id: "bridge-123".to_string(),
            })
        });
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(Arc::new(backend));
        sweeper.tick().await.expect("tick should submit");
    }

    #[tokio::test]
    async fn tick_skips_submission_when_balance_is_zero() {
        let mut backend = MockBridgeBackend::new();
        backend
            .expect_get_source_balance()
            .times(1)
            .returning(|_, _, _| Ok(0.0));
        backend.expect_submit_bridge().times(0);
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(Arc::new(backend));
        sweeper.tick().await.expect("tick should no-op");
    }

    #[tokio::test]
    async fn provider_submit_error_bubbles() {
        let mut backend = MockBridgeBackend::new();
        backend
            .expect_get_source_balance()
            .times(1)
            .returning(|_, _, _| Ok(2.0));
        backend
            .expect_submit_bridge()
            .times(1)
            .returning(|_| Err("provider down".to_string()));
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(Arc::new(backend));
        let err = sweeper.tick().await.expect_err("tick should fail");
        assert!(err.contains("bridge submission failed"));
    }
}
