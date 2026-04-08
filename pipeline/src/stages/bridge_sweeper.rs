use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use candid::Principal;
use futures::future::join_all;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BridgeBackend, BridgeDestination, BridgeRequest, BridgeSubmission, BridgeSweepRoute,
};
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeBalanceSnapshot {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub amount: f64,
    pub min_sweep_amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeAction {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub destination: BridgeDestination,
    pub amount: f64,
}

pub type BridgeDestinationResolver = Arc<dyn Fn(&BridgeBalanceSnapshot) -> BridgeDestination + Send + Sync>;

pub fn liquidator_destination_resolver(liquidator_principal: Principal) -> BridgeDestinationResolver {
    Arc::new(move |_snapshot: &BridgeBalanceSnapshot| {
        BridgeDestination::IcpAccount(Account {
            owner: liquidator_principal,
            subaccount: None,
        })
    })
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
    pub routes: Vec<BridgeSweepRoute>,
    pub destination_resolver: BridgeDestinationResolver,
    pub poll_interval: Duration,
}

impl<B> BridgeSweeper<B>
where
    B: BridgeBackend + Send + Sync,
{
    pub fn new(
        backend: Arc<B>,
        source_address: String,
        routes: Vec<BridgeSweepRoute>,
        destination_resolver: BridgeDestinationResolver,
        poll_interval: Duration,
    ) -> Self {
        Self {
            backend,
            source_address,
            routes,
            destination_resolver,
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
        if self.routes.is_empty() {
            return Err("bridge sweeper has no routes configured".to_string());
        }

        // Parallel read phase: fetch balances for all routes in one async join.
        let source_address = self.source_address.clone();
        let backend = self.backend.clone();
        let balance_reads = self.routes.iter().cloned().map(|route| {
            let backend = backend.clone();
            let source_address = source_address.clone();
            async move {
                let balance = backend
                    .get_source_balance(&route.source_asset, &route.source_chain, &source_address)
                    .await;
                (route, balance)
            }
        });
        let balance_results = join_all(balance_reads).await;

        // Serial submission phase: preserve deterministic route order for nonce safety.
        for (route, balance) in balance_results {
            let balance = match balance {
                Ok(v) => v,
                Err(err) => {
                    warn!(
                        "[bridge] balance read failed for {}@{} -> {}: {}",
                        route.source_asset, route.source_chain, route.target_asset, err
                    );
                    continue;
                }
            };

            let action = self.plan(BridgeBalanceSnapshot {
                asset: route.source_asset.clone(),
                source_chain: route.source_chain.clone(),
                source_address: self.source_address.clone(),
                target_asset: route.target_asset.clone(),
                amount: balance,
                min_sweep_amount: route.min_sweep_amount,
            });

            let Some(action) = action else {
                continue;
            };

            match self.submit(action.clone()).await {
                Ok(submission) => {
                    info!(
                        "[bridge] submitted full sweep for {}@{} -> {} amount={} provider_bridge_id={}",
                        action.asset, action.source_chain, action.target_asset, action.amount, submission.bridge_id
                    );
                }
                Err(err) => {
                    warn!(
                        "[bridge] submission failed for {}@{} -> {}: {}",
                        action.asset, action.source_chain, action.target_asset, err
                    );
                }
            }
        }

        Ok(())
    }

    fn plan(&self, balance_snapshot: BridgeBalanceSnapshot) -> Option<BridgeAction> {
        if !balance_snapshot.amount.is_finite() || balance_snapshot.amount <= 0.0 {
            return None;
        }
        if balance_snapshot.amount <= balance_snapshot.min_sweep_amount {
            return None;
        }

        let destination = (self.destination_resolver)(&balance_snapshot);
        Some(BridgeAction {
            asset: balance_snapshot.asset,
            source_chain: balance_snapshot.source_chain,
            source_address: balance_snapshot.source_address,
            target_asset: balance_snapshot.target_asset,
            destination,
            amount: balance_snapshot.amount,
        })
    }

    async fn submit(&self, action: BridgeAction) -> Result<BridgeSubmission, String> {
        let request = BridgeRequest {
            asset: action.asset,
            source_chain: action.source_chain,
            source_address: action.source_address,
            target_asset: action.target_asset,
            destination: action.destination,
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

    use candid::Principal;
    use liquidium_pipeline_connectors::backend::bridge_backend::{
        BridgeDestination, BridgeSubmission, BridgeSweepRoute, MockBridgeBackend,
    };
    use mockall::Sequence;

    use super::{BridgeBalanceSnapshot, BridgeService, BridgeSweeper, liquidator_destination_resolver};

    fn route(source_asset: &str, target_asset: &str, min_sweep_amount: f64) -> BridgeSweepRoute {
        BridgeSweepRoute {
            source_asset: source_asset.to_string(),
            source_chain: "ETH".to_string(),
            target_asset: target_asset.to_string(),
            min_sweep_amount,
        }
    }

    fn make_sweeper(
        backend: Arc<MockBridgeBackend>,
        routes: Vec<BridgeSweepRoute>,
    ) -> BridgeSweeper<MockBridgeBackend> {
        BridgeSweeper::new(
            backend,
            "0x1111111111111111111111111111111111111111".to_string(),
            routes,
            liquidator_destination_resolver(Principal::management_canister()),
            Duration::from_secs(5),
        )
    }

    #[test]
    fn plan_respects_min_sweep_threshold() {
        let backend = Arc::new(MockBridgeBackend::new());
        let sweeper = make_sweeper(backend, vec![route("USDC", "ckUSDC", 1.0)]);

        assert!(
            sweeper
                .plan(BridgeBalanceSnapshot {
                    asset: "USDC".to_string(),
                    source_chain: "ETH".to_string(),
                    source_address: "0xsource".to_string(),
                    target_asset: "ckUSDC".to_string(),
                    amount: 0.5,
                    min_sweep_amount: 1.0,
                })
                .is_none()
        );

        let action = sweeper
            .plan(BridgeBalanceSnapshot {
                asset: "USDC".to_string(),
                source_chain: "ETH".to_string(),
                source_address: "0xsource".to_string(),
                target_asset: "ckUSDC".to_string(),
                amount: 2.0,
                min_sweep_amount: 1.0,
            })
            .expect("expected action above threshold");
        assert_eq!(action.asset, "USDC");
        assert_eq!(action.target_asset, "ckUSDC");
    }

    #[tokio::test]
    async fn tick_processes_routes_in_serial_order() {
        let mut backend = MockBridgeBackend::new();
        let mut seq = Sequence::new();

        backend
            .expect_get_source_balance()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|asset, chain, address| {
                asset == "USDC" && chain == "ETH" && address == "0x1111111111111111111111111111111111111111"
            })
            .returning(|_, _, _| Ok(3.5));
        backend
            .expect_get_source_balance()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|asset, chain, address| {
                asset == "USDT" && chain == "ETH" && address == "0x1111111111111111111111111111111111111111"
            })
            .returning(|_, _, _| Ok(4.0));
        backend
            .expect_submit_bridge()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|request| request.asset == "USDC" && request.target_asset == "ckUSDC")
            .returning(|request| {
                assert_eq!(
                    request.destination,
                    BridgeDestination::IcpAccount(icrc_ledger_types::icrc1::account::Account {
                        owner: Principal::management_canister(),
                        subaccount: None,
                    })
                );
                Ok(BridgeSubmission {
                    bridge_id: "bridge-usdc".to_string(),
                })
            });
        backend
            .expect_submit_bridge()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|request| request.asset == "USDT" && request.target_asset == "ckUSDT")
            .returning(|_| {
                Ok(BridgeSubmission {
                    bridge_id: "bridge-usdt".to_string(),
                })
            });
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(
            Arc::new(backend),
            vec![route("USDC", "ckUSDC", 0.0), route("USDT", "ckUSDT", 0.0)],
        );
        sweeper.tick().await.expect("tick should complete");
    }

    #[tokio::test]
    async fn tick_skips_submission_when_balance_below_threshold() {
        let mut backend = MockBridgeBackend::new();
        backend
            .expect_get_source_balance()
            .times(1)
            .returning(|_, _, _| Ok(0.5));
        backend.expect_submit_bridge().times(0);
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(Arc::new(backend), vec![route("USDC", "ckUSDC", 1.0)]);
        sweeper.tick().await.expect("tick should no-op");
    }

    #[tokio::test]
    async fn tick_continues_when_a_route_fails() {
        let mut backend = MockBridgeBackend::new();
        let mut seq = Sequence::new();
        backend
            .expect_get_source_balance()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|asset, _, _| asset == "USDC")
            .returning(|_, _, _| Err("provider down".to_string()));
        backend
            .expect_get_source_balance()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|asset, _, _| asset == "USDT")
            .returning(|_, _, _| Ok(2.0));
        backend
            .expect_submit_bridge()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|request| request.asset == "USDT")
            .returning(|_| {
                Ok(BridgeSubmission {
                    bridge_id: "bridge-usdt".to_string(),
                })
            });
        backend.expect_get_bridge_status().times(0);

        let sweeper = make_sweeper(
            Arc::new(backend),
            vec![route("USDC", "ckUSDC", 0.0), route("USDT", "ckUSDT", 0.0)],
        );
        sweeper
            .tick()
            .await
            .expect("tick should continue across route failures");
    }

    #[tokio::test]
    async fn tick_errors_when_no_routes_configured() {
        let backend = Arc::new(MockBridgeBackend::new());
        let sweeper = make_sweeper(backend, vec![]);
        let err = sweeper.tick().await.expect_err("tick must fail");
        assert!(err.contains("no routes configured"));
    }
}
