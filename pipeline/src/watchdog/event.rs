//! What the watchdog reports, and the trait every alert channel implements.
use async_trait::async_trait;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub enum WatchdogEvent<'a> {
    Heartbeat {
        stage: &'a str,
    },
    BalanceMissing {
        asset: &'a str,
    },
    InsufficientFunds {
        asset: &'a str,
        available: String,
    },
    LowBalance {
        account: String,
        asset: String,
        asset_id: String,
        current: String,
        threshold: String,
    },
    Lifecycle {
        state: String,
        details: String,
    },
    OperatorRequired {
        execution_id: String,
        venue: String,
        pending_step: String,
        owner: String,
        details: String,
    },
    LiquidationFinalized {
        liquidation_id: String,
        borrower: String,
        debt_asset: String,
        collateral_asset: String,
        status: String,
        debt_repaid: String,
        collateral_received: String,
        swap_output: String,
        swapper: String,
        expected_profit: String,
        realized_profit: String,
        profit_delta: String,
        round_trip_secs: String,
    },
}

#[async_trait]
pub trait Watchdog: Send + Sync {
    async fn notify(&self, ev: WatchdogEvent<'_>);
}

pub struct NoopWatchdog;
#[async_trait]
impl Watchdog for NoopWatchdog {
    async fn notify(&self, _ev: WatchdogEvent<'_>) {}
}

// helpers for wiring
pub fn noop_watchdog() -> Arc<dyn Watchdog> {
    Arc::new(NoopWatchdog)
}
