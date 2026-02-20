use async_trait::async_trait;
use std::sync::Arc;

use crate::finalizers::liquidation_outcome::LiquidationOutcome;

pub mod telegram;

#[async_trait]
pub trait LiquidationNotifier: Send + Sync {
    async fn notify_startup(&self) {}
    async fn notify_shutdown(&self) {}
    async fn notify_liquidations(&self, outcomes: &[LiquidationOutcome]);
}

pub struct NoopLiquidationNotifier;

#[async_trait]
impl LiquidationNotifier for NoopLiquidationNotifier {
    async fn notify_liquidations(&self, _outcomes: &[LiquidationOutcome]) {}
}

pub fn noop_liquidation_notifier() -> Arc<dyn LiquidationNotifier> {
    Arc::new(NoopLiquidationNotifier)
}
