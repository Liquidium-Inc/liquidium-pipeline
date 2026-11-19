//! Decides whether a liquidation takes the seized collateral as supply shares.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use tracing::warn;

use crate::liquidation::liquidation_math::BPS_ONE;
use crate::liquidation::share_position_adapter::SharePositions;
use crate::utils::now_ts;

/// One liquidity read serves the candidates of one strategy cycle.
const LIQUIDITY_CACHE_SECS: i64 = 5;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SharePayoutPolicy: Send + Sync {
    /// True when the pool cannot pay `expected_seized` as tokens. The liquidation then asks for shares.
    async fn wants_shares(&self, collateral_pool: Principal, expected_seized: Nat) -> bool;
}

/// True when `available` is below `expected_seized` plus the margin, for example 104 against 100 with a 5% margin.
pub fn pool_cannot_pay(available: &Nat, expected_seized: &Nat, margin_bps: u32) -> bool {
    available.clone() * Nat::from(BPS_ONE) < expected_seized.clone() * Nat::from(BPS_ONE + u64::from(margin_bps))
}

/// Asks for shares when the pool's free liquidity does not cover the seized collateral plus a margin.
pub struct LiquidityPayoutPolicy {
    positions: Arc<dyn SharePositions>,
    margin_bps: u32,
    cache: Mutex<HashMap<Principal, (i64, Nat)>>,
}

impl LiquidityPayoutPolicy {
    pub fn new(positions: Arc<dyn SharePositions>, margin_bps: u32) -> Self {
        Self {
            positions,
            margin_bps,
            cache: Mutex::new(HashMap::new()),
        }
    }

    async fn liquidity(&self, pool: Principal) -> Result<Nat, String> {
        let now = now_ts();
        if let Some((at, liquidity)) = self.cache.lock().expect("liquidity cache").get(&pool)
            && now - *at < LIQUIDITY_CACHE_SECS
        {
            return Ok(liquidity.clone());
        }
        let liquidity = self.positions.available_liquidity(pool).await?;
        self.cache
            .lock()
            .expect("liquidity cache")
            .insert(pool, (now, liquidity.clone()));
        Ok(liquidity)
    }
}

impl LiquidityPayoutPolicy {
    fn reserve(&self, pool: Principal, amount: &Nat) {
        if let Some((_, liquidity)) = self.cache.lock().expect("liquidity cache").get_mut(&pool) {
            *liquidity = if *liquidity > *amount {
                liquidity.clone() - amount.clone()
            } else {
                Nat::from(0u8)
            };
        }
    }
}

#[async_trait]
impl SharePayoutPolicy for LiquidityPayoutPolicy {
    async fn wants_shares(&self, collateral_pool: Principal, expected_seized: Nat) -> bool {
        match self.liquidity(collateral_pool).await {
            Ok(available) => {
                let as_shares = pool_cannot_pay(&available, &expected_seized, self.margin_bps);
                if !as_shares {
                    // A token payout uses this liquidity. The next candidate of the cycle sees less.
                    self.reserve(collateral_pool, &expected_seized);
                }
                as_shares
            }
            Err(error) => {
                // Without a liquidity figure, the liquidation keeps the token payout.
                warn!(pool = %collateral_pool, "share payout check skipped: liquidity read failed: {error}");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::liquidation::share_position_adapter::MockSharePositions;

    fn nat(value: u64) -> Nat {
        Nat::from(value)
    }

    #[test]
    fn the_margin_decides_at_the_boundary() {
        assert!(pool_cannot_pay(&nat(0), &nat(100), 500));
        assert!(pool_cannot_pay(&nat(104), &nat(100), 500));
        assert!(!pool_cannot_pay(&nat(105), &nat(100), 500));
        assert!(!pool_cannot_pay(&nat(1_000), &nat(100), 500));
    }

    #[tokio::test]
    async fn an_empty_pool_gets_a_share_payout_and_a_liquid_pool_does_not() {
        let mut positions = MockSharePositions::new();
        positions
            .expect_available_liquidity()
            .times(1)
            .returning(|_| Ok(Nat::from(50u8)));
        let policy = LiquidityPayoutPolicy::new(Arc::new(positions), 500);
        assert!(policy.wants_shares(Principal::anonymous(), nat(100)).await);
        // The second check uses the saved figure.
        assert!(!policy.wants_shares(Principal::anonymous(), nat(40)).await);
    }

    /// Two borrowers with the same collateral in one cycle: the pool can pay the first one only.
    #[tokio::test]
    async fn a_token_payout_uses_up_the_liquidity_for_the_next_candidate() {
        let mut positions = MockSharePositions::new();
        positions
            .expect_available_liquidity()
            .times(1)
            .returning(|_| Ok(Nat::from(100u8)));
        let policy = LiquidityPayoutPolicy::new(Arc::new(positions), 500);
        assert!(!policy.wants_shares(Principal::anonymous(), nat(60)).await);
        assert!(policy.wants_shares(Principal::anonymous(), nat(60)).await);
    }

    #[tokio::test]
    async fn a_failed_liquidity_read_keeps_the_token_payout() {
        let mut positions = MockSharePositions::new();
        positions
            .expect_available_liquidity()
            .returning(|_| Err("query failed".to_string()));
        let policy = LiquidityPayoutPolicy::new(Arc::new(positions), 500);
        assert!(!policy.wants_shares(Principal::anonymous(), nat(100)).await);
    }
}
