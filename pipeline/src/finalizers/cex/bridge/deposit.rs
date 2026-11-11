//! A bridged deposit on the exchange side: the exchange balance baseline and the
//! deposit address around the shared leg out, and its outcome mapped onto the steps.
use super::*;
use super::funding::credit_refusal;
use liquidium_pipeline_connectors::backend::cex_backend::FundingRoute;

use crate::finalizers::bridge_legs::{
    BridgeDestinationProvider, BridgeLegError, BridgeLegOutcome, BridgeOutInputs, FundingSlot, advance_bridge_out,
    out_route,
};

/// The exchange deposit address, looked up when the provider call is saved.
struct ExchangeDepositAddress<'a, C: CexBackend> {
    backend: &'a C,
    asset: &'a str,
    network: &'a str,
    /// The route the exchange checks; its deposit amount is set per arrival.
    route: FundingRoute,
}

#[async_trait::async_trait]
impl<C: CexBackend> BridgeDestinationProvider for ExchangeDepositAddress<'_, C> {
    async fn destination(&self) -> Result<String, String> {
        Ok(self
            .backend
            .get_deposit_address(self.asset, self.network)
            .await?
            .address)
    }

    /// The exchange's word, at the fresh size, before any funds move. A deposit
    /// under its minimum is never credited.
    async fn accepts_arrival(&self, expected_amount: f64) -> Result<(), BridgeLegError> {
        let route = FundingRoute {
            deposit_amount: expected_amount,
            ..self.route.clone()
        };
        self.backend.check_funding_route(&route).await.map_err(credit_refusal)
    }
}

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    /// The bridge route from the collateral to the asset the exchange receives.
    pub(in crate::finalizers::cex) fn deposit_route(
        state: &CexState,
        planned_asset: &str,
    ) -> Result<&'static BridgeRouteSpec, String> {
        out_route(&state.deposit.deposit_asset, planned_asset)
    }

    /// A deposit through a bridge: the shared leg out, then the exchange balance
    /// check once the bridge completes.
    pub(super) async fn deposit_bridged(
        &self,
        state: &mut CexState,
        planned_asset: &str,
        planned_network: &str,
    ) -> Result<(), CexBackendError> {
        let bridge = self.bridge_runtime()?;
        let route = Self::deposit_route(state, planned_asset)?;
        // The baseline is taken once, before the funding transfer is sized.
        if state.deposit.deposit_txid.is_none() && state.deposit.deposit_funding.is_none() {
            self.record_balance_before_deposit(state, planned_asset).await;
        }
        let destination = ExchangeDepositAddress {
            backend: self.backend.as_ref(),
            asset: planned_asset,
            network: planned_network,
            route: self.funding_route(state, 0.0)?,
        };
        // While the funding transfer is saved or in flight the step stays at
        // `Deposit`: the funds are still in the trader account, and a restart
        // settles the saved transfer instead of asking an operator.
        let funding_phase = state.deposit.deposit_txid.is_none();
        let outcome = advance_bridge_out(
            bridge,
            &mut state.deposit.bridge,
            FundingSlot {
                record: &mut state.deposit.deposit_funding,
                txid: &mut state.deposit.deposit_txid,
                sent_at_ts: &mut state.deposit.deposit_sent_at_ts,
            },
            BridgeOutInputs {
                leg_id: &state.liq_id,
                route,
                asset: &state.deposit.deposit_asset,
                size_in: &state.size_in,
                outbound: self.outbound.as_ref(),
                destination: &destination,
            },
        )
        .await?;
        match outcome {
            BridgeLegOutcome::Waiting { stalled } => {
                if !funding_phase {
                    state.step = CexStep::DepositPending;
                }
                if let Some(line) = stalled {
                    state.last_error = Some(line);
                }
            }
            BridgeLegOutcome::Saved { side_costs } => {
                state.record_side_costs(&side_costs);
                state.send_saved = true;
                if !funding_phase {
                    state.step = CexStep::DepositPending;
                }
            }
            BridgeLegOutcome::Funded { side_costs } => {
                state.record_side_costs(&side_costs);
                state.step = CexStep::DepositPending;
            }
            BridgeLegOutcome::Submitted => {
                state.trade.trade_next_amount_in = state.deposit.bridge.deposit_bridge_expected_amount;
                state.step = CexStep::DepositPending;
            }
            BridgeLegOutcome::Completed { .. } => {
                self.check_deposit(state).await?;
                if !matches!(state.step, CexStep::Trade) {
                    state.step = CexStep::DepositPending;
                }
            }
        }
        Ok(())
    }
}
