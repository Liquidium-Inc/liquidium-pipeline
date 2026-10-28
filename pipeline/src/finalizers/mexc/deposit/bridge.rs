//! A bridged deposit: the funding of the bridge account, then the bridge
//! status poll. The provider call itself lives in `bridge_submission`.
use super::*;

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    /// The bridge route from the collateral to the asset MEXC receives.
    pub(in crate::finalizers::mexc) fn deposit_route(
        state: &CexState,
        planned_asset: &str,
    ) -> Result<&'static BridgeRouteSpec, String> {
        let source_symbol = state.deposit.deposit_asset.symbol();
        let source_chain = state.deposit.deposit_asset.chain();
        resolve_route(&source_symbol, &source_chain, planned_asset).ok_or_else(|| {
            format!("bridge route not found for deposit {source_symbol}@{source_chain} -> {planned_asset}")
        })
    }

    /// A deposit through a bridge: fund the bridge account, send the provider
    /// call for a bridge handle, then poll the bridge until it completes.
    pub(super) async fn deposit_bridged(
        &self,
        state: &mut CexState,
        planned_asset: &str,
    ) -> Result<(), CexBackendError> {
        let bridge = self.bridge_runtime()?;
        let route = Self::deposit_route(state, planned_asset)?;

        // Phase A: move the seized collateral into the configured bridge source.
        if state.deposit.deposit_txid.is_none() {
            let intent = if state.deposit.deposit_funding.is_some() {
                None
            } else {
                Some(
                    self.prepare_bridged_funding(state, bridge, route, planned_asset)
                        .await?,
                )
            };
            if !self.drive_funding(state, intent).await? {
                return Ok(());
            }
            // This build funded the bridge, so no call ran without a record.
            state.deposit.bridge.deposit_bridge_call_unrecorded = false;
            state.step = CexStep::DepositPending;
            return Ok(());
        }

        if state.deposit.bridge.deposit_bridge_id.is_none() {
            return self.advance_bridged_deposit_submission(state, bridge, route).await;
        }

        self.poll_deposit_bridge(state, bridge).await
    }

    /// Reads the bridge status. A completed bridge hands over to the MEXC
    /// balance check; a failed or canceled bridge returns an error.
    async fn poll_deposit_bridge(&self, state: &mut CexState, bridge: &BridgeRuntime) -> Result<(), CexBackendError> {
        state.deposit.bridge.deposit_bridge_polled_at_ts = Some(now_ts());
        let bridge_id = state
            .deposit
            .bridge
            .deposit_bridge_id
            .clone()
            .ok_or_else(|| "missing bridge id while polling bridged deposit".to_string())?;
        let status = match bridge.backend.get_bridge_status(&bridge_id).await {
            Ok(status) => status,
            // A failed read says nothing about the bridge, so it is a wait, not a strike.
            Err(BridgeError::Unreadable(error)) => {
                let submitted_at = state.deposit.bridge.deposit_bridge_submitted_at_ts;
                state.step = CexStep::DepositPending;
                return Self::note_bridge_wait(state, submitted_at, &bridge_id, Some(&error));
            }
            Err(error) => return Err(error.into()),
        };
        match status {
            BridgeStatus::Completed | BridgeStatus::Minted(_) => {
                self.check_deposit(state).await?;
                if !matches!(state.step, CexStep::Trade) {
                    state.step = CexStep::DepositPending;
                }
                Ok(())
            }
            BridgeStatus::Pending | BridgeStatus::Unknown => {
                let submitted_at = state.deposit.bridge.deposit_bridge_submitted_at_ts;
                state.step = CexStep::DepositPending;
                Self::note_bridge_wait(state, submitted_at, &bridge_id, None)
            }
            BridgeStatus::Failed { reason } => Err(format!(
                "bridged deposit failed for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "unknown failure".to_string())
            )
            .into()),
            BridgeStatus::Canceled { reason } => Err(format!(
                "bridged deposit canceled for liq_id {} bridge_id {}: {}",
                state.liq_id,
                bridge_id,
                reason.unwrap_or_else(|| "no reason provided".to_string())
            )
            .into()),
        }
    }
}
