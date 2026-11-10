//! The leg back on the Rift side. An Ethereum payout at the bridge EVM wallet
//! goes to its ck form through the shared leg. A Bitcoin payout went to the
//! ckBTC minter's address for the final account, so only the mint is watched.
use liquidium_pipeline_connectors::backend::{
    bridge_backend::{BridgeRouteSpec, BridgeStatus, DepositNotice},
    rift_backend::RiftBackend,
};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use log::info;

use super::{RiftChain, RiftVenueFinalizer, VenueStepError, leg_routes, rift_amount};
use crate::{
    finalizers::{
        bridge_legs::{
            BridgeInInputs, BridgeLegOutcome, WaitBound, advance_bridge_in, bridge_failure, read_bridge_status,
            wait_within_bound,
        },
        bridge_planner::bridge_destination,
        rift::state::{RiftState, RiftStep},
    },
    utils::now_ts,
};

/// What Rift delivered, read from the order once for either chain.
struct Payout {
    route: &'static BridgeRouteSpec,
    chain: RiftChain,
    amount: ChainTokenAmount,
    transaction: Option<String>,
}

impl Payout {
    fn of(state: &RiftState) -> Result<Self, VenueStepError> {
        let route = leg_routes(&state.size_in.token, &state.final_asset)?.1;
        let order = state
            .order
            .as_ref()
            .ok_or("missing Rift order before the bridge back")?;
        let delivered = order
            .amount_out
            .as_deref()
            .ok_or("Rift order delivered without an amount")?;
        Ok(Self {
            route,
            chain: state.receive_chain()?,
            amount: rift_amount(&state.final_asset, delivered)?,
            transaction: order.payout_tx.clone(),
        })
    }
}

/// Finishes the leg with what reached ICP.
fn complete(state: &mut RiftState, minted: ChainTokenAmount) {
    state.size_out = Some(minted);
    state.step = RiftStep::Completed;
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub(super) async fn bridge_back(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        let payout = Payout::of(state)?;
        match payout.chain {
            RiftChain::Ethereum => self.bridge_back_from_ethereum(state, payout).await,
            RiftChain::Bitcoin => self.mint_bitcoin_payout(state, payout).await,
        }
    }

    /// The shared leg: the payout at the bridge wallet funds the ckERC20 or
    /// native ETH bridge.
    async fn bridge_back_from_ethereum(&self, state: &mut RiftState, payout: Payout) -> Result<(), VenueStepError> {
        let order = state
            .order
            .as_mut()
            .ok_or("missing Rift order before the bridge back")?;
        let outcome = advance_bridge_in(
            &self.bridge,
            &mut state.bridge_in,
            BridgeInInputs {
                leg_id: &state.liq_id,
                route: payout.route,
                amount: payout.amount.to_f64(),
                final_asset: &state.final_asset,
                final_destination: &state.final_destination,
                wait: WaitBound {
                    since: &mut order.delivered_at_ts,
                    timeout_secs: payout.chain.return_timeout_secs(),
                },
            },
        )
        .await?;
        match outcome {
            BridgeLegOutcome::Waiting { stalled } => {
                if let Some(line) = stalled {
                    state.last_error = Some(line);
                }
            }
            BridgeLegOutcome::Saved { .. } => state.send_saved = true,
            BridgeLegOutcome::Funded { .. } | BridgeLegOutcome::Submitted => {}
            // The mint receipt is what reached ICP. Without one, the
            // delivered amount is the best record.
            BridgeLegOutcome::Completed { receipt } => complete(
                state,
                match receipt {
                    Some(receipt) => ChainTokenAmount::from_raw(state.final_asset.clone(), receipt.amount),
                    None => payout.amount,
                },
            ),
        }
        Ok(())
    }

    /// Rift paid the minter's address for the final account, so nothing of
    /// ours moves. The minter mints once the deposit has its confirmations;
    /// the leg asks until it does, within the chain's bound.
    async fn mint_bitcoin_payout(&self, state: &mut RiftState, payout: Payout) -> Result<(), VenueStepError> {
        let destination = bridge_destination(payout.route.destination_kind, &state.final_destination)?;
        let payout_tx = payout.transaction.ok_or_else(|| {
            VenueStepError::Ambiguous("Rift delivered Bitcoin without a payout transaction".to_string())
        })?;
        let liq_id = state.liq_id.clone();
        let order = state
            .order
            .as_mut()
            .ok_or("missing Rift order before the bridge back")?;
        let mut wait = WaitBound {
            since: &mut order.delivered_at_ts,
            timeout_secs: payout.chain.return_timeout_secs(),
        };

        let bridge_id = match state.bridge_in.withdraw_bridge_id.clone() {
            Some(bridge_id) => bridge_id,
            None => {
                let notice = self
                    .bridge
                    .backend
                    .track_deposit(
                        payout.route.source_asset,
                        payout.route.source_chain,
                        &destination,
                        "",
                        &payout_tx,
                    )
                    .await
                    .map_err(VenueStepError::Other)?;
                match notice {
                    DepositNotice::Accepted(submission) => {
                        state.bridge_in.withdraw_bridge_id = Some(submission.bridge_id.clone());
                        state.bridge_in.withdraw_bridge_submitted_at_ts = Some(now_ts());
                        submission.bridge_id
                    }
                    DepositNotice::NotYet { reason } => {
                        wait_within_bound(&liq_id, &mut wait, "ckBTC mint", &reason)?;
                        return Ok(());
                    }
                    DepositNotice::Held { reason } => return Err(VenueStepError::Ambiguous(reason)),
                }
            }
        };

        let (status, note) = read_bridge_status(&self.bridge, &bridge_id).await?;
        match status {
            BridgeStatus::Minted(receipt) => {
                info!(
                    "[rift] liq_id={liq_id} Bitcoin payout {payout_tx} minted {} ckBTC units in block {}",
                    receipt.amount, receipt.mint_block_index
                );
                state.bridge_in.withdraw_bridge_mint_receipt = Some(receipt.clone());
                complete(
                    state,
                    ChainTokenAmount::from_raw(state.final_asset.clone(), receipt.amount),
                );
                Ok(())
            }
            BridgeStatus::Pending | BridgeStatus::Completed | BridgeStatus::Unknown => {
                let reason = note.unwrap_or_else(|| "the mint is not readable yet".to_string());
                wait_within_bound(&liq_id, &mut wait, "ckBTC mint", &reason)?;
                Ok(())
            }
            failed => Err(bridge_failure("ckBTC mint", &liq_id, &bridge_id, failed).into()),
        }
    }
}
