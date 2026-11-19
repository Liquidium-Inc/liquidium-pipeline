//! Operator actions on parked rows: list a row's open questions, write an
//! answer and release a parked leg. The finalize stage does not read parked
//! rows, so an action and a finalize cycle never write the same row together.
use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use tracing::info;

use crate::finalizers::multi_venue::{MultiVenueFinalizer, derive_outcome, set_meta_v2};
use crate::{
    finalizers::multi_venue::MultiVenueAdapter,
    operator::{OperatorConsole, ReconcileAction, ReconcileRequest},
    outbound::driver::OperatorDecision,
    persistance::{
        FinalizerMetaPayload, LiqMetaWrapper, LiqResultRecord, MultiVenueExecutionState, OpenWithdraw, ResultStatus,
        VenueLegStatus, WalStore,
    },
    wal::{decode_receipt_wrapper, wal_load},
};

pub struct OperatorDesk {
    wal: Arc<dyn WalStore>,
    finalizer: Arc<MultiVenueFinalizer>,
    lock: tokio::sync::Mutex<()>,
}

impl OperatorDesk {
    pub fn new(wal: Arc<dyn WalStore>, finalizer: Arc<MultiVenueFinalizer>) -> Self {
        Self {
            wal,
            finalizer,
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl OperatorConsole for OperatorDesk {
    async fn reconcile(&self, request: ReconcileRequest) -> Result<String, String> {
        // One action at a time, so two operators cannot release one leg twice.
        let _guard = self.lock.lock().await;
        self.finalizer.reconcile(self.wal.as_ref(), &request).await
    }
}

impl MultiVenueFinalizer {
    /// Shows a row, or answers and releases one parked leg of it. A release
    /// only clears the park; the leg acts in a later finalize cycle.
    pub async fn reconcile(&self, wal: &dyn WalStore, request: &ReconcileRequest) -> Result<String, String> {
        let mut row = wal_load(wal, &request.liq_id)
            .await?
            .ok_or_else(|| format!("no WAL row {}", request.liq_id))?;
        let mut wrapper =
            decode_receipt_wrapper(&row)?.ok_or_else(|| format!("row {} has no receipt wrapper", row.id))?;
        if wrapper.meta_v2.is_none()
            && let Some(report) = self.reconcile_share_row(wal, &mut row, &mut wrapper, request).await?
        {
            return Ok(report);
        }
        let mut state = match wrapper.meta_v2.as_ref().map(|meta| &meta.payload) {
            Some(FinalizerMetaPayload::MultiVenueSwap(state)) => state.clone(),
            _ => {
                return Err(format!(
                    "row {} has no multi-venue swap state; reconcile works on those rows only",
                    row.id
                ));
            }
        };
        let report = self.describe(&row.id, row.status, row.last_error.as_deref(), &state);
        let answer = match &request.action {
            ReconcileAction::Show => return Ok(report),
            ReconcileAction::Release => None,
            ReconcileAction::Answer(answer) => Some(answer),
        };
        if row.status != ResultStatus::OperatorRequired {
            return Err(format!("row {} is {:?}, not parked", row.id, row.status));
        }

        // A budget parks the row while its legs still run. A release then
        // queues the whole row again.
        if !has_parked_leg(&state) && answer.is_none() && request.leg_id.is_none() {
            requeue(&mut row);
            self.persist_wrapper(wal, &mut row, &wrapper).await?;
            info!(liquidation_id = %row.id, "Operator released a parked row");
            return Ok(format!("{report}released row {}; it is queued again\n", row.id));
        }

        let index = parked_leg_index(&state, request.leg_id.as_deref())?;
        let leg_id = state.legs[index].leg_id.clone();
        let adapter = self.operator_adapter(&state.legs[index].venue_id)?;
        let execution = adapter.operator_release(&state.legs[index], answer)?;
        state.legs[index].execution = execution;
        state.legs[index].status = VenueLegStatus::Running;
        state.outcome = derive_outcome(&state.legs);
        set_meta_v2(&mut wrapper, state).map_err(|error| error.to_string())?;
        requeue(&mut row);
        self.persist_wrapper(wal, &mut row, &wrapper).await?;
        info!(
            liquidation_id = %row.id,
            leg_id = %leg_id,
            action = ?request.action,
            "Operator released a parked leg"
        );
        Ok(format!(
            "{report}released leg {leg_id}; row {} is queued again\n",
            row.id
        ))
    }

    /// Shows or releases a parked row of the share payout, which has no swap state yet.
    /// Returns `None` for any other row.
    async fn reconcile_share_row(
        &self,
        wal: &dyn WalStore,
        row: &mut LiqResultRecord,
        wrapper: &mut LiqMetaWrapper,
        request: &ReconcileRequest,
    ) -> Result<Option<String>, String> {
        if wrapper.receipt.request.liquidation.collateral_as_shares != Some(true) {
            return Ok(None);
        }
        let report = share_row_report(row, wrapper);
        if matches!(request.action, ReconcileAction::Show) {
            return Ok(Some(report));
        }
        if row.status != ResultStatus::OperatorRequired {
            return Err(format!("row {} is {:?}, not parked", row.id, row.status));
        }
        let released_to = match wrapper.share_holding.as_mut() {
            Some(holding) => {
                let release = share_release(&row.id, holding.open.as_ref(), &request.action, crate::utils::now_ts())?;
                holding.open = release.open;
                holding.withdrawn += release.newly_withdrawn;
                holding.next_try_at = 0;
                holding.last_note = None;
                row.status = ResultStatus::HeldAsShares;
                row.error_count = 0;
                row.attempt = 0;
                "the share withdraw step"
            }
            None if matches!(request.action, ReconcileAction::Release) => {
                requeue(row);
                "the swap"
            }
            None => return Err(format!("row {} has no open question; use --release", row.id)),
        };
        row.last_error = None;
        self.persist_wrapper(wal, row, wrapper).await?;
        info!(liquidation_id = %row.id, action = ?request.action, "Operator released a parked share row");
        Ok(Some(format!("{report}released row {} to {released_to}\n", row.id)))
    }

    fn operator_adapter(&self, venue_id: &str) -> Result<&dyn MultiVenueAdapter, String> {
        self.venue_adapter(venue_id)
            .ok_or_else(|| format!("no adapter for venue `{venue_id}`"))
    }

    fn describe(
        &self,
        id: &str,
        status: ResultStatus,
        last_error: Option<&str>,
        state: &MultiVenueExecutionState,
    ) -> String {
        let mut lines = vec![format!("row {id}: {status:?}")];
        if let Some(error) = last_error {
            lines.push(format!("  last error: {error}"));
        }
        if status == ResultStatus::OperatorRequired && !has_parked_leg(state) {
            lines.push("  no leg is parked: --release queues the row again".to_string());
        }
        for leg in &state.legs {
            lines.push(format!("  leg {} at {}: {:?}", leg.leg_id, leg.venue_id, leg.status));
            if let Some(error) = &leg.last_error {
                lines.push(format!("    last error: {error}"));
            }
            if leg.status != VenueLegStatus::OperatorRequired {
                continue;
            }
            let questions = self
                .operator_adapter(&leg.venue_id)
                .and_then(|adapter| adapter.operator_questions(leg));
            match questions {
                Ok(questions) if questions.is_empty() => {
                    lines.push("    no open question: --release lets the leg check again".to_string());
                }
                Ok(questions) => {
                    lines.extend(
                        questions
                            .into_iter()
                            .map(|question| format!("    question: {}", question.detail)),
                    );
                }
                Err(error) => lines.push(format!("    questions unavailable: {error}")),
            }
        }
        lines.push(String::new());
        lines.join("\n")
    }
}

/// Returns the lines that `reconcile` shows for a row of the share payout.
fn share_row_report(row: &LiqResultRecord, wrapper: &LiqMetaWrapper) -> String {
    let token = &wrapper.receipt.request.collateral_asset;
    let shown = |amount: &Nat| ChainTokenAmount::from_raw(token.clone(), amount.clone()).formatted();
    let mut lines = vec![format!("row {}: {:?}", row.id, row.status)];
    if let Some(error) = &row.last_error {
        lines.push(format!("  last error: {error}"));
    }
    match &wrapper.share_holding {
        Some(holding) => {
            lines.push(format!(
                "  collateral held as shares in pool {} by profile {}",
                holding.pool, holding.profile
            ));
            lines.push(format!(
                "  seized {} | withdrawn {} | arrived {} | still held {}",
                shown(&holding.seized),
                shown(&holding.withdrawn),
                shown(&holding.arrived),
                shown(&holding.remaining())
            ));
            lines.push(match &holding.open {
                Some(OpenWithdraw::Submitting { amount, .. }) => format!(
                    "  a withdraw of {} has an unknown outcome: answer --landed <withdraw id> if it ran, or --absent if it did not",
                    shown(amount)
                ),
                Some(OpenWithdraw::Accepted { outflow_id, amount, .. }) => format!(
                    "  withdraw {outflow_id} of {} waits for its payout: --release waits for it again",
                    shown(amount)
                ),
                None => "  no open withdraw: --release lets the bot withdraw what is still held".to_string(),
            });
        }
        None => {
            lines.push("  the canister paid the collateral as tokens to the main account".to_string());
            lines.push("  --release queues the row for the swap, which spends from the trader account".to_string());
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

/// The result of an operator action on the open withdraw of a parked share row.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShareRelease {
    /// The open withdraw after the action.
    open: Option<OpenWithdraw>,
    /// The amount that the action counts as withdrawn now.
    newly_withdrawn: Nat,
}

/// Returns the new state of a parked share row after an operator action, or an error when the action does not fit.
/// It changes nothing. Thus a refused action leaves the row as it is.
fn share_release(
    row_id: &str,
    open: Option<&OpenWithdraw>,
    action: &ReconcileAction,
    now: i64,
) -> Result<ShareRelease, String> {
    let nothing = || Nat::from(0u8);
    match (open, action) {
        // Rule 1: the operator saw that the withdraw ran. The bot counts the amount as withdrawn and looks for the payout.
        (
            Some(OpenWithdraw::Submitting {
                amount, after_index, ..
            }),
            ReconcileAction::Answer(OperatorDecision::Landed { receipt }),
        ) => Ok(ShareRelease {
            open: Some(OpenWithdraw::Accepted {
                outflow_id: receipt.clone(),
                amount: amount.clone(),
                after_index: *after_index,
                at: now,
            }),
            newly_withdrawn: amount.clone(),
        }),
        // Rule 2: the operator saw that the withdraw did not run. The bot withdraws the amount again later.
        (Some(OpenWithdraw::Submitting { .. }), ReconcileAction::Answer(OperatorDecision::Absent)) => {
            Ok(ShareRelease {
                open: None,
                newly_withdrawn: nothing(),
            })
        }
        // Rule 3: a withdraw with an unknown outcome needs an answer, because a plain release could withdraw twice.
        (Some(OpenWithdraw::Submitting { .. }), _) => Err(format!(
            "row {row_id} has a withdraw with an unknown outcome; answer --landed <withdraw id> or --absent"
        )),
        // Rule 4: a payout is late. The bot waits for it again from now, without a new withdraw.
        (
            Some(OpenWithdraw::Accepted {
                outflow_id,
                amount,
                after_index,
                ..
            }),
            ReconcileAction::Release,
        ) => Ok(ShareRelease {
            open: Some(OpenWithdraw::Accepted {
                outflow_id: outflow_id.clone(),
                amount: amount.clone(),
                after_index: *after_index,
                at: now,
            }),
            newly_withdrawn: nothing(),
        }),
        // Rule 5: no open withdraw. The bot withdraws the amount that is still held.
        (None, ReconcileAction::Release) => Ok(ShareRelease {
            open: None,
            newly_withdrawn: nothing(),
        }),
        // Rule 6: an answer to a row that has no open question.
        _ => Err(format!("row {row_id} has no open question; use --release")),
    }
}

fn has_parked_leg(state: &MultiVenueExecutionState) -> bool {
    state
        .legs
        .iter()
        .any(|leg| leg.status == VenueLegStatus::OperatorRequired)
}

/// Queues a released row again. Old failures and attempts must not count
/// against it.
fn requeue(row: &mut LiqResultRecord) {
    row.status = ResultStatus::Enqueued;
    row.error_count = 0;
    row.attempt = 0;
}

fn parked_leg_index(state: &MultiVenueExecutionState, leg_id: Option<&str>) -> Result<usize, String> {
    let parked: Vec<usize> = state
        .legs
        .iter()
        .enumerate()
        .filter(|(_, leg)| leg.status == VenueLegStatus::OperatorRequired)
        .map(|(index, _)| index)
        .collect();
    match (leg_id, parked.as_slice()) {
        (Some(id), _) => parked
            .into_iter()
            .find(|&index| state.legs[index].leg_id == id)
            .ok_or_else(|| format!("leg {id} is not parked")),
        (None, [index]) => Ok(*index),
        (None, []) => Err("no leg of this row is parked".to_string()),
        (None, _) => Err(format!("{} legs are parked; choose one with --leg", parked.len())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submitting() -> OpenWithdraw {
        OpenWithdraw::Submitting {
            amount: Nat::from(40_000u64),
            shares_before: Nat::from(777u64),
            after_index: 5,
            at: 1,
        }
    }

    fn accepted() -> OpenWithdraw {
        OpenWithdraw::Accepted {
            outflow_id: "withdraw_3".to_string(),
            amount: Nat::from(40_000u64),
            after_index: 5,
            at: 1,
        }
    }

    fn landed() -> ReconcileAction {
        ReconcileAction::Answer(OperatorDecision::Landed {
            receipt: "withdraw_9".to_string(),
        })
    }

    /// One row for each pair of open withdraw and operator action. `None` means that the action is refused.
    #[test]
    fn share_release_follows_its_rules_in_order() {
        let (landed, absent, release) = (
            landed(),
            ReconcileAction::Answer(OperatorDecision::Absent),
            ReconcileAction::Release,
        );
        let wait_for = |id: &str, newly_withdrawn: u64| {
            Some(ShareRelease {
                open: Some(OpenWithdraw::Accepted {
                    outflow_id: id.to_string(),
                    amount: Nat::from(40_000u64),
                    after_index: 5,
                    at: 100,
                }),
                newly_withdrawn: Nat::from(newly_withdrawn),
            })
        };
        let withdraw_again = || {
            Some(ShareRelease {
                open: None,
                newly_withdrawn: Nat::from(0u8),
            })
        };
        #[rustfmt::skip]
        let cases = [
            ("rule 1: unknown outcome, landed", Some(submitting()), &landed, wait_for("withdraw_9", 40_000)),
            ("rule 2: unknown outcome, absent", Some(submitting()), &absent, withdraw_again()),
            ("rule 3: unknown outcome, release", Some(submitting()), &release, None),
            ("rule 4: late payout, release", Some(accepted()), &release, wait_for("withdraw_3", 0)),
            ("rule 5: nothing open, release", None, &release, withdraw_again()),
            ("rule 6: late payout, landed", Some(accepted()), &landed, None),
            ("rule 6: late payout, absent", Some(accepted()), &absent, None),
            ("rule 6: nothing open, landed", None, &landed, None),
            ("rule 6: nothing open, absent", None, &absent, None),
        ];
        for (name, open, action, expected) in cases {
            assert_eq!(share_release("7", open.as_ref(), action, 100).ok(), expected, "{name}");
        }
    }
}
