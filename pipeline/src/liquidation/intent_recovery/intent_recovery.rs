//! Finds the result of a liquidation call whose answer the bot did not get, and makes the missing row.
//! The offer transfer on the debt ledger has the liquidation id as memo, so the ledger connects the journal entry to the canister result.
use std::collections::HashSet;
use std::sync::Arc;

use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::ledger_history_backend::LedgerHistoryBackend;
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use tracing::{info, warn};

use crate::executors::executor::ExecutorRequest;
use crate::liquidation::lending_queries::get_liquidation;
use crate::persistance::{LiquidationIntentRecord, LiquidationIntentStore, WalStore};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::now_ts;
use crate::watchdog::{Watchdog, WatchdogEvent, noop_watchdog};

#[path = "offer_matching.rs"]
mod offer_matching;
use offer_matching::{OfferQuery, OfferTransfer, choose_offer, confirms, offer_candidates};

/// How many of the newest transactions of the liquidator one search reads.
const OFFER_SCAN_DEPTH: u64 = 200;
/// The recovery leaves an older entry to an operator, who possibly moved its collateral by hand already.
const MAX_ENTRY_AGE_SECS: i64 = 7 * 24 * 3_600;

/// What one run of the recovery did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoverySummary {
    /// Entries that now have a row.
    pub recovered: usize,
    /// Entries whose liquidation failed in the canister. They are closed.
    pub failed: usize,
    /// Entries that stay open, for the next run or for an operator.
    pub open: usize,
}

/// What the recovery did with one journal entry.
enum Resolution {
    Recovered,
    Failed,
    Open,
}

pub struct IntentRecovery<A: PipelineAgent, D: LiquidationIntentStore + WalStore> {
    agent: Arc<A>,
    store: Arc<D>,
    history: Arc<dyn LedgerHistoryBackend>,
    lending_canister: Principal,
    /// The account that pays the offers.
    liquidator: Principal,
    watchdog: Arc<dyn Watchdog>,
}

impl<A: PipelineAgent, D: LiquidationIntentStore + WalStore> IntentRecovery<A, D> {
    pub fn new(
        agent: Arc<A>,
        store: Arc<D>,
        history: Arc<dyn LedgerHistoryBackend>,
        lending_canister: Principal,
        liquidator: Principal,
    ) -> Self {
        Self {
            agent,
            store,
            history,
            lending_canister,
            liquidator,
            watchdog: noop_watchdog(),
        }
    }

    pub fn with_watchdog(mut self, watchdog: Arc<dyn Watchdog>) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// Tries to close each journal entry with an unknown outcome. An entry that it cannot prove stays as it is.
    pub async fn resolve_ambiguous(&self) -> Result<RecoverySummary, String> {
        let entries = self.store.list_ambiguous().await.map_err(|error| error.to_string())?;
        let mut summary = RecoverySummary::default();
        // An id that one entry took in this run is not free for the next entry.
        let mut taken: HashSet<u128> = HashSet::new();
        for entry in entries {
            match self.resolve(&entry, &mut taken).await {
                Ok(Resolution::Recovered) => summary.recovered += 1,
                Ok(Resolution::Failed) => summary.failed += 1,
                Ok(Resolution::Open) => summary.open += 1,
                Err(error) => {
                    summary.open += 1;
                    warn!(intent_id = %entry.intent_id, "[recovery] entry stays open: {error}");
                }
            }
        }
        Ok(summary)
    }

    async fn resolve(&self, entry: &LiquidationIntentRecord, taken: &mut HashSet<u128>) -> Result<Resolution, String> {
        if now_ts() - entry.created_at > MAX_ENTRY_AGE_SECS {
            return Ok(Resolution::Open);
        }
        let request: ExecutorRequest = serde_json::from_str(&entry.request_json)
            .map_err(|error| format!("the saved request cannot be read: {error}"))?;
        let ChainToken::Icp { ledger, .. } = &request.debt_asset else {
            return Err("the debt asset is not on an ICP ledger".to_string());
        };
        let account = Account {
            owner: self.liquidator,
            subaccount: None,
        };
        let transfers = self
            .history
            .recent_transfers(*ledger, account, OFFER_SCAN_DEPTH)
            .await?;
        let query = OfferQuery {
            liquidator: self.liquidator,
            debt_pool: request.liquidation.debt_pool_id,
            offer: request.liquidation.debt_amount.clone(),
            created_at: entry.created_at,
        };
        let mut open_candidates = Vec::new();
        for id in offer_candidates(&query, &transfers) {
            let has_row = self
                .store
                .get_result(&id.to_string())
                .await
                .map_err(|error| error.to_string())?
                .is_some();
            if !has_row && !taken.contains(&id) {
                open_candidates.push(id);
            }
        }
        let id = match choose_offer(&open_candidates) {
            OfferTransfer::Found(id) => id,
            // The index can be behind, so a missing transfer closes nothing.
            OfferTransfer::NotFound => return Ok(Resolution::Open),
            OfferTransfer::Ambiguous => {
                return Err(format!(
                    "{} offer transfers fit this entry: {open_candidates:?}",
                    open_candidates.len()
                ));
            }
        };

        // The lending canister must agree with the ledger before the bot saves anything.
        let result = get_liquidation(self.agent.as_ref(), &self.lending_canister, id).await?;
        confirms(&request, id, &result)
            .map_err(|mismatch| format!("liquidation {id} does not fit the saved request: {mismatch:?}"))?;
        taken.insert(id);

        let receipt = ExecutionReceipt::from_result(request, result);
        if let ExecutionStatus::FailedLiquidation(error) = &receipt.status {
            let error = error.clone();
            self.store
                .mark_failed(&entry.intent_id, Some(id.to_string()), Some(receipt), &error)
                .await
                .map_err(|store_error| store_error.to_string())?;
            info!(intent_id = %entry.intent_id, liq_id = id, "[recovery] the interrupted liquidation failed in the canister: {error}");
            return Ok(Resolution::Failed);
        }
        self.store
            .mark_accepted(&entry.intent_id, &id.to_string(), &receipt)
            .await
            .map_err(|store_error| store_error.to_string())?;
        let details = format!(
            "liquidation {id} ran while its answer was lost; the bot found it by the offer transfer and made its row"
        );
        info!(event = "liquidation_recovered", intent_id = %entry.intent_id, liq_id = id, "[recovery] ✅ {details}");
        self.watchdog
            .notify(WatchdogEvent::Lifecycle {
                state: "interrupted liquidation recovered".to_string(),
                details,
            })
            .await;
        Ok(Resolution::Recovered)
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
