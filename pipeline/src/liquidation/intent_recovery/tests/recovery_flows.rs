//! Flow tests of the recovery on a real SQLite store.
use std::sync::Arc;

use candid::Decode;
use liquidium_pipeline_connectors::backend::ledger_history_backend::MockLedgerHistoryBackend;
use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
use liquidium_pipeline_core::types::protocol_types::ProtocolError;

use super::super::{IntentRecovery, RecoverySummary};
use super::*;
use crate::persistance::sqlite::SqliteWalStore;
use crate::persistance::{LiquidationIntentStatus, LiquidationIntentStore, ResultStatus, WalStore};
use crate::wal::decode_receipt_wrapper;

struct Fixture {
    _file: tempfile::NamedTempFile,
    store: Arc<SqliteWalStore>,
}

impl Fixture {
    /// A store with one journal entry per request, each with an unknown outcome, as after a stop during the call.
    async fn with_interrupted(requests: Vec<(&str, ExecutorRequest)>) -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        let store = Arc::new(SqliteWalStore::new(file.path().to_str().unwrap()).unwrap());
        for (intent_id, request) in requests {
            store.create_submitting(intent_id, &request).await.unwrap();
        }
        store
            .recover_submitting_as_ambiguous("process restarted")
            .await
            .unwrap();
        Self { _file: file, store }
    }

    /// Runs the recovery with the given ledger transfers and canister results.
    async fn recover(&self, transfers: Vec<LedgerTransfer>, results: Vec<LiquidationResult>) -> RecoverySummary {
        let mut history = MockLedgerHistoryBackend::new();
        history.expect_recent_transfers().returning(move |ledger, account, _| {
            assert_eq!(ledger, debt_ledger());
            assert_eq!(account.owner, liquidator());
            Ok(transfers.clone())
        });
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Result<LiquidationResult, ProtocolError>>()
            .returning(move |_, method, arg| {
                assert_eq!(method, "get_liquidation");
                let id = Decode!(&arg, u128).unwrap();
                results
                    .iter()
                    .find(|result| result.id == id)
                    .cloned()
                    .map(Ok)
                    .ok_or_else(|| format!("no liquidation {id}"))
            });
        IntentRecovery::new(
            Arc::new(agent),
            self.store.clone(),
            Arc::new(history),
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            liquidator(),
        )
        .resolve_ambiguous()
        .await
        .unwrap()
    }

    async fn intent_status(&self, intent_id: &str) -> LiquidationIntentStatus {
        self.store.get_intent(intent_id).await.unwrap().unwrap().status
    }
}

/// The offer transfer of a journal entry that the test wrote now.
fn fresh_offer(id: u128) -> LedgerTransfer {
    let mut transfer = offer_transfer(id, OFFER, 0);
    transfer.timestamp_nanos = Some(crate::utils::now_ts() as u64 * 1_000_000_000);
    transfer
}

fn recovered(count: usize) -> RecoverySummary {
    RecoverySummary {
        recovered: count,
        ..RecoverySummary::default()
    }
}

fn open(count: usize) -> RecoverySummary {
    RecoverySummary {
        open: count,
        ..RecoverySummary::default()
    }
}

/// Liquidation 1833 on staging: the bot stopped during the call, and the canister paid shares.
#[tokio::test]
async fn an_interrupted_share_payout_gets_its_held_row() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(true))]).await;
    let profile = p("g4oj7-diaaa-aaaaa-aaaaa-aaaaa-aaaaa-phai");

    let summary = fixture
        .recover(vec![fresh_offer(1833)], vec![result(1833, Some(profile))])
        .await;

    assert_eq!(summary, recovered(1));
    assert_eq!(
        fixture.intent_status("intent-1").await,
        LiquidationIntentStatus::Accepted
    );
    let row = fixture.store.get_result("1833").await.unwrap().unwrap();
    assert_eq!(row.status, ResultStatus::HeldAsShares);
    let holding = decode_receipt_wrapper(&row).unwrap().unwrap().share_holding.unwrap();
    assert_eq!(holding.profile, profile);
    assert_eq!(holding.seized, Nat::from(6_554_695_335_714_284u64));
}

#[tokio::test]
async fn an_interrupted_token_payout_gets_a_row_for_the_swap() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;

    let summary = fixture.recover(vec![fresh_offer(1833)], vec![result(1833, None)]).await;

    assert_eq!(summary, recovered(1));
    let row = fixture.store.get_result("1833").await.unwrap().unwrap();
    assert_eq!(row.status, ResultStatus::Enqueued);
}

#[tokio::test]
async fn an_interrupted_liquidation_that_failed_closes_the_entry_without_a_row() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;
    let mut failed = result(1833, None);
    failed.status = LiquidationStatus::FailedLiquidation("NoLiquidity".to_string());

    let summary = fixture.recover(vec![fresh_offer(1833)], vec![failed]).await;

    assert_eq!(summary.failed, 1);
    assert_eq!(fixture.intent_status("intent-1").await, LiquidationIntentStatus::Failed);
    assert!(fixture.store.get_result("1833").await.unwrap().is_none());
}

/// The index can be behind, so a missing transfer is no proof that the call did not run.
#[tokio::test]
async fn an_entry_without_an_offer_transfer_stays_open() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(true))]).await;

    let summary = fixture.recover(Vec::new(), Vec::new()).await;

    assert_eq!(summary, open(1));
    assert_eq!(
        fixture.intent_status("intent-1").await,
        LiquidationIntentStatus::Ambiguous
    );
}

#[tokio::test]
async fn a_result_that_does_not_fit_the_request_leaves_the_entry_open() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;
    let mut other = result(1833, None);
    other.collateral_asset = AssetType::CkAsset(debt_ledger());

    let summary = fixture.recover(vec![fresh_offer(1833)], vec![other]).await;

    assert_eq!(summary, open(1));
    assert_eq!(
        fixture.intent_status("intent-1").await,
        LiquidationIntentStatus::Ambiguous
    );
    assert!(fixture.store.get_result("1833").await.unwrap().is_none());
}

/// An operator possibly moved the collateral of an old entry by hand, so the recovery does not make a row for it.
#[tokio::test]
async fn an_entry_older_than_a_week_stays_for_an_operator() {
    use diesel::connection::SimpleConnection;
    use diesel::{Connection, SqliteConnection};
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;
    let eight_days_ago = crate::utils::now_ts() - 8 * 24 * 3_600;
    SqliteConnection::establish(fixture._file.path().to_str().unwrap())
        .unwrap()
        .batch_execute(&format!("UPDATE liquidation_intents SET created_at = {eight_days_ago}"))
        .unwrap();

    // The mocks have no expectation for a ledger read or a canister call, so either one fails the test.
    let recovery = IntentRecovery::new(
        Arc::new(MockPipelineAgent::new()),
        fixture.store.clone(),
        Arc::new(MockLedgerHistoryBackend::new()),
        p("nja4y-2yaaa-aaaae-qddxa-cai"),
        liquidator(),
    );

    assert_eq!(recovery.resolve_ambiguous().await.unwrap(), open(1));
    assert_eq!(
        fixture.intent_status("intent-1").await,
        LiquidationIntentStatus::Ambiguous
    );
}

#[tokio::test]
async fn a_second_run_changes_nothing() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;
    assert_eq!(
        fixture.recover(vec![fresh_offer(1833)], vec![result(1833, None)]).await,
        recovered(1)
    );
    let before = fixture.store.get_result("1833").await.unwrap().unwrap();

    let again = fixture.recover(vec![fresh_offer(1833)], vec![result(1833, None)]).await;

    assert_eq!(again, RecoverySummary::default());
    let after = fixture.store.get_result("1833").await.unwrap().unwrap();
    assert_eq!(after.meta_json, before.meta_json);
    assert_eq!(after.status, before.status);
}

/// A transfer whose liquidation already has a row belongs to an answered call, not to the open entry.
#[tokio::test]
async fn an_offer_transfer_of_a_liquidation_with_a_row_is_not_taken_again() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false))]).await;
    fixture.recover(vec![fresh_offer(1833)], vec![result(1833, None)]).await;
    // A new interrupted call with the same offer amount.
    fixture
        .store
        .create_submitting("intent-2", &request(false))
        .await
        .unwrap();
    fixture
        .store
        .recover_submitting_as_ambiguous("process restarted")
        .await
        .unwrap();

    let summary = fixture.recover(vec![fresh_offer(1833)], vec![result(1833, None)]).await;

    assert_eq!(summary, open(1));
    assert_eq!(
        fixture.intent_status("intent-2").await,
        LiquidationIntentStatus::Ambiguous
    );
}

#[tokio::test]
async fn two_entries_with_the_same_offer_and_two_transfers_stay_open() {
    let fixture = Fixture::with_interrupted(vec![("intent-1", request(false)), ("intent-2", request(false))]).await;

    let summary = fixture
        .recover(
            vec![fresh_offer(1833), fresh_offer(1834)],
            vec![result(1833, None), result(1834, None)],
        )
        .await;

    assert_eq!(summary, open(2));
    assert!(fixture.store.get_result("1833").await.unwrap().is_none());
    assert!(fixture.store.get_result("1834").await.unwrap().is_none());
}
