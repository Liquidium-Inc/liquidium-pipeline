use anyhow::{Context as _, Result};
use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    sql_types::{BigInt, Integer, Nullable, Text},
};

use crate::{
    executors::executor::ExecutorRequest,
    persistance::{
        LiqMetaWrapper, LiquidationIntentRecord, LiquidationIntentStatus, LiquidationIntentStore, ResultStatus,
        ShareHolding, WalProfitSnapshot, now_secs,
    },
    stages::executor::ExecutionReceipt,
};

use crate::liquidation::collateral_payout_utils::{CollateralPayout, classify_collateral_payout};

use super::sqlite::SqliteWalStore;
use liquidium_pipeline_core::types::protocol_types::{LiquidationResult, TransferStatus};

/// Names a payout result that needs reconciliation before collateral can be spent.
pub const SHARE_PROFILE_MISMATCH: &str = "collateral payout does not match the requested lending profile; reconcile the position and transfers before releasing this row";

/// Returns the initial row status from the credited profile and token settlement.
fn first_status(receipt: &ExecutionReceipt, liquidation: &LiquidationResult) -> (ResultStatus, Option<&'static str>) {
    match classify_collateral_payout(receipt.request.liquidation.collateral_share_receiver, liquidation) {
        Err(_) => (ResultStatus::OperatorRequired, Some(SHARE_PROFILE_MISMATCH)),
        Ok(CollateralPayout::Shares(_)) => (ResultStatus::HeldAsShares, None),
        Ok(CollateralPayout::NoCollateral) => (ResultStatus::Enqueued, None),
        Ok(CollateralPayout::Underlying) if matches!(liquidation.collateral_tx.status, TransferStatus::Success) => {
            (ResultStatus::Enqueued, None)
        }
        Ok(CollateralPayout::Underlying) => (ResultStatus::WaitingCollateral, None),
    }
}

impl SqliteWalStore {
    pub fn set_daemon_paused(&self, paused: bool) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        diesel::sql_query(
            "INSERT INTO daemon_control_state (singleton_id, paused, updated_at) VALUES (1, ?, ?) \
             ON CONFLICT(singleton_id) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at",
        )
        .bind::<Integer, _>(if paused { 1 } else { 0 })
        .bind::<BigInt, _>(now_secs())
        .execute(&mut conn)?;
        Ok(())
    }

    pub fn daemon_paused(&self) -> Result<bool> {
        #[derive(QueryableByName)]
        struct StateRow {
            #[diesel(sql_type = Integer)]
            paused: i32,
        }

        let mut conn = self.get_conn()?;
        let row = diesel::sql_query("SELECT paused FROM daemon_control_state WHERE singleton_id = 1 LIMIT 1")
            .get_result::<StateRow>(&mut conn)
            .optional()?;
        Ok(row.is_some_and(|row| row.paused != 0))
    }
}

#[derive(QueryableByName)]
struct IntentSqlRow {
    #[diesel(sql_type = Text)]
    intent_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    liquidation_id: Option<String>,
    #[diesel(sql_type = Integer)]
    status: i32,
    #[diesel(sql_type = Text)]
    request_json: String,
    #[diesel(sql_type = Nullable<Text>)]
    receipt_json: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = BigInt)]
    created_at: i64,
    #[diesel(sql_type = BigInt)]
    updated_at: i64,
}

impl TryFrom<IntentSqlRow> for LiquidationIntentRecord {
    type Error = anyhow::Error;

    fn try_from(row: IntentSqlRow) -> Result<Self> {
        Ok(Self {
            intent_id: row.intent_id,
            liquidation_id: row.liquidation_id,
            status: row.status.try_into()?,
            request_json: row.request_json,
            receipt_json: row.receipt_json,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[async_trait]
impl LiquidationIntentStore for SqliteWalStore {
    async fn create_submitting(&self, intent_id: &str, request: &ExecutorRequest) -> Result<()> {
        self.ensure_writable()?;
        let request_json = serde_json::to_string(request).context("encode liquidation intent request")?;
        let now = now_secs();
        let mut conn = self.get_conn()?;
        diesel::sql_query(
            "INSERT INTO liquidation_intents \
             (intent_id, status, request_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind::<Text, _>(intent_id)
        .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
        .bind::<Text, _>(request_json)
        .bind::<BigInt, _>(now)
        .bind::<BigInt, _>(now)
        .execute(&mut conn)?;
        Ok(())
    }

    async fn mark_accepted(&self, intent_id: &str, liquidation_id: &str, receipt: &ExecutionReceipt) -> Result<()> {
        self.ensure_writable()?;
        let liquidation = receipt.liquidation_result.as_ref().context("accepted receipt has no liquidation result")?;
        anyhow::ensure!(liquidation.id.to_string() == liquidation_id, "accepted receipt liquidation id mismatch");
        let now = now_secs();
        let share_holding = liquidation.collateral_share_receiver.map(|profile| {
            ShareHolding::new(
                profile,
                receipt.request.liquidation.collateral_pool_id,
                liquidation.amounts.collateral_received.clone(),
                now,
            )
        });
        let (status, note) = first_status(receipt, liquidation);
        let last_error = note.map(str::to_string);
        let receipt_json = serde_json::to_string(receipt).context("encode accepted liquidation receipt")?;
        let meta_json = serde_json::to_string(&LiqMetaWrapper {
            share_holding,
            profit_snapshot: Some(WalProfitSnapshot {
                expected_profit_raw: receipt.request.expected_profit.to_string(),
                realized_profit_raw: None,
                debt_symbol: receipt.request.debt_asset.symbol().to_string(),
                debt_decimals: receipt.request.debt_asset.decimals(),
                updated_at: now,
            }),
            ..LiqMetaWrapper::new(receipt.clone())
        })?;
        let mut conn = self.get_conn()?;
        // Acceptance and runnable work commit together. No importer/cursor can
        // lose the handoff, and a duplicate receipt must never overwrite execution.
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            let updated = diesel::sql_query(
                "UPDATE liquidation_intents SET liquidation_id = ?, status = ?, receipt_json = ?, \
                 last_error = NULL, updated_at = ? WHERE intent_id = ? AND status IN (?, ?)",
            )
            .bind::<Text, _>(liquidation_id)
            .bind::<Integer, _>(LiquidationIntentStatus::Accepted as i32)
            .bind::<Text, _>(&receipt_json)
            .bind::<BigInt, _>(now)
            .bind::<Text, _>(intent_id)
            .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
            .bind::<Integer, _>(LiquidationIntentStatus::Ambiguous as i32)
            .execute(conn)?;
            if updated != 1 {
                anyhow::bail!("intent {intent_id} is missing or already closed");
            }
            diesel::sql_query(
                "INSERT INTO liquidation_results (liq_id, status, created_at, updated_at, meta_json, last_error) \
                 SELECT ?, ?, created_at, ?, ?, ? FROM liquidation_intents WHERE intent_id = ?",
            )
                .bind::<Text, _>(liquidation_id)
                .bind::<Integer, _>(status as i32)
                .bind::<BigInt, _>(now)
                .bind::<Text, _>(&meta_json)
                .bind::<Nullable<Text>, _>(&last_error)
                .bind::<Text, _>(intent_id)
                .execute(conn)?;
            Ok(())
        })
    }

    async fn mark_failed(
        &self,
        intent_id: &str,
        liquidation_id: Option<String>,
        receipt: Option<ExecutionReceipt>,
        error: &str,
    ) -> Result<()> {
        self.ensure_writable()?;
        let receipt_json = receipt
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .context("encode failed liquidation receipt")?;
        let mut conn = self.get_conn()?;
        let updated = diesel::sql_query(
            "UPDATE liquidation_intents SET liquidation_id = ?, status = ?, receipt_json = ?, \
             last_error = ?, updated_at = ? WHERE intent_id = ? AND status IN (?, ?)",
        )
        .bind::<Nullable<Text>, _>(liquidation_id)
        .bind::<Integer, _>(LiquidationIntentStatus::Failed as i32)
        .bind::<Nullable<Text>, _>(receipt_json)
        .bind::<Text, _>(error)
        .bind::<BigInt, _>(now_secs())
        .bind::<Text, _>(intent_id)
        .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
        .bind::<Integer, _>(LiquidationIntentStatus::Ambiguous as i32)
        .execute(&mut conn)?;
        if updated != 1 {
            anyhow::bail!("intent {intent_id} is missing or already closed");
        }
        Ok(())
    }

    async fn mark_ambiguous(&self, intent_id: &str, error: &str, receipt: Option<ExecutionReceipt>) -> Result<()> {
        self.ensure_writable()?;
        let liquidation_id = receipt.as_ref().and_then(|receipt| {
            receipt.liquidation_result.as_ref().map(|liquidation| liquidation.id.to_string())
        });
        let receipt_json = receipt.as_ref().map(serde_json::to_string).transpose()?;
        let mut conn = self.get_conn()?;
        let updated = diesel::sql_query(
            "UPDATE liquidation_intents SET liquidation_id = ?, receipt_json = ?, status = ?, last_error = ?, updated_at = ? \
             WHERE intent_id = ? AND status = ?",
        )
        .bind::<Nullable<Text>, _>(liquidation_id)
        .bind::<Nullable<Text>, _>(receipt_json)
        .bind::<Integer, _>(LiquidationIntentStatus::Ambiguous as i32)
        .bind::<Text, _>(error)
        .bind::<BigInt, _>(now_secs())
        .bind::<Text, _>(intent_id)
        .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
        .execute(&mut conn)?;
        if updated != 1 {
            anyhow::bail!("intent {intent_id} is missing or no longer submitting");
        }
        Ok(())
    }

    async fn recover_submitting_as_ambiguous(&self, reason: &str) -> Result<usize> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        let updated = diesel::sql_query(
            "UPDATE liquidation_intents SET status = ?, last_error = ?, updated_at = ? WHERE status = ?",
        )
        .bind::<Integer, _>(LiquidationIntentStatus::Ambiguous as i32)
        .bind::<Text, _>(reason)
        .bind::<BigInt, _>(now_secs())
        .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
        .execute(&mut conn)?;
        Ok(updated)
    }

    async fn list_ambiguous(&self) -> Result<Vec<LiquidationIntentRecord>> {
        let mut conn = self.get_conn()?;
        diesel::sql_query(
            "SELECT intent_id, liquidation_id, status, request_json, receipt_json, last_error, \
             created_at, updated_at FROM liquidation_intents WHERE status = ? ORDER BY created_at ASC",
        )
        .bind::<Integer, _>(LiquidationIntentStatus::Ambiguous as i32)
        .load::<IntentSqlRow>(&mut conn)?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    async fn get_intent(&self, intent_id: &str) -> Result<Option<LiquidationIntentRecord>> {
        let mut conn = self.get_conn()?;
        diesel::sql_query(
            "SELECT intent_id, liquidation_id, status, request_json, receipt_json, last_error, \
             created_at, updated_at FROM liquidation_intents WHERE intent_id = ? LIMIT 1",
        )
        .bind::<Text, _>(intent_id)
        .get_result::<IntentSqlRow>(&mut conn)
        .optional()?
        .map(TryInto::try_into)
        .transpose()
    }

}

pub(super) fn initialize_schema(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        r#"
        CREATE TABLE IF NOT EXISTS liquidation_intents (
            intent_id TEXT PRIMARY KEY NOT NULL,
            liquidation_id TEXT UNIQUE,
            status INTEGER NOT NULL,
            request_json TEXT NOT NULL,
            receipt_json TEXT,
            last_error TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_liquidation_intents_status ON liquidation_intents(status);

        CREATE TABLE IF NOT EXISTS daemon_control_state (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
            updated_at BIGINT NOT NULL
        );
        INSERT OR IGNORE INTO daemon_control_state (singleton_id, paused, updated_at)
        VALUES (1, 0, CAST(strftime('%s','now') AS INTEGER));
        "#,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::{
        tokens::chain_token::ChainToken,
        types::protocol_types::{
            AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
            TxStatus,
        },
    };

    use crate::{
        executors::executor::ExecutorRequest,
        persistance::{LiquidationIntentStatus, LiquidationIntentStore, ResultStatus, WalStore},
        stages::executor::{ExecutionReceipt, ExecutionStatus},
    };

    use super::SqliteWalStore;

    fn principal(text: &str) -> Principal {
        Principal::from_text(text).expect("valid principal")
    }

    fn request() -> ExecutorRequest {
        let token = ChainToken::Icp {
            ledger: principal("ryjl3-tyaaa-aaaaa-aaaba-cai"),
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        };
        ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: principal("2vxsx-fae"),
                debt_pool_id: principal("hkmli-faaaa-aaaar-qb4ba-cai"),
                collateral_pool_id: principal("hnnn4-iyaaa-aaaar-qb4bq-cai"),
                debt_amount: Nat::from(100_000u64),
                receiver_address: principal("2vxsx-fae"),
                buy_bad_debt: false,
                collateral_share_receiver: None,
            },
            swap_args: None,
            debt_asset: token.clone(),
            collateral_asset: token,
            expected_profit: 10,
            ref_price: Nat::from(1u8),
            debt_ref_price: Nat::from(1u8),
            ref_price_at: 1,
            debt_approval_needed: false,
            min_collateral_amount: Nat::from(90_000u64),
        }
    }

    fn receipt(id: u128) -> ExecutionReceipt {
        ExecutionReceipt {
            request: request(),
            liquidation_result: Some(LiquidationResult {
                amounts: LiquidationAmounts {
                    collateral_received: Nat::from(100_000u64),
                    debt_repaid: Nat::from(90_000u64),
                },
                collateral_asset: AssetType::Unknown,
                debt_asset: AssetType::Unknown,
                status: LiquidationStatus::Success,
                timestamp: 1,
                change_tx: TxStatus {
                    tx_id: None,
                    status: TransferStatus::Success,
                },
                collateral_tx: TxStatus {
                    tx_id: None,
                    status: TransferStatus::Success,
                },
                id,
                collateral_share_receiver: None,
            }),
            status: ExecutionStatus::Success,
            change_received: true,
        }
    }

    fn swap_request() -> crate::swappers::model::SwapRequest {
        let token = request().collateral_asset;
        crate::swappers::model::SwapRequest {
            pay_asset: token.asset_id(),
            pay_amount: liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount::from_raw(
                token.clone(),
                Nat::from(100_000u64),
            ),
            receive_asset: token.asset_id(),
            receive_address: None,
            max_slippage_bps: None,
            venue_hint: None,
        }
    }

    async fn accepted_row(receipt: ExecutionReceipt) -> crate::persistance::LiqResultRecord {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let store = SqliteWalStore::new(temp.path().to_str().expect("database path")).expect("store");
        store.create_submitting("intent-1", &receipt.request).await.unwrap();
        store.mark_accepted("intent-1", "42", &receipt).await.unwrap();
        store.get_result("42").await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn accepted_intent_creates_one_execution_without_overwriting_it() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let store = SqliteWalStore::new(path).expect("store");

        store
            .create_submitting("intent-1", &request())
            .await
            .expect("create intent");
        store
            .mark_accepted("intent-1", "42", &receipt(42))
            .await
            .expect("accept intent");

        let intent = store
            .get_intent("intent-1")
            .await
            .expect("read intent")
            .expect("stored intent");
        assert_eq!(intent.status, LiquidationIntentStatus::Accepted);
        assert_eq!(intent.liquidation_id.as_deref(), Some("42"));

        let mut row = store.get_result("42").await.unwrap().unwrap();
        // No swap, but the finalize stage still records the outcome (audit L03).
        assert_eq!(row.status, ResultStatus::Enqueued);
        row.status = ResultStatus::InFlight;
        row.attempt = 3;
        row.meta_json = "checkpoint".into();
        store.upsert_result(row).await.unwrap();
        assert!(store.mark_accepted("intent-1", "42", &receipt(42)).await.is_err());
        let row = store.get_result("42").await.unwrap().unwrap();
        assert_eq!(row.meta_json, "checkpoint");
        assert_eq!(row.attempt, 3);

        // A conflicting execution insert rolls the acceptance update back too.
        store.create_submitting("intent-2", &request()).await.unwrap();
        assert!(store.mark_accepted("intent-2", "42", &receipt(42)).await.is_err());
        assert_eq!(store.get_intent("intent-2").await.unwrap().unwrap().status, LiquidationIntentStatus::Submitting);
    }

    /// One row for each case of each rule of `first_status`.
    #[test]
    fn first_status_follows_its_rules_in_order() {
        struct Case {
            name: &'static str,
            asked_for_shares: bool,
            share_receiver: bool,
            collateral_received: u64,
            swap: bool,
            transfer: TransferStatus,
            status: ResultStatus,
            note: bool,
        }
        let pending = || TransferStatus::Pending;
        let done = || TransferStatus::Success;
        #[rustfmt::skip]
        let cases = [
            Case { name: "rule 1: share payout with a swap", asked_for_shares: true, share_receiver: true, collateral_received: 100_000, swap: true, transfer: done(), status: ResultStatus::HeldAsShares, note: false },
            Case { name: "rule 1: share payout without a swap", asked_for_shares: true, share_receiver: true, collateral_received: 100_000, swap: false, transfer: done(), status: ResultStatus::HeldAsShares, note: false },
            Case { name: "rule 2: missing credited profile, swap follows", asked_for_shares: true, share_receiver: false, collateral_received: 100_000, swap: true, transfer: done(), status: ResultStatus::OperatorRequired, note: true },
            Case { name: "rule 2: missing credited profile, transfer not done", asked_for_shares: true, share_receiver: false, collateral_received: 100_000, swap: true, transfer: pending(), status: ResultStatus::OperatorRequired, note: true },
            Case { name: "missing credited profile without swap", asked_for_shares: true, share_receiver: false, collateral_received: 100_000, swap: false, transfer: done(), status: ResultStatus::OperatorRequired, note: true },
            Case { name: "rule 3: missing credited profile, no collateral", asked_for_shares: true, share_receiver: false, collateral_received: 0, swap: true, transfer: done(), status: ResultStatus::Enqueued, note: false },
            Case { name: "rule 3: token payout done", asked_for_shares: false, share_receiver: false, collateral_received: 100_000, swap: true, transfer: done(), status: ResultStatus::Enqueued, note: false },
            Case { name: "rule 4: token payout not done", asked_for_shares: false, share_receiver: false, collateral_received: 100_000, swap: true, transfer: pending(), status: ResultStatus::WaitingCollateral, note: false },
            Case { name: "rule 4: token payout failed", asked_for_shares: false, share_receiver: false, collateral_received: 100_000, swap: true, transfer: TransferStatus::Failed("x".to_string()), status: ResultStatus::WaitingCollateral, note: false },
        ];
        for case in cases {
            let mut receipt = receipt(42);
            receipt.request.liquidation.collateral_share_receiver =
                case.asked_for_shares.then_some(Principal::anonymous());
            receipt.request.swap_args = case.swap.then(swap_request);
            let liquidation = receipt.liquidation_result.as_mut().unwrap();
            liquidation.collateral_share_receiver = case.share_receiver.then(|| principal("2vxsx-fae"));
            liquidation.amounts.collateral_received = Nat::from(case.collateral_received);
            liquidation.collateral_tx.status = case.transfer;
            let liquidation = receipt.liquidation_result.clone().unwrap();

            let (status, note) = super::first_status(&receipt, &liquidation);
            assert_eq!(status, case.status, "{}", case.name);
            assert_eq!(note.is_some(), case.note, "{}", case.name);
        }
    }

    /// A share payout has `collateral_tx = Success` and no tokens. Thus the row must not go to the swap.
    #[tokio::test]
    async fn a_share_payout_starts_as_a_held_row_with_its_position() {
        let profile = principal("hnnn4-iyaaa-aaaar-qb4bq-cai");
        let mut receipt = receipt(42);
        receipt.request.liquidation.collateral_share_receiver = Some(profile);
        receipt.request.swap_args = Some(swap_request());
        receipt.liquidation_result.as_mut().unwrap().collateral_share_receiver = Some(profile);

        let row = accepted_row(receipt).await;
        assert_eq!(row.status, ResultStatus::HeldAsShares);
        let wrapper = crate::wal::decode_receipt_wrapper(&row).unwrap().unwrap();
        let holding = wrapper.share_holding.expect("share holding");
        assert_eq!(holding.profile, profile);
        assert_eq!(holding.pool, principal("hnnn4-iyaaa-aaaar-qb4bq-cai"));
        assert_eq!(holding.seized, Nat::from(100_000u64));
        assert_eq!(holding.remaining(), Nat::from(100_000u64));
        assert_eq!(holding.open, None);
    }

    /// Parks a nonzero payout with no credited profile for reconciliation.
    #[tokio::test]
    async fn a_missing_credited_profile_with_collateral_parks_the_row() {
        let mut receipt = receipt(42);
        receipt.request.liquidation.collateral_share_receiver = Some(Principal::anonymous());
        receipt.request.swap_args = Some(swap_request());

        let row = accepted_row(receipt).await;
        assert_eq!(row.status, ResultStatus::OperatorRequired);
        assert_eq!(row.last_error.as_deref(), Some(super::SHARE_PROFILE_MISMATCH));
        let wrapper = crate::wal::decode_receipt_wrapper(&row).unwrap().unwrap();
        assert!(wrapper.share_holding.is_none());
    }

    /// Finishes a share request with no seized collateral even without a credited profile.
    #[tokio::test]
    async fn a_share_request_without_seized_collateral_needs_no_transfer() {
        let mut receipt = receipt(42);
        receipt.request.liquidation.collateral_share_receiver = Some(Principal::anonymous());
        let result = receipt.liquidation_result.as_mut().unwrap();
        result.amounts.collateral_received = Nat::from(0u8);
        result.collateral_tx.status = TransferStatus::Pending;

        let row = accepted_row(receipt).await;
        assert_eq!(row.status, ResultStatus::Enqueued);
        assert_eq!(row.last_error, None);
    }

    #[tokio::test]
    async fn failed_and_recovered_intents_never_create_execution_rows() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let store = SqliteWalStore::new(path).expect("store");

        store
            .create_submitting("failed", &request())
            .await
            .expect("failed intent");
        store
            .mark_failed("failed", None, None, "canister rejected request")
            .await
            .expect("mark failed");
        store
            .create_submitting("stale", &request())
            .await
            .expect("stale intent");
        assert_eq!(
            store
                .recover_submitting_as_ambiguous("process restarted during submission")
                .await
                .expect("recover stale intents"),
            1
        );

        assert_eq!(
            store.get_intent("failed").await.unwrap().unwrap().status,
            LiquidationIntentStatus::Failed
        );
        assert_eq!(
            store.get_intent("stale").await.unwrap().unwrap().status,
            LiquidationIntentStatus::Ambiguous
        );
        assert!(store.list_recent(10).unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_execution_insert_rolls_back_acceptance() {
        use diesel::connection::SimpleConnection;
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let store = SqliteWalStore::new(path).unwrap();
        store.create_submitting("intent", &request()).await.unwrap();
        store.get_conn().unwrap().batch_execute(
            "CREATE TRIGGER refuse_execution BEFORE INSERT ON liquidation_results BEGIN SELECT RAISE(ABORT, 'test failure'); END;",
        ).unwrap();

        assert!(store.mark_accepted("intent", "42", &receipt(42)).await.is_err());
        assert_eq!(store.get_intent("intent").await.unwrap().unwrap().status, LiquidationIntentStatus::Submitting);
        store.mark_ambiguous("intent", "execution insert failed", Some(receipt(42))).await.unwrap();
        drop(store);
        let reopened = SqliteWalStore::new(path).unwrap();
        let intent = reopened.get_intent("intent").await.unwrap().unwrap();
        assert_eq!(intent.status, LiquidationIntentStatus::Ambiguous);
        assert_eq!(intent.liquidation_id.as_deref(), Some("42"));
        let saved: ExecutionReceipt = serde_json::from_str(intent.receipt_json.as_deref().unwrap()).unwrap();
        assert_eq!(saved.liquidation_result.unwrap().id, Nat::from(42u64));
        assert!(reopened.get_result("42").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_only_store_reads_but_cannot_change_intents_or_control_state() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let writer = SqliteWalStore::new(path).expect("writer");
        writer.create_submitting("intent", &request()).await.expect("intent");
        writer.set_daemon_paused(true).expect("pause state");

        let reader =
            SqliteWalStore::new_read_only_with_busy_timeout(path, 5_000).expect("read-only store");
        assert!(reader.get_intent("intent").await.unwrap().is_some());
        assert!(reader.daemon_paused().expect("read pause state"));
        assert!(reader.mark_ambiguous("intent", "must fail", None).await.is_err());
        assert!(reader.set_daemon_paused(false).is_err());
    }
}
