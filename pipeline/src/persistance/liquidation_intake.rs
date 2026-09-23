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
        WalProfitSnapshot, now_secs,
    },
    stages::executor::ExecutionReceipt,
};

use super::sqlite::SqliteWalStore;
use liquidium_pipeline_core::types::protocol_types::TransferStatus;

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
        let status = match liquidation.collateral_tx.status {
            TransferStatus::Success if receipt.request.swap_args.is_none() => ResultStatus::Succeeded,
            TransferStatus::Success => ResultStatus::Enqueued,
            TransferStatus::Pending | TransferStatus::Failed(_) => ResultStatus::WaitingCollateral,
        };
        let receipt_json = serde_json::to_string(receipt).context("encode accepted liquidation receipt")?;
        let now = now_secs();
        let meta_json = serde_json::to_string(&LiqMetaWrapper {
            receipt: receipt.clone(),
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: Some(WalProfitSnapshot {
                expected_profit_raw: receipt.request.expected_profit.to_string(),
                realized_profit_raw: None,
                debt_symbol: receipt.request.debt_asset.symbol().to_string(),
                debt_decimals: receipt.request.debt_asset.decimals(),
                updated_at: now,
            }),
            venue_execution: None,
            meta_v2: None,
        })?;
        let mut conn = self.get_conn()?;
        // Acceptance and runnable work commit together. No importer/cursor can
        // lose the handoff, and a duplicate receipt must never overwrite execution.
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            let updated = diesel::sql_query(
                "UPDATE liquidation_intents SET liquidation_id = ?, status = ?, receipt_json = ?, \
                 last_error = NULL, updated_at = ? WHERE intent_id = ? AND status = ?",
            )
            .bind::<Text, _>(liquidation_id)
            .bind::<Integer, _>(LiquidationIntentStatus::Accepted as i32)
            .bind::<Text, _>(&receipt_json)
            .bind::<BigInt, _>(now)
            .bind::<Text, _>(intent_id)
            .bind::<Integer, _>(LiquidationIntentStatus::Submitting as i32)
            .execute(conn)?;
            if updated != 1 {
                anyhow::bail!("intent {intent_id} is missing or no longer submitting");
            }
            diesel::sql_query(
                "INSERT INTO liquidation_results (liq_id, status, created_at, updated_at, meta_json) \
                 SELECT ?, ?, created_at, ?, ? FROM liquidation_intents WHERE intent_id = ?",
            )
                .bind::<Text, _>(liquidation_id)
                .bind::<Integer, _>(status as i32)
                .bind::<BigInt, _>(now)
                .bind::<Text, _>(&meta_json)
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
             last_error = ?, updated_at = ? WHERE intent_id = ? AND status = ?",
        )
        .bind::<Nullable<Text>, _>(liquidation_id)
        .bind::<Integer, _>(LiquidationIntentStatus::Failed as i32)
        .bind::<Nullable<Text>, _>(receipt_json)
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
            }),
            status: ExecutionStatus::Success,
            change_received: true,
        }
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
