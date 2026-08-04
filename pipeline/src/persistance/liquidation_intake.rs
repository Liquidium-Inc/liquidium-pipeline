use anyhow::{Context as _, Result};
use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sql_types::{BigInt, Integer, Nullable, Text},
};

use crate::{
    executors::executor::ExecutorRequest,
    persistance::{
        LiquidationHandoff, LiquidationIntentRecord, LiquidationIntentStatus, LiquidationIntentStore, now_secs,
    },
    stages::executor::ExecutionReceipt,
};

use super::sqlite::{DatabaseRole, initialize_role_and_schema, validate_database_role};

pub struct SqliteLiquidationIntentStore {
    pool: Pool<ConnectionManager<SqliteConnection>>,
    busy_timeout_ms: i64,
    read_only: bool,
    db_path: String,
}

impl SqliteLiquidationIntentStore {
    pub fn new(path: &str) -> Result<Self> {
        Self::new_with_busy_timeout(path, 5_000)
    }

    pub fn new_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(path);
        let pool = Pool::builder()
            .max_size(2)
            .build(manager)
            .with_context(|| format!("open liquidation intake pool (mode=rw path={path})"))?;
        let mut conn = pool
            .get()
            .with_context(|| format!("open liquidation intake connection (mode=rw path={path})"))?;
        initialize_role_and_schema(&mut conn, DatabaseRole::LiquidationIntake, initialize_schema)
            .with_context(|| format!("initialize liquidation intake schema (path={path})"))?;
        apply_pragmas(&mut conn, busy_timeout_ms)?;
        Ok(Self {
            pool,
            busy_timeout_ms,
            read_only: false,
            db_path: path.to_string(),
        })
    }

    pub fn new_read_only_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(format!("file:{path}?mode=ro"));
        let pool = Pool::builder()
            .max_size(2)
            .build(manager)
            .with_context(|| format!("open liquidation intake pool (mode=ro path={path})"))?;
        let mut conn = pool
            .get()
            .with_context(|| format!("open liquidation intake connection (mode=ro path={path})"))?;
        apply_read_only_pragmas(&mut conn, busy_timeout_ms)?;
        validate_database_role(&mut conn, DatabaseRole::LiquidationIntake)
            .with_context(|| format!("validate liquidation intake database role (path={path})"))?;
        Ok(Self {
            pool,
            busy_timeout_ms,
            read_only: true,
            db_path: path.to_string(),
        })
    }

    fn get_conn(&self) -> Result<r2d2::PooledConnection<ConnectionManager<SqliteConnection>>> {
        let mut conn = self
            .pool
            .get()
            .with_context(|| format!("open liquidation intake connection (path={})", self.db_path))?;
        if self.read_only {
            apply_read_only_pragmas(&mut conn, self.busy_timeout_ms)?;
        } else {
            apply_pragmas(&mut conn, self.busy_timeout_ms)?;
        }
        Ok(conn)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            anyhow::bail!("liquidation intent store opened read-only");
        }
        Ok(())
    }

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

#[derive(QueryableByName)]
struct HandoffSqlRow {
    #[diesel(sql_type = BigInt)]
    sequence: i64,
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

#[async_trait]
impl LiquidationIntentStore for SqliteLiquidationIntentStore {
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
        let receipt_json = serde_json::to_string(receipt).context("encode accepted liquidation receipt")?;
        let now = now_secs();
        let mut conn = self.get_conn()?;
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
            diesel::sql_query("INSERT INTO liquidation_handoffs (intent_id) VALUES (?)")
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

    async fn mark_ambiguous(&self, intent_id: &str, error: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        let updated = diesel::sql_query(
            "UPDATE liquidation_intents SET status = ?, last_error = ?, updated_at = ? \
             WHERE intent_id = ? AND status = ?",
        )
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

    async fn list_handoffs_after(&self, sequence: i64, limit: usize) -> Result<Vec<LiquidationHandoff>> {
        let mut conn = self.get_conn()?;
        let rows = diesel::sql_query(
            "SELECT h.sequence, i.intent_id, i.liquidation_id, i.status, i.request_json, \
             i.receipt_json, i.last_error, i.created_at, i.updated_at \
             FROM liquidation_handoffs h JOIN liquidation_intents i ON i.intent_id = h.intent_id \
             WHERE h.sequence > ? ORDER BY h.sequence ASC LIMIT ?",
        )
        .bind::<BigInt, _>(sequence)
        .bind::<BigInt, _>(limit as i64)
        .load::<HandoffSqlRow>(&mut conn)?;
        rows.into_iter()
            .map(|row| {
                let sequence = row.sequence;
                let intent = IntentSqlRow {
                    intent_id: row.intent_id,
                    liquidation_id: row.liquidation_id,
                    status: row.status,
                    request_json: row.request_json,
                    receipt_json: row.receipt_json,
                    last_error: row.last_error,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                }
                .try_into()?;
                Ok(LiquidationHandoff { sequence, intent })
            })
            .collect()
    }
}

fn initialize_schema(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        r#"
        CREATE TABLE liquidation_intents (
            intent_id TEXT PRIMARY KEY NOT NULL,
            liquidation_id TEXT UNIQUE,
            status INTEGER NOT NULL,
            request_json TEXT NOT NULL,
            receipt_json TEXT,
            last_error TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        );
        CREATE INDEX idx_liquidation_intents_status ON liquidation_intents(status);

        CREATE TABLE liquidation_handoffs (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id TEXT NOT NULL UNIQUE REFERENCES liquidation_intents(intent_id)
        );

        CREATE TABLE daemon_control_state (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
            updated_at BIGINT NOT NULL
        );
        INSERT INTO daemon_control_state (singleton_id, paused, updated_at)
        VALUES (1, 0, CAST(strftime('%s','now') AS INTEGER));
        "#,
    )?;
    Ok(())
}

fn apply_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> Result<()> {
    conn.batch_execute(&format!(
        "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; \
         PRAGMA temp_store=FILE; PRAGMA busy_timeout={busy_timeout_ms};"
    ))?;
    Ok(())
}

fn apply_read_only_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> Result<()> {
    conn.batch_execute(&format!(
        "PRAGMA query_only=ON; PRAGMA temp_store=FILE; PRAGMA busy_timeout={busy_timeout_ms};"
    ))?;
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
        persistance::{LiquidationIntentStatus, LiquidationIntentStore},
        stages::executor::{ExecutionReceipt, ExecutionStatus},
    };

    use super::SqliteLiquidationIntentStore;

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
    async fn accepted_intent_creates_one_ordered_handoff() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let store = SqliteLiquidationIntentStore::new(path).expect("intake store");

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

        let handoffs = store.list_handoffs_after(0, 10).await.expect("handoffs");
        assert_eq!(handoffs.len(), 1);
        assert_eq!(handoffs[0].sequence, 1);
        assert_eq!(handoffs[0].intent.intent_id, "intent-1");
        assert!(store.list_handoffs_after(1, 10).await.expect("next page").is_empty());
    }

    #[tokio::test]
    async fn failed_and_recovered_intents_never_create_handoffs() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let store = SqliteLiquidationIntentStore::new(path).expect("intake store");

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
        assert!(store.list_handoffs_after(0, 10).await.expect("handoffs").is_empty());
    }

    #[tokio::test]
    async fn read_only_store_reads_but_cannot_change_intents_or_control_state() {
        let temp = tempfile::NamedTempFile::new().expect("temporary database");
        let path = temp.path().to_str().expect("database path");
        let writer = SqliteLiquidationIntentStore::new(path).expect("writer");
        writer.create_submitting("intent", &request()).await.expect("intent");
        writer.set_daemon_paused(true).expect("pause state");

        let reader =
            SqliteLiquidationIntentStore::new_read_only_with_busy_timeout(path, 5_000).expect("read-only store");
        assert!(reader.get_intent("intent").await.unwrap().is_some());
        assert!(reader.daemon_paused().expect("read pause state"));
        assert!(reader.mark_ambiguous("intent", "must fail").await.is_err());
        assert!(reader.set_daemon_paused(false).is_err());
    }
}
