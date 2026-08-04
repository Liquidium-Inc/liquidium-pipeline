use anyhow::{Context as _, Result};
use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    dsl::count_star,
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sql_types::{BigInt, Integer, Text},
};
use std::collections::HashMap;

mod models;
mod schema;

use crate::persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs};

use self::models::LiquidationResultRow as Row;
use self::schema::liquidation_results as tbl;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatabaseRole {
    ExecutionWal,
    LiquidationIntake,
}

impl DatabaseRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionWal => "execution_wal",
            Self::LiquidationIntake => "liquidation_intake",
        }
    }
}

pub struct SqliteWalStore {
    pool: Pool<ConnectionManager<SqliteConnection>>,
    busy_timeout_ms: i64,
    read_only: bool,
    db_path: String,
}

impl SqliteWalStore {
    pub fn new(path: &str) -> Result<Self> {
        Self::new_with_busy_timeout(path, 5_000)
    }

    pub fn new_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(path);
        let pool = Pool::builder()
            .max_size(2)
            .build(manager)
            .with_context(|| format!("open sqlite pool (mode=rw path={path})"))?;
        let mut conn = pool
            .get()
            .with_context(|| format!("open sqlite connection (mode=rw path={path})"))?;
        initialize_role_and_schema(&mut conn, DatabaseRole::ExecutionWal, initialize_schema)
            .with_context(|| format!("initialize sqlite schema (path={path})"))?;
        apply_pragmas(&mut conn, busy_timeout_ms)
            .with_context(|| format!("apply sqlite pragmas (mode=rw path={path})"))?;
        Ok(Self {
            pool,
            busy_timeout_ms,
            read_only: false,
            db_path: path.to_string(),
        })
    }

    pub fn new_read_only_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(read_only_dsn(path));
        let pool = Pool::builder()
            .max_size(2)
            .build(manager)
            .with_context(|| format!("open sqlite pool (mode=ro path={path})"))?;
        let mut conn = pool
            .get()
            .with_context(|| format!("open sqlite connection (mode=ro path={path})"))?;
        apply_read_only_pragmas(&mut conn, busy_timeout_ms)
            .with_context(|| format!("apply sqlite pragmas (mode=ro path={path})"))?;
        validate_database_role(&mut conn, DatabaseRole::ExecutionWal)
            .with_context(|| format!("validate sqlite database role (mode=ro path={path})"))?;
        Ok(Self {
            pool,
            busy_timeout_ms,
            read_only: true,
            db_path: path.to_string(),
        })
    }

    fn get_conn(&self) -> Result<r2d2::PooledConnection<ConnectionManager<SqliteConnection>>> {
        let mode = if self.read_only { "ro" } else { "rw" };
        let mut conn = self
            .pool
            .get()
            .with_context(|| format!("open sqlite connection (mode={mode} path={})", self.db_path))?;
        if self.read_only {
            apply_read_only_pragmas(&mut conn, self.busy_timeout_ms)
                .with_context(|| format!("apply sqlite pragmas (mode=ro path={})", self.db_path))?;
        } else {
            apply_pragmas(&mut conn, self.busy_timeout_ms)
                .with_context(|| format!("apply sqlite pragmas (mode=rw path={})", self.db_path))?;
        }
        Ok(conn)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            anyhow::bail!("wal store opened read-only");
        }
        Ok(())
    }

    fn to_row(r: &LiqResultRecord) -> Row {
        Row {
            liq_id: r.id.clone(),
            status: r.status as i32,
            attempt: r.attempt,
            error_count: r.error_count,
            last_error: r.last_error.clone(),
            created_at: r.created_at,
            updated_at: r.updated_at,
            meta_json: r.meta_json.clone(),
        }
    }

    fn from_row(r: Row) -> LiqResultRecord {
        LiqResultRecord {
            id: r.liq_id,
            status: r.status.into(),
            attempt: r.attempt,
            error_count: r.error_count,
            last_error: r.last_error,
            created_at: r.created_at,
            updated_at: r.updated_at,
            meta_json: r.meta_json,
        }
    }

    pub fn status_counts(&self) -> Result<HashMap<ResultStatus, i64>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(i32, i64)> = tbl::table
            .group_by(tbl::status)
            .select((tbl::status, count_star()))
            .load(&mut conn)?;

        let mut out: HashMap<ResultStatus, i64> = HashMap::new();
        for (status, count) in rows {
            *out.entry(ResultStatus::from(status)).or_insert(0) += count;
        }
        Ok(out)
    }

    pub fn list_recent(&self, limit: usize) -> Result<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let rows = tbl::table
            .order(tbl::updated_at.desc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    /// Returns rows whose committed venue work may still require execution or
    /// reconciliation. Startup inspects these rows and parks incompatible ones
    /// as `Unresumable` before normal polling begins.
    pub fn list_unfinished_execution_rows(&self) -> Result<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let statuses = [
            ResultStatus::Enqueued as i32,
            ResultStatus::InFlight as i32,
            ResultStatus::FailedRetryable as i32,
            ResultStatus::OperatorRequired as i32,
        ];
        let rows = tbl::table
            .filter(tbl::status.eq_any(statuses))
            .order(tbl::created_at.asc())
            .load::<Row>(&mut conn)?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    pub fn set_daemon_paused(&self, paused: bool) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        diesel::sql_query(
            r#"
            INSERT INTO daemon_control_state (singleton_id, paused, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(singleton_id) DO UPDATE SET
                paused = excluded.paused,
                updated_at = excluded.updated_at
        "#,
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
        match diesel::sql_query("SELECT paused FROM daemon_control_state WHERE singleton_id = 1 LIMIT 1")
            .get_result::<StateRow>(&mut conn)
            .optional()
        {
            Ok(Some(row)) => Ok(row.paused != 0),
            Ok(None) => Ok(false),
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("no such table: daemon_control_state") {
                    Ok(false)
                } else {
                    Err(err.into())
                }
            }
        }
    }
}

#[async_trait]
impl WalStore for SqliteWalStore {
    async fn get_pending(&self, limit: usize) -> Result<Vec<LiqResultRecord>> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let inflight_stale_cutoff = now.saturating_sub(240);

        // Requeue stale in-flight rows so they can be retried.
        diesel::update(
            tbl::table.filter(
                tbl::status
                    .eq(ResultStatus::InFlight as i32)
                    .and(tbl::updated_at.le(inflight_stale_cutoff)),
            ),
        )
        .set((tbl::status.eq(ResultStatus::Enqueued as i32), tbl::updated_at.eq(now)))
        .execute(&mut conn)?;

        let rows = tbl::table
            .filter(
                tbl::status
                    .eq(ResultStatus::Enqueued as i32)
                    .or(tbl::status.eq(ResultStatus::FailedRetryable as i32)),
            )
            .order(tbl::created_at.asc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    async fn upsert_result(&self, row: LiqResultRecord) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        diesel::insert_into(tbl::table)
            .values(&Self::to_row(&row))
            .on_conflict(tbl::liq_id)
            .do_update()
            .set((
                tbl::status.eq(row.status as i32),
                tbl::attempt.eq(row.attempt),
                tbl::error_count.eq(row.error_count),
                tbl::last_error.eq(row.last_error.clone()),
                tbl::updated_at.eq(row.updated_at),
                tbl::meta_json.eq(row.meta_json.clone()),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> Result<Option<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let res = tbl::table.find(liq_id.to_string()).first::<Row>(&mut conn).optional()?;
        Ok(res.map(Self::from_row))
    }

    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> Result<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let rows = tbl::table
            .filter(tbl::status.eq(status as i32))
            .order(tbl::created_at.asc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    async fn update_status(&self, liq_id: &str, next: ResultStatus, bump_attempt: bool) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let attempt_delta = if bump_attempt { 1 } else { 0 };
        let current: Option<i32> = tbl::table
            .find(liq_id.to_string())
            .select(tbl::attempt)
            .first::<i32>(&mut conn)
            .optional()?;
        let new_attempt = current.unwrap_or(0) + attempt_delta;

        diesel::update(tbl::table.find(liq_id.to_string()))
            .set((
                tbl::status.eq(next as i32),
                tbl::attempt.eq(new_attempt),
                tbl::updated_at.eq(now),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    async fn update_failure(
        &self,
        liq_id: &str,
        next: ResultStatus,
        last_error: String,
        bump_attempt: bool,
    ) -> Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let attempt_delta = if bump_attempt { 1 } else { 0 };
        let current: Option<(i32, i32)> = tbl::table
            .find(liq_id.to_string())
            .select((tbl::attempt, tbl::error_count))
            .first::<(i32, i32)>(&mut conn)
            .optional()?;
        let (cur_attempt, cur_error) = current.unwrap_or((0, 0));
        let new_attempt = cur_attempt + attempt_delta;
        let new_error_count = cur_error + 1;

        diesel::update(tbl::table.find(liq_id.to_string()))
            .set((
                tbl::status.eq(next as i32),
                tbl::attempt.eq(new_attempt),
                tbl::error_count.eq(new_error_count),
                tbl::last_error.eq(Some(last_error)),
                tbl::updated_at.eq(now),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    async fn delete(&self, liq_id: &str) -> anyhow::Result<()> {
        self.ensure_writable()?;
        let mut conn = self.get_conn()?;
        diesel::delete(tbl::table.find(liq_id.to_string())).execute(&mut conn)?;
        Ok(())
    }
}

pub(crate) fn initialize_role_and_schema(
    conn: &mut SqliteConnection,
    expected_role: DatabaseRole,
    initialize_schema: fn(&mut SqliteConnection) -> Result<()>,
) -> Result<()> {
    if metadata_table_exists(conn)? {
        return validate_database_role(conn, expected_role);
    }

    let user_tables = user_table_names(conn)?;
    if !user_tables.is_empty() {
        anyhow::bail!(
            "database is untagged and already contains schema ({}); move or remove it before startup",
            user_tables.join(", ")
        );
    }

    conn.transaction::<_, anyhow::Error, _>(|conn| {
        conn.batch_execute(
            "CREATE TABLE database_metadata (metadata_key TEXT PRIMARY KEY NOT NULL, metadata_value TEXT NOT NULL);",
        )?;
        diesel::sql_query("INSERT INTO database_metadata (metadata_key, metadata_value) VALUES ('role', ?)")
            .bind::<Text, _>(expected_role.as_str())
            .execute(conn)?;
        initialize_schema(conn)?;
        Ok(())
    })
}

pub(crate) fn validate_database_role(conn: &mut SqliteConnection, expected_role: DatabaseRole) -> Result<()> {
    #[derive(QueryableByName)]
    struct RoleRow {
        #[diesel(sql_type = Text)]
        metadata_value: String,
    }

    if !metadata_table_exists(conn)? {
        anyhow::bail!("database is missing role metadata; move or remove this legacy database before startup");
    }
    let role = diesel::sql_query("SELECT metadata_value FROM database_metadata WHERE metadata_key = 'role' LIMIT 1")
        .get_result::<RoleRow>(conn)
        .optional()?
        .ok_or_else(|| anyhow::anyhow!("database role metadata is missing the role value"))?;
    if role.metadata_value != expected_role.as_str() {
        anyhow::bail!(
            "database role mismatch: expected {}, found {}",
            expected_role.as_str(),
            role.metadata_value
        );
    }
    Ok(())
}

fn metadata_table_exists(conn: &mut SqliteConnection) -> Result<bool> {
    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }
    let row = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = 'database_metadata'",
    )
    .get_result::<CountRow>(conn)?;
    Ok(row.count != 0)
}

fn user_table_names(conn: &mut SqliteConnection) -> Result<Vec<String>> {
    #[derive(QueryableByName)]
    struct TableRow {
        #[diesel(sql_type = Text)]
        name: String,
    }
    Ok(diesel::sql_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .load::<TableRow>(conn)?
    .into_iter()
    .map(|row| row.name)
    .collect())
}

pub fn initialize_schema(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        r#"
        CREATE TABLE IF NOT EXISTS liquidation_results (
            liq_id TEXT NOT NULL,
            status INTEGER NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (liq_id)
        );
        CREATE INDEX IF NOT EXISTS idx_liq_status ON liquidation_results(status);

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

pub fn apply_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> Result<()> {
    conn.batch_execute(&format!(
        r#"
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        PRAGMA fullfsync=ON;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=1000;
        PRAGMA mmap_size=0;
        PRAGMA auto_vacuum=FULL;
        PRAGMA secure_delete=ON;
        PRAGMA busy_timeout={};
    "#,
        busy_timeout_ms
    ))?;
    Ok(())
}

fn read_only_dsn(path: &str) -> String {
    format!("file:{}?mode=ro", path)
}

fn apply_read_only_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> Result<()> {
    conn.batch_execute(&format!(
        r#"
        PRAGMA query_only=ON;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=1000;
        PRAGMA mmap_size=0;
        PRAGMA busy_timeout={};
    "#,
        busy_timeout_ms
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use diesel::{Connection, connection::SimpleConnection};

    use crate::persistance::liquidation_intake::SqliteLiquidationIntentStore;
    use crate::persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs};

    use super::SqliteWalStore;

    #[test]
    fn read_only_store_can_read_counts_and_rows() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();

        let writer = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("writer");
        let now = now_secs();
        let row = LiqResultRecord {
            id: "liq-1".to_string(),
            status: ResultStatus::Enqueued,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
            meta_json: "{}".to_string(),
        };
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer.upsert_result(row).await.expect("upsert");
        });

        let reader = SqliteWalStore::new_read_only_with_busy_timeout(&path, 5_000).expect("reader");
        let counts = reader.status_counts().expect("counts");
        assert_eq!(*counts.get(&ResultStatus::Enqueued).unwrap_or(&0), 1);
        let rows = reader.list_recent(10).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "liq-1");
    }

    #[test]
    fn read_only_store_blocks_writes() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let writer = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("writer");
        let rt = tokio::runtime::Runtime::new().expect("runtime");

        // Ensure schema exists.
        rt.block_on(async {
            writer
                .upsert_result(LiqResultRecord {
                    id: "liq-1".to_string(),
                    status: ResultStatus::Enqueued,
                    attempt: 0,
                    error_count: 0,
                    last_error: None,
                    created_at: now_secs(),
                    updated_at: now_secs(),
                    meta_json: "{}".to_string(),
                })
                .await
                .expect("seed");
        });

        let reader = Arc::new(SqliteWalStore::new_read_only_with_busy_timeout(&path, 5_000).expect("reader"));
        let err = rt
            .block_on(async { reader.delete("liq-1").await })
            .expect_err("write should fail");
        assert!(err.to_string().contains("read-only"));
    }

    #[test]
    fn daemon_pause_state_roundtrip_in_writable_and_read_only_store() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();

        let writer = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("writer");
        assert!(!writer.daemon_paused().expect("default state"));
        writer.set_daemon_paused(true).expect("set paused");
        assert!(writer.daemon_paused().expect("paused state"));

        let reader = SqliteWalStore::new_read_only_with_busy_timeout(&path, 5_000).expect("reader");
        assert!(reader.daemon_paused().expect("read-only read paused state"));
    }

    #[test]
    fn read_only_store_blocks_daemon_state_writes() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();

        let writer = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("writer");
        writer.set_daemon_paused(false).expect("seed state");

        let reader = SqliteWalStore::new_read_only_with_busy_timeout(&path, 5_000).expect("reader");
        let err = reader.set_daemon_paused(true).expect_err("write should fail");
        assert!(err.to_string().contains("read-only"));
    }

    #[test]
    fn operator_required_rows_are_persisted_but_not_returned_as_pending() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let store = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("store");
        let now = now_secs();
        let make_row = |id: &str, status| LiqResultRecord {
            id: id.to_string(),
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
            meta_json: "{}".to_string(),
        };
        let rt = tokio::runtime::Runtime::new().expect("runtime");

        rt.block_on(async {
            store
                .upsert_result(make_row("runnable", ResultStatus::Enqueued))
                .await
                .expect("seed runnable row");
            store
                .upsert_result(make_row("parked", ResultStatus::OperatorRequired))
                .await
                .expect("seed operator-required row");

            let pending = store.get_pending(10).await.expect("pending rows");
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].id, "runnable");

            let parked = store
                .list_by_status(ResultStatus::OperatorRequired, 10)
                .await
                .expect("operator-required rows");
            assert_eq!(parked.len(), 1);
            assert_eq!(parked[0].id, "parked");
            assert_eq!(parked[0].status, ResultStatus::OperatorRequired);
        });
    }

    #[test]
    fn unfinished_execution_scan_includes_only_resumable_or_operator_rows() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let store = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("store");
        let now = now_secs();
        let row = |id: &str, status| LiqResultRecord {
            id: id.to_string(),
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
            meta_json: "{}".to_string(),
        };
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            for (id, status) in [
                ("enqueued", ResultStatus::Enqueued),
                ("in-flight", ResultStatus::InFlight),
                ("retryable", ResultStatus::FailedRetryable),
                ("operator", ResultStatus::OperatorRequired),
                ("unresumable", ResultStatus::Unresumable),
                ("waiting", ResultStatus::WaitingCollateral),
                ("succeeded", ResultStatus::Succeeded),
                ("permanent", ResultStatus::FailedPermanent),
            ] {
                store.upsert_result(row(id, status)).await.expect("seed row");
            }
        });

        let ids = store
            .list_unfinished_execution_rows()
            .expect("unfinished rows")
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["enqueued", "in-flight", "retryable", "operator"]);
    }

    #[test]
    fn unresumable_rows_round_trip_but_are_never_pending() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let store = SqliteWalStore::new_with_busy_timeout(&path, 5_000).expect("store");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            store
                .upsert_result(LiqResultRecord {
                    id: "unresumable".to_string(),
                    status: ResultStatus::Unresumable,
                    attempt: 0,
                    error_count: 1,
                    last_error: Some("disabled venue".to_string()),
                    created_at: now_secs(),
                    updated_at: now_secs(),
                    meta_json: "{}".to_string(),
                })
                .await
                .expect("seed row");

            let stored = store.get_result("unresumable").await.expect("read").expect("row");
            assert_eq!(stored.status, ResultStatus::Unresumable);
            assert_eq!(stored.last_error.as_deref(), Some("disabled venue"));
            assert!(store.get_pending(10).await.expect("pending").is_empty());
        });
    }

    #[test]
    fn rejects_untagged_legacy_database() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let mut conn = diesel::SqliteConnection::establish(&path).expect("legacy connection");
        conn.batch_execute("CREATE TABLE legacy_rows (id INTEGER PRIMARY KEY);")
            .expect("legacy schema");
        drop(conn);

        let error = SqliteWalStore::new(&path).err().expect("legacy database must fail");
        assert!(format!("{error:#}").contains("untagged"), "unexpected error: {error:#}");
    }

    #[test]
    fn rejects_database_tagged_for_the_other_role() {
        let temp = tempfile::NamedTempFile::new().expect("tmp db");
        let path = temp.path().display().to_string();
        let _intake = SqliteLiquidationIntentStore::new(&path).expect("intake database");

        let error = SqliteWalStore::new(&path).err().expect("wrong database role must fail");
        assert!(
            format!("{error:#}").contains("role mismatch"),
            "unexpected error: {error:#}"
        );
    }
}
