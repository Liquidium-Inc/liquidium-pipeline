use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    dsl::count_star,
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use std::collections::HashMap;

use crate::error::{AppError, AppResult, error_codes};

mod models;
mod schema;

use crate::persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs};

use self::models::LiquidationResultRow as Row;
use self::schema::liquidation_results as tbl;

pub struct SqliteWalStore {
    pool: Pool<ConnectionManager<SqliteConnection>>,
    busy_timeout_ms: i64,
}

fn persistence_error(context: impl Into<String>) -> AppError {
    AppError::from_def(error_codes::PERSISTENCE_ERROR).with_context(context)
}

fn with_persistence_context(context: &str, err: impl std::fmt::Display) -> AppError {
    persistence_error(format!("{}: {}", context, err))
}

fn serialize_wal_error(err: &AppError) -> String {
    serde_json::to_string(err).unwrap_or_else(|_| err.to_string())
}

impl SqliteWalStore {
    pub fn new(path: &str) -> AppResult<Self> {
        Self::new_with_busy_timeout(path, 5_000)
    }

    pub fn new_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> AppResult<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(path);
        let pool = Pool::builder()
            .max_size(2)
            .build(manager)
            .map_err(|e| with_persistence_context("failed to build sqlite pool", e))?;
        let mut conn = pool
            .get()
            .map_err(|e| with_persistence_context("failed to get sqlite connection", e))?;
        initialize_schema(&mut conn)?;
        apply_pragmas(&mut conn, busy_timeout_ms)?;
        Ok(Self { pool, busy_timeout_ms })
    }

    fn get_conn(&self) -> AppResult<r2d2::PooledConnection<ConnectionManager<SqliteConnection>>> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| with_persistence_context("failed to get sqlite connection", e))?;
        apply_pragmas(&mut conn, self.busy_timeout_ms)?;
        Ok(conn)
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
            status: match r.status {
                0 => ResultStatus::Enqueued,
                1 => ResultStatus::InFlight,
                2 => ResultStatus::Succeeded,
                3 => ResultStatus::FailedRetryable,
                4 => ResultStatus::FailedPermanent,
                5 => ResultStatus::WaitingCollateral,
                6 => ResultStatus::WaitingProfit,
                _ => ResultStatus::FailedPermanent,
            },
            attempt: r.attempt,
            error_count: r.error_count,
            last_error: r.last_error,
            created_at: r.created_at,
            updated_at: r.updated_at,
            meta_json: r.meta_json,
        }
    }

    pub fn status_counts(&self) -> AppResult<HashMap<ResultStatus, i64>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<(i32, i64)> = tbl::table
            .group_by(tbl::status)
            .select((tbl::status, count_star()))
            .load(&mut conn)
            .map_err(|e| with_persistence_context("failed to load status counts", e))?;

        let mut out: HashMap<ResultStatus, i64> = HashMap::new();
        for (status, count) in rows {
            let status = match status {
                0 => ResultStatus::Enqueued,
                1 => ResultStatus::InFlight,
                2 => ResultStatus::Succeeded,
                3 => ResultStatus::FailedRetryable,
                4 => ResultStatus::FailedPermanent,
                5 => ResultStatus::WaitingCollateral,
                6 => ResultStatus::WaitingProfit,
                _ => ResultStatus::FailedPermanent,
            };
            *out.entry(status).or_insert(0) += count;
        }
        Ok(out)
    }

    pub fn list_recent(&self, limit: usize) -> AppResult<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let rows = tbl::table
            .order(tbl::updated_at.desc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)
            .map_err(|e| with_persistence_context("failed to load recent rows", e))?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }
}

#[async_trait]
impl WalStore for SqliteWalStore {
    async fn get_pending(&self, limit: usize) -> AppResult<Vec<LiqResultRecord>> {
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
        .execute(&mut conn)
        .map_err(|e| with_persistence_context("failed to requeue stale in-flight rows", e))?;

        let rows = tbl::table
            .filter(
                tbl::status
                    .eq(ResultStatus::Enqueued as i32)
                    .or(tbl::status.eq(ResultStatus::FailedRetryable as i32)),
            )
            .order(tbl::created_at.asc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)
            .map_err(|e| with_persistence_context("failed to load pending rows", e))?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    async fn upsert_result(&self, row: LiqResultRecord) -> AppResult<()> {
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
            .execute(&mut conn)
            .map_err(|e| with_persistence_context("failed to upsert wal row", e))?;
        Ok(())
    }

    async fn get_result(&self, liq_id: &str) -> AppResult<Option<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let res = tbl::table
            .find(liq_id.to_string())
            .first::<Row>(&mut conn)
            .optional()
            .map_err(|e| with_persistence_context("failed to fetch wal row", e))?;
        Ok(res.map(Self::from_row))
    }

    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> AppResult<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let rows = tbl::table
            .filter(tbl::status.eq(status as i32))
            .order(tbl::created_at.asc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)
            .map_err(|e| with_persistence_context("failed to list wal rows by status", e))?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    async fn update_status(&self, liq_id: &str, next: ResultStatus, bump_attempt: bool) -> AppResult<()> {
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let attempt_delta = if bump_attempt { 1 } else { 0 };
        let current: Option<i32> = tbl::table
            .find(liq_id.to_string())
            .select(tbl::attempt)
            .first::<i32>(&mut conn)
            .optional()
            .map_err(|e| with_persistence_context("failed to read wal attempt", e))?;
        let new_attempt = current.unwrap_or(0) + attempt_delta;

        diesel::update(tbl::table.find(liq_id.to_string()))
            .set((
                tbl::status.eq(next as i32),
                tbl::attempt.eq(new_attempt),
                tbl::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .map_err(|e| with_persistence_context("failed to update wal status", e))?;
        Ok(())
    }

    async fn update_failure(
        &self,
        liq_id: &str,
        next: ResultStatus,
        last_error: AppError,
        bump_attempt: bool,
    ) -> AppResult<()> {
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let attempt_delta = if bump_attempt { 1 } else { 0 };
        let current: Option<(i32, i32)> = tbl::table
            .find(liq_id.to_string())
            .select((tbl::attempt, tbl::error_count))
            .first::<(i32, i32)>(&mut conn)
            .optional()
            .map_err(|e| with_persistence_context("failed to read wal failure counters", e))?;
        let (cur_attempt, cur_error) = current.unwrap_or((0, 0));
        let new_attempt = cur_attempt + attempt_delta;
        let new_error_count = cur_error + 1;

        diesel::update(tbl::table.find(liq_id.to_string()))
            .set((
                tbl::status.eq(next as i32),
                tbl::attempt.eq(new_attempt),
                tbl::error_count.eq(new_error_count),
                tbl::last_error.eq(Some(serialize_wal_error(&last_error))),
                tbl::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .map_err(|e| with_persistence_context("failed to update wal failure state", e))?;
        Ok(())
    }

    async fn delete(&self, liq_id: &str) -> AppResult<()> {
        let mut conn = self.get_conn()?;
        diesel::delete(tbl::table.find(liq_id.to_string()))
            .execute(&mut conn)
            .map_err(|e| with_persistence_context("failed to delete wal row", e))?;
        Ok(())
    }
}

pub fn initialize_schema(conn: &mut SqliteConnection) -> AppResult<()> {
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
    "#,
    )
    .map_err(|e| with_persistence_context("failed to initialize sqlite schema", e))?;
    Ok(())
}

pub fn apply_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> AppResult<()> {
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
    ))
    .map_err(|e| with_persistence_context("failed to apply sqlite pragmas", e))?;
    Ok(())
}
