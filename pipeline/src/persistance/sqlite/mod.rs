use anyhow::Result;
use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};

mod models;
mod schema;

use crate::persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs};

use self::models::LiquidationResultRow as Row;
use self::schema::liquidation_results as tbl;

pub struct SqliteWalStore {
    pool: Pool<ConnectionManager<SqliteConnection>>,
    busy_timeout_ms: i64,
}

impl SqliteWalStore {
    pub fn new(path: &str) -> Result<Self> {
        Self::new_with_busy_timeout(path, 5_000)
    }

    pub fn new_with_busy_timeout(path: &str, busy_timeout_ms: i64) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(path);
        let pool = Pool::builder().max_size(2).build(manager)?;
        let mut conn = pool.get()?;
        initialize_schema(&mut conn)?;
        apply_pragmas(&mut conn, busy_timeout_ms)?;
        Ok(Self {
            pool,
            busy_timeout_ms,
        })
    }

    fn get_conn(&self) -> Result<r2d2::PooledConnection<ConnectionManager<SqliteConnection>>> {
        let mut conn = self.pool.get()?;
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
}

#[async_trait]
impl WalStore for SqliteWalStore {
    async fn get_pending(&self, limit: usize) -> Result<Vec<LiqResultRecord>> {
        let mut conn = self.get_conn()?;
        let now = now_secs();
        let inflight_stale_cutoff = now.saturating_sub(300);

        // Requeue stale in-flight rows so they can be retried.
        diesel::update(
            tbl::table.filter(
                tbl::status
                    .eq(ResultStatus::InFlight as i32)
                    .and(tbl::updated_at.le(inflight_stale_cutoff)),
            ),
        )
        .set((
            tbl::status.eq(ResultStatus::Enqueued as i32),
            tbl::updated_at.eq(now),
        ))
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
        let mut conn = self.get_conn()?;
        diesel::delete(tbl::table.find(liq_id.to_string())).execute(&mut conn)?;
        Ok(())
    }
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
    "#,
    )?;
    Ok(())
}

pub fn apply_pragmas(conn: &mut SqliteConnection, busy_timeout_ms: i64) -> Result<()> {
    conn.batch_execute(
        &format!(
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
        ),
    )?;
    Ok(())
}
