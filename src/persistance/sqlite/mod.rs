use anyhow::Result;
use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection, prelude::*, r2d2::{ConnectionManager, Pool}
};

mod models;
mod schema;

use crate::persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs};

use self::models::LiquidationResultRow as Row;
use self::schema::liquidation_results as tbl;

pub struct SqliteWalStore {
    pool: Pool<ConnectionManager<SqliteConnection>>,
}

impl SqliteWalStore {
    pub fn new(path: &str) -> Result<Self> {
        let manager = ConnectionManager::<SqliteConnection>::new(path);
        let pool = Pool::builder().max_size(2).build(manager)?;
        let mut conn = pool.get()?;
        initialize_schema(&mut conn)?;
        apply_pragmas(&mut conn)?;
        Ok(Self { pool })
    }

    fn to_row(r: &LiqResultRecord) -> Row {
        Row {
            liq_id: r.liq_id.clone(),
            idx: r.idx,
            status: r.status as i32,
            attempt: r.attempt,
            created_at: r.created_at,
            updated_at: r.updated_at,
            meta_json: r.meta_json.clone(),
        }
    }

    fn from_row(r: Row) -> LiqResultRecord {
        LiqResultRecord {
            liq_id: r.liq_id,
            idx: r.idx,
            status: match r.status {
                0 => ResultStatus::Enqueued,
                1 => ResultStatus::InFlight,
                2 => ResultStatus::Succeeded,
                3 => ResultStatus::FailedRetryable,
                _ => ResultStatus::FailedPermanent,
            },
            attempt: r.attempt,
            created_at: r.created_at,
            updated_at: r.updated_at,
            meta_json: r.meta_json,
        }
    }
}

#[async_trait]
impl WalStore for SqliteWalStore {
    async fn upsert_result(&self, row: LiqResultRecord) -> Result<()> {
        let mut conn = self.pool.get()?;
        diesel::insert_into(tbl::table)
            .values(&Self::to_row(&row))
            .on_conflict((tbl::liq_id, tbl::idx))
            .do_update()
            .set((
                tbl::status.eq(row.status as i32),
                tbl::attempt.eq(row.attempt),
                tbl::updated_at.eq(row.updated_at),
                tbl::meta_json.eq(row.meta_json.clone()),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    async fn get_result(&self, liq_id: &str, idx: i32) -> Result<Option<LiqResultRecord>> {
        let mut conn = self.pool.get()?;
        let res = tbl::table
            .find((liq_id.to_string(), idx))
            .first::<Row>(&mut conn)
            .optional()?;
        Ok(res.map(Self::from_row))
    }

    async fn list_by_status(&self, status: ResultStatus, limit: usize) -> Result<Vec<LiqResultRecord>> {
        let mut conn = self.pool.get()?;
        let rows = tbl::table
            .filter(tbl::status.eq(status as i32))
            .order(tbl::created_at.asc())
            .limit(limit as i64)
            .load::<Row>(&mut conn)?;
        Ok(rows.into_iter().map(Self::from_row).collect())
    }

    async fn update_status(&self, liq_id: &str, idx: i32, next: ResultStatus, bump_attempt: bool) -> Result<()> {
        let mut conn = self.pool.get()?;
        let now = now_secs();
        let attempt_delta = if bump_attempt { 1 } else { 0 };
        let current: Option<i32> = tbl::table
            .find((liq_id.to_string(), idx))
            .select(tbl::attempt)
            .first::<i32>(&mut conn)
            .optional()?;
        let new_attempt = current.unwrap_or(0) + attempt_delta;

        diesel::update(tbl::table.find((liq_id.to_string(), idx)))
            .set((
                tbl::status.eq(next as i32),
                tbl::attempt.eq(new_attempt),
                tbl::updated_at.eq(now),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    async fn delete(&self, liq_id: &str, idx: i32) -> anyhow::Result<()> {
        let mut conn = self.pool.get()?;
        diesel::delete(tbl::table.find((liq_id.to_string(), idx))).execute(&mut conn)?;
        Ok(())
    }
}

pub fn initialize_schema(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        r#"
        CREATE TABLE IF NOT EXISTS liquidation_results (
            liq_id TEXT NOT NULL,
            idx INTEGER NOT NULL,
            status INTEGER NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            meta_json TEXT NOT NULL,
            PRIMARY KEY (liq_id, idx)
        );
        CREATE INDEX IF NOT EXISTS idx_liq_status ON liquidation_results(status);
    "#,
    )?;
    Ok(())
}

pub fn apply_pragmas(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        r#"
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        PRAGMA fullfsync=ON;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=1000;
        PRAGMA mmap_size=0;
        PRAGMA auto_vacuum=FULL;
        PRAGMA secure_delete=ON;
        PRAGMA busy_timeout=5000;
    "#,
    )?;
    Ok(())
}
