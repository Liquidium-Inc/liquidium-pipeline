use super::schema::liquidation_results;
use diesel::prelude::{AsChangeset, Identifiable, Insertable, Queryable};
use serde::{Deserialize, Serialize};

#[derive(Queryable, Insertable, Identifiable, AsChangeset, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = liquidation_results, primary_key(liq_id, idx))]
pub struct LiquidationResultRow {
    pub liq_id: String,
    pub idx: i32,
    pub status: i32,
    pub attempt: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub meta_json: String,
}