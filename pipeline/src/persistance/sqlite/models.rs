use super::schema::liquidation_results;
use diesel::prelude::{AsChangeset, Identifiable, Insertable, Queryable};
use serde::{Deserialize, Serialize};

#[derive(Queryable, Insertable, Identifiable, AsChangeset, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = liquidation_results, primary_key(liq_id))]
pub struct LiquidationResultRow {
    pub liq_id: String,
    pub status: i32,
    pub attempt: i32,
    pub error_count: i32,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub meta_json: String,
}
