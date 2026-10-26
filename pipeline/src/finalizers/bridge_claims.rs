//! Bridge handles and open attempts held by other liquidation rows. The bridge
//! account is shared, so a lost submission is attached only when no other row
//! can own the same provider record.
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::{
    finalizers::cex_finalizer::{BridgeSubmitAttempt, BridgeSubmitPhase},
    persistance::{LiqResultRecord, ResultStatus, WalStore},
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BridgeClaims {
    /// Every bridge handle another row holds, on either leg side.
    pub bridge_ids: Vec<String>,
    /// Attempts other rows persisted and have not resolved.
    pub open_attempts: Vec<BridgeSubmitAttempt>,
    /// Deposit sends that rows started and the provider has not minted.
    pub open_deposit_sends: Vec<OpenDepositSend>,
}

/// A send to a provider deposit address whose mint has not arrived. The
/// address is shared, so a second send would merge with this deposit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenDepositSend {
    pub liq_id: String,
    pub asset: String,
    pub network: String,
}

/// Reads an open deposit send from a saved leg state.
fn open_deposit_send(fields: &Map<String, Value>) -> Option<OpenDepositSend> {
    let present = |key: &str| fields.get(key).is_some_and(|value| !value.is_null());
    let started = present("withdraw_bridge_funding") || present("withdraw_bridge_funding_txid");
    if !started || present("withdraw_bridge_mint_receipt") {
        return None;
    }
    let text = |key: &str| fields.get(key).and_then(Value::as_str).unwrap_or_default().to_string();
    Some(OpenDepositSend {
        liq_id: text("liq_id"),
        asset: text("withdraw_planned_asset"),
        network: text("withdraw_planned_network"),
    })
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BridgeClaimLookup: Send + Sync {
    /// Claims of every row that is still open, plus rows written since
    /// `since_ts`. The caller's own attempt is in the result; it filters it by id.
    async fn claims_since(&self, since_ts: i64) -> Result<BridgeClaims, String>;
}

/// Rows in these statuses may still hold an unresolved attempt however old they are.
const OPEN_STATUSES: [ResultStatus; 7] = [
    ResultStatus::Enqueued,
    ResultStatus::InFlight,
    ResultStatus::FailedRetryable,
    ResultStatus::WaitingCollateral,
    ResultStatus::WaitingProfit,
    ResultStatus::OperatorRequired,
    ResultStatus::Unresumable,
];
/// A scan that fills its limit may have left an owner out, so it is an error.
const SCAN_LIMIT: usize = 20_000;

pub struct WalBridgeClaims {
    wal: Arc<dyn WalStore>,
}

impl WalBridgeClaims {
    pub fn new(wal: Arc<dyn WalStore>) -> Self {
        Self { wal }
    }

    fn collect(row: &LiqResultRecord, claims: &mut BridgeClaims) {
        let Ok(value) = serde_json::from_str::<Value>(&row.meta_json) else {
            return;
        };
        visit(&value, claims);
        // Rows from the single-venue finalizer keep their state as JSON bytes.
        if let Some(bytes) = value.get("meta").and_then(Value::as_array) {
            let bytes: Vec<u8> = bytes.iter().filter_map(Value::as_u64).map(|b| b as u8).collect();
            if let Ok(legacy) = serde_json::from_slice::<Value>(&bytes) {
                visit(&legacy, claims);
            }
        }
    }
}

fn visit(value: &Value, claims: &mut BridgeClaims) {
    match value {
        Value::Object(fields) => {
            if let Some(send) = open_deposit_send(fields) {
                claims.open_deposit_sends.push(send);
            }
            for (key, child) in fields {
                if key.ends_with("_bridge_id") {
                    if let Some(id) = child.as_str() {
                        claims.bridge_ids.push(id.to_string());
                    }
                } else if key == "deposit_bridge_submit_phase" {
                    if let Ok(BridgeSubmitPhase::Submitting(attempt)) = serde_json::from_value(child.clone()) {
                        claims.open_attempts.push(attempt);
                    }
                } else {
                    visit(child, claims);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| visit(item, claims)),
        _ => {}
    }
}

#[async_trait]
impl BridgeClaimLookup for WalBridgeClaims {
    async fn claims_since(&self, since_ts: i64) -> Result<BridgeClaims, String> {
        let mut rows = Vec::new();
        for status in OPEN_STATUSES {
            let page = self
                .wal
                .list_by_status(status, SCAN_LIMIT)
                .await
                .map_err(|e| format!("bridge claim scan failed for {status:?}: {e}"))?;
            if page.len() >= SCAN_LIMIT {
                return Err(format!(
                    "bridge claim scan truncated at {SCAN_LIMIT} rows for {status:?}"
                ));
            }
            rows.extend(page);
        }
        let recent = self
            .wal
            .list_updated_since(since_ts, SCAN_LIMIT)
            .await
            .map_err(|e| format!("bridge claim scan failed since {since_ts}: {e}"))?;
        if recent.len() >= SCAN_LIMIT {
            return Err(format!(
                "bridge claim scan truncated at {SCAN_LIMIT} rows since {since_ts}"
            ));
        }
        rows.extend(recent);

        let mut claims = BridgeClaims::default();
        let mut seen = HashSet::new();
        for row in rows {
            if !seen.insert(row.id.clone()) {
                continue;
            }
            Self::collect(&row, &mut claims);
        }
        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistance::MockWalStore;

    fn row(id: &str, meta_json: &str) -> LiqResultRecord {
        LiqResultRecord {
            id: id.into(),
            status: ResultStatus::InFlight,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 0,
            updated_at: 0,
            meta_json: meta_json.into(),
        }
    }

    fn attempt_json(amount: f64) -> String {
        format!(
            r#"{{"Submitting":{{"id":"a1","asset":"ckUSDC","destination":"0xabc","amount":{amount},"cursor":100,"started_at_ts":5,"called_at_ts":null}}}}"#
        )
    }

    #[tokio::test]
    async fn claims_cover_both_leg_sides_legacy_bytes_and_open_attempts_once_per_row() {
        let own = row(
            "1720",
            r#"{"venue_execution":{"state":{"deposit_bridge_id":"ic-withdraw:1:2"}}}"#,
        );
        let multi_venue = row(
            "1721",
            &format!(
                r#"{{"meta_v2":{{"legs":[{{"execution":{{"state":{{"deposit_bridge_id":"ic-withdraw:3:4","withdraw_bridge_id":"0xdead","deposit_bridge_submit_phase":{}}}}}}},{{"execution":{{"state":{{"deposit_bridge_submit_phase":"WaitingFunds"}}}}}}]}}}}"#,
                attempt_json(1.5)
            ),
        );
        let legacy_bytes = serde_json::to_string(&serde_json::json!({
            "meta": br#"{"deposit_bridge_id":"ic-withdraw-eth:9"}"#.to_vec(),
        }))
        .unwrap();
        let legacy = row("1700", &legacy_bytes);
        let broken = row("1699", "not json");

        let mut wal = MockWalStore::new();
        let open_rows = vec![own.clone(), multi_venue.clone(), broken];
        wal.expect_list_by_status()
            .times(OPEN_STATUSES.len())
            .returning(move |status, _| {
                Ok(if status == ResultStatus::InFlight {
                    open_rows.clone()
                } else {
                    vec![]
                })
            });
        let recent = vec![multi_venue.clone(), legacy.clone()];
        wal.expect_list_updated_since().times(1).returning(move |since, _| {
            assert_eq!(since, 42);
            Ok(recent.clone())
        });

        let claims = WalBridgeClaims::new(Arc::new(wal)).claims_since(42).await.unwrap();

        assert_eq!(
            claims.bridge_ids,
            vec!["ic-withdraw:1:2", "ic-withdraw:3:4", "0xdead", "ic-withdraw-eth:9"]
        );
        assert_eq!(claims.open_attempts.len(), 1);
        assert_eq!(claims.open_attempts[0].id, "a1");
        assert_eq!(claims.open_attempts[0].amount, 1.5);
        assert_eq!(claims.open_attempts[0].cursor, Some(100));
    }

    #[tokio::test]
    async fn claims_list_deposit_sends_until_their_mint_arrives() {
        let leg = |liq_id: &str, fields: &str| {
            format!(
                r#"{{"meta_v2":{{"legs":[{{"execution":{{"state":{{"liq_id":"{liq_id}","withdraw_planned_asset":"SOL","withdraw_planned_network":"SOL",{fields}}}}}}}]}}}}"#
            )
        };
        let rows = vec![
            row("1", &leg("saved", r#""withdraw_bridge_funding":{"phase":"Prepared"}"#)),
            row("2", &leg("landed", r#""withdraw_bridge_funding_txid":"sig","withdraw_bridge_mint_receipt":null"#)),
            row("3", &leg("minted", r#""withdraw_bridge_funding_txid":"sig","withdraw_bridge_mint_receipt":{"amount":1}"#)),
            row("4", &leg("none", r#""withdraw_bridge_funding":null,"withdraw_bridge_funding_txid":null"#)),
        ];
        let mut wal = MockWalStore::new();
        wal.expect_list_by_status().returning(move |status, _| {
            Ok(if status == ResultStatus::InFlight { rows.clone() } else { vec![] })
        });
        wal.expect_list_updated_since().returning(|_, _| Ok(vec![]));

        let claims = WalBridgeClaims::new(Arc::new(wal)).claims_since(0).await.unwrap();

        let open: Vec<&str> = claims.open_deposit_sends.iter().map(|send| send.liq_id.as_str()).collect();
        assert_eq!(open, vec!["saved", "landed"]);
        assert_eq!(claims.open_deposit_sends[0].asset, "SOL");
        assert_eq!(claims.open_deposit_sends[0].network, "SOL");
    }

    #[tokio::test]
    async fn a_scan_that_fills_its_limit_is_an_error_not_a_partial_answer() {
        for truncate_open in [true, false] {
            let mut wal = MockWalStore::new();
            let full: Vec<LiqResultRecord> = (0..SCAN_LIMIT).map(|i| row(&i.to_string(), "{}")).collect();
            let open_page = if truncate_open { full.clone() } else { vec![] };
            wal.expect_list_by_status().returning(move |_, _| Ok(open_page.clone()));
            let recent_page = if truncate_open { vec![] } else { full.clone() };
            wal.expect_list_updated_since()
                .returning(move |_, _| Ok(recent_page.clone()));

            let error = WalBridgeClaims::new(Arc::new(wal)).claims_since(0).await.unwrap_err();

            assert!(error.contains("truncated"), "{error}");
        }
    }
}
