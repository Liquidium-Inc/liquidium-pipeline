//! Bridge provider calls as outbound sends. A provider such as the ckETH
//! minter gives a call no id of ours, so a lost answer is settled by searching
//! the provider history for the call's shape after a cursor taken before it.
use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BRIDGE_SUBMISSION_DEFERRED_PREFIX, BridgeBackend, BridgeRequest, FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
    resolve_route,
};
use liquidium_pipeline_connectors::outbound::{decode_payload, encode_payload};
use liquidium_pipeline_core::outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome};
use serde::{Deserialize, Serialize};

use crate::{bridge::claims::BridgeClaimLookup, finalizers::bridge_planner::deposit_destination};

pub const NAME: &str = "bridge-call";

/// The IC drops an unexecuted ingress after about five minutes, but a call
/// that started can still finish later, so the wait is a grace period and not
/// a proof.
const LOST_CALL_GRACE_SECS: i64 = 10 * 60;
/// Rows written this long before the call may hold a handle after its cursor.
const CLAIM_SCAN_MARGIN_SECS: i64 = 60 * 60;

/// One bridge call, saved before it runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeCall {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    /// The provider destination as text, for example an EVM address.
    pub destination: String,
    pub amount: f64,
    #[serde(default)]
    pub provider_fee_budget_native_units: Option<Nat>,
    /// Provider history position taken before the call. `None` when the
    /// provider cannot look calls up.
    pub cursor: Option<u64>,
    pub started_at_ts: i64,
}

impl BridgeCall {
    pub fn request(&self) -> Result<BridgeRequest, String> {
        let route = resolve_route(&self.asset, &self.source_chain, &self.target_asset).ok_or_else(|| {
            format!(
                "bridge route not found for {}@{} -> {}",
                self.asset, self.source_chain, self.target_asset
            )
        })?;
        Ok(BridgeRequest {
            asset: self.asset.clone(),
            source_chain: self.source_chain.clone(),
            source_address: self.source_address.clone(),
            target_asset: self.target_asset.clone(),
            destination: deposit_destination(route.destination_kind, &self.destination)?,
            amount: self.amount,
            provider_fee_budget_native_units: self.provider_fee_budget_native_units.clone(),
        })
    }

    /// True when this call could have produced the provider record at
    /// `sequence` that `other` matched: the same shape, made after its cursor.
    pub fn may_own(&self, other: &BridgeCall, sequence: u64) -> bool {
        self.asset.eq_ignore_ascii_case(&other.asset)
            && self.destination.eq_ignore_ascii_case(&other.destination)
            && (self.amount - other.amount).abs() <= 1e-9
            && self.cursor.is_none_or(|cursor| cursor < sequence)
    }
}

/// A call that some row saved and has not resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenBridgeCall {
    pub id: String,
    pub call: BridgeCall,
}

/// The saved form of a call.
pub fn prepare_bridge_call(id: String, call: &BridgeCall) -> Result<PreparedSend, String> {
    Ok(PreparedSend {
        sender: NAME.to_string(),
        identity: id,
        payload: encode_payload(call)?,
        // The provider has no id of ours, so a second call is a second
        // withdrawal of the same funds.
        replay_safe: false,
    })
}

pub fn saved_call(prepared: &PreparedSend) -> Result<BridgeCall, String> {
    decode_payload(&prepared.payload)
}

/// Sends bridge calls and settles them from the provider history.
pub struct BridgeCallSender {
    backend: Arc<dyn BridgeBackend>,
    claims: Arc<dyn BridgeClaimLookup>,
}

impl BridgeCallSender {
    pub fn new(backend: Arc<dyn BridgeBackend>, claims: Arc<dyn BridgeClaimLookup>) -> Self {
        Self { backend, claims }
    }

    /// Exactly one record that no other row holds and no other open call
    /// could own is this call's handle.
    async fn look_up(&self, prepared: &PreparedSend, call: &BridgeCall) -> Result<SendOutcome, String> {
        let Some(cursor) = call.cursor else {
            return Ok(SendOutcome::Open {
                reason: "the provider cannot look up lost calls".to_string(),
            });
        };
        let claims = self
            .claims
            .claims_since(call.started_at_ts - CLAIM_SCAN_MARGIN_SECS)
            .await
            .map_err(|error| format!("claim registry unavailable: {error}"))?;
        let candidates = self
            .backend
            .find_submissions(&call.request()?, cursor, &claims.bridge_ids)
            .await?;
        let contested = candidates.iter().any(|candidate| {
            claims
                .open_calls
                .iter()
                .filter(|other| other.id != prepared.identity)
                .any(|other| other.call.may_own(call, candidate.sequence))
        });
        Ok(match candidates.as_slice() {
            [only] if !contested => SendOutcome::Landed {
                receipt: only.bridge_id.clone(),
            },
            [] => SendOutcome::Open {
                reason: format!(
                    "no provider record of {} {} to {} yet",
                    call.amount, call.asset, call.destination
                ),
            },
            _ => SendOutcome::Open {
                reason: format!(
                    "{} unclaimed provider records match {:?}{}",
                    candidates.len(),
                    candidates
                        .iter()
                        .map(|candidate| candidate.bridge_id.as_str())
                        .collect::<Vec<_>>(),
                    if contested {
                        " and another open call could own one"
                    } else {
                        ""
                    }
                ),
            },
        })
    }
}

#[async_trait]
impl OutboundSender for BridgeCallSender {
    async fn prepare(&self, _intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Err("a bridge call is prepared from the leg state".to_string())
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        let request = match saved_call(prepared).and_then(|call| call.request()) {
            Ok(request) => request,
            Err(reason) => return SendOutcome::NeverLanded { reason },
        };
        match self.backend.submit_bridge(request).await {
            Ok(submission) => SendOutcome::Landed {
                receipt: submission.bridge_id,
            },
            // Refused before any provider call: a busy wallet or a floor.
            Err(reason)
                if reason.starts_with(BRIDGE_SUBMISSION_DEFERRED_PREFIX)
                    || reason.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX) =>
            {
                SendOutcome::NeverLanded { reason }
            }
            // Any other failure may follow a call that ran, so the provider
            // history decides it.
            Err(reason) => SendOutcome::Open { reason },
        }
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        let call = match saved_call(prepared) {
            Ok(call) => call,
            Err(reason) => return SendOutcome::Open { reason },
        };
        self.look_up(prepared, &call)
            .await
            .unwrap_or_else(|reason| SendOutcome::Open { reason })
    }

    fn open_window_secs(&self, prepared: &PreparedSend) -> i64 {
        // Without a cursor nothing can be looked up, so waiting adds nothing.
        match saved_call(prepared) {
            Ok(call) if call.cursor.is_some() => LOST_CALL_GRACE_SECS,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::claims::{BridgeClaims, MockBridgeClaimLookup};
    use liquidium_pipeline_connectors::backend::bridge_backend::{
        BridgeSubmission, BridgeSubmissionCandidate, MockBridgeBackend,
    };

    const DESTINATION: &str = "0x1111111111111111111111111111111111111111";

    fn call(cursor: Option<u64>) -> BridgeCall {
        BridgeCall {
            asset: "ckUSDC".into(),
            source_chain: "ICP".into(),
            source_address: "bridge-owner".into(),
            target_asset: "USDC".into(),
            destination: DESTINATION.into(),
            amount: 0.92,
            provider_fee_budget_native_units: None,
            cursor,
            started_at_ts: 1_000,
        }
    }

    fn open(id: &str, cursor: Option<u64>) -> OpenBridgeCall {
        OpenBridgeCall {
            id: id.into(),
            call: call(cursor),
        }
    }

    fn candidate(sequence: u64) -> BridgeSubmissionCandidate {
        BridgeSubmissionCandidate {
            bridge_id: format!("ic-withdraw:recovered:{sequence}"),
            sequence,
        }
    }

    fn claims(open_calls: Vec<OpenBridgeCall>) -> MockBridgeClaimLookup {
        let mut claims = MockBridgeClaimLookup::new();
        claims.expect_claims_since().returning(move |since| {
            assert_eq!(since, 1_000 - CLAIM_SCAN_MARGIN_SECS);
            Ok(BridgeClaims {
                bridge_ids: vec!["ic-withdraw:1:2".into()],
                open_calls: open_calls.clone(),
                ..BridgeClaims::default()
            })
        });
        claims
    }

    fn sender(backend: MockBridgeBackend, claims: MockBridgeClaimLookup) -> BridgeCallSender {
        BridgeCallSender::new(Arc::new(backend), Arc::new(claims))
    }

    fn mine(cursor: Option<u64>) -> PreparedSend {
        prepare_bridge_call("mine".into(), &call(cursor)).unwrap()
    }

    #[test]
    fn a_call_owns_a_record_only_with_the_same_shape_made_before_it() {
        let mine = call(Some(100));
        assert!(call(Some(90)).may_own(&mine, 105));
        assert!(call(None).may_own(&mine, 105));
        assert!(!call(Some(105)).may_own(&mine, 105));
        let mut other = call(Some(90));
        other.destination = DESTINATION.to_uppercase().replace("0X", "0x");
        other.asset = "CKUSDC".into();
        assert!(other.may_own(&mine, 105));
        other.amount = 0.92 + 1e-6;
        assert!(!other.may_own(&mine, 105));
    }

    #[tokio::test]
    async fn only_a_refusal_before_any_call_counts_as_never_landed() {
        let cases = [
            (
                Ok(BridgeSubmission {
                    bridge_id: "ic-withdraw:7:8".into(),
                }),
                "landed",
            ),
            (
                Err(format!("{BRIDGE_SUBMISSION_DEFERRED_PREFIX}: wallet busy")),
                "never",
            ),
            (
                Err(format!("{FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX}: below minimum")),
                "never",
            ),
            (Err("minter call timed out".to_string()), "open"),
        ];
        for (answer, expected) in cases {
            let mut backend = MockBridgeBackend::new();
            backend.expect_submit_bridge().times(1).return_once(move |request| {
                assert!((request.amount - 0.92).abs() < 1e-12);
                answer
            });
            let outcome = sender(backend, MockBridgeClaimLookup::new())
                .submit(&mine(Some(100)))
                .await;
            let kind = match outcome {
                SendOutcome::Landed { .. } => "landed",
                SendOutcome::NeverLanded { .. } | SendOutcome::BelowMinimum { .. } => "never",
                SendOutcome::Open { .. } => "open",
            };
            assert_eq!(kind, expected);
        }
    }

    #[tokio::test]
    async fn the_history_settles_a_call_only_from_one_uncontested_record() {
        let cases: Vec<(Vec<u64>, Vec<OpenBridgeCall>, bool)> = vec![
            (vec![105], vec![], true),
            // The row's own call is in the registry, and a later call cannot own record 105.
            (
                vec![105],
                vec![open("mine", Some(100)), open("theirs", Some(110))],
                true,
            ),
            (vec![], vec![], false),
            (vec![105, 106], vec![], false),
            (vec![105], vec![open("theirs", Some(90))], false),
            (vec![105], vec![open("theirs", None)], false),
        ];
        for (records, open_calls, lands) in cases {
            let mut backend = MockBridgeBackend::new();
            backend.expect_submit_bridge().times(0);
            backend
                .expect_find_submissions()
                .times(1)
                .returning(move |request, cursor, claimed| {
                    assert_eq!(request.asset, "ckUSDC");
                    assert_eq!(cursor, 100);
                    assert_eq!(claimed, ["ic-withdraw:1:2"]);
                    Ok(records.iter().map(|sequence| candidate(*sequence)).collect())
                });
            let outcome = sender(backend, claims(open_calls)).settle(&mine(Some(100))).await;
            match (outcome, lands) {
                (SendOutcome::Landed { receipt }, true) => assert_eq!(receipt, "ic-withdraw:recovered:105"),
                (SendOutcome::Open { .. }, false) => {}
                (outcome, lands) => panic!("unexpected {outcome:?} (lands={lands})"),
            }
        }
    }

    #[tokio::test]
    async fn without_a_cursor_or_a_registry_the_call_stays_open() {
        let mut backend = MockBridgeBackend::new();
        backend.expect_find_submissions().times(0);
        let no_cursor = sender(backend, MockBridgeClaimLookup::new());
        assert!(matches!(no_cursor.settle(&mine(None)).await, SendOutcome::Open { .. }));
        assert_eq!(no_cursor.open_window_secs(&mine(None)), 0, "waiting adds nothing");
        assert_eq!(no_cursor.open_window_secs(&mine(Some(100))), LOST_CALL_GRACE_SECS);

        let mut backend = MockBridgeBackend::new();
        backend.expect_find_submissions().times(0);
        let mut registry = MockBridgeClaimLookup::new();
        registry
            .expect_claims_since()
            .returning(|_| Err("bridge claim scan truncated at 20000 rows".into()));
        let outcome = sender(backend, registry).settle(&mine(Some(100))).await;
        let SendOutcome::Open { reason } = outcome else {
            panic!("no partial answers");
        };
        assert!(reason.contains("truncated"), "{reason}");
    }
}
