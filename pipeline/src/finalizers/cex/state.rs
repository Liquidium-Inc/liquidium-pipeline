use candid::Nat;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::types::side_cost::SideCost;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CexStep {
    Deposit,
    DepositPending,
    Trade,
    TradePending,
    Withdraw,
    WithdrawPending,
    Completed,
    Failed,
}

/// One executed CEX slice for observability and export analytics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexTradeSlice {
    pub leg_index: u32,
    pub market: String,
    pub side: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub mid_price: f64,
    pub exec_price: f64,
    pub slippage_bps: f64,
}

/// Persisted resolved route leg for deterministic trade resumes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexRouteLeg {
    pub market: String,
    pub side: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexDepositState {
    pub deposit_asset: ChainToken,
    pub deposit_txid: Option<String>,
    pub deposit_balance_before: Option<f64>,
    #[serde(default)]
    pub deposit_sent_at_ts: Option<i64>,
    #[serde(default)]
    pub approval_bump_count: Option<u32>,
    /// The funding transfer, saved before it is sent. Retries and restarts send
    /// this exact transfer again instead of measuring and sizing a new one.
    #[serde(default)]
    pub deposit_funding: Option<crate::outbound::driver::OutboundRecord>,
    #[serde(flatten)]
    pub bridge: CexDepositBridgeState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CexDepositBridgeState {
    /// The provider call that bridges the deposit, saved before it runs.
    #[serde(default)]
    pub deposit_bridge_call: Option<crate::outbound::driver::OutboundRecord>,
    /// From rows saved before the outbound driver. It stays until the bridge
    /// step upgrades it, so no save between can drop the call.
    #[serde(default, rename = "deposit_bridge_submit_phase", skip_serializing_if = "Option::is_none")]
    pub legacy_bridge_submit_phase: Option<crate::bridge::legacy::LegacyBridgeSubmitPhase>,
    /// True on rows saved before the outbound driver, whose build called the
    /// provider without a record. The bridge step clears it.
    #[serde(default = "crate::bridge::legacy::saved_before_driver")]
    pub deposit_bridge_call_unrecorded: bool,
    #[serde(default)]
    pub deposit_planned_asset: Option<String>,
    #[serde(default)]
    pub deposit_planned_network: Option<String>,
    #[serde(default)]
    pub deposit_bridge_required: bool,
    #[serde(default)]
    pub deposit_bridge_id: Option<String>,
    #[serde(default)]
    pub deposit_bridge_submitted_at_ts: Option<i64>,
    #[serde(default)]
    pub deposit_bridge_polled_at_ts: Option<i64>,
    #[serde(default)]
    pub deposit_bridge_destination_snapshot: Option<String>,
    #[serde(default)]
    pub deposit_bridge_submit_amount: Option<f64>,
    #[serde(default)]
    pub deposit_bridge_expected_amount: Option<f64>,
    #[serde(default)]
    pub deposit_bridge_provider_fee_budget_native_units: Option<Nat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexTradeState {
    /// One-based index of the current trade leg being executed.
    /// `None` means no leg has started yet.
    pub trade_leg_index: Option<u32>,
    /// Total number of legs in the resolved route.
    pub trade_leg_total: Option<u32>,
    /// Persisted resolved route used to keep retries deterministic.
    #[serde(default)]
    pub trade_resolved_legs: Vec<CexRouteLeg>,
    /// Market used by the most recently attempted leg/slice.
    pub trade_last_market: Option<String>,
    /// Side (`buy`/`sell`) used by the most recently attempted leg/slice.
    pub trade_last_side: Option<String>,
    /// Input amount requested/consumed for the most recent leg update.
    pub trade_last_amount_in: Option<f64>,
    /// Output amount produced by the most recent leg update.
    pub trade_last_amount_out: Option<f64>,
    /// Amount to carry into the next leg after a completed leg.
    pub trade_next_amount_in: Option<f64>,
    /// Adverse slippage of the whole route against the preview mids, in bps.
    pub trade_weighted_slippage_bps: Option<f64>,
    /// Recorded slice-level executions for telemetry.
    #[serde(default)]
    pub trade_slices: Vec<CexTradeSlice>,
    /// Whether residual was skipped as dust (< min execution USD).
    #[serde(default)]
    pub trade_dust_skipped: bool,
    /// Residual notional skipped as dust, in USD.
    #[serde(default)]
    pub trade_dust_usd: Option<f64>,
    /// Remaining input for the current leg persisted for retry-safe resume.
    #[serde(default)]
    pub trade_progress_remaining_in: Option<f64>,
    /// Accumulated output for the current leg persisted for retry-safe resume.
    #[serde(default)]
    pub trade_progress_total_out: Option<f64>,
    /// Deterministic client order id for the in-flight slice, if any.
    #[serde(default)]
    pub trade_pending_client_order_id: Option<String>,
    /// Market of the in-flight slice.
    #[serde(default)]
    pub trade_pending_market: Option<String>,
    /// Side of the in-flight slice.
    #[serde(default)]
    pub trade_pending_side: Option<String>,
    /// Requested input of the in-flight slice.
    #[serde(default)]
    pub trade_pending_requested_in: Option<f64>,
    /// Buy mode selected for the in-flight slice / next buy hint.
    #[serde(default)]
    pub trade_pending_buy_mode: Option<String>,
    /// Count of inverse buy retries executed in the current leg.
    #[serde(default)]
    pub trade_inverse_retry_count: u32,
    /// Residual input that could not be executed due to constraints.
    #[serde(default)]
    pub trade_unexecutable_residual_in: Option<f64>,
    /// When the venue first reported this leg's funds as not tradable yet.
    ///
    /// Set on the first pending-settlement rejection and cleared by the next
    /// error-free step, so the wait is measured from the venue's first refusal
    /// rather than restarted by every retry.
    #[serde(default)]
    pub trade_settlement_waiting_since_ts: Option<i64>,
    /// Client order ids whose fills are already in the leg totals.
    ///
    /// MEXC answers a reused id with the original fill, so a reused id would
    /// count that fill again. An id in this list is never submitted again.
    #[serde(default)]
    pub trade_applied_client_order_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexWithdrawState {
    pub withdraw_asset: ChainToken,
    pub withdraw_address: String,
    pub withdraw_id: Option<String>,
    pub withdraw_txid: Option<String>,
    /// When the exchange accepted a direct withdrawal that had no transaction id yet.
    #[serde(default)]
    pub withdraw_accepted_at_ts: Option<i64>,
    /// Bumped after each refused request, so a new request gets a new order id.
    #[serde(default)]
    pub withdraw_attempt: u32,
    /// How often the exchange failed or canceled an accepted withdrawal.
    #[serde(default)]
    pub withdraw_failures: u32,
    /// The withdrawal request, saved before it is sent.
    #[serde(default)]
    pub withdraw_send: Option<crate::outbound::driver::OutboundRecord>,
    pub size_out: Option<ChainTokenAmount>,
    #[serde(flatten)]
    pub bridge: CexWithdrawBridgeState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CexWithdrawBridgeState {
    #[serde(default)]
    pub withdraw_bridge_mint_receipt: Option<liquidium_pipeline_connectors::backend::bridge_backend::BridgeMintReceipt>,
    #[serde(default)]
    pub withdraw_planned_asset: Option<String>,
    #[serde(default)]
    pub withdraw_planned_network: Option<String>,
    #[serde(default)]
    pub withdraw_bridge_required: bool,
    #[serde(default)]
    pub withdraw_bridge_id: Option<String>,
    #[serde(default)]
    pub withdraw_bridge_submitted_at_ts: Option<i64>,
    #[serde(default)]
    pub withdraw_bridge_polled_at_ts: Option<i64>,
    #[serde(default)]
    pub withdraw_bridge_destination_snapshot: Option<String>,
    /// The send that funds the return bridge, saved before it is sent.
    #[serde(default)]
    pub withdraw_bridge_funding: Option<crate::outbound::driver::OutboundRecord>,
    /// The provider handle saved with that send. It turns the receipt into
    /// a bridge id.
    #[serde(default)]
    pub withdraw_bridge_send_handle: Option<String>,
    /// The chain receipt of that send, once it landed.
    #[serde(default)]
    pub withdraw_bridge_funding_txid: Option<String>,
    /// When the send landed. The provider must accept it within a window.
    #[serde(default)]
    pub withdraw_bridge_funded_at_ts: Option<i64>,
    /// How often that send reverted and was made again.
    #[serde(default)]
    pub withdraw_bridge_reverts: u32,
}

impl CexWithdrawBridgeState {
    /// True once a deposit send exists. The source balance then no longer
    /// holds the bridge amount, so the step must not size a new send.
    pub fn deposit_send_started(&self) -> bool {
        self.withdraw_bridge_funding.is_some() || self.withdraw_bridge_funding_txid.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CexState {
    pub liq_id: String,
    pub step: CexStep,
    pub last_error: Option<String>,
    pub market: String,
    pub side: String,
    pub size_in: ChainTokenAmount,
    #[serde(flatten)]
    pub deposit: CexDepositState,
    #[serde(flatten)]
    pub trade: CexTradeState,
    #[serde(flatten)]
    pub withdraw: CexWithdrawState,
    /// What this leg commits to pay from other balances: bridge ckETH, gas or
    /// cycles. One entry per label, so a retried step does not count twice.
    #[serde(default)]
    pub side_costs: Vec<SideCost>,
    /// Set by a step that saved a new send in this call. The venue writes the
    /// send to the WAL, then runs the step again to send it in the same cycle.
    #[serde(skip)]
    pub send_saved: bool,
}

impl CexState {
    /// Records side costs, replacing any earlier entry with the same label.
    pub fn record_side_costs(&mut self, costs: &[SideCost]) {
        for cost in costs {
            self.side_costs.retain(|recorded| recorded.label != cost.label);
            self.side_costs.push(cost.clone());
        }
    }

    /// Runs once after a restart. A saved send that is not safe to repeat may
    /// have gone out, so it becomes a settle; true when the step then only reads.
    pub fn resume_after_restart(&mut self, now: i64) -> bool {
        for record in [
            &mut self.deposit.deposit_funding,
            &mut self.deposit.bridge.deposit_bridge_call,
            &mut self.withdraw.withdraw_send,
            &mut self.withdraw.bridge.withdraw_bridge_funding,
        ]
        .into_iter()
        .flatten()
        {
            record.mark_uncertain(now);
        }
        // Each saved send now only settles or repeats safely. Without one,
        // the step may send, unless a later id proves the send happened.
        match self.step {
            CexStep::Deposit => self.deposit.deposit_funding.is_some(),
            CexStep::Withdraw => self.withdraw.withdraw_send.is_some(),
            // A bridge call is saved before it runs, so a missing record means
            // no call is in flight.
            CexStep::DepositPending => self.deposit.deposit_txid.is_some(),
            CexStep::WithdrawPending => {
                let bridge = &self.withdraw.bridge;
                self.withdraw.withdraw_id.is_some()
                    && (!bridge.withdraw_bridge_required
                        || bridge.withdraw_bridge_id.is_some()
                        || bridge.deposit_send_started())
            }
            _ => false,
        }
    }

    /// True once the funding transfer has left the trader account. From then
    /// on, a minimum refusal cannot end the leg, because the funds are elsewhere.
    pub fn funds_left_trader_account(&self) -> bool {
        self.step != CexStep::Deposit || self.deposit.deposit_txid.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Principal;

    fn sample_state(liq_id: &str) -> CexState {
        let pay = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        };
        let recv = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        };

        CexState {
            liq_id: liq_id.to_string(),
            send_saved: false,
            side_costs: Vec::new(),
            step: CexStep::Trade,
            last_error: None,
            market: "CKBTC_BTC".to_string(),
            side: "sell".to_string(),
            size_in: ChainTokenAmount::from_raw(pay.clone(), Nat::from(1_000u64)),
            deposit: CexDepositState {
                deposit_asset: pay.clone(),
                deposit_txid: None,
                deposit_balance_before: None,
                deposit_sent_at_ts: None,
                approval_bump_count: None,
                deposit_funding: None,
                bridge: CexDepositBridgeState {
                    deposit_planned_asset: None,
                    deposit_planned_network: None,
                    deposit_bridge_required: false,
                    deposit_bridge_id: None,
                    deposit_bridge_submitted_at_ts: None,
                    deposit_bridge_polled_at_ts: None,
                    deposit_bridge_destination_snapshot: None,
                    deposit_bridge_submit_amount: None,
                    deposit_bridge_expected_amount: None,
                    deposit_bridge_provider_fee_budget_native_units: None,
                    deposit_bridge_call: None,
                    legacy_bridge_submit_phase: None,
                    deposit_bridge_call_unrecorded: false,
                },
            },
            trade: CexTradeState {
                trade_leg_index: None,
                trade_leg_total: None,
                trade_resolved_legs: Vec::new(),
                trade_last_market: None,
                trade_last_side: None,
                trade_last_amount_in: None,
                trade_last_amount_out: None,
                trade_next_amount_in: None,
                trade_weighted_slippage_bps: None,
                trade_slices: Vec::new(),
                trade_dust_skipped: false,
                trade_dust_usd: None,
                trade_progress_remaining_in: None,
                trade_progress_total_out: None,
                trade_pending_client_order_id: None,
                trade_pending_market: None,
                trade_pending_side: None,
                trade_pending_requested_in: None,
                trade_pending_buy_mode: None,
                trade_inverse_retry_count: 0,
                trade_unexecutable_residual_in: None,
                trade_settlement_waiting_since_ts: None,
                trade_applied_client_order_ids: Vec::new(),
            },
            withdraw: CexWithdrawState {
                withdraw_asset: recv,
                withdraw_address: "dest".to_string(),
                withdraw_id: None,
                withdraw_txid: None,
                withdraw_accepted_at_ts: None,
                withdraw_attempt: 0,
                withdraw_failures: 0,
                withdraw_send: None,
                size_out: None,
                bridge: CexWithdrawBridgeState {
                    withdraw_bridge_reverts: 0,
                    withdraw_planned_asset: None,
                    withdraw_planned_network: None,
                    withdraw_bridge_required: false,
                    withdraw_bridge_id: None,
                    withdraw_bridge_submitted_at_ts: None,
                    withdraw_bridge_polled_at_ts: None,
                    withdraw_bridge_mint_receipt: None,
                    withdraw_bridge_destination_snapshot: None,
                    withdraw_bridge_funding: None,
                    withdraw_bridge_send_handle: None,
                    withdraw_bridge_funding_txid: None,
                    withdraw_bridge_funded_at_ts: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn cex_state_deserializes_when_new_trade_progress_fields_are_missing() {
        let state = sample_state("888");

        let mut value = serde_json::to_value(state).expect("serialize state");
        let map = value.as_object_mut().expect("state should serialize to object");
        map.remove("trade_resolved_legs");
        map.remove("trade_progress_remaining_in");
        map.remove("trade_progress_total_out");
        map.remove("trade_pending_client_order_id");
        map.remove("trade_pending_market");
        map.remove("trade_pending_side");
        map.remove("trade_pending_requested_in");
        map.remove("trade_pending_buy_mode");
        map.remove("trade_inverse_retry_count");
        map.remove("trade_unexecutable_residual_in");

        let decoded: CexState = serde_json::from_value(value).expect("legacy deserialize should succeed");
        assert!(decoded.trade.trade_resolved_legs.is_empty());
        assert_eq!(decoded.trade.trade_progress_remaining_in, None);
        assert_eq!(decoded.trade.trade_progress_total_out, None);
        assert_eq!(decoded.trade.trade_pending_client_order_id, None);
        assert_eq!(decoded.trade.trade_pending_market, None);
        assert_eq!(decoded.trade.trade_pending_side, None);
        assert_eq!(decoded.trade.trade_pending_requested_in, None);
        assert_eq!(decoded.trade.trade_pending_buy_mode, None);
        assert_eq!(decoded.trade.trade_inverse_retry_count, 0);
        assert_eq!(decoded.trade.trade_unexecutable_residual_in, None);
    }

    #[test]
    fn a_legacy_journal_phase_survives_a_save_until_it_is_upgraded() {
        let saved = r#"{"deposit_bridge_required":true,"deposit_bridge_submit_phase":{"Submitting":{"id":"a1","asset":"ckUSDC","destination":"0xabc","amount":1.5,"cursor":100,"started_at_ts":5,"called_at_ts":null,"parked_for_operator":true}}}"#;
        let state: CexDepositBridgeState = serde_json::from_str(saved).unwrap();
        use crate::bridge::legacy::LegacyBridgeSubmitPhase;
        let Some(LegacyBridgeSubmitPhase::Submitting(attempt)) = &state.legacy_bridge_submit_phase else {
            panic!("the saved attempt is read");
        };
        assert_eq!((attempt.id.as_str(), attempt.cursor), ("a1", Some(100)));
        assert!(attempt.parked_for_operator);
        let saved_again: CexDepositBridgeState = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert_eq!(saved_again.legacy_bridge_submit_phase, state.legacy_bridge_submit_phase);
        assert!(state.deposit_bridge_call_unrecorded, "an old row may hold a call without a record");
        let upgraded = CexDepositBridgeState::default();
        let upgraded_json = serde_json::to_string(&upgraded).unwrap();
        assert!(!upgraded_json.contains("deposit_bridge_submit_phase"));
        let read_back: CexDepositBridgeState = serde_json::from_str(&upgraded_json).unwrap();
        assert!(!read_back.deposit_bridge_call_unrecorded);

        let waiting: CexDepositBridgeState =
            serde_json::from_str(r#"{"deposit_bridge_submit_phase":"WaitingFunds"}"#).unwrap();
        assert_eq!(waiting.legacy_bridge_submit_phase, Some(LegacyBridgeSubmitPhase::WaitingFunds));
    }

    #[test]
    fn a_restart_marks_only_sends_that_are_not_safe_to_repeat() {
        use crate::outbound::driver::{OutboundPhase, OutboundRecord};
        use liquidium_pipeline_core::outbound::PreparedSend;
        let record = |replay_safe| {
            Some(OutboundRecord::prepared(PreparedSend {
                sender: "test".into(),
                identity: "send-1".into(),
                payload: String::new(),
                replay_safe,
            }))
        };
        let mut bridge = CexDepositBridgeState {
            deposit_bridge_call: record(false),
            ..Default::default()
        };
        bridge.deposit_bridge_call.as_mut().unwrap().mark_uncertain(7);
        assert_eq!(bridge.deposit_bridge_call.unwrap().phase, OutboundPhase::Submitted { at: 7 });

        let mut safe = record(true).unwrap();
        safe.mark_uncertain(7);
        assert_eq!(safe.phase, OutboundPhase::Prepared, "a safe send may simply run again");
    }
}
