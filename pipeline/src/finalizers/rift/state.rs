//! The persisted state of a Rift leg. The two bridge legs keep their shared
//! shapes; the order and its status sit between them.
use liquidium_pipeline_connectors::backend::rift_backend::RiftOrderStatus;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::types::side_cost::SideCost;
use serde::{Deserialize, Serialize};

use super::chain::RiftChain;
use crate::{
    finalizers::{
        bridge_legs::{BridgeInState, BridgeOutState},
        multi_venue::VenueStepError,
    },
    outbound::driver::OutboundRecord,
    persistance::VenueLegFailure,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RiftStep {
    /// A fresh quote, then the order that names the deposit address.
    Order,
    /// The pay asset to the bridge wallet, the minter call that pays the
    /// order's deposit address, then the bridge poll.
    BridgeOut,
    /// The order status, until Rift delivers to the bridge EVM wallet.
    Wait,
    /// The delivered asset from the bridge EVM wallet to its ck form on ICP.
    BridgeBack,
    Completed,
    Failed,
}

/// The order of this leg, saved before the bridge funds it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiftOrderRecord {
    pub id: String,
    pub quote_id: String,
    pub deposit_address: String,
    /// Decimal text, as Rift reads it. The bridge must deliver exactly this.
    pub amount_in: String,
    pub deposit_deadline: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub amount_out: Option<String>,
    #[serde(default)]
    pub payout_tx: Option<String>,
    #[serde(default)]
    pub polled_at_ts: Option<i64>,
    /// When the wait for Rift started: the bridge completion, or the last
    /// operator release. The wait bounds count from here.
    #[serde(default)]
    pub wait_since_ts: Option<i64>,
    /// When Rift delivered. The leg back counts its wait from here.
    #[serde(default)]
    pub delivered_at_ts: Option<i64>,
    /// The refund transaction an operator confirmed. It ends the leg.
    #[serde(default)]
    pub refund_tx: Option<String>,
}

fn ethereum() -> String {
    "ethereum".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiftState {
    pub liq_id: String,
    pub step: RiftStep,
    pub last_error: Option<String>,
    pub size_in: ChainTokenAmount,
    pub final_asset: ChainToken,
    /// The ICP account the final asset goes to.
    pub final_destination: String,
    /// The chain Rift takes the pay asset on: `ethereum` or `bitcoin`.
    #[serde(default = "ethereum")]
    pub pay_chain: String,
    /// The symbols on each side of Rift, `USDC` -> `USDT`. The payout is
    /// always on Ethereum.
    pub pay_symbol: String,
    pub receive_symbol: String,
    /// What the order asks for, in the pay asset's units: the expected
    /// bridge arrival less the headroom.
    pub amount_in: ChainTokenAmount,
    /// What the order asks below the expected arrival. A Bitcoin fee can rise
    /// between the estimate and the burn, and an underfunded order is refunded.
    pub arrival_headroom: ChainTokenAmount,
    /// The planned floor for a fresh quote, in the receive asset. A quote
    /// under it is refused.
    pub min_amount_out: ChainTokenAmount,
    /// The fresh quote, saved before the order is created from it. Rift
    /// returns the same order for the same quote id, so a repeat is safe.
    #[serde(default)]
    pub quote_id: Option<String>,
    #[serde(default)]
    pub order: Option<RiftOrderRecord>,
    /// The transfer that funds the bridge wallet, saved before it is sent.
    #[serde(default)]
    pub funding: Option<OutboundRecord>,
    #[serde(default)]
    pub funding_txid: Option<String>,
    #[serde(default)]
    pub funding_sent_at_ts: Option<i64>,
    #[serde(default)]
    pub bridge_out: BridgeOutState,
    #[serde(default)]
    pub bridge_in: BridgeInState,
    #[serde(default)]
    pub size_out: Option<ChainTokenAmount>,
    /// Why the leg ended as `Failed`, for the parent to act on.
    #[serde(default)]
    pub failure: Option<VenueLegFailure>,
    /// What this leg commits to pay from other balances: bridge ckETH and gas.
    /// One entry per label, so a retried step does not count twice.
    #[serde(default)]
    pub side_costs: Vec<SideCost>,
    /// Set by a step that saved a new send or order input in this call. The
    /// venue writes it to the WAL, then runs the step again in the same cycle.
    #[serde(skip)]
    pub send_saved: bool,
}

impl RiftState {
    /// The chain the pay asset is on, as the leg saved it.
    pub(super) fn pay_chain(&self) -> Result<RiftChain, VenueStepError> {
        RiftChain::parse(&self.pay_chain)
    }

    /// Records side costs, replacing any earlier entry with the same label.
    pub fn record_side_costs(&mut self, costs: &[SideCost]) {
        for cost in costs {
            self.side_costs.retain(|recorded| recorded.label != cost.label);
            self.side_costs.push(cost.clone());
        }
    }

    /// True once the funding transfer has left the trader account. From then
    /// on, a minimum refusal cannot end the leg, because the funds are elsewhere.
    pub fn funds_left_trader_account(&self) -> bool {
        match self.step {
            RiftStep::Order => false,
            RiftStep::BridgeOut => self.funding_txid.is_some(),
            _ => true,
        }
    }

    /// Runs once after a restart. Every send is saved before it runs and the
    /// order is idempotent per saved quote id, so the leg always resumes; a
    /// saved send that is not safe to repeat becomes a settle first.
    pub fn settle_saved_sends_after_restart(&mut self, now: i64) {
        for record in [
            &mut self.funding,
            &mut self.bridge_out.deposit_bridge_call,
            &mut self.bridge_in.withdraw_bridge_call,
            &mut self.bridge_in.withdraw_bridge_funding,
        ]
        .into_iter()
        .flatten()
        {
            record.mark_uncertain(now);
        }
    }

    /// True while Rift reports the order refunded. The money is back at the
    /// refund address, outside the pipeline's accounts.
    pub fn order_refunded(&self) -> bool {
        self.order
            .as_ref()
            .and_then(|order| order.status.as_deref())
            .is_some_and(|status| RiftOrderStatus::from_api(status) == RiftOrderStatus::Refunded)
    }

    /// Ends the leg on a confirmed refund. The parent reports the row failed;
    /// the operator moves the refunded funds by hand.
    pub fn write_off_refund(&mut self, refund_tx: String) -> Result<(), String> {
        let order = self.order.as_mut().ok_or("no Rift order to write off")?;
        self.last_error = Some(format!(
            "Rift order {} was refunded in {refund_tx}; the funds are at the refund address, outside the pipeline",
            order.id
        ));
        order.refund_tx = Some(refund_tx);
        self.failure = Some(VenueLegFailure::Refunded);
        self.step = RiftStep::Failed;
        Ok(())
    }

    /// Drops an unfunded order so the next cycle quotes and orders again.
    /// Nothing funded it, so it expires at Rift on its own.
    pub fn reorder(&mut self) {
        self.order = None;
        self.quote_id = None;
        self.funding = None;
        self.step = RiftStep::Order;
    }
}
