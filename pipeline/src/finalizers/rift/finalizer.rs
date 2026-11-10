//! The Rift step machine: the order, the leg out to the order's deposit
//! address, the wait for delivery, then the leg back. Each step is one file.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use candid::{Nat, Principal};

use super::chain::RiftChain;
pub use super::chain::RiftRefundAddresses;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::BridgeRouteSpec,
    rift_backend::{RiftBackend, RiftError, RiftVenueLists},
};
use liquidium_pipeline_core::{
    outbound::OutboundSender,
    tokens::chain_token_amount::ChainTokenAmount,
    tokens::{chain_token::ChainToken, token_registry::TokenRegistryTrait},
};

use super::state::{RiftState, RiftStep};
use crate::{
    bridge::runtime::BridgeRuntime,
    finalizers::{
        bridge_legs::{in_route, out_route},
        multi_venue::VenueStepError,
    },
};

#[path = "steps/bridge_back.rs"]
mod bridge_back;
#[path = "steps/bridge_out.rs"]
mod bridge_out;
#[path = "steps/order.rs"]
mod order;
#[path = "steps/wait.rs"]
mod wait;

/// The ck assets this venue carries, with the chain and symbol Rift knows
/// them by.
const RIFT_ASSETS: [(&str, RiftChain, &str); 4] = [
    ("ckUSDC", RiftChain::Ethereum, "USDC"),
    ("ckUSDT", RiftChain::Ethereum, "USDT"),
    ("ckETH", RiftChain::Ethereum, "ETH"),
    ("ckBTC", RiftChain::Bitcoin, "BTC"),
];

/// A throttled or unreachable Rift is a wait; every other refusal is a retry.
impl From<RiftError> for VenueStepError {
    fn from(error: RiftError) -> Self {
        match error {
            RiftError::Unreachable(_) | RiftError::RateLimited { .. } => Self::Unreachable(error.to_string()),
            other => Self::Other(other.to_string()),
        }
    }
}

/// The amount a Rift answer reports, in the token's exact units. Rift may
/// write more fraction digits than the token has; the extra are cut, never
/// rounded up past what the wallet holds. A zero reads as zero.
pub(super) fn rift_amount(token: &ChainToken, text: &str) -> Result<ChainTokenAmount, VenueStepError> {
    let text = text.trim();
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let decimals = usize::from(token.decimals());
    let fraction = &fraction[..fraction.len().min(decimals)];
    let digits = format!("{whole}{fraction:0<decimals$}");
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(VenueStepError::Other(format!(
            "Rift amount `{text}` is not a plain decimal"
        )));
    }
    let units = digits
        .parse::<u128>()
        .map_err(|error| VenueStepError::Other(format!("Rift amount `{text}` is out of range: {error}")))?;
    Ok(ChainTokenAmount::from_raw(token.clone(), Nat::from(units)))
}

/// An amount as Rift reads it: a plain decimal with no trailing zeros.
pub(super) fn decimal_text(amount: &ChainTokenAmount) -> String {
    let decimals = usize::from(amount.token.decimals());
    let digits = format!("{:0>width$}", amount.value.0, width = decimals + 1);
    let (whole, fraction) = digits.split_at(digits.len() - decimals);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

pub struct RiftVenueFinalizer<R: RiftBackend> {
    pub(super) backend: Arc<R>,
    pub(super) bridge: Arc<BridgeRuntime>,
    /// Sends the funding transfer from the trader account.
    pub(super) outbound: Arc<dyn OutboundSender>,
    pub(super) token_registry: Option<Arc<dyn TokenRegistryTrait>>,
    pub(super) liquidator_principal: Principal,
    /// Where a refunded order pays back, on the pay asset's chain.
    pub(super) refund_addresses: RiftRefundAddresses,
    pub(super) quote_route_fee_bps: u32,
    pub(super) quote_delay_buffer_bps: u32,
    /// Which Rift venues may route an order. A venue's own confirmation
    /// count sets the duration of a leg, and Rift does not tell it.
    pub(super) venues: RiftVenueLists,
    /// Intents armed in this process. A leg whose intent is not here restarted.
    pub(super) multi_venue_armed_intents: Mutex<HashSet<String>>,
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub fn new(
        backend: Arc<R>,
        bridge: Arc<BridgeRuntime>,
        outbound: Arc<dyn OutboundSender>,
        liquidator_principal: Principal,
        refund_addresses: RiftRefundAddresses,
    ) -> Self {
        Self {
            backend,
            bridge,
            outbound,
            token_registry: None,
            liquidator_principal,
            refund_addresses,
            quote_route_fee_bps: 0,
            quote_delay_buffer_bps: 0,
            venues: RiftVenueLists::default(),
            multi_venue_armed_intents: Mutex::new(HashSet::new()),
        }
    }

    pub fn with_token_registry(mut self, token_registry: Arc<dyn TokenRegistryTrait>) -> Self {
        self.token_registry = Some(token_registry);
        self
    }

    pub fn with_quote_costs(mut self, route_fee_bps: u32, delay_buffer_bps: u32) -> Self {
        self.quote_route_fee_bps = route_fee_bps;
        self.quote_delay_buffer_bps = delay_buffer_bps;
        self
    }

    pub fn with_venues(mut self, venues: RiftVenueLists) -> Self {
        self.venues = venues;
        self
    }

    /// Where Rift delivers: the bridge EVM wallet, which the leg back signs from.
    pub(super) fn delivery_address(&self) -> &str {
        &self.bridge.sources.evm
    }

    /// The chain and symbol Rift knows a ck asset by.
    pub(super) fn rift_asset(asset: &ChainToken) -> Option<(RiftChain, &'static str)> {
        let symbol = asset.symbol();
        RIFT_ASSETS
            .iter()
            .find(|(ck, _, _)| ck.eq_ignore_ascii_case(&symbol))
            .map(|(_, chain, symbol)| (*chain, *symbol))
    }

    /// The two bridge routes of a leg: the pay asset out to its chain, and
    /// the delivered asset back from Ethereum to its ck form.
    pub(super) fn leg_routes(
        pay: &ChainToken,
        receive: &ChainToken,
    ) -> Result<(&'static BridgeRouteSpec, &'static BridgeRouteSpec), String> {
        let (_, pay_symbol) = Self::rift_asset(pay).ok_or_else(|| format!("Rift does not carry {}", pay.symbol()))?;
        let (receive_chain, receive_symbol) =
            Self::rift_asset(receive).ok_or_else(|| format!("Rift does not deliver {}", receive.symbol()))?;
        if receive_chain != RiftChain::PAYOUT {
            return Err(format!(
                "Rift delivers only to Ethereum in this build, not {} on {}",
                receive.symbol(),
                receive_chain.name()
            ));
        }
        if pay_symbol == receive_symbol {
            return Err(format!("Rift cannot swap {} for itself", pay.symbol()));
        }
        let out = out_route(pay, pay_symbol)?;
        let back = in_route(receive_symbol, "ETH", &receive.symbol()).ok_or_else(|| {
            format!(
                "bridge route not found for {receive_symbol}@ETH -> {}",
                receive.symbol()
            )
        })?;
        Ok((out, back))
    }

    pub(super) async fn advance_current_step(&self, state: &mut RiftState) -> Result<(), VenueStepError> {
        match state.step {
            RiftStep::Order => self.order(state).await,
            RiftStep::BridgeOut => self.bridge_out(state).await,
            RiftStep::Wait => self.wait(state).await,
            RiftStep::BridgeBack => self.bridge_back(state).await,
            RiftStep::Completed | RiftStep::Failed => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
