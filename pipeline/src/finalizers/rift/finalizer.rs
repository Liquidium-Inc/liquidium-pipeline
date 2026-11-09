//! The Rift step machine: the order, the leg out to the order's deposit
//! address, the wait for delivery, then the leg back. Each step is one file.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use candid::Principal;
use liquidium_pipeline_connectors::backend::{
    bridge_backend::BridgeRouteSpec,
    rift_backend::{RiftBackend, RiftError},
};
use liquidium_pipeline_core::{
    outbound::OutboundSender,
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

/// The chain Rift swaps on for every asset this venue carries.
pub(super) const RIFT_CHAIN: &str = "ethereum";

/// The ck assets this venue carries, with their Ethereum symbol. ckETH waits
/// for a fix to the fee drift between the order and the native bridge.
const RIFT_ASSETS: [(&str, &str); 2] = [("ckUSDC", "USDC"), ("ckUSDT", "USDT")];

/// A throttled or unreachable Rift is a wait; every other refusal is a retry.
impl From<RiftError> for VenueStepError {
    fn from(error: RiftError) -> Self {
        match error {
            RiftError::Unreachable(_) | RiftError::RateLimited { .. } => Self::Unreachable(error.to_string()),
            other => Self::Other(other.to_string()),
        }
    }
}

/// The amount a Rift answer reports, as a number.
pub(super) fn parse_amount(text: &str) -> Result<f64, VenueStepError> {
    let amount = text
        .trim()
        .parse::<f64>()
        .map_err(|error| VenueStepError::Other(format!("Rift amount `{text}` is not a number: {error}")))?;
    if !amount.is_finite() || amount < 0.0 {
        return Err(VenueStepError::Other(format!(
            "Rift amount `{text}` is not a valid amount"
        )));
    }
    Ok(amount)
}

/// Decimal text with at most `decimals` places and no trailing zeros, as
/// Rift reads an amount.
pub(super) fn decimal_text(amount: f64, decimals: u8) -> String {
    let text = format!("{amount:.*}", usize::from(decimals));
    if !text.contains('.') {
        return text;
    }
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

pub struct RiftVenueFinalizer<R: RiftBackend> {
    pub(super) backend: Arc<R>,
    pub(super) bridge: Arc<BridgeRuntime>,
    /// Sends the funding transfer from the trader account.
    pub(super) outbound: Arc<dyn OutboundSender>,
    pub(super) token_registry: Option<Arc<dyn TokenRegistryTrait>>,
    pub(super) liquidator_principal: Principal,
    /// Where a refunded order pays back: the main EVM wallet.
    pub(super) refund_address: String,
    pub(super) quote_route_fee_bps: u32,
    pub(super) quote_delay_buffer_bps: u32,
    /// Intents armed in this process. A leg whose intent is not here restarted.
    pub(super) multi_venue_armed_intents: Mutex<HashSet<String>>,
}

impl<R: RiftBackend> RiftVenueFinalizer<R> {
    pub fn new(
        backend: Arc<R>,
        bridge: Arc<BridgeRuntime>,
        outbound: Arc<dyn OutboundSender>,
        liquidator_principal: Principal,
        refund_address: String,
    ) -> Self {
        Self {
            backend,
            bridge,
            outbound,
            token_registry: None,
            liquidator_principal,
            refund_address,
            quote_route_fee_bps: 0,
            quote_delay_buffer_bps: 0,
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

    /// Where Rift delivers: the bridge EVM wallet, which the leg back signs from.
    pub(super) fn delivery_address(&self) -> &str {
        &self.bridge.sources.evm
    }

    /// The Ethereum symbol of a ck asset this venue carries.
    pub(super) fn rift_symbol(asset: &ChainToken) -> Option<&'static str> {
        let symbol = asset.symbol();
        RIFT_ASSETS
            .iter()
            .find(|(ck, _)| ck.eq_ignore_ascii_case(&symbol))
            .map(|(_, evm)| *evm)
    }

    /// The two bridge routes of a leg: the pay asset out to Ethereum, and the
    /// delivered asset back to its ck form.
    pub(super) fn leg_routes(
        pay: &ChainToken,
        receive: &ChainToken,
    ) -> Result<(&'static BridgeRouteSpec, &'static BridgeRouteSpec), String> {
        let pay_symbol = Self::rift_symbol(pay).ok_or_else(|| format!("Rift does not carry {}", pay.symbol()))?;
        let receive_symbol =
            Self::rift_symbol(receive).ok_or_else(|| format!("Rift does not deliver {}", receive.symbol()))?;
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
