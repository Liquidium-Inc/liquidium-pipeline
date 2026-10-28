//! What the shared CEX step machine needs to know about one exchange. The
//! exchange's API lives in its `CexBackend`; this holds only the machine's policy.
use super::utils::{TradeLeg, mexc_special_trade_legs};

/// The exchange-specific facts the step machine reads.
#[derive(Debug, Clone, Copy)]
pub struct CexVenueProfile {
    venue_id: &'static str,
    name: &'static str,
    special_trade_legs: Option<fn(&str, &str) -> Option<Vec<TradeLeg>>>,
    approval_bumps: bool,
    client_order_id_max_len: usize,
}

impl CexVenueProfile {
    /// MEXC: fixed routes for the legacy ck pairs, and approval bumps on direct
    /// ICP deposits so that the exchange notices them.
    pub fn mexc() -> Self {
        Self {
            venue_id: "mexc",
            name: "MEXC",
            special_trade_legs: Some(mexc_special_trade_legs),
            approval_bumps: true,
            client_order_id_max_len: 32,
        }
    }

    /// The venue id the planner and the saved leg state use.
    pub fn venue_id(&self) -> &'static str {
        self.venue_id
    }

    /// The exchange name in messages for operators.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// A fixed route that replaces market discovery for this pair.
    pub(super) fn special_trade_legs(&self, deposit: &str, withdraw: &str) -> Option<Vec<TradeLeg>> {
        self.special_trade_legs.and_then(|legs| legs(deposit, withdraw))
    }

    /// Whether direct ICP deposits send approval bumps, and so reserve their fees.
    pub(super) fn approval_bumps(&self) -> bool {
        self.approval_bumps
    }

    /// The longest client order id the exchange accepts.
    pub(super) fn client_order_id_max_len(&self) -> usize {
        self.client_order_id_max_len
    }
}

#[cfg(test)]
impl CexVenueProfile {
    /// A venue that sends no approval bumps, as most exchanges do.
    pub(super) fn without_approval_bumps(self) -> Self {
        Self {
            approval_bumps: false,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mexc_keeps_its_legacy_routes_and_approval_bumps() {
        let mexc = CexVenueProfile::mexc();
        assert_eq!((mexc.venue_id(), mexc.name()), ("mexc", "MEXC"));
        assert!(mexc.approval_bumps());
        assert!(mexc.special_trade_legs("ckBTC", "ckUSDT").is_some());
        assert!(mexc.special_trade_legs("ICP", "ckUSDT").is_none());
    }
}
