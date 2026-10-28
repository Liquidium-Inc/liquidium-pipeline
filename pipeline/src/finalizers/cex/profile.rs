//! What the shared CEX step machine needs to know about one exchange. The
//! exchange's API lives in its `CexBackend`; this holds only the machine's policy.
use super::utils::{TradeLeg, mexc_special_trade_legs};
use crate::finalizers::multi_venue::{KRAKEN_VENUE_ID, MEXC_VENUE_ID};

/// The exchange-specific facts the step machine reads.
#[derive(Debug, Clone, Copy)]
pub struct CexVenueProfile {
    venue_id: &'static str,
    name: &'static str,
    special_trade_legs: Option<fn(&str, &str) -> Option<Vec<TradeLeg>>>,
    /// The only market SOL trades on, when the exchange restricts SOL to one.
    sol_market: Option<&'static str>,
    approval_bumps: bool,
    client_order_id_max_len: usize,
}

impl CexVenueProfile {
    /// MEXC: fixed routes for the legacy ck pairs, SOL only on `SOL_USDT`, and
    /// approval bumps on direct ICP deposits so that the exchange notices them.
    pub fn mexc() -> Self {
        Self {
            venue_id: MEXC_VENUE_ID,
            name: "MEXC",
            special_trade_legs: Some(mexc_special_trade_legs),
            sol_market: Some("SOL_USDT"),
            approval_bumps: true,
            client_order_id_max_len: 32,
        }
    }

    /// Kraken: no fixed routes and no approval bumps. It takes free-text client
    /// order ids of at most 18 characters.
    pub fn kraken() -> Self {
        Self {
            venue_id: KRAKEN_VENUE_ID,
            name: "Kraken",
            special_trade_legs: None,
            sol_market: None,
            approval_bumps: false,
            client_order_id_max_len: 18,
        }
    }

    /// The profile of the exchange with this venue id, or `None` when the
    /// venue is not an exchange.
    pub fn for_venue(venue_id: &str) -> Option<Self> {
        [Self::mexc(), Self::kraken()]
            .into_iter()
            .find(|profile| profile.venue_id == venue_id)
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

    /// The only market SOL trades on, if the exchange restricts SOL to one.
    pub(super) fn sol_market(&self) -> Option<&'static str> {
        self.sol_market
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
mod tests {
    use super::*;

    #[test]
    fn mexc_keeps_its_legacy_routes_and_approval_bumps() {
        let mexc = CexVenueProfile::mexc();
        assert_eq!((mexc.venue_id(), mexc.name()), ("mexc", "MEXC"));
        assert!(mexc.approval_bumps());
        assert!(mexc.special_trade_legs("ckBTC", "ckUSDT").is_some());
        assert!(mexc.special_trade_legs("ICP", "ckUSDT").is_none());
        assert_eq!(mexc.sol_market(), Some("SOL_USDT"));
        assert_eq!(mexc.client_order_id_max_len(), 32);
    }

    #[test]
    fn kraken_has_no_fixed_routes_or_bumps_and_short_client_ids() {
        let kraken = CexVenueProfile::kraken();
        assert_eq!((kraken.venue_id(), kraken.name()), ("kraken", "Kraken"));
        assert!(!kraken.approval_bumps());
        assert!(kraken.special_trade_legs("ckBTC", "ckUSDT").is_none());
        assert_eq!(kraken.sol_market(), None);
        assert_eq!(kraken.client_order_id_max_len(), 18);
    }
}
