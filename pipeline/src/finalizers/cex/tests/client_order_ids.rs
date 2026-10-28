//! Client order ids fit each exchange's cap and stay distinct.
use super::*;
use crate::finalizers::cex::{profile::CexVenueProfile, utils::TradeLeg};

fn leg() -> TradeLeg {
    TradeLeg {
        market: "ICP_USD".into(),
        side: "sell".into(),
    }
}

/// The ids a finalizer with `profile` gives two slices of one leg.
async fn two_slice_ids(profile: CexVenueProfile, liq_id: &str) -> (String, String) {
    let mut finalizer = test_finalizer(Arc::new(MockCexBackend::new()), Arc::new(MockTransferActions::new()));
    finalizer.profile = profile;
    let mut state = finalizer.prepare(liq_id, &make_execution_receipt(42)).await.unwrap();
    let first = finalizer
        .prepare_pending_slice_request(&mut state, &leg(), 0, 1.0, BuyOrderInputMode::Auto)
        .client_order_id;
    // A spent id is never sent again, so the next slice gets a new one.
    state.trade.trade_applied_client_order_ids.push(first.clone());
    let second = finalizer
        .prepare_pending_slice_request(&mut state, &leg(), 0, 1.0, BuyOrderInputMode::Auto)
        .client_order_id;
    (first, second)
}

/// Kraken takes free text of at most 18 characters, so a long id becomes its
/// full 64-bit hash, which keeps slices apart.
#[tokio::test]
async fn kraken_ids_fit_18_characters_and_stay_distinct() {
    let (first, second) = two_slice_ids(CexVenueProfile::kraken(), "123456789012").await;

    for id in [&first, &second] {
        assert_eq!(id.len(), 18, "{id}");
        assert!(id.starts_with("lq"), "{id}");
    }
    assert_ne!(first, second);
}

#[tokio::test]
async fn a_short_id_stays_readable_on_kraken() {
    let (first, _) = two_slice_ids(CexVenueProfile::kraken(), "42").await;
    assert_eq!(first, "liq-42-l1-s1-a");
}

#[tokio::test]
async fn mexc_keeps_ids_up_to_32_characters() {
    let (first, _) = two_slice_ids(CexVenueProfile::mexc(), "123456789012").await;
    assert_eq!(first, "liq-123456789012-l1-s1-a");
}
