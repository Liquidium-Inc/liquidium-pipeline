//! The market allowlist and the pair lookup.
use std::sync::Arc;

use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use super::support::{btc_usd_client, btc_usd_pair};
use crate::swappers::kraken::{
    api::MockKrakenApi,
    client::{KrakenClient, api_asset, normalize_market},
};

/// The allowlist keeps routing off every market Kraken lists, so a client
/// cannot exist without one.
#[test]
fn a_client_cannot_be_built_without_a_market_allowlist() {
    let error = match KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![]) {
        Ok(_) => panic!("an empty allowlist must be refused"),
        Err(error) => error,
    };
    assert!(error.contains("allowlist"), "{error}");
    assert!(KrakenClient::with_api(Arc::new(MockKrakenApi::new()), vec![" ".into(), "".into()]).is_err());
}

#[test]
fn kraken_btc_codes_and_market_separators_are_canonical() {
    assert_eq!(api_asset("BTC"), "XBT");
    assert_eq!(normalize_market("xbt/usd"), "BTC_USD");
    assert_eq!(normalize_market("xxbt-usd"), "BTC_USD");
}

#[tokio::test]
async fn a_market_outside_the_allowlist_is_refused_without_a_read() {
    let mut api = MockKrakenApi::new();
    api.expect_pairs().never();
    let client = btc_usd_client(api);

    let error = client.get_orderbook("ETH_USD", None).await.expect_err("not allowed");
    assert!(error.contains("not allowed"), "{error}");
}

#[tokio::test]
async fn offline_and_ambiguous_pairs_are_refused() {
    let mut offline = btc_usd_pair();
    offline.online = false;
    let mut api = MockKrakenApi::new();
    api.expect_pairs().once().return_once(move || Ok(vec![offline]));
    let error = btc_usd_client(api).get_orderbook("BTC_USD", None).await.unwrap_err();
    assert!(error.contains("not online"), "{error}");

    let pair = btc_usd_pair();
    let mut api = MockKrakenApi::new();
    api.expect_pairs()
        .once()
        .return_once(move || Ok(vec![pair.clone(), pair]));
    let error = btc_usd_client(api).get_orderbook("BTC_USD", None).await.unwrap_err();
    assert!(error.contains("ambiguous"), "{error}");
}
