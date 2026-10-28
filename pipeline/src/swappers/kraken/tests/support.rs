//! Fixtures for the Kraken tests.
use std::sync::Arc;

use rust_decimal::Decimal;

use crate::swappers::kraken::{
    api::{KrakenBook, KrakenBookLevel, KrakenPair, MockKrakenApi},
    client::KrakenClient,
};

pub(super) fn btc_usd_pair() -> KrakenPair {
    KrakenPair {
        api_name: "XXBTZUSD".into(),
        base: "BTC".into(),
        quote: "USD".into(),
        price_decimals: 1,
        lot_decimals: 8,
        order_min: Decimal::new(1, 4),
        cost_min: Decimal::new(5, 0),
        online: true,
    }
}

/// A client allowed to trade the one market these tests use.
pub(super) fn btc_usd_client(api: MockKrakenApi) -> KrakenClient {
    KrakenClient::with_api(Arc::new(api), vec!["BTC_USD".into()]).expect("a non-empty allowlist")
}

fn level(price: i64, quantity: Decimal) -> KrakenBookLevel {
    KrakenBookLevel {
        price: Decimal::new(price, 0),
        quantity,
    }
}

/// One bid at 60,000 and one ask at 60,000, one coin each.
pub(super) fn one_coin_book() -> KrakenBook {
    KrakenBook {
        bids: vec![level(60_000, Decimal::ONE)],
        asks: vec![level(60_000, Decimal::ONE)],
    }
}
