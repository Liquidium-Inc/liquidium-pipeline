mod kraken_adapter;
mod kraken_api;

pub use kraken_adapter::KrakenClient;
pub(crate) use kraken_adapter::normalize_market;
pub use kraken_api::{KrakenApi, KrakenApiError, KrakenRestApi};
