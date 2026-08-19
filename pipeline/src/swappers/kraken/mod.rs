//! Kraken as a CEX venue.
//!
//! - `kraken_api`: the wire types and the [`KrakenApi`] trait (mockable).
//! - `kraken_rest`: the live [`KrakenRestApi`] over `kraken_async_rs`.
//! - `kraken_adapter`: [`KrakenClient`], the `CexBackend` the pipeline uses,
//!   with its trading and funding concerns in `kraken_trading` and
//!   `kraken_funding` beneath it.
mod kraken_adapter;
mod kraken_api;
mod kraken_rest;

pub use kraken_adapter::KrakenClient;
pub(crate) use kraken_adapter::normalize_market;
pub use kraken_api::{KrakenApi, KrakenApiError};
pub use kraken_rest::KrakenRestApi;
