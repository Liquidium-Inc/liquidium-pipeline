//! MEXC as a CEX venue.
//!
//! - `mexc_adapter`: [`mexc_adapter::MexcClient`], the `CexBackend` the
//!   pipeline uses, with its symbol, order, trading and funding concerns in
//!   child modules beneath it.
//! - `mexc_swapper`: the legacy `SwapVenue` over the same client.
//! - `orderbook_quote`: depth simulation shared with the CEX state machine.
pub mod mexc_adapter;
pub mod mexc_swapper;
pub(crate) mod orderbook_quote;
