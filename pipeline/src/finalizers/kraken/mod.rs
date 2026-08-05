pub mod runtime;

use crate::{
    finalizers::mexc::mexc_finalizer::MexcFinalizer,
    swappers::kraken::KrakenClient,
};

/// Kraken uses the shared CEX execution core that was originally introduced
/// by MEXC. Persisted execution state remains the venue-neutral CEX schema.
pub type KrakenFinalizer = MexcFinalizer<KrakenClient>;
