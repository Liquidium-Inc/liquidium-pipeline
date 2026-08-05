pub mod runtime;

use crate::{finalizers::cex_finalizer::CexFinalizer, swappers::kraken::KrakenClient};

/// Kraken uses the shared CEX execution core that was originally introduced
/// by MEXC. Persisted execution state remains the venue-neutral CEX schema.
pub type KrakenFinalizer = CexFinalizer<KrakenClient>;
