//! The chains Rift trades on, and every answer that depends on the chain:
//! its name, where a refund goes, and how long a deposit may take to be seen.
use crate::finalizers::multi_venue::VenueStepError;

/// Where a refunded order pays back. Rift refunds on the pay asset's chain,
/// so each chain names a wallet we hold the key of.
#[derive(Debug, Clone)]
pub struct RiftRefundAddresses {
    /// The main EVM wallet.
    pub ethereum: String,
    /// The bridge Bitcoin wallet, derived from the mnemonic.
    pub bitcoin: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RiftChain {
    Ethereum,
    Bitcoin,
}

impl RiftChain {
    /// The chain as Rift names it in `chain.symbol`.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Bitcoin => "bitcoin",
        }
    }

    /// The chain a saved leg names.
    pub(super) fn parse(name: &str) -> Result<Self, VenueStepError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "ethereum" => Ok(Self::Ethereum),
            "bitcoin" => Ok(Self::Bitcoin),
            other => Err(VenueStepError::Other(format!("unknown Rift chain `{other}`"))),
        }
    }

    pub(super) fn refund_address(self, addresses: &RiftRefundAddresses) -> &str {
        match self {
            Self::Ethereum => &addresses.ethereum,
            Self::Bitcoin => &addresses.bitcoin,
        }
    }

    /// The chain as the bridge route catalog names it.
    pub(super) fn catalog_chain(self) -> &'static str {
        match self {
            Self::Ethereum => "ETH",
            Self::Bitcoin => "BTC",
        }
    }

    /// How long Rift may take to see a deposit the bridge already paid. A
    /// Bitcoin deposit needs Rift's own confirmations after the minter's.
    pub(super) fn deposit_seen_timeout_secs(self) -> i64 {
        match self {
            Self::Ethereum => 2 * 60 * 60,
            Self::Bitcoin => 4 * 60 * 60,
        }
    }

    /// How long Rift's payout may take to come back as the ck asset. The
    /// ckETH return waits for Ethereum finality; the ckBTC mint for the
    /// minter's confirmations.
    pub(super) fn return_timeout_secs(self) -> i64 {
        match self {
            Self::Ethereum => 60 * 60,
            Self::Bitcoin => 4 * 60 * 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_chain_name_round_trips_and_an_unknown_one_is_refused() {
        for chain in [RiftChain::Ethereum, RiftChain::Bitcoin] {
            assert_eq!(RiftChain::parse(chain.name()).unwrap(), chain);
        }
        assert!(RiftChain::parse("solana").is_err());
        assert_eq!(RiftChain::Bitcoin.catalog_chain(), "BTC");
        assert!(RiftChain::Bitcoin.return_timeout_secs() > RiftChain::Ethereum.return_timeout_secs());
    }
}
