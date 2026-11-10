//! The chains Rift trades on, and every answer that depends on the chain:
//! its name, where a refund goes, how long a deposit may take to be seen,
//! and what an order asks below the expected arrival.
use liquidium_pipeline_connectors::backend::bridge_backend::BridgeFeeBudget;

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
    /// The chain Rift delivers to: the ckERC20 bridge brings the payout back.
    pub(super) const PAYOUT: RiftChain = RiftChain::Ethereum;

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

    /// How long Rift may take to see a deposit the bridge already paid. A
    /// Bitcoin deposit needs Rift's own confirmations after the minter's.
    pub(super) fn deposit_seen_timeout_secs(self) -> i64 {
        match self {
            Self::Ethereum => 2 * 60 * 60,
            Self::Bitcoin => 4 * 60 * 60,
        }
    }

    /// What the order asks below the expected arrival. The ckBTC minter takes
    /// the Bitcoin fee at burn time, so one more fee estimate covers a rise.
    pub(super) fn order_headroom(self, budget: &BridgeFeeBudget) -> f64 {
        match self {
            Self::Ethereum => 0.0,
            Self::Bitcoin => budget.destination_fee_budget.max(0.0),
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
    }

    #[test]
    fn only_bitcoin_asks_for_headroom() {
        let budget = BridgeFeeBudget {
            destination_fee_budget: 0.000_015,
            ..BridgeFeeBudget::default()
        };
        assert_eq!(RiftChain::Bitcoin.order_headroom(&budget), 0.000_015);
        assert_eq!(RiftChain::Ethereum.order_headroom(&budget), 0.0);
    }
}
