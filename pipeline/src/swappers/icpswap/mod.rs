// Keep the public module names stable while grouping implementation files by
// lifecycle boundary for auditability.
use candid::Principal;
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token::ChainToken};

use crate::utils::{CKUSDC_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL};

#[path = "protocol/client.rs"]
pub mod client;
#[path = "execution/store.rs"]
pub mod execution;
#[path = "execution/identity.rs"]
pub mod identity;
#[path = "execution/ledger_transfers.rs"]
pub mod ledger_transfers;
#[path = "execution/session.rs"]
pub mod session;
#[path = "execution/transfer_state.rs"]
pub mod transfer_state;
#[path = "execution/manual.rs"]
pub mod manual;
#[path = "planning/plan.rs"]
pub mod plan;
#[path = "execution/reconciliation.rs"]
pub(crate) mod reconciliation;
#[path = "execution/state.rs"]
pub mod state;
#[path = "protocol/types.rs"]
pub mod types;
#[path = "planning/venue.rs"]
pub mod venue;

pub const VENUE_ID: &str = "icpswap";

/// ICPSwap is intentionally restricted to the canonical ICP/ckUSDC market.
/// Ledger principals, rather than symbols, enforce the policy so a mislabeled
/// or similarly named token cannot be routed to the venue.
pub(crate) fn supports_pair(pay_token: &ChainToken, receive_asset: &AssetId) -> bool {
    let ChainToken::Icp { ledger: pay_ledger, .. } = pay_token else {
        return false;
    };
    if receive_asset.chain != "icp" {
        return false;
    }
    let Ok(receive_ledger) = Principal::from_text(&receive_asset.address) else {
        return false;
    };
    let native_icp =
        Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("configured native ICP ledger principal must be valid");
    let ckusdc =
        Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).expect("configured ckUSDC ledger principal must be valid");

    (*pay_ledger == native_icp && receive_ledger == ckusdc) || (*pay_ledger == ckusdc && receive_ledger == native_icp)
}

#[cfg(test)]
mod pair_policy_tests {
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;

    use super::supports_pair;
    use crate::utils::{CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL};

    fn token(ledger: &str, symbol: &str) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_text(ledger).expect("test ledger"),
            symbol: symbol.to_string(),
            decimals: 8,
            fee: Nat::from(1u8),
        }
    }

    #[test]
    fn allows_only_canonical_icp_ckusdc_directions() {
        let icp = token(ICP_LEDGER_PRINCIPAL, "ICP");
        let ckusdc = token(CKUSDC_LEDGER_PRINCIPAL, "ckUSDC");
        let ckusdt = token(CKUSDT_LEDGER_PRINCIPAL, "ckUSDT");

        assert!(supports_pair(&icp, &ckusdc.asset_id()));
        assert!(supports_pair(&ckusdc, &icp.asset_id()));
        assert!(!supports_pair(&icp, &ckusdt.asset_id()));
        assert!(!supports_pair(&ckusdt, &icp.asset_id()));
        assert!(!supports_pair(&icp, &icp.asset_id()));
        assert!(!supports_pair(&ckusdc, &ckusdc.asset_id()));
    }

    #[test]
    fn uses_ledgers_not_symbols_for_pair_policy() {
        let fake_ckusdc = token(CKUSDT_LEDGER_PRINCIPAL, "ckUSDC");
        let icp = token(ICP_LEDGER_PRINCIPAL, "ICP");

        assert!(!supports_pair(&icp, &fake_ckusdc.asset_id()));
    }
}

#[cfg(test)]
#[path = "tests/manual.rs"]
mod manual_tests;
#[cfg(test)]
#[path = "tests/protocol.rs"]
mod tests;
#[cfg(test)]
#[path = "tests/planning.rs"]
mod venue_tests;
#[cfg(test)]
#[path = "tests/state_versioning.rs"]
mod state_versioning_tests;
#[cfg(test)]
#[path = "tests/ledger_transfers.rs"]
mod ledger_transfer_tests;
