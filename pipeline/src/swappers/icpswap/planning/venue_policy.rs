//! The ICPSwap venue id and the pair policy it trades under.
use candid::Principal;
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token::ChainToken};

use crate::{
    swappers::model::SwapRequest,
    utils::{
        CKBTC_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL,
        ICP_LEDGER_PRINCIPAL,
    },
};

use super::types::IcpswapQuoteError;

pub const VENUE_ID: &str = "icpswap";

/// The pairs ICPSwap may trade, in either direction. Each pair has a direct
/// pool with real depth; a pair joins the list only after its pool is checked.
///
/// ckUSDT/ckBTC and ckUSDT/ckETH are left out: the first pool is empty and the
/// second does not exist (checked 2026-11-15).
const SUPPORTED_PAIRS: [(&str, &str); 8] = [
    (ICP_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL),
    (ICP_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL),
    (ICP_LEDGER_PRINCIPAL, CKBTC_LEDGER_PRINCIPAL),
    (ICP_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL),
    (CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL),
    (CKUSDC_LEDGER_PRINCIPAL, CKBTC_LEDGER_PRINCIPAL),
    (CKUSDC_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL),
    (CKBTC_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL),
];

/// Assets a route may pass through between its input and its output.
const ROUTE_HUBS: [&str; 2] = [ICP_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL];

fn listed_ledger(text: &str) -> Principal {
    Principal::from_text(text).expect("a listed ICPSwap ledger principal must be valid")
}

/// True when a direct pool is listed for the two ledgers.
fn has_direct_pool(pay_ledger: &Principal, receive_ledger: &Principal) -> bool {
    // The listed constants are in canonical text form, so text compares exactly.
    let (pay, receive) = (pay_ledger.to_text(), receive_ledger.to_text());
    SUPPORTED_PAIRS
        .iter()
        .any(|(left, right)| (pay == *left && receive == *right) || (pay == *right && receive == *left))
}

/// Every listed path between two ledgers, shortest first: the direct pool,
/// then one hub, then both hubs. Each step of a path is a listed pool.
fn route_paths(pay_ledger: Principal, receive_ledger: Principal) -> Vec<Vec<Principal>> {
    if pay_ledger == receive_ledger {
        return Vec::new();
    }
    let hubs: Vec<Principal> = ROUTE_HUBS
        .iter()
        .map(|hub| listed_ledger(hub))
        .filter(|hub| *hub != pay_ledger && *hub != receive_ledger)
        .collect();

    let mut paths = Vec::new();
    if has_direct_pool(&pay_ledger, &receive_ledger) {
        paths.push(vec![pay_ledger, receive_ledger]);
    }
    for hub in &hubs {
        if has_direct_pool(&pay_ledger, hub) && has_direct_pool(hub, &receive_ledger) {
            paths.push(vec![pay_ledger, *hub, receive_ledger]);
        }
    }
    for first in &hubs {
        for second in &hubs {
            if first != second
                && has_direct_pool(&pay_ledger, first)
                && has_direct_pool(first, second)
                && has_direct_pool(second, &receive_ledger)
            {
                paths.push(vec![pay_ledger, *first, *second, receive_ledger]);
            }
        }
    }
    paths
}

/// The two ledgers of an ICP-chain pair. Ledger principals, not symbols,
/// decide every policy answer, so a mislabeled token cannot reach the venue.
fn pair_ledgers(pay_token: &ChainToken, receive_asset: &AssetId) -> Option<(Principal, Principal)> {
    let ChainToken::Icp { ledger: pay_ledger, .. } = pay_token else {
        return None;
    };
    if receive_asset.chain != "icp" {
        return None;
    }
    let receive_ledger = Principal::from_text(&receive_asset.address).ok()?;
    Some((*pay_ledger, receive_ledger))
}

/// Every listed path a request may take. Empty means ICPSwap cannot trade the pair.
pub(crate) fn listed_paths(pay_token: &ChainToken, receive_asset: &AssetId) -> Vec<Vec<Principal>> {
    pair_ledgers(pay_token, receive_asset)
        .map(|(pay, receive)| route_paths(pay, receive))
        .unwrap_or_default()
}

/// True when ICPSwap can trade this pair, in one pool or along a listed path.
pub(crate) fn supports_pair(pay_token: &ChainToken, receive_asset: &AssetId) -> bool {
    !listed_paths(pay_token, receive_asset).is_empty()
}

/// True when one listed pool trades this pair. A single execution needs this.
fn supports_direct_pair(pay_token: &ChainToken, receive_asset: &AssetId) -> bool {
    pair_ledgers(pay_token, receive_asset).is_some_and(|(pay, receive)| has_direct_pool(&pay, &receive))
}

/// The one refusal every caller reports for a pair ICPSwap does not list.
pub(crate) fn unsupported_pair(request: &SwapRequest) -> IcpswapQuoteError {
    IcpswapQuoteError::UnsupportedPair {
        pay_asset: request.pay_asset.to_string(),
        receive_asset: request.receive_asset.to_string(),
    }
}

/// Rejects a pair with no listed pool. One execution trades one pool only.
pub(crate) fn ensure_direct_pair(request: &SwapRequest) -> Result<(), IcpswapQuoteError> {
    if supports_direct_pair(&request.pay_amount.token, &request.receive_asset) {
        return Ok(());
    }
    Err(unsupported_pair(request))
}

#[cfg(test)]
mod pair_policy_tests {
    use candid::{Nat, Principal};
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;

    use super::{SUPPORTED_PAIRS, route_paths, supports_direct_pair, supports_pair};
    use crate::utils::{
        CKBTC_LEDGER_PRINCIPAL, CKETH_LEDGER_PRINCIPAL, CKUSDC_LEDGER_PRINCIPAL, CKUSDT_LEDGER_PRINCIPAL,
        ICP_LEDGER_PRINCIPAL,
    };

    const CKSOL_LEDGER: &str = "ls5lp-lqaaa-aaaar-qb5oa-cai";

    fn token(ledger: &str, symbol: &str) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_text(ledger).expect("test ledger"),
            symbol: symbol.to_string(),
            decimals: 8,
            fee: Nat::from(1u8),
        }
    }

    #[test]
    fn every_listed_pair_trades_in_both_directions() {
        for (left, right) in SUPPORTED_PAIRS {
            let (left, right) = (token(left, "LEFT"), token(right, "RIGHT"));

            assert!(supports_direct_pair(&left, &right.asset_id()));
            assert!(supports_direct_pair(&right, &left.asset_id()));
        }
    }

    fn ledger(text: &str) -> Principal {
        Principal::from_text(text).expect("test ledger")
    }

    #[test]
    fn a_pair_without_a_usable_pool_trades_only_along_a_path() {
        let ckusdt = token(CKUSDT_LEDGER_PRINCIPAL, "ckUSDT");
        let ckbtc = token(CKBTC_LEDGER_PRINCIPAL, "ckBTC");
        let cketh = token(CKETH_LEDGER_PRINCIPAL, "ckETH");

        for volatile in [&ckbtc, &cketh] {
            assert!(!supports_direct_pair(volatile, &ckusdt.asset_id()));
            assert!(!supports_direct_pair(&ckusdt, &volatile.asset_id()));
            assert!(supports_pair(volatile, &ckusdt.asset_id()));
            assert!(supports_pair(&ckusdt, &volatile.asset_id()));
        }
    }

    #[test]
    fn paths_run_direct_first_then_through_the_hubs() {
        let (icp, ckusdc) = (ledger(ICP_LEDGER_PRINCIPAL), ledger(CKUSDC_LEDGER_PRINCIPAL));
        let (ckusdt, ckbtc) = (ledger(CKUSDT_LEDGER_PRINCIPAL), ledger(CKBTC_LEDGER_PRINCIPAL));

        assert_eq!(
            route_paths(ckbtc, ckusdc),
            vec![vec![ckbtc, ckusdc], vec![ckbtc, icp, ckusdc]]
        );
        assert_eq!(
            route_paths(ckbtc, ckusdt),
            vec![
                vec![ckbtc, icp, ckusdt],
                vec![ckbtc, ckusdc, ckusdt],
                vec![ckbtc, icp, ckusdc, ckusdt],
                vec![ckbtc, ckusdc, icp, ckusdt],
            ]
        );
        assert_eq!(
            route_paths(icp, ckusdt),
            vec![vec![icp, ckusdt], vec![icp, ckusdc, ckusdt]]
        );
        assert!(route_paths(icp, icp).is_empty());
    }

    #[test]
    fn an_unlisted_asset_and_a_same_asset_pair_are_refused() {
        let icp = token(ICP_LEDGER_PRINCIPAL, "ICP");
        let ckusdc = token(CKUSDC_LEDGER_PRINCIPAL, "ckUSDC");
        let cksol = token(CKSOL_LEDGER, "ckSOL");

        assert!(!supports_pair(&cksol, &ckusdc.asset_id()));
        assert!(!supports_pair(&icp, &cksol.asset_id()));
        assert!(!supports_pair(&icp, &icp.asset_id()));
        assert!(!supports_pair(&ckusdc, &ckusdc.asset_id()));
    }

    #[test]
    fn uses_ledgers_not_symbols_for_pair_policy() {
        let fake_ckusdc = token(CKSOL_LEDGER, "ckUSDC");
        let icp = token(ICP_LEDGER_PRINCIPAL, "ICP");

        assert!(!supports_pair(&icp, &fake_ckusdc.asset_id()));
    }
}
