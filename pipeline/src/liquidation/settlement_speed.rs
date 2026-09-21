//! How quickly a liquidation's capital returns as spendable balance.
//!
//! Every liquidation spends wallet balance to repay debt and only gets that
//! capital back once the seized collateral has been sold and the proceeds have
//! come home. That round trip has two legs, and each is independently fast or
//! slow:
//!
//! - **Out**: the seized collateral travels to a venue. A collateral the venue
//!   lists on the ICP network takes one ICRC-1 transfer; anything else burns
//!   through the ckETH minter and waits on Ethereum.
//! - **Back**: the sale proceeds return as the repayment asset. This is set by
//!   the *debt* asset, not the collateral -- MEXC lists ckUSDT on the ICP
//!   network but not ckUSDC, so repaying ckUSDT comes home in seconds while
//!   repaying ckUSDC withdraws USDC on Ethereum and bridges back.
//!
//! Both legs cost 15-20 minutes when they bridge, and the two are ranked
//! differently because they trade off against different things. The outbound
//! leg is nearly free to choose: given two collateral positions that can each
//! back the same repayment, either seizes the same value, so only time
//! differs. The return leg is not -- it is fixed by which debt you repay, and a
//! bigger debt earns a bigger bonus. See `simple_strategy` for how each is
//! weighted.
//!
//! The classification is deliberately coarse and purely local -- no quotes, no
//! network calls -- because it only orders candidates and never decides an
//! actual route. The finalizer's `BridgePlanner` remains the single authority
//! on what a leg really does.

use liquidium_pipeline_core::tokens::chain_token::ChainToken;

/// How soon one leg of the round trip completes.
///
/// Ordered so that sorting ascending puts the fast leg first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SettlementSpeed {
    /// No bridge stands in the way: a venue lists the asset on the ICP network
    /// and one ICRC-1 transfer carries it, or no venue is involved at all.
    Direct,
    /// The leg has to cross the ckETH minter, or no enabled venue takes the
    /// asset and we cannot claim it is quick.
    Delayed,
}

/// Whether both sides of a pair are one asset, in which case the strategy
/// skips the swap entirely and neither leg of the round trip exists.
fn needs_no_venue(collateral: &ChainToken, repayment: &ChainToken) -> bool {
    collateral.asset_id().address == repayment.asset_id().address
}

fn lists_natively(symbol: &str, venue_native_symbols: &[String]) -> bool {
    venue_native_symbols
        .iter()
        .any(|native| native.eq_ignore_ascii_case(symbol))
}

/// How the seized collateral reaches the venue that sells it.
///
/// Only a venue actually listing the asset earns [`SettlementSpeed::Direct`].
/// Nothing is inferred from the *absence* of a bridge route -- an asset the
/// ckETH minter has no path for is not therefore fast, it is an asset we have
/// no described route for at all, and ranking must not float it above
/// collateral we know settles in seconds. Assuming delay is the safe direction
/// to be wrong in: the cost is a missed reordering, not capital parked for a
/// quarter of an hour.
///
/// `venue_native_symbols` is the *union* across enabled CEX venues, so
/// `Direct` means "some enabled venue takes this on the ICP network", not "the
/// venue this leg lands on takes it". Which venue receives the leg is a
/// quote-time decision the planner makes long after ranking, so no cheaper
/// answer exists here. A venue that cannot take the asset at all fails its own
/// funding preflight rather than acting on this hint.
///
/// The list deliberately covers CEX venues only. ICPSwap settles everything on
/// the IC, but the waterfall sends it only what its liquidity can absorb
/// without moving the price, so treating it as making every asset direct would
/// flatten the distinction this exists to draw.
pub fn deposit_speed(
    collateral: &ChainToken,
    repayment: &ChainToken,
    venue_native_symbols: &[String],
) -> SettlementSpeed {
    if needs_no_venue(collateral, repayment) || lists_natively(&collateral.symbol(), venue_native_symbols) {
        SettlementSpeed::Direct
    } else {
        SettlementSpeed::Delayed
    }
}

/// How the sale proceeds come back as the repayment asset.
///
/// Determined by the debt asset, because that is what the venue must withdraw:
/// `resolve_bridge_plan_for_assets` takes the collateral as its deposit and the
/// repayment token as its withdrawal. A debt the venue lists on the ICP network
/// is withdrawn straight to an ICRC-1 account; anything else is withdrawn on
/// Ethereum and bridged home, which is why repaying ckUSDC costs 15-20 minutes
/// that repaying ckUSDT does not.
pub fn repayment_speed(
    collateral: &ChainToken,
    repayment: &ChainToken,
    venue_native_symbols: &[String],
) -> SettlementSpeed {
    if needs_no_venue(collateral, repayment) || lists_natively(&repayment.symbol(), venue_native_symbols) {
        SettlementSpeed::Direct
    } else {
        SettlementSpeed::Delayed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};

    /// What `venue_icp_native_assets("mexc")` reports, spelled out so a change
    /// there shows up here as a ranking change rather than passing unnoticed.
    fn mexc_native() -> Vec<String> {
        vec!["ckUSDT".to_string(), "ckBTC".to_string(), "ICP".to_string()]
    }

    fn icp_token(symbol: &str) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_slice(symbol.as_bytes()),
            symbol: symbol.to_string(),
            decimals: 8,
            fee: Nat::from(10u8),
        }
    }

    #[test]
    fn collateral_the_venue_lists_goes_out_directly() {
        for symbol in ["ckBTC", "ckUSDT", "ICP"] {
            assert_eq!(
                deposit_speed(&icp_token(symbol), &icp_token("ckUSDC"), &mexc_native()),
                SettlementSpeed::Direct,
                "{symbol} is listed on the ICP network"
            );
        }
    }

    #[test]
    fn cketh_collateral_is_delayed_because_reaching_a_venue_needs_the_minter() {
        assert_eq!(
            deposit_speed(&icp_token("ckETH"), &icp_token("ckUSDT"), &mexc_native()),
            SettlementSpeed::Delayed
        );
    }

    /// The return leg follows the debt asset, not the collateral. MEXC lists
    /// ckUSDT on the ICP network but not ckUSDC, so the same ckBTC collateral
    /// comes home in seconds against one debt and bridges against the other.
    #[test]
    fn the_return_leg_is_decided_by_the_debt_asset() {
        let ckbtc = icp_token("ckBTC");

        assert_eq!(
            repayment_speed(&ckbtc, &icp_token("ckUSDT"), &mexc_native()),
            SettlementSpeed::Direct
        );
        assert_eq!(
            repayment_speed(&ckbtc, &icp_token("ckUSDC"), &mexc_native()),
            SettlementSpeed::Delayed
        );
    }

    /// Collateral that is already the repayment asset touches no venue at all,
    /// so neither leg of the round trip exists.
    #[test]
    fn a_same_asset_pair_has_no_slow_leg_on_either_side() {
        // ckUSDC is in no venue's native list, so both legs would otherwise
        // read as delayed.
        let ckusdc = icp_token("ckUSDC");

        assert_eq!(deposit_speed(&ckusdc, &ckusdc, &mexc_native()), SettlementSpeed::Direct);
        assert_eq!(
            repayment_speed(&ckusdc, &ckusdc, &mexc_native()),
            SettlementSpeed::Direct
        );
        // And with no venue enabled at all, since no venue is consulted.
        assert_eq!(deposit_speed(&ckusdc, &ckusdc, &[]), SettlementSpeed::Direct);
        assert_eq!(repayment_speed(&ckusdc, &ckusdc, &[]), SettlementSpeed::Direct);
    }

    #[test]
    fn an_asset_no_venue_lists_is_delayed_rather_than_assumed_quick() {
        // ckSOL has no ckETH-minter route either, and an earlier version read
        // that absence as "nothing to bridge, therefore fast". No venue takes
        // it on the IC, so nothing here justifies ranking it ahead of ckBTC.
        assert_eq!(
            deposit_speed(&icp_token("ckSOL"), &icp_token("ckUSDT"), &mexc_native()),
            SettlementSpeed::Delayed
        );
    }

    #[test]
    fn no_enabled_cex_leaves_both_legs_delayed() {
        for symbol in ["ckBTC", "ckETH", "ICP"] {
            assert_eq!(
                deposit_speed(&icp_token(symbol), &icp_token("ckUSDC"), &[]),
                SettlementSpeed::Delayed,
                "{symbol} has no venue to reach"
            );
        }
        assert_eq!(
            repayment_speed(&icp_token("ckBTC"), &icp_token("ckUSDT"), &[]),
            SettlementSpeed::Delayed
        );
    }

    #[test]
    fn venue_native_symbols_are_matched_case_insensitively() {
        assert_eq!(
            deposit_speed(&icp_token("ckUSDT"), &icp_token("ckUSDC"), &["CKUSDT".to_string()]),
            SettlementSpeed::Direct
        );
    }

    #[test]
    fn direct_sorts_ahead_of_delayed() {
        let mut speeds = vec![SettlementSpeed::Delayed, SettlementSpeed::Direct];
        speeds.sort();

        assert_eq!(speeds, vec![SettlementSpeed::Direct, SettlementSpeed::Delayed]);
    }
}
