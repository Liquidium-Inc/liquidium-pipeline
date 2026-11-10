//! How fast a liquidation's capital comes back as spendable balance.
//! A leg is fast when an enabled venue takes the asset on ICP, and slow when it must bridge.

use liquidium_pipeline_core::tokens::chain_token::ChainToken;

/// How soon one leg of the round trip completes. Fast sorts first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SettlementSpeed {
    /// An enabled venue takes the asset on ICP, or the pair needs no venue.
    Direct,
    /// The leg crosses a bridge, or no enabled venue takes the asset.
    Delayed,
}

/// A pair of one asset needs no swap, so it touches no venue.
fn needs_no_venue(collateral: &ChainToken, repayment: &ChainToken) -> bool {
    collateral.asset_id().address == repayment.asset_id().address
}

/// Only a listed asset is fast. A missing bridge route is no evidence of speed.
fn listed_speed(symbol: &str, venue_native_symbols: &[String]) -> SettlementSpeed {
    if venue_native_symbols
        .iter()
        .any(|native| native.eq_ignore_ascii_case(symbol))
    {
        SettlementSpeed::Direct
    } else {
        SettlementSpeed::Delayed
    }
}

/// Both legs of a pair: the collateral to a venue, then the proceeds back as the debt asset.
/// `venue_native_symbols` is the union across enabled venues, because the planner picks the venue later.
pub fn pair_speeds(
    collateral: &ChainToken,
    repayment: &ChainToken,
    venue_native_symbols: &[String],
) -> (SettlementSpeed, SettlementSpeed) {
    if needs_no_venue(collateral, repayment) {
        return (SettlementSpeed::Direct, SettlementSpeed::Direct);
    }
    (
        listed_speed(&collateral.symbol(), venue_native_symbols),
        listed_speed(&repayment.symbol(), venue_native_symbols),
    )
}

/// How a venue returns proceeds in this debt asset, whatever the collateral.
/// Ranking weighs each debt with this, so every pair on one debt has the same weight.
pub fn debt_return_speed(repayment: &ChainToken, venue_native_symbols: &[String]) -> SettlementSpeed {
    listed_speed(&repayment.symbol(), venue_native_symbols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Nat, Principal};

    /// What `CexVenueProfile::mexc().direct_assets()` reports. A change there shows up here as a ranking change.
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
                pair_speeds(&icp_token(symbol), &icp_token("ckUSDC"), &mexc_native()).0,
                SettlementSpeed::Direct,
                "{symbol} is listed on the ICP network"
            );
        }
    }

    #[test]
    fn cketh_collateral_is_delayed_because_reaching_a_venue_needs_the_minter() {
        assert_eq!(
            pair_speeds(&icp_token("ckETH"), &icp_token("ckUSDT"), &mexc_native()).0,
            SettlementSpeed::Delayed
        );
    }

    /// MEXC lists ckUSDT on ICP but not ckUSDC, so the same collateral comes home fast against one debt only.
    #[test]
    fn the_return_leg_is_decided_by_the_debt_asset() {
        let ckbtc = icp_token("ckBTC");

        assert_eq!(
            pair_speeds(&ckbtc, &icp_token("ckUSDT"), &mexc_native()).1,
            SettlementSpeed::Direct
        );
        assert_eq!(
            pair_speeds(&ckbtc, &icp_token("ckUSDC"), &mexc_native()).1,
            SettlementSpeed::Delayed
        );
    }

    /// A same-asset pair has no legs, but its debt keeps the weight of its asset.
    /// Otherwise a same-asset dust pair would outrank collateral that covers the debt.
    #[test]
    fn a_same_asset_pair_has_no_slow_leg_but_its_debt_keeps_its_weight() {
        let ckusdc = icp_token("ckUSDC");
        let both_fast = (SettlementSpeed::Direct, SettlementSpeed::Direct);

        assert_eq!(pair_speeds(&ckusdc, &ckusdc, &mexc_native()), both_fast);
        assert_eq!(pair_speeds(&ckusdc, &ckusdc, &[]), both_fast);
        assert_eq!(debt_return_speed(&ckusdc, &mexc_native()), SettlementSpeed::Delayed);
        assert_eq!(
            debt_return_speed(&icp_token("ckUSDT"), &mexc_native()),
            SettlementSpeed::Direct
        );
    }

    /// ckSOL has no ckETH-minter route either, and no venue takes it on ICP.
    #[test]
    fn an_asset_no_venue_lists_is_delayed_rather_than_assumed_quick() {
        assert_eq!(
            pair_speeds(&icp_token("ckSOL"), &icp_token("ckUSDT"), &mexc_native()).0,
            SettlementSpeed::Delayed
        );
    }

    #[test]
    fn no_enabled_cex_leaves_both_legs_delayed() {
        for symbol in ["ckBTC", "ckETH", "ICP"] {
            assert_eq!(
                pair_speeds(&icp_token(symbol), &icp_token("ckUSDC"), &[]),
                (SettlementSpeed::Delayed, SettlementSpeed::Delayed),
                "{symbol} has no venue to reach"
            );
        }
        assert_eq!(debt_return_speed(&icp_token("ckUSDT"), &[]), SettlementSpeed::Delayed);
    }

    #[test]
    fn venue_native_symbols_are_matched_case_insensitively() {
        assert_eq!(
            pair_speeds(&icp_token("ckUSDT"), &icp_token("ckUSDC"), &["CKUSDT".to_string()]).0,
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
