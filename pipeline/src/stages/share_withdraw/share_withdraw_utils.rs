//! Small helpers that every part of the withdraw step uses.
use super::*;

impl<D: WalStore> ShareWithdrawWatcher<D> {
    pub(super) fn destination(&self, entry: &HeldRow) -> Principal {
        if entry.wrapper.receipt.request.swap_args.is_some() {
            self.trader
        } else {
            self.liquidator
        }
    }

    /// Returns the least withdraw that the bot sends for the row's collateral.
    pub(super) fn min_withdraw(entry: &HeldRow) -> Nat {
        entry.wrapper.receipt.request.collateral_asset.fee() * Nat::from(MIN_WITHDRAW_FEES)
    }

    /// Returns an amount of the row's collateral as text with its symbol.
    pub(super) fn shown(entry: &HeldRow, amount: &Nat) -> String {
        ChainTokenAmount::from_raw(entry.wrapper.receipt.request.collateral_asset.clone(), amount.clone()).formatted()
    }
}
