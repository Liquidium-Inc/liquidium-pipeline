//! Costs a route pays from a balance other than the one that flows.
use candid::{CandidType, Nat};
use serde::{Deserialize, Serialize};

/// The symbol of a cycles cost. No oracle prices cycles, so the planner uses
/// the XDR rate: one trillion cycles is one XDR.
pub const CYCLES_SYMBOL: &str = "CYCLES";
/// Cycles count in trillions, the unit the XDR rate prices.
pub const CYCLES_DECIMALS: u8 = 12;

/// A route cost paid from another balance: ETH gas from the bridge wallet,
/// ckETH for a ckERC20 burn, or cycles for a minter call.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideCost {
    /// What the cost pays for, for logs and reports.
    pub label: String,
    /// The oracle symbol of the asset (`ETH` for ckETH), or `CYCLES_SYMBOL`.
    pub symbol: String,
    pub decimals: u8,
    /// In native units of the asset.
    pub amount: Nat,
}

impl SideCost {
    pub fn new(label: impl Into<String>, symbol: impl Into<String>, decimals: u8, amount: Nat) -> Self {
        Self {
            label: label.into(),
            symbol: symbol.into(),
            decimals,
            amount,
        }
    }
}
