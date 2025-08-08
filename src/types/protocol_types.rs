use core::fmt;

use candid::{CandidType, Nat, Principal};
use serde::{Deserialize, Serialize};

// Max liquidation ratio
pub const MAX_LIQUIDATION_RATIO: u64 = 500; // 50% of debt can be liquidated at once
// Bonus multipler scale
pub const LIQUIDATION_BONUS_SCALE: u128 = 10_000u128;

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationRequest {
    pub borrower: Principal,                // Portfolio to be liquidated
    pub debt_pool_id: Principal,            // Pool containing the debt to be repaid
    pub collateral_pool_id: Principal,      // Pool containing collateral to be liquidated
    pub debt_amount: Option<Nat>,           // Amount of debt to repay
    pub min_collateral_amount: Option<Nat>, // Minimum collateral amount to receive
}

pub trait Asset {
    fn decimals(&self) -> u32;
    fn symbol(&self) -> String;
}

#[derive(Debug, CandidType, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum Assets {
    BTC,
    SOL,
    USDC,
    USDT,
}

impl fmt::Display for Assets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self) // Uses Debug formatting, which works for simple enums
    }
}

impl Asset for Assets {
    fn decimals(&self) -> u32 {
        match self {
            Assets::BTC => 8,
            Assets::SOL => 18,
            Assets::USDC => 6,
            Assets::USDT => 6,
        }
    }

    fn symbol(&self) -> String {
        match self {
            Assets::BTC => "BTC".to_string(),
            Assets::SOL => "SOL".to_string(),
            Assets::USDC => "USDC".to_string(),
            Assets::USDT => "USDT".to_string(),
        }
    }
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationAmounts {
    pub collateral_received: Nat, // Amount of collateral received by liquidator
    pub debt_repaid: Nat,         // Amount of debt repaid
    pub bonus_earned: Nat,        // Value of bonus earned (in debt asset)
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationResult {
    pub amounts: LiquidationAmounts,
    pub tx_id: String,               // Reference to the collateral claim transaction
    pub collateral_asset: AssetType, // The collateral type that the liquidator receives
    pub debt_asset: AssetType,       // The debt type that the liquidator spends
    pub status: LiquidationStatus,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AssetType {
    CkAsset(Principal), // Only one collateral type for now
    Unknown,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum LiquidationStatus {
    Success,
    Failed(String),
}

impl fmt::Display for LiquidationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[tx_id: {}, collateral: {:?}, debt: {:?}, status: {:?}]",
            self.tx_id, self.collateral_asset, self.debt_asset, self.status,
        )
    }
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct LiquidateblePosition {
    pub pool_id: Principal,     // Pool containing the debt to be repaid
    pub debt_amount: Nat,       // The debt amount
    pub collateral_amount: Nat, // The collateral amount on this position
    pub asset: Assets,          // The debt asset
    pub asset_type: AssetType,  // The collateral type
    pub account: Principal,     // Account that will be liquidated
    pub liquidation_bonus: Nat, // The bonus received when liquidation this position
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct LiquidatebleUser {
    pub account: Principal,                   // The user's account
    pub health_factor: Nat,                   // The user's health factor
    pub positions: Vec<LiquidateblePosition>, // The user's positions
    pub total_debt: Nat,                      // Users total debt in $
}
