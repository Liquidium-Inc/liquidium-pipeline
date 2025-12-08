use core::fmt;

use candid::{CandidType, Nat, Principal};
use serde::{Deserialize, Serialize};

// Max liquidation ratio
pub const MAX_LIQUIDATION_RATIO: u64 = 500; // 50% of debt can be liquidated at once

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationRequest {
    pub borrower: Principal,           // Portfolio to be liquidated
    pub debt_pool_id: Principal,       // Pool containing the debt to be repaid
    pub collateral_pool_id: Principal, // Pool containing collateral to be liquidated
    pub debt_amount: Nat,              // Amount of debt to repay
    pub receiver_address: Principal,   // Account that receives collateral and change
    pub buy_bad_debt: bool
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
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationResult {
    pub amounts: LiquidationAmounts,
    pub collateral_asset: AssetType, // The collateral type that the liquidator receives
    pub debt_asset: AssetType,       // The debt type that the liquidator spends
    pub status: LiquidationStatus,
    pub change_tx: TxStatus,
    pub collateral_tx: TxStatus,
    pub id: u128,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Success,
    Failed(String),
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TxStatus {
    // Some chains may not have a tx id yet (queued) — keep it optional.
    pub tx_id: Option<String>,
    pub status: TransferStatus,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum LiquidationStatus {
    Success,
    FailedLiquidation(String),
    CollateralTransferFailed(String),
    ChangeTransferFailed(String),
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AssetType {
    CkAsset(Principal), // Only one collateral type for now
    Unknown,
}

impl fmt::Display for LiquidationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[collateral: {:?}, debt: {:?}, status: {:?}]",
            self.collateral_asset, self.debt_asset, self.status,
        )
    }
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct LiquidateblePosition {
    pub pool_id: Principal,         // Pool containing the debt to be repaid
    pub debt_amount: Nat,           // The debt amount
    pub collateral_amount: Nat,     // The collateral amount on this position
    pub asset: Assets,              // The debt asset
    pub asset_type: AssetType,      // The collateral type
    pub account: Principal,         // Account that will be liquidated
    pub liquidation_bonus: u64,     // The collateral pool's liquidation bonus in  (in basis points, 1000 = 10%)
    pub protocol_fee: u64,          // The fee on the liquidation bonus un  (in basis points, 1000 = 10%)
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct LiquidatebleUser {
    pub account: Principal,                   // The user's account
    pub health_factor: Nat,                   // The user's health factor
    pub positions: Vec<LiquidateblePosition>, // The user's positions
    pub total_debt: Nat,                      // Users total debt in $
}
