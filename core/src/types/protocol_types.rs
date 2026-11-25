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
    pub buy_bad_debt: bool,
    /// Credits collateral supply shares to this existing profile of the caller’s default ICP account.
    /// Pays underlying tokens to `receiver_address` when absent.
    #[serde(default)]
    pub collateral_share_receiver: Option<Principal>,
}

pub trait Asset {
    fn decimals(&self) -> u32;
    fn symbol(&self) -> String;
}

#[derive(Debug, CandidType, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum Assets {
    BTC,
    ICP,
    SOL,
    USDC,
    USDT,
    ETH,
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
            Assets::ICP => 8,
            Assets::SOL => 9,
            Assets::USDC => 6,
            Assets::USDT => 6,
            Assets::ETH => 18,
        }
    }

    fn symbol(&self) -> String {
        match self {
            Assets::BTC => "BTC".to_string(),
            Assets::ICP => "ICP".to_string(),
            Assets::SOL => "SOL".to_string(),
            Assets::USDC => "USDC".to_string(),
            Assets::USDT => "USDT".to_string(),
            Assets::ETH => "ETH".to_string(),
        }
    }
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationAmounts {
    pub collateral_received: Nat, // Amount of collateral received by liquidator
    pub debt_repaid: Nat,         // Amount of debt repaid
}

/// The pool's record of what one liquidation still owes, from
/// `get_liquidation_inflow`. The pool deletes it once every payment has landed.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationInflowRecord {
    pub debt_amount: Nat,
    pub collateral_pool_id: Principal,
    pub timestamp: u64,
    pub liquidator: Principal,
    pub debt_pool_id: Principal,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LiquidationResult {
    pub amounts: LiquidationAmounts,
    pub collateral_asset: AssetType, // The collateral type that the liquidator receives
    pub debt_asset: AssetType,       // The debt type that the liquidator spends
    pub status: LiquidationStatus,
    pub timestamp: u64, // Seconds since Unix epoch
    pub change_tx: TxStatus,
    pub collateral_tx: TxStatus,
    pub id: u128, // The liquidation id
    /// Identifies the profile credited with collateral shares, or `None` when no shares were seized.
    #[serde(default)]
    pub collateral_share_receiver: Option<Principal>,
}

/// The destination of a lending `withdraw`. The bot uses only the `Native` kind of the canister.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum WithdrawDestination {
    Native(Principal),
}

/// Identifies the signer of a lending request as the caller of the call.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IcpCallerAuth {
    pub principal: Principal,
    pub subaccount: Option<Vec<u8>>,
}

/// The signature kinds of a lending request that the bot uses.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum LendingSignature {
    IcpCaller(IcpCallerAuth),
}

/// A lending `withdraw` request. An `amount` of 0 withdraws the whole position.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LendingWithdrawRequest {
    pub expiry_timestamp: u64, // Seconds since Unix epoch, at most 300 s ahead
    pub account: WithdrawDestination,
    pub pool_id: Principal,
    pub amount: Nat,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedLendingWithdraw {
    pub data: LendingWithdrawRequest,
    pub signature_info: LendingSignature,
}

/// The part of the canister's `OutflowDetails` that the bot reads.
/// `id` is also the memo of the ledger transfer that pays the withdraw.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LendingOutflow {
    pub id: String,
    pub amount: Nat,
}

/// The part of the canister's `PositionView` that the bot reads.
#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LendingPositionView {
    pub deposited_native_now: Nat,
    /// The supply shares. Interest does not change this number.
    pub deposit_scaled: Nat,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Success,
    Failed(String),
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SignatureVerificationError {
    InvalidEthSignature,
    IcpCallerMismatch,
    UnsupportedChain,
    InvalidSolSignature,
    InvalidEthAddress,
    AnonymousIcpCaller,
    CouldNotDecode(String),
    ProfileNotFound,
    InvalidBtcSignature,
    InvalidIcpSubaccount,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    PositionNotFound,
    Internal(String),
    InvalidAddress(String),
    InvalidTargetPrincipal,
    SignatureError(SignatureVerificationError),
    SupplyCapExceeded,
    AccountNotFound,
    NotAllowed(String),
    FeeClaimReceiverNotConfigured,
    InsufficientCollateral,
    ProfileNotFound,
    SignatureExpiryTooFarInFuture,
    MaxLtvExceeded,
    ProfileAlreadyExists,
    HealthFactorTooLow,
    NoLiquidity,
    SignatureExpired,
    PoolNotFound(String),
    BorrowingDisabled,
    SameAssetBorrowingDisabled,
    BorrowCapExceeded,
    PoolFrozen,
    TransferFailed(String),
    AccountAlreadyLinked,
    CannotRemoveSoleAccount,
    InsufficientFunds,
    LiquidationNotFound(String),
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
    InflowProcessed,
    CoreExecuted,
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
    pub liquidation_threshold: u64, // The collateral pool's liquidation threshold (bps)
    pub protocol_fee: u64,          // The fee on the liquidation bonus un  (in basis points, 1000 = 10%)
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct ScanResult {
    pub users: Vec<LiquidatebleUser>,
    pub next_cursor: Option<Principal>,
    pub scanned: u64,
}

#[derive(Debug, Clone, Deserialize, CandidType, PartialEq, Eq)]
pub struct LiquidatebleUser {
    pub account: Principal,                   // The user's account
    pub health_factor: Nat,                   // The user's health factor
    pub weighted_liquidation_threshold: Nat,  // Weighted liquidation threshold (bps)
    pub positions: Vec<LiquidateblePosition>, // The user's positions
    pub total_debt: Nat,                      // Users total debt in $
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InitializeAccountRequest {
    pub expiry_timestamp: u64,
}

#[derive(Debug, CandidType, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedInitializeAccountRequest {
    pub data: InitializeAccountRequest,
    pub signature_info: LendingSignature,
}

#[cfg(test)]
mod share_payout_tests {
    use super::*;
    use candid::{Decode, Encode};

    // Matches the liquidation request in lending.did at 334c66ac.
    #[derive(CandidType, Deserialize)]
    struct LendingRequest {
        borrower: Principal,
        debt_pool_id: Principal,
        collateral_pool_id: Principal,
        debt_amount: Nat,
        receiver_address: Principal,
        buy_bad_debt: bool,
        collateral_share_receiver: Option<Principal>,
    }

    #[test]
    fn both_payout_modes_encode_the_explicit_optional_profile() {
        let profile = Principal::self_authenticating(b"lending profile");
        let receiver = Principal::self_authenticating(b"token receiver");
        for payout in [None, Some(profile)] {
            let request = LiquidationRequest {
                borrower: Principal::anonymous(),
                debt_pool_id: Principal::management_canister(),
                collateral_pool_id: Principal::management_canister(),
                debt_amount: Nat::from(7u8),
                receiver_address: receiver,
                buy_bad_debt: false,
                collateral_share_receiver: payout,
            };
            let wire = Decode!(&Encode!(&request).unwrap(), LendingRequest).unwrap();
            assert_eq!(wire.collateral_share_receiver, payout);
            assert_eq!(wire.receiver_address, receiver);
            let decoded = Decode!(&Encode!(&wire).unwrap(), LiquidationRequest).unwrap();
            assert_eq!(decoded, request);
        }
    }
}
