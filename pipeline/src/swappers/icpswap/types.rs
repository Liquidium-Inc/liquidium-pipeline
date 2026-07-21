use candid::{CandidType, Int, Nat, Principal};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// ICPSwap's public token descriptor. The address is text in the upstream
/// Candid interface even though it contains a ledger principal.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapToken {
    pub address: String,
    pub standard: String,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapGetPoolArgs {
    pub token0: IcpswapToken,
    pub token1: IcpswapToken,
    pub fee: Nat,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapPoolData {
    pub key: String,
    pub token0: IcpswapToken,
    pub token1: IcpswapToken,
    pub fee: Nat,
    #[serde(rename = "tickSpacing")]
    pub tick_spacing: Int,
    #[serde(rename = "canisterId")]
    pub canister_id: Principal,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapError {
    CommonError,
    InternalError(String),
    UnsupportedToken(String),
    InsufficientFunds,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapResult<T> {
    #[serde(rename = "ok")]
    Ok(T),
    #[serde(rename = "err")]
    Err(IcpswapError),
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapSwapArgs {
    #[serde(rename = "zeroForOne")]
    pub zero_for_one: bool,
    #[serde(rename = "amountIn")]
    pub amount_in: String,
    #[serde(rename = "amountOutMinimum")]
    pub amount_out_minimum: String,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapDepositAndSwapArgs {
    #[serde(rename = "zeroForOne")]
    pub zero_for_one: bool,
    #[serde(rename = "tokenInFee")]
    pub token_in_fee: Nat,
    #[serde(rename = "tokenOutFee")]
    pub token_out_fee: Nat,
    #[serde(rename = "amountIn")]
    pub amount_in: String,
    #[serde(rename = "amountOutMinimum")]
    pub amount_out_minimum: String,
}

/// Complete pool-specific quote data needed by a later durable execution.
/// All token quantities remain in ledger-native integer units.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapExecutionPlan {
    pub pool: Principal,
    pub token_in: Principal,
    pub token_out: Principal,
    pub token0: Principal,
    pub token1: Principal,
    pub zero_for_one: bool,
    pub fee_tier: Nat,
    pub amount_in: ChainTokenAmount,
    pub gross_quoted_out: ChainTokenAmount,
    pub output_ledger_fee: ChainTokenAmount,
    pub net_expected_output: ChainTokenAmount,
    pub max_slippage_bps: u32,
    pub amount_out_minimum: ChainTokenAmount,
    pub quoted_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapPlanError {
    #[error("ICPSwap only supports ICP ledger tokens, got {0}")]
    NonIcpToken(String),
    #[error(
        "token pair does not match pool ordering: input={token_in}, output={token_out}, token0={token0}, token1={token1}"
    )]
    UnsupportedTokenPair {
        token_in: Principal,
        token_out: Principal,
        token0: Principal,
        token1: Principal,
    },
    #[error("max slippage {0} bps exceeds 10000 bps")]
    InvalidSlippage(u32),
    #[error("output ledger fee {fee} exceeds gross quote {gross}")]
    OutputFeeExceedsQuote { gross: Nat, fee: Nat },
    #[error("{field} token does not match quoted output token")]
    OutputTokenMismatch { field: &'static str },
}
