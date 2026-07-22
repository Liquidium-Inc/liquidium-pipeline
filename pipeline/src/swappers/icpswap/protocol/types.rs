use candid::{CandidType, Int, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
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
pub struct IcpswapDepositArgs {
    pub token: String,
    pub amount: Nat,
    pub fee: Nat,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapWithdrawArgs {
    pub token: String,
    pub fee: Nat,
    pub amount: Nat,
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
    pub input_ledger_fee: ChainTokenAmount,
    pub gross_quoted_out: ChainTokenAmount,
    pub output_ledger_fee: ChainTokenAmount,
    pub net_expected_output: ChainTokenAmount,
    pub max_slippage_bps: u32,
    pub amount_out_minimum: ChainTokenAmount,
    pub quoted_at: u64,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapStep {
    Deposit,
    DepositPending,
    Trade,
    TradePending,
    Withdraw,
    WithdrawPending,
    Recover,
    RecoverPending,
    Completed,
    Refunded,
    OperatorRequired,
    Failed,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapDepositState {
    #[serde(default, rename = "deposit_input_pool_balance_before")]
    pub input_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "deposit_output_pool_balance_before")]
    pub output_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "deposit_approval_block_index")]
    pub approval_block_index: Option<Nat>,
    #[serde(default, rename = "deposit_approval_created_at")]
    pub approval_created_at: Option<u64>,
    #[serde(default, rename = "deposit_args")]
    pub deposit_args: Option<IcpswapDepositArgs>,
    #[serde(default, rename = "deposit_returned_amount")]
    pub deposit_returned_amount: Option<Nat>,
    #[serde(default, rename = "deposit_submitted_at")]
    pub deposit_submitted_at: Option<u64>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapTradeState {
    /// Absolute gross-output floor accepted with the original route.
    #[serde(rename = "trade_original_hard_minimum_out")]
    pub original_hard_minimum_out: ChainTokenAmount,
    /// Number of retry evaluations already entered after the initial attempt.
    #[serde(default, rename = "trade_retry_count")]
    pub retry_count: u32,
    #[serde(default, rename = "trade_effective_slippage_bps")]
    pub effective_slippage_bps: u32,
    #[serde(default, rename = "trade_current_quote")]
    pub current_quote: Option<ChainTokenAmount>,
    #[serde(rename = "trade_current_amount_out_minimum")]
    pub current_amount_out_minimum: ChainTokenAmount,
    #[serde(default, rename = "trade_next_retry_at_nanos")]
    pub next_retry_at_nanos: Option<u64>,
    #[serde(default, rename = "trade_input_pool_balance_before")]
    pub input_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "trade_output_pool_balance_before")]
    pub output_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "trade_swap_args")]
    pub swap_args: Option<IcpswapSwapArgs>,
    #[serde(default, rename = "trade_swap_returned_amount")]
    pub swap_returned_amount: Option<Nat>,
    #[serde(default, rename = "trade_swap_protocol_error")]
    pub swap_protocol_error: Option<IcpswapError>,
    #[serde(default, rename = "trade_swap_submitted_at")]
    pub swap_submitted_at: Option<u64>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapWithdrawState {
    #[serde(default, rename = "withdraw_pool_balance_before")]
    pub pool_balance_before: Option<Nat>,
    #[serde(default, rename = "withdraw_wallet_balance_before")]
    pub wallet_balance_before: Option<Nat>,
    #[serde(default, rename = "withdraw_args")]
    pub withdraw_args: Option<IcpswapWithdrawArgs>,
    #[serde(default, rename = "withdraw_returned_amount")]
    pub withdraw_returned_amount: Option<Nat>,
    #[serde(default, rename = "withdraw_submitted_at")]
    pub withdraw_submitted_at: Option<u64>,
    #[serde(default, rename = "withdraw_wallet_credited_amount")]
    pub wallet_credited_amount: Option<Nat>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapManualRecoveryState {
    #[serde(default, rename = "recovery_pool_balance_before")]
    pub pool_balance_before: Option<Nat>,
    #[serde(default, rename = "recovery_wallet_balance_before")]
    pub wallet_balance_before: Option<Nat>,
    #[serde(default, rename = "recovery_withdraw_args")]
    pub withdraw_args: Option<IcpswapWithdrawArgs>,
    #[serde(default, rename = "recovery_withdraw_returned_amount")]
    pub withdraw_returned_amount: Option<Nat>,
    #[serde(default, rename = "recovery_withdraw_submitted_at")]
    pub withdraw_submitted_at: Option<u64>,
    #[serde(default, rename = "recovery_wallet_credited_amount")]
    pub wallet_credited_amount: Option<Nat>,
}

/// CEX-style durable state for the official manual ICPSwap workflow.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapState {
    pub execution_id: String,
    pub owner: Account,
    pub step: IcpswapStep,
    #[serde(default)]
    pub operator_pending_step: Option<IcpswapStep>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub plan: IcpswapExecutionPlan,
    #[serde(flatten)]
    pub deposit: IcpswapDepositState,
    #[serde(flatten)]
    pub trade: IcpswapTradeState,
    #[serde(flatten)]
    pub withdraw: IcpswapWithdrawState,
    #[serde(flatten)]
    pub recovery: IcpswapManualRecoveryState,
}

/// The only supported durable ICPSwap execution state. Older one-step records
/// intentionally fail deserialization and cannot be resumed.
pub type IcpswapExecutionState = IcpswapState;

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
    #[error("{field} token does not match quoted input token")]
    InputTokenMismatch { field: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapClientError {
    #[error("failed to encode arguments for ICPSwap {method}: {message}")]
    Encode { method: &'static str, message: String },
    #[error("ICPSwap {method} call to {canister} failed: {message}")]
    Transport {
        canister: Principal,
        method: &'static str,
        message: String,
    },
    #[error("ICPSwap {method} returned an error: {error:?}")]
    Protocol { method: &'static str, error: IcpswapError },
    #[error("ICRC-1 fee lookup failed on ledger {ledger}: {message}")]
    LedgerFee { ledger: Principal, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapManualClientError {
    #[error("ICRC-1 balance lookup failed on ledger {ledger}: {message}")]
    LedgerBalance { ledger: Principal, message: String },
    #[error("ICRC-2 allowance lookup failed on ledger {ledger}: {message}")]
    Allowance { ledger: Principal, message: String },
    #[error("ICRC-2 approval failed on ledger {ledger}: {message}")]
    Approval { ledger: Principal, message: String },
    #[error("ICPSwap query {method} to {pool} failed: {message}")]
    Query {
        pool: Principal,
        method: &'static str,
        message: String,
    },
    #[error("failed to encode arguments for ICPSwap {method}: {message}")]
    Encode { method: &'static str, message: String },
    #[error("ICPSwap {method} submission to {pool} has an ambiguous outcome: {message}")]
    SubmissionUnknown {
        pool: Principal,
        method: &'static str,
        message: String,
    },
    #[error("ICPSwap {method} returned an error: {error:?}")]
    Protocol { method: &'static str, error: IcpswapError },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcpswapApprovalRequest {
    pub ledger: Principal,
    pub owner: Account,
    pub spender: Account,
    pub current_allowance: Nat,
    pub required_allowance: Nat,
    pub created_at_time: u64,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapUnusedBalance {
    pub balance0: Nat,
    pub balance1: Nat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcpswapTokenMetadata {
    pub token: ChainToken,
    pub standard: String,
}

#[derive(Debug, Clone)]
pub struct IcpswapRoutePreview {
    pub quote: super::super::model::SwapQuote,
    pub route: IcpswapExecutionPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapQuoteError {
    #[error("ICPSwap fee tiers cannot be empty")]
    EmptyFeeTiers,
    #[error("default ICPSwap slippage {0} bps exceeds 10000 bps")]
    InvalidDefaultSlippage(u32),
    #[error("ICPSwap token metadata is missing for {0}")]
    MissingToken(String),
    #[error("swap request pay asset does not match pay amount token")]
    PayAssetMismatch,
    #[error("ICPSwap input budget {budget} cannot cover approval and transfer fees totaling {fees}")]
    InputFeesExceedBudget { budget: Nat, fees: Nat },
    #[error("invalid principal '{address}' returned as {field}: {message}")]
    InvalidPoolPrincipal {
        field: &'static str,
        address: String,
        message: String,
    },
    #[error("pool {pool} returned fee tier {actual}, expected {expected}")]
    PoolFeeMismatch {
        pool: Principal,
        expected: Nat,
        actual: Nat,
    },
    #[error("ledger fee lookup failed: {0}")]
    LedgerFee(IcpswapClientError),
    #[error(transparent)]
    Plan(#[from] IcpswapPlanError),
    #[error("no usable ICPSwap pool quote: {failures:?}")]
    NoUsablePools { failures: Vec<String> },
}
