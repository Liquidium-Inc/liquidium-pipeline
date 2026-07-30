use candid::{CandidType, Int, Nat, Principal};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::identity::IcpswapExecutionIdentity;
pub use super::transfer_state::{
    IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementKind, IcpswapSettlementState,
};

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

/// Price-bearing subset of the ICPSwap pool's public `metadata` response.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapPoolMetadata {
    pub token0: IcpswapToken,
    pub token1: IcpswapToken,
    #[serde(rename = "sqrtPriceX96")]
    pub sqrt_price_x96: Nat,
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
    pub zero_for_one: bool,
    pub fee_tier: Nat,
    pub amount_in: ChainTokenAmount,
    pub input_ledger_fee: ChainTokenAmount,
    pub gross_quoted_out: ChainTokenAmount,
    pub output_ledger_fee: ChainTokenAmount,
    pub max_slippage_bps: u32,
    pub amount_out_minimum: ChainTokenAmount,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapStep {
    Funding,
    FundingPending,
    FundingSurplusPending,
    Transfer,
    TransferPending,
    Deposit,
    DepositPending,
    Trade,
    TradePending,
    Withdraw,
    WithdrawPending,
    Recover,
    RecoverPending,
    Forward,
    ForwardPending,
    Completed,
    Refunded,
    OperatorRequired,
    Failed,
}

pub const ICPSWAP_STATE_VERSION: u32 = 2;

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapTransferState {
    #[serde(default, rename = "transfer_args")]
    pub args: Option<TransferArg>,
    #[serde(default, rename = "transfer_block_index")]
    pub block_index: Option<Nat>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapDepositState {
    #[serde(default, rename = "deposit_input_pool_balance_before")]
    pub input_pool_balance_before: Option<Nat>,
    /// Ledger balance of the pool deposit subaccount immediately before the
    /// canister deposit call. An unchanged value proves that no sweep occurred.
    #[serde(default, rename = "deposit_input_ledger_balance_before")]
    pub input_ledger_balance_before: Option<Nat>,
    /// The deposit intent and both reconciliation baselines are durable, but
    /// the canister call has not yet been attempted by the current state.
    #[serde(default, rename = "deposit_ready_to_submit")]
    pub ready_to_submit: bool,
    /// Read-only reconciliation attempts made after an ambiguous deposit call.
    #[serde(default, rename = "deposit_observation_attempts")]
    pub observation_attempts: u32,
    /// Safe deposit-only resubmissions made after proving no sweep occurred.
    #[serde(default, rename = "deposit_submission_retry_count")]
    pub submission_retry_count: u32,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapTradeState {
    /// Number of retry evaluations scheduled after the initial attempt. This
    /// bounds all automatic trade retries, regardless of why they were needed.
    #[serde(default, rename = "trade_retry_count")]
    pub retry_evaluation_count: u32,
    /// Number of confirmed slippage failures. Only this counter widens the
    /// `amountOutMinimum` tolerance used by a later attempt.
    #[serde(default, rename = "trade_slippage_retry_count")]
    pub slippage_retry_count: u32,
    #[serde(rename = "trade_current_amount_out_minimum")]
    pub current_amount_out_minimum: ChainTokenAmount,
    #[serde(default, rename = "trade_next_retry_at_nanos")]
    pub next_retry_at_nanos: Option<u64>,
    #[serde(default, rename = "trade_pending_since_nanos")]
    pub pending_since_nanos: Option<u64>,
    #[serde(default, rename = "trade_unchanged_observations")]
    pub unchanged_observations: u32,
    #[serde(default, rename = "trade_input_pool_balance_before")]
    pub input_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "trade_output_pool_balance_before")]
    pub output_pool_balance_before: Option<Nat>,
    #[serde(default, rename = "trade_swap_args")]
    pub swap_args: Option<IcpswapSwapArgs>,
    /// Gross output returned by a decoded successful swap response or proven
    /// later through pool-balance reconciliation; safe to withdraw.
    #[serde(default, rename = "trade_gross_output_amount", alias = "trade_swap_returned_amount")]
    pub gross_output_amount: Option<Nat>,
    #[serde(default, rename = "trade_swap_protocol_error")]
    pub swap_protocol_error: Option<IcpswapError>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapWithdrawState {
    #[serde(default, rename = "withdraw_pool_balance_before")]
    pub pool_balance_before: Option<Nat>,
    #[serde(default, rename = "withdraw_wallet_balance_before")]
    pub wallet_balance_before: Option<Nat>,
    #[serde(default, rename = "withdraw_args")]
    pub withdraw_args: Option<IcpswapWithdrawArgs>,
    #[serde(default, rename = "withdraw_wallet_credited_amount")]
    pub wallet_credited_amount: Option<Nat>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IcpswapRecoveryState {
    #[serde(default, rename = "recovery_pool_balance_before")]
    pub pool_balance_before: Option<Nat>,
    #[serde(default, rename = "recovery_wallet_balance_before")]
    pub wallet_balance_before: Option<Nat>,
    #[serde(default, rename = "recovery_withdraw_args")]
    pub withdraw_args: Option<IcpswapWithdrawArgs>,
    #[serde(default, rename = "recovery_wallet_credited_amount")]
    pub wallet_credited_amount: Option<Nat>,
}

/// CEX-style durable state for the official manual ICPSwap workflow.
#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapState {
    pub execution_id: String,
    pub owner: Account,
    pub schema_version: u32,
    pub step: IcpswapStep,
    #[serde(default)]
    pub operator_pending_step: Option<IcpswapStep>,
    #[serde(default)]
    pub last_error: Option<String>,
    /// Earliest time the finalizer may advance this execution again.
    ///
    /// A non-terminal step that errors re-enqueues the row without consuming the
    /// finalize stage's retry budget, so nothing else paces it -- the exponential
    /// backoff there only gates `FailedRetryable`. Without this, a persistently
    /// failing step re-runs every daemon cycle indefinitely.
    #[serde(default)]
    pub next_attempt_at_nanos: Option<u64>,
    pub plan: IcpswapExecutionPlan,
    pub identity: IcpswapExecutionIdentity,
    pub funding: IcpswapFundingState,
    pub settlement: IcpswapSettlementState,
    #[serde(flatten)]
    pub transfer: IcpswapTransferState,
    #[serde(flatten)]
    pub deposit: IcpswapDepositState,
    #[serde(flatten)]
    pub trade: IcpswapTradeState,
    #[serde(flatten)]
    pub withdraw: IcpswapWithdrawState,
    #[serde(flatten)]
    pub recovery: IcpswapRecoveryState,
}

/// The only supported durable ICPSwap execution state. Records from older
/// funding workflows intentionally fail deserialization and cannot be resumed.
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
    #[error("ICRC-1 balance lookup failed on ledger {ledger}: {message}")]
    LedgerBalance { ledger: Principal, message: String },
    #[error("ICRC-1 transfer failed on ledger {ledger}: {message}")]
    LedgerTransfer { ledger: Principal, message: String },
    /// The persisted `created_at_time` has aged out of the ledger's transaction
    /// window. Replaying these arguments is refused permanently, and the
    /// deduplication that made the replay safe expired with the window.
    #[error("ICRC-1 transfer on ledger {ledger} is outside the deduplication window: {message}")]
    LedgerTransferTooOld { ledger: Principal, message: String },
    /// The ledger considers `created_at_time` future-dated, so this transfer was
    /// definitely not applied -- by this attempt or any earlier identical one.
    #[error("ICRC-1 transfer on ledger {ledger} was created in the future: {message}")]
    LedgerTransferCreatedInFuture { ledger: Principal, message: String },
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
    #[error("ICPSwap {method} submission to {pool} has an ambiguous outcome: {message}")]
    SubmissionUnknown {
        pool: Principal,
        method: &'static str,
        message: String,
    },
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
    #[error("ICPSwap only supports canonical ICP <-> ckUSDC swaps; received {pay_asset} -> {receive_asset}")]
    UnsupportedPair { pay_asset: String, receive_asset: String },
    #[error("ICPSwap input budget {budget} cannot cover transfer and deposit fees totaling {fees}")]
    InputFeesExceedBudget { budget: Nat, fees: Nat },
    #[error("ICPSwap pool {pool} returned an invalid zero spot price")]
    InvalidPoolPrice { pool: Principal },
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
