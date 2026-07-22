use candid::{CandidType, Int, Nat, Principal, Reserved};
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
pub enum IcpswapExecutionPhase {
    Planned,
    Approved,
    SubmissionUnknown,
    AwaitingOutput,
    RefundPending,
    FundsInPool,
    RecoveryWithdrawSubmitted,
    Completed,
    Refunded,
    RecoveryTransferPending,
    RecoveryTransferSubmitted,
    Recovered,
    FailedTerminal,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapExecutionState {
    pub plan: IcpswapExecutionPlan,
    pub phase: IcpswapExecutionPhase,
    #[serde(default)]
    pub gross_swap_output: Option<ChainTokenAmount>,
    #[serde(default)]
    pub input_balance_before: Option<ChainTokenAmount>,
    #[serde(default)]
    pub output_balance_before: Option<ChainTokenAmount>,
    #[serde(default)]
    pub approval_block_index: Option<Nat>,
    #[serde(default)]
    pub approval_created_at: Option<u64>,
    #[serde(default)]
    pub pool_transaction_start: Option<Nat>,
    #[serde(default)]
    pub pool_transaction_id: Option<Nat>,
    #[serde(default)]
    pub settlement_ledger_block_index: Option<Nat>,
    #[serde(default)]
    pub refund_transaction_id: Option<Nat>,
    #[serde(default)]
    pub refund_ledger_block_index: Option<Nat>,
    #[serde(default)]
    pub recovery_amount: Option<ChainTokenAmount>,
    #[serde(default)]
    pub recovery_transaction_start: Option<Nat>,
    #[serde(default)]
    pub recovery_transaction_id: Option<Nat>,
    #[serde(default)]
    pub recovery_ledger_block_index: Option<Nat>,
    #[serde(default)]
    pub recovery_submitted_at: Option<u64>,
    #[serde(default)]
    pub returned_gross_amount: Option<ChainTokenAmount>,
    #[serde(default)]
    pub recovery_destination: Option<Account>,
    #[serde(default)]
    pub recovery_transfer_amount: Option<ChainTokenAmount>,
    #[serde(default)]
    pub recovery_transfer_created_at: Option<u64>,
    #[serde(default)]
    pub recovery_transfer_block_index: Option<Nat>,
    #[serde(default)]
    pub submitted_at: Option<u64>,
    #[serde(default)]
    pub recovery_attempted: bool,
    #[serde(default)]
    pub last_error: Option<String>,
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
pub enum IcpswapExecutionClientError {
    #[error("ICPSwap transaction cursor query failed on pool {pool}: {message}")]
    TransactionQuery { pool: Principal, message: String },
    #[error("ICRC-2 allowance lookup failed on ledger {ledger}: {message}")]
    Allowance { ledger: Principal, message: String },
    #[error("ICRC-2 approval failed on ledger {ledger}: {message}")]
    Approval { ledger: Principal, message: String },
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapRecoveryClientError {
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapRecoveryError {
    #[error(transparent)]
    Client(#[from] IcpswapRecoveryClientError),
    #[error("ICPSwap recovery query failed: {0}")]
    Query(String),
    #[error("failed to persist ICPSwap recovery state: {0}")]
    Persistence(String),
    #[error("no persisted ICPSwap recovery state exists")]
    MissingState,
    #[error("ICPSwap recovery requires phase FundsInPool or RecoveryWithdrawSubmitted, got {0:?}")]
    InvalidPhase(IcpswapExecutionPhase),
    #[error("ICPSwap recovery state has no recoverable amount")]
    MissingRecoveryAmount,
    #[error("ICPSwap recovery state has no swap transaction ID")]
    MissingSwapTransactionId,
    #[error("ICPSwap recovery state has no recovery transaction cursor")]
    MissingRecoveryTransactionCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcpswapRecoveryTransferRequest {
    pub ledger: Principal,
    pub from: Account,
    pub to: Account,
    pub amount: Nat,
    pub fee: Nat,
    pub created_at_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IcpswapRecoveryTransferOutcome {
    Completed(Nat),
    Duplicate(Nat),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapRecoveryTransferClientError {
    #[error("failed to encode ICRC-1 recovery transfer arguments: {0}")]
    Encode(String),
    #[error("ICRC-1 recovery transfer submission to ledger {ledger} has an ambiguous outcome: {message}")]
    SubmissionUnknown { ledger: Principal, message: String },
    #[error("ICRC-1 recovery transfer was rejected by ledger {ledger}: {message}")]
    Rejected { ledger: Principal, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapRecoveryTransferError {
    #[error(transparent)]
    Client(#[from] IcpswapRecoveryTransferClientError),
    #[error("failed to persist ICPSwap recovery transfer state: {0}")]
    Persistence(String),
    #[error("no persisted ICPSwap recovery state exists")]
    MissingState,
    #[error("ICPSwap recovery transfer cannot run from phase {0:?}")]
    InvalidPhase(IcpswapExecutionPhase),
    #[error("confirmed ICPSwap refund has no persisted gross returned amount")]
    MissingReturnedAmount,
    #[error("persisted recovery destination {persisted} differs from configured destination {configured}")]
    DestinationMismatch { persisted: Account, configured: Account },
    #[error("ICPSwap recovery transfer must originate from the trader root account")]
    NonRootTraderAccount,
    #[error("invalid persisted ICPSwap recovery transfer state: {0}")]
    InvalidPersistedState(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapExecutionError {
    #[error(transparent)]
    Client(#[from] IcpswapExecutionClientError),
    #[error("failed to persist ICPSwap execution state: {0}")]
    Persistence(String),
    #[error("ICPSwap execution cannot submit from phase {0:?}")]
    SubmissionAlreadyStarted(IcpswapExecutionPhase),
    #[error("no persisted ICPSwap execution state or new execution plan was provided")]
    MissingPlan,
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
pub struct IcpswapPoolToken {
    pub address: Principal,
    pub standard: String,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapTransfer {
    pub token: Principal,
    pub standard: String,
    pub from: Account,
    pub to: Account,
    pub amount: Nat,
    pub fee: Nat,
    pub memo: Option<Vec<u8>>,
    pub index: Nat,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapDepositStatus {
    Created,
    TransferCompleted,
    Completed,
    Failed,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapWithdrawStatus {
    Created,
    CreditCompleted,
    Completed,
    Failed,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapRefundStatus {
    Created,
    CreditCompleted,
    Completed,
    Failed,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapSwapStatus {
    Created,
    Completed,
    Failed,
}

#[derive(CandidType, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapOneStepSwapStatus {
    Created,
    DepositTransferCompleted,
    DepositCreditCompleted,
    PreSwapCompleted,
    SwapCompleted,
    WithdrawCreditCompleted,
    Completed,
    Failed,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapDepositInfo {
    pub transfer: IcpswapTransfer,
    pub status: IcpswapDepositStatus,
    pub err: Option<String>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapWithdrawInfo {
    pub transfer: IcpswapTransfer,
    pub status: IcpswapWithdrawStatus,
    pub err: Option<String>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapSwapInfo {
    #[serde(rename = "tokenIn")]
    pub token_in: IcpswapPoolToken,
    #[serde(rename = "tokenOut")]
    pub token_out: IcpswapPoolToken,
    #[serde(rename = "amountIn")]
    pub amount_in: Nat,
    #[serde(rename = "amountOut")]
    pub amount_out: Nat,
    #[serde(rename = "amountInFee")]
    pub amount_in_fee: Nat,
    #[serde(rename = "amountOutFee")]
    pub amount_out_fee: Nat,
    pub status: IcpswapSwapStatus,
    pub err: Option<String>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapOneStepSwapInfo {
    pub deposit: IcpswapDepositInfo,
    pub withdraw: IcpswapWithdrawInfo,
    pub swap: IcpswapSwapInfo,
    pub status: IcpswapOneStepSwapStatus,
    pub err: Option<String>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapRefundInfo {
    #[serde(rename = "relatedIndex")]
    pub related_index: Nat,
    pub transfer: IcpswapTransfer,
    pub status: IcpswapRefundStatus,
    pub err: Option<String>,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcpswapTransactionAction {
    Deposit(Reserved),
    Withdraw(IcpswapWithdrawInfo),
    Refund(IcpswapRefundInfo),
    AddLiquidity(Reserved),
    DecreaseLiquidity(Reserved),
    Claim(Reserved),
    Swap(Reserved),
    OneStepSwap(IcpswapOneStepSwapInfo),
    TransferPosition(Reserved),
    AddLimitOrder(Reserved),
    RemoveLimitOrder(Reserved),
    ExecuteLimitOrder(Reserved),
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapTransaction {
    pub id: Nat,
    pub timestamp: Int,
    pub owner: Principal,
    #[serde(rename = "canisterId")]
    pub canister_id: Principal,
    pub action: IcpswapTransactionAction,
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcpswapUnusedBalance {
    pub balance0: Nat,
    pub balance1: Nat,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcpswapReconciliationError {
    #[error("ICPSwap reconciliation query failed: {0}")]
    Query(String),
    #[error("multiple ICPSwap transactions match the persisted execution plan")]
    AmbiguousTransaction,
    #[error("ICPSwap transaction {0} does not match the persisted execution plan")]
    TransactionMismatch(Nat),
    #[error("ICPSwap execution state has no pre-submission transaction cursor")]
    MissingTransactionCursor,
    #[error("failed to persist ICPSwap reconciliation state: {0}")]
    Persistence(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcpswapTokenMetadata {
    pub token: ChainToken,
    pub standard: String,
}

#[derive(Debug, Clone)]
pub struct IcpswapQuoteResult {
    pub quote: super::super::model::SwapQuote,
    pub plan: IcpswapExecutionPlan,
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
