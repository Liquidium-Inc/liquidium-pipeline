use alloy::primitives::Address;
use async_trait::async_trait;
use candid::{CandidType, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeDestination {
    IcpAccount(Account),
    EvmAddress(Address),
    BtcAddress(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDestinationKind {
    IcpAccount,
    EvmAddress,
    BtcAddress,
}

impl BridgeDestination {
    pub fn kind(&self) -> BridgeDestinationKind {
        match self {
            Self::IcpAccount(_) => BridgeDestinationKind::IcpAccount,
            Self::EvmAddress(_) => BridgeDestinationKind::EvmAddress,
            Self::BtcAddress(_) => BridgeDestinationKind::BtcAddress,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeRouteKind {
    CkEthErc20Forward,
    CkEthErc20Reverse,
    BtcToCkBtc,
    CkBtcToBtc,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BridgeRouteSpec {
    pub source_asset: &'static str,
    pub source_chain: &'static str,
    pub target_asset: &'static str,
    pub destination_kind: BridgeDestinationKind,
    pub route_kind: BridgeRouteKind,
    pub evm_token_address: Option<&'static str>,
    pub ckerc20_ledger_id: Option<&'static str>,
    pub min_sweep_amount: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BridgeSweepRoute {
    pub source_asset: String,
    pub source_chain: String,
    pub target_asset: String,
    pub min_sweep_amount: f64,
}

/// A normalized request for moving assets between chains/assets.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeRequest {
    pub asset: String,
    pub source_chain: String,
    pub source_address: String,
    pub target_asset: String,
    pub destination: BridgeDestination,
    pub amount: f64,
}

/// Provider submission handle returned after a bridge transaction is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSubmission {
    pub bridge_id: String,
}

/// High-level bridge lifecycle state from the provider/backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeStatus {
    Pending,
    Completed,
    Failed { reason: Option<String> },
    Canceled { reason: Option<String> },
    Unknown,
}

/// Backend contract for reading balances and executing bridge routes.
#[mockall::automock]
#[async_trait]
pub trait BridgeBackend: Send + Sync {
    /// Returns a human-readable source balance for a route input asset/account.
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String>;

    /// Submits a bridge transfer for a supported route and returns a tracking handle.
    ///
    /// The backend validates route metadata, source/destination constraints, and amount
    /// conversion before sending provider calls.
    ///
    /// `bridge_id` in the returned [`BridgeSubmission`] is backend-specific and is later
    /// consumed by [`BridgeBackend::get_bridge_status`] for polling.
    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String>;

    /// Polls current status for a previously submitted bridge operation.
    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvmReceiptStatus {
    pub success: bool,
    pub block_number: Option<u64>,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct CkEthMinterInfo {
    #[serde(default)]
    pub deposit_with_subaccount_helper_contract_address: Option<String>,
    #[serde(default)]
    pub erc20_helper_contract_address: Option<String>,
    #[serde(default)]
    pub cketh_ledger_id: Option<Principal>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum HelperContract {
    WithSubaccount(Address),
    Native(Address),
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct Eip1559TransactionPriceArg {
    pub ckerc20_ledger_id: Principal,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct Eip1559TransactionPrice {
    pub max_priority_fee_per_gas: Nat,
    pub max_fee_per_gas: Nat,
    pub max_transaction_fee: Nat,
    pub timestamp: Option<u64>,
    pub gas_limit: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct WithdrawErc20Arg {
    pub ckerc20_ledger_id: Principal,
    pub recipient: String,
    pub from_cketh_subaccount: Option<Vec<u8>>,
    pub from_ckerc20_subaccount: Option<Vec<u8>>,
    pub amount: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct RetrieveErc20Request {
    pub ckerc20_block_index: Nat,
    pub cketh_block_index: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct CkErc20Token {
    pub erc20_contract_address: String,
    pub ledger_canister_id: Principal,
    pub ckerc20_token_symbol: String,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum LedgerError {
    TemporarilyUnavailable(String),
    InsufficientAllowance {
        token_symbol: String,
        ledger_id: Principal,
        allowance: Nat,
        failed_burn_amount: Nat,
    },
    AmountTooLow {
        minimum_burn_amount: Nat,
        token_symbol: String,
        ledger_id: Principal,
        failed_burn_amount: Nat,
    },
    InsufficientFunds {
        balance: Nat,
        token_symbol: String,
        ledger_id: Principal,
        failed_burn_amount: Nat,
    },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum WithdrawErc20Error {
    TokenNotSupported { supported_tokens: Vec<CkErc20Token> },
    TemporarilyUnavailable(String),
    CkErc20LedgerError { error: LedgerError, cketh_block_index: Nat },
    CkEthLedgerError { error: LedgerError },
    RecipientAddressBlocked { address: String },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum WithdrawErc20Ret {
    Ok(RetrieveErc20Request),
    Err(WithdrawErc20Error),
}
