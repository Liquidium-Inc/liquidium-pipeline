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
    EthToCkEth,
    CkEthToEth,
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
    /// Index canister for `ckerc20_ledger_id`, used to find a mint by the
    /// deposit that caused it. Verified against the ledger at startup.
    pub ckerc20_index_id: Option<&'static str>,
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
    /// Optional exact provider fee quote, in source-native base units, captured during
    /// preflight sizing and reused at submit time to avoid quote drift.
    pub provider_fee_budget_native_units: Option<Nat>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BridgeFeeBudget {
    pub source_fee_budget: f64,
    pub destination_fee_budget: f64,
    pub provider_fee_budget_native_units: Option<Nat>,
}

/// Provider submission handle returned after a bridge transaction is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSubmission {
    pub bridge_id: String,
}

/// Why a bridge submission ended without delivering, and — the part the caller
/// actually needs — whether the source funds are still where they were.
///
/// A resubmit is only safe when nothing moved. Deciding that from the failure
/// message would mean rewording a string could start or stop a second transfer
/// of the same money, so it is a variant the caller matches on instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeFailure {
    /// Another transaction took this one's nonce, so it never executed at all.
    Superseded,
    /// It executed and reverted. Reverting rolls back every state change, so
    /// the source funds are untouched and only gas was spent.
    Reverted,
    /// Anything else. It may have moved funds, so it must never be repeated
    /// without a human deciding first.
    Indeterminate,
}

impl BridgeFailure {
    /// Whether the source funds are provably still available to resubmit.
    ///
    /// True for a transaction that never ran and for one that ran and undid
    /// itself. Both leave the balance exactly as it was.
    pub fn funds_untouched(self) -> bool {
        matches!(self, Self::Superseded | Self::Reverted)
    }
}

/// High-level bridge lifecycle state from the provider/backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeStatus {
    Pending,
    Completed,
    Failed {
        cause: BridgeFailure,
        reason: Option<String>,
    },
    Canceled {
        reason: Option<String>,
    },
    Unknown,
}

/// Backend contract for reading balances and executing bridge routes.
#[mockall::automock]
#[async_trait]
pub trait BridgeBackend: Send + Sync {
    /// Returns a human-readable source balance for a route input asset/account.
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String>;

    /// Returns the source-asset amount that must remain on the source account
    /// in addition to the submitted bridge amount.
    async fn get_source_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let _ = (asset, chain, target_asset);
        Ok(0.0)
    }

    /// Returns the target-asset amount expected to be deducted before the bridge
    /// credit arrives at the destination.
    async fn get_destination_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let _ = (asset, chain, target_asset);
        Ok(0.0)
    }

    /// Returns source and destination fee budgets from one coherent quote when
    /// the backend can share an underlying provider quote across both values.
    async fn get_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<BridgeFeeBudget, String> {
        Ok(BridgeFeeBudget {
            source_fee_budget: self.get_source_fee_budget(asset, chain, target_asset).await?,
            destination_fee_budget: self.get_destination_fee_budget(asset, chain, target_asset).await?,
            provider_fee_budget_native_units: None,
        })
    }

    /// Returns the minimum bridge amount for a route input. Routes without a
    /// provider-enforced floor return zero.
    async fn get_minimum_bridge_amount(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let _ = (asset, chain, target_asset);
        Ok(0.0)
    }

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

    /// Returns the amount credited at `destination` by this specific bridge
    /// submission, or `None` while nothing there names it.
    ///
    /// A credit identified by its originating transaction is attributable; a
    /// balance increase on an account several legs settle into is not. Backends
    /// whose destinations carry no such link report `None`, leaving the caller to
    /// fall back on whatever weaker evidence it has.
    async fn find_bridge_credit(
        &self,
        target_asset: &str,
        destination: &BridgeDestination,
        bridge_id: &str,
    ) -> Result<Option<f64>, String> {
        let _ = (target_asset, destination, bridge_id);
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvmReceiptStatus {
    pub success: bool,
    pub block_number: Option<u64>,
}

/// What a node can still say about a transaction that has no receipt yet.
///
/// A missing receipt is not proof that a transaction is unmined, so minedness is
/// reported in its own right rather than inferred from the receipt's absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxLiveness {
    /// Mined; the receipt has simply not caught up yet.
    Mined,
    /// Unmined, and another hash has taken its nonce, so it can never mine.
    Replaced { tx_nonce: u64, sender_next_nonce: u64 },
    /// Unmined and still holding its nonce.
    Pending,
    /// The node no longer knows the hash, so nothing can be concluded.
    Unknown,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct CkEthMinterInfo {
    #[serde(default)]
    pub deposit_with_subaccount_helper_contract_address: Option<String>,
    #[serde(default)]
    pub eth_helper_contract_address: Option<String>,
    #[serde(default)]
    pub erc20_helper_contract_address: Option<String>,
    #[serde(default)]
    pub cketh_ledger_id: Option<Principal>,
    #[serde(default)]
    pub minimum_withdrawal_amount: Option<Nat>,
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
pub(super) struct WithdrawalArg {
    pub amount: Nat,
    pub recipient: String,
    pub from_subaccount: Option<Vec<u8>>,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct RetrieveErc20Request {
    pub ckerc20_block_index: Nat,
    pub cketh_block_index: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct RetrieveEthRequest {
    pub block_index: Nat,
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
pub(super) enum WithdrawalError {
    AmountTooLow { min_withdrawal_amount: Nat },
    InsufficientFunds { balance: Nat },
    InsufficientAllowance { allowance: Nat },
    TemporarilyUnavailable(String),
    RecipientAddressBlocked { address: String },
    GenericError { error_code: Nat, message: String },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum WithdrawErc20Ret {
    Ok(RetrieveErc20Request),
    Err(WithdrawErc20Error),
}

#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum WithdrawalRet {
    Ok(RetrieveEthRequest),
    Err(WithdrawalError),
}
