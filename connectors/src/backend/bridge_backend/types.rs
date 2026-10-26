use alloy::primitives::Address;
use async_trait::async_trait;
use candid::{CandidType, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::outbound::PreparedSend;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeDestination {
    IcpAccount(Account),
    EvmAddress(Address),
    BtcAddress(String),
    SolanaAddress(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDestinationKind {
    IcpAccount,
    EvmAddress,
    BtcAddress,
    SolanaAddress,
}

impl BridgeDestination {
    pub fn kind(&self) -> BridgeDestinationKind {
        match self {
            Self::IcpAccount(_) => BridgeDestinationKind::IcpAccount,
            Self::EvmAddress(_) => BridgeDestinationKind::EvmAddress,
            Self::BtcAddress(_) => BridgeDestinationKind::BtcAddress,
            Self::SolanaAddress(_) => BridgeDestinationKind::SolanaAddress,
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
    SolanaToIcp,
    IcpToSolana,
}

impl BridgeRouteKind {
    /// True when one send funds the route: the bridge signs it, and the
    /// outbound driver saves, sends and settles it.
    pub fn funded_by_signed_send(self) -> bool {
        matches!(self, Self::SolanaToIcp | Self::CkEthErc20Forward | Self::EthToCkEth)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BridgeRouteSpec {
    pub source_asset: &'static str,
    pub source_chain: &'static str,
    pub target_asset: &'static str,
    pub destination_kind: BridgeDestinationKind,
    pub route_kind: BridgeRouteKind,
    /// The exchange cannot hold the ICP asset, so exchange legs move through
    /// this bridge. False for assets the exchange lists on the ICP network.
    pub exchange_transport: bool,
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

/// A provider record that may belong to an attempt whose handle was lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSubmissionCandidate {
    pub bridge_id: String,
    /// Provider-side monotonic position. Attempt cursors compare against it.
    pub sequence: u64,
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

/// A signed send that funds a bridge route, and the provider handle that
/// turns its receipt into a bridge id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeSend {
    pub send: PreparedSend,
    /// Opaque to the caller. It is saved with the send and given back to
    /// `track_deposit`.
    pub handle: String,
}

/// What the provider says about a deposit that already landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepositNotice {
    /// The provider tracks the deposit under this handle.
    Accepted(BridgeSubmission),
    /// The provider does not track it yet. Asking again is safe.
    NotYet { reason: String },
}

/// High-level bridge lifecycle state from the provider/backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeStatus {
    Pending,
    Completed,
    Minted(BridgeMintReceipt),
    Failed { reason: Option<String> },
    Canceled { reason: Option<String> },
    Unknown,
}

/// Deposit-specific ledger evidence; unrelated wallet credits cannot satisfy it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeMintReceipt {
    pub transaction_hash: String,
    pub log_index: Nat,
    pub mint_block_index: Nat,
    pub amount: Nat,
    pub recipient: Account,
    pub token_symbol: String,
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

    /// Checks a request and signs the send that funds it. It sends nothing.
    /// The caller saves the send, sends it through the outbound driver, then
    /// calls `track_deposit` with its receipt.
    async fn prepare_bridge_send(&self, request: &BridgeRequest) -> Result<BridgeSend, String> {
        let _ = request;
        Err("signed bridge sends are not supported by this bridge backend".to_string())
    }

    /// The bridge handle for a send that landed with `receipt`. Some
    /// providers must be told about the deposit first; asking again is safe.
    async fn track_deposit(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
        handle: &str,
        receipt: &str,
    ) -> Result<DepositNotice, String> {
        let _ = (asset, chain, destination, handle, receipt);
        Err("deposit tracking is not supported by this bridge backend".to_string())
    }

    /// Reads the provider's history position before a submission, so a handle
    /// lost between the call and its persistence can be found again. `None`
    /// means this provider cannot look up lost submissions.
    async fn prepare_submission(&self, request: &BridgeRequest) -> Result<Option<u64>, String> {
        let _ = request;
        Ok(None)
    }

    /// Returns provider records after `cursor` that match the request exactly
    /// and are not in `claimed_bridge_ids`. The read must be certified, because
    /// the caller may submit again when nothing is found.
    async fn find_submissions(
        &self,
        request: &BridgeRequest,
        cursor: u64,
        claimed_bridge_ids: &[String],
    ) -> Result<Vec<BridgeSubmissionCandidate>, String> {
        let _ = (request, cursor, claimed_bridge_ids);
        Err("submission lookup is not supported by this bridge backend".to_string())
    }
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

// Variant names are the minter's candid tags.
#[allow(clippy::enum_variant_names)]
#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) enum WithdrawalSearchParameter {
    ByRecipient(String),
    BySenderAccount(Account),
    ByWithdrawalId(u64),
}

/// One minter withdrawal as `withdrawal_status` reports it. The status variant
/// is kept opaque: the record proves the burn happened whatever its state.
#[derive(CandidType, Deserialize, Clone, Debug)]
pub(super) struct WithdrawalDetail {
    pub token_symbol: String,
    pub withdrawal_amount: Nat,
    pub max_transaction_fee: Option<Nat>,
    pub withdrawal_id: u64,
    pub from: Principal,
    pub from_subaccount: Option<Vec<u8>>,
    pub recipient_address: String,
    pub status: candid::Reserved,
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
