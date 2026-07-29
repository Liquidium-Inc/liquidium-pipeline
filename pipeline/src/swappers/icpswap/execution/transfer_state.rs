use candid::{CandidType, Nat};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_connectors::account::icp_account::RECOVERY_ACCOUNT;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use serde::{Deserialize, Serialize};

/// Durable evidence for one identity-to-identity ledger transfer. Arguments
/// and balance baselines are persisted before submission in Stage 3.
#[derive(CandidType, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct IcpswapLedgerTransferState {
    #[serde(default)]
    pub args: Option<TransferArg>,
    #[serde(default)]
    pub block_index: Option<Nat>,
    #[serde(default)]
    pub source_balance_before: Option<Nat>,
    #[serde(default)]
    pub destination_balance_before: Option<Nat>,
    #[serde(default)]
    pub credited_amount: Option<Nat>,
}

/// Input funding transfer from the shared trader to the isolated liquidation
/// principal.
#[derive(CandidType, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcpswapFundingState {
    pub source: Account,
    pub destination: Account,
    /// Trader-owned recovery subaccount receiving child funds above the exact
    /// committed pool budget.
    pub surplus_destination: Account,
    pub fee: ChainTokenAmount,
    pub transfer: IcpswapLedgerTransferState,
    #[serde(default)]
    pub surplus_transfer: IcpswapLedgerTransferState,
    /// Surplus too small to pay its own recovery transfer fee.
    #[serde(default)]
    pub residual_dust: Option<Nat>,
}

impl IcpswapFundingState {
    pub fn new(source: Account, destination: Account, fee: ChainTokenAmount) -> Self {
        Self {
            source,
            destination,
            surplus_destination: Account {
                owner: source.owner,
                subaccount: Some(*RECOVERY_ACCOUNT),
            },
            fee,
            transfer: IcpswapLedgerTransferState::default(),
            surplus_transfer: IcpswapLedgerTransferState::default(),
            residual_dust: None,
        }
    }
}

#[derive(CandidType, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IcpswapSettlementKind {
    Output,
    Recovery,
    /// Remaining output from an ambiguous forwarding attempt is returned to
    /// the trader recovery account instead of parking the liquidation.
    OutputRecovery,
}

/// Final transfer from the isolated principal to either the swap recipient or
/// the funding trader after recovery.
#[derive(CandidType, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcpswapSettlementState {
    #[serde(default)]
    pub kind: Option<IcpswapSettlementKind>,
    pub destination: Account,
    pub fee: ChainTokenAmount,
    #[serde(default)]
    pub transfer: IcpswapLedgerTransferState,
    /// Original forwarding intent retained when an aged transfer produces an
    /// unexpected isolated-account debit.
    #[serde(default)]
    pub interrupted_transfer: Option<IcpswapLedgerTransferState>,
    /// Debit observed while reconciling `interrupted_transfer`.
    #[serde(default)]
    pub interrupted_observed_debit: Option<Nat>,
    /// Exact remaining execution credit eligible for recovery forwarding.
    #[serde(default)]
    pub recovery_credit: Option<Nat>,
    /// Remaining credit too small to pay a recovery forwarding fee.
    #[serde(default)]
    pub residual_dust: Option<Nat>,
}
