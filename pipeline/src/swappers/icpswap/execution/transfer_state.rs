use candid::{CandidType, Nat};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
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
    pub fee: ChainTokenAmount,
    pub transfer: IcpswapLedgerTransferState,
}

#[derive(CandidType, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IcpswapSettlementKind {
    Output,
    Recovery,
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
}
