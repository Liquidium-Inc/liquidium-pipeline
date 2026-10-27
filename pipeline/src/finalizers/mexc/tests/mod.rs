use super::*;

use std::collections::HashMap;
use std::sync::Arc;

use candid::{Nat, Principal};
use ic_ledger_types::{AccountIdentifier, Subaccount};
use liquidium_pipeline_connectors::backend::bridge_backend::{
    BRIDGE_SUBMISSION_DEFERRED_PREFIX, BridgeDestination, BridgeFeeBudget, BridgeStatus, BridgeSubmission,
    MockBridgeBackend,
};
use liquidium_pipeline_connectors::backend::cex_backend::{
    BuyOrderInputMode, CexBackendError, DepositAddress, MockCexBackend, OrderBook, OrderBookLevel, SwapFillReport,
    WithdrawError, WithdrawStatus, WithdrawStatusSnapshot, WithdrawTerms,
};
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::outbound::SIDE_EFFECT_AMBIGUOUS_PREFIX;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use liquidium_pipeline_core::transfer::actions::MockTransferActions;
use liquidium_pipeline_core::types::protocol_types::{
    AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus, TxStatus,
};
use liquidium_pipeline_core::types::side_cost::{CYCLES_DECIMALS, CYCLES_SYMBOL, SideCost};

use crate::executors::executor::ExecutorRequest;
use crate::finalizers::cex_finalizer::{CexDepositState, CexState, CexStep};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::utils::ICP_LEDGER_PRINCIPAL;
use proptest::prelude::*;

mod support;
use support::*;

mod bridged_deposit;
mod bridged_withdraw;
mod cksol_return;
mod deposit;
mod direct_withdraw;
mod execution_records;
mod fill_accounting;
mod fuzz;
mod operator_answers;
mod planning;
mod price_limited_orders;
mod route_costs;
mod routing;
mod saved_bridge_call;
mod trade;
mod uncertain_side_effects;
