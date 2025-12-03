use std::sync::Arc;

use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, DepositAddress};
use liquidium_pipeline_core::{
    account::model::ChainAccount, tokens::chain_token_amount::ChainTokenAmount,
    transfer::{actions::TransferActions, transfer_service::TransferService},
};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::{
    finalizers::cex_finalizer::{CexFinalizerLogic, CexState, CexStep},
    stages::executor::ExecutionReceipt,
    swappers::model::SwapExecution,
};

use num_traits::ToPrimitive;

/// MEXC-specific implementation of the generic CEX finalizer logic.
///
/// This is a thin state machine wrapper around the generic `CexState`:
/// - `prepare` initializes the state from a liquidation id / receipt plus static config
/// - `deposit` transitions Deposit -> Trade
/// - `trade` transitions Trade -> Withdraw
/// - `withdraw` transitions Withdraw -> Completed
pub struct MexcFinalizer<B>
where
    B: CexBackend,
{
    pub backend: Arc<B>,
    pub transfer_service: Arc<dyn TransferActions>,
}

impl<B> MexcFinalizer<B>
where
    B: CexBackend,
{
    pub fn new(backend: Arc<B>, transfer_service: Arc<dyn TransferActions>) -> Self {
        Self {
            backend,
            transfer_service,
        }
    }

    pub async fn check_deposit(&self, state: &mut CexState) -> Result<(), String> {
        let symbol = state.deposit_asset.symbol();
        let current_bal = self.backend.get_balance(&symbol).await?;

        match state.deposit_balance_before {
            Some(b0) => {
                if current_bal - b0 >= state.size_in.to_f64() - 0.00001 {
                    debug!(
                        "[mexc] liq_id={} deposit confirmed on CEX, before={} after={}",
                        state.liq_id, b0, current_bal
                    );
                    state.step = CexStep::Trade;
                } else {
                    debug!(
                        "[mexc] liq_id={} deposit not yet visible on CEX (before={}, after={}), staying in Deposit",
                        state.liq_id, b0, current_bal
                    );
                }
                Ok(())
            }
            None => {
                // No baseline recorded yet (should normally be set when we send the transfer),
                // so record the current balance as the baseline and stay in Deposit.
                debug!(
                    "[mexc] liq_id={} no baseline balance recorded, setting deposit_balance_before={}",
                    state.liq_id, current_bal
                );
                state.deposit_balance_before = Some(current_bal);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl<B> CexFinalizerLogic for MexcFinalizer<B>
where
    B: CexBackend,
{
    async fn prepare(&self, liq_id: &str, receipt: &ExecutionReceipt) -> Result<CexState, String> {
        let amount = &receipt
            .liquidation_result
            .as_ref()
            .expect("liq result unaavailable")
            .amounts
            .collateral_received;

        let size_in = ChainTokenAmount {
            token: receipt.request.collateral_asset.clone(),
            value: amount.clone(),
        };

        Ok(CexState {
            liq_id: liq_id.to_string(),
            step: CexStep::Deposit,
            last_error: None,

            // deposit leg
            deposit_asset: receipt.request.collateral_asset.clone(),
            deposit_txid: None,
            deposit_balance_before: None,

            // trade leg
            market: format!(
                "{}_{}",
                receipt.request.collateral_asset.symbol(),
                receipt.request.debt_asset.symbol()
            ),
            side: "sell".to_string(),
            size_in,

            // withdraw leg
            withdraw_asset: receipt.request.debt_asset.clone(),
            withdraw_address: receipt.request.liquidation.receiver_address.to_text(),
            withdraw_id: None,
            withdraw_txid: None,
            size_out: None,
        })
    }

    async fn deposit(&self, state: &mut CexState) -> Result<(), String> {
        debug!(
            "[mexc] liq_id={} step=Deposit asset={} network={}",
            state.liq_id,
            state.deposit_asset,
            state.deposit_asset.chain()
        );

        // Phase A: no transfer yet -> snapshot balance and send once, then stay in Deposit.
        if state.deposit_txid.is_none() {
            let symbol = state.deposit_asset.symbol();
            let baseline = match self.backend.get_balance(&symbol).await {
                Ok(bal) => bal,
                Err(e) => {
                    debug!(
                        "[mexc] liq_id={} could not get baseline balance before deposit: {} (using 0.0)",
                        state.liq_id, e
                    );
                    0.0
                }
            };

            debug!(
                "[mexc] liq_id={} baseline balance before deposit: {}",
                state.liq_id, baseline
            );
            state.deposit_balance_before = Some(baseline);

            let addr = self
                .backend
                .get_deposit_address(&state.deposit_asset.symbol(), &state.deposit_asset.chain())
                .await?;

            debug!(
                "[mexc] liq_id={} got deposit address={} tag={:?}",
                state.liq_id, addr.address, addr.tag
            );

            let amount = &state.size_in;

            let actions = &self.transfer_service;
            let tx_id = actions
                .transfer(
                    &state.deposit_asset,
                    &ChainAccount::Icp(Account {
                        owner: Principal::from_text(addr.address)
                            .map_err(|e| format!("invalid deposit address: {e}"))?,
                        subaccount: None,
                    }),
                    amount.value.clone(),
                )
                .await?;

            debug!("[mexc] liq_id={} sent deposit txid={}", state.liq_id, tx_id);

            state.deposit_txid = Some(tx_id);
            state.step = CexStep::DepositPending;

            // Keep step as Deposit. WAL will persist, and a later finalize run
            // will call check_deposit to see if funds landed on the CEX.
            return Ok(());
        }

        // Phase B: transfer already sent -> just check whether it is now credited.
        self.check_deposit(state).await
    }

    async fn trade(&self, state: &mut CexState) -> Result<(), String> {
        debug!(
            "[mexc] liq_id={} step=Trade market={} side={} size_in={}",
            state.liq_id,
            state.market,
            state.side,
            state.size_in.formatted(),
        );

        let amount_in = state.size_in.to_f64();

        if amount_in <= 0.0 {
            debug!(
                "[mexc] liq_id={} trade skipped: non-positive amount_in={}",
                state.liq_id, amount_in
            );
            // Nothing to trade, move on to Withdraw leg (or you could keep it at Trade).
            state.step = CexStep::Withdraw;
            return Ok(());
        }

        // Execute the spot trade on MEXC. The backend is responsible for:
        // - placing the order (market)
        // - handling partial fills / retries
        // - updating internal CEX balances
        let filled = self.backend.execute_swap(&state.market, &state.side, amount_in).await?;

        debug!(
            "[mexc] liq_id={} trade executed: market={} side={} amount_in={} filled={}",
            state.liq_id, state.market, state.side, amount_in, filled
        );

        // For now we don't feed `filled` back into on-chain amounts; withdraw leg
        // can either use backend-specific balance checks or later extend CexState
        // with an explicit post-trade amount.
        state.step = CexStep::Withdraw;

        state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), filled));
        Ok(())
    }

    async fn withdraw(&self, state: &mut CexState) -> Result<(), String> {
        debug!(
            "[mexc] liq_id={} step=Withdraw asset={} network={} address={}",
            state.liq_id,
            state.withdraw_asset,
            state.withdraw_asset.chain(),
            state.withdraw_address,
        );

        // Idempotency: if we already have a withdraw id or txid, just advance.
        if state.withdraw_id.is_some() || state.withdraw_txid.is_some() {
            debug!(
                "[mexc] liq_id={} withdraw already recorded (id={:?}, txid={:?}), skipping",
                state.liq_id, state.withdraw_id, state.withdraw_txid,
            );
            state.step = CexStep::Completed;
            return Ok(());
        }

        // Amount to withdraw. For now, reuse the size_in amount expressed in withdraw-asset units.
        // Later we can switch to a post-trade amount or explicit CEX balance if needed.
        let amount = state.size_in.to_f64();

        if amount <= 0.0 {
            return Err(format!(
                "withdrawal amount is zero or negative for liq_id {} (amount={})",
                state.liq_id, amount
            ));
        }

        // Execute withdrawal on MEXC via the backend.
        let receipt = self
            .backend
            .withdraw(
                &state.withdraw_asset.symbol(),
                &state.withdraw_asset.chain(),
                &state.withdraw_address,
                amount,
            )
            .await?;

        debug!(
            "[mexc] liq_id={} withdraw executed: asset={} network={} amount={} txid={:?} internal_id={:?}",
            state.liq_id,
            state.withdraw_asset.symbol(),
            state.withdraw_asset.chain(),
            amount,
            receipt.txid,
            receipt.internal_id,
        );

        // Persist identifiers for idempotency.
        state.withdraw_id = receipt.internal_id.clone();
        state.withdraw_txid = receipt.txid.clone();

        // Mark the CEX leg as completed.
        state.step = CexStep::Completed;
        Ok(())
    }

    async fn finish(&self, _receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {

        let receive_amount = state.size_out.clone();

        // Pay side: seized collateral we sent in, as recorded on the CEX state.
        let pay_amount = state.size_in.clone();

        // Compute an effective execution price in native units: receive / pay.
        let pay_f = pay_amount.to_f64();
        let recv_f = receive_amount.clone().expect("receive amount missing").to_f64();

        let exec_price = if pay_f > 0.0 { recv_f / pay_f } else { 0.0 };

        // We do not have a separate mid-price or detailed legs from MEXC here,
        // so we approximate:
        //  - mid_price == exec_price
        //  - slippage == 0.0
        //  - legs == empty (single synthetic hop via MEXC)
        //
        // swap_id / request_id are set to 0 for now since there is no natural
        // mapping from the CEX API to these fields yet. If you later add a
        // CEX-side id, you can thread it into CexState and use it here.

        let exec = SwapExecution {
            swap_id: 0,
            request_id: 0,
            status: "completed".to_string(),
            pay_asset: state.deposit_asset.asset_id(),
            pay_amount: pay_amount.value,
            receive_asset: state.withdraw_asset.asset_id(),
            receive_amount: receive_amount.unwrap().value,
            mid_price: exec_price,
            exec_price,
            slippage: 0.0,
            legs: Vec::new(),
            ts: 0,
        };

        Ok(exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use candid::{Nat, Principal};
    use liquidium_pipeline_connectors::backend::cex_backend::{DepositAddress, MockCexBackend};
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::transfer::actions::MockTransferActions;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };

    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::cex_finalizer::{CexState, CexStep};
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};

    fn make_execution_receipt(liq_id: u128) -> ExecutionReceipt {
        let collateral_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(1_000u64),
        };

        let debt_token = ChainToken::Icp {
            ledger: Principal::anonymous(),
            symbol: "ckUSDT".to_string(),
            decimals: 6,
            fee: Nat::from(1_000u64),
        };

        let liquidation = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(0u32),
            receiver_address: Principal::from_text("aaaaa-aa").unwrap(),
            buy_bad_debt: false,
        };

        let liq_result = LiquidationResult {
            id: liq_id,
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(1_000_000u64),
                debt_repaid: Nat::from(2_000_000u64),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            change_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
            collateral_tx: TxStatus {
                tx_id: None,
                status: TransferStatus::Pending,
            },
        };

        let req = ExecutorRequest {
            liquidation,
            swap_args: None,
            debt_asset: debt_token.clone(),
            collateral_asset: collateral_token.clone(),
            expected_profit: 0,
        };

        ExecutionReceipt {
            request: req,
            liquidation_result: Some(liq_result),
            status: ExecutionStatus::Success,
            change_received: false,
        }
    }

    #[tokio::test]
    async fn mexc_prepare_builds_initial_cex_state() {
        let backend = Arc::new(MockCexBackend::new());
        let transfer_service = Arc::new(MockTransferActions::new());

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let state: CexState = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        assert_eq!(state.liq_id, "42");
        assert!(matches!(state.step, CexStep::Deposit));

        // deposit leg
        assert_eq!(state.deposit_asset, receipt.request.collateral_asset);
        assert!(state.deposit_txid.is_none());
        assert!(state.deposit_balance_before.is_none());

        // trade leg
        let expected_market = format!(
            "{}_{}",
            receipt.request.collateral_asset.symbol(),
            receipt.request.debt_asset.symbol()
        );
        assert_eq!(state.market, expected_market);
        assert_eq!(state.side, "sell");
        assert_eq!(state.size_in.token, receipt.request.collateral_asset);
        assert_eq!(
            state.size_in.value,
            receipt
                .liquidation_result
                .as_ref()
                .unwrap()
                .amounts
                .collateral_received
        );

        // withdraw leg
        assert_eq!(state.withdraw_asset, receipt.request.debt_asset);
        assert_eq!(
            state.withdraw_address,
            receipt.request.liquidation.receiver_address.to_text()
        );
        assert!(state.withdraw_id.is_none());
        assert!(state.withdraw_txid.is_none());
        assert!(state.size_out.is_none());
    }

    #[tokio::test]
    async fn mexc_deposit_phase_a_snapshots_baseline_and_sends_transfer() {
        let mut backend = MockCexBackend::new();
        let mut transfers = MockTransferActions::new();

        backend
            .expect_get_balance()
            .returning(|_symbol| Ok(10.0));

        backend
            .expect_get_deposit_address()
            .returning(|_symbol, _chain| {
                Ok(DepositAddress {
                    asset: "CkBTC".to_string(),
                    network: "ICP".to_string(),
                    address: "aaaaa-aa".to_string(),
                    tag: None,
                })
            });

        transfers
            .expect_transfer()
            .returning(|_token, _to, _amount| Ok("tx-123".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Pre-conditions: no deposit has been sent yet
        assert!(state.deposit_txid.is_none());
        assert!(state.deposit_balance_before.is_none());
        assert!(matches!(state.step, CexStep::Deposit));

        // Phase A: snapshot baseline and send transfer
        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit should succeed");

        assert_eq!(state.deposit_balance_before, Some(10.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_without_baseline_sets_baseline_and_keeps_step() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend
            .expect_get_balance()
            .returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Simulate that Phase A already ran and sent a tx, but baseline was never recorded.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = None;
        state.step = CexStep::DepositPending;

        // Phase B: deposit() delegates to check_deposit, which should set the baseline
        // and keep the step unchanged.
        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_moves_to_trade_when_balance_increased() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend
            .expect_get_balance()
            .returning(|_symbol| Ok(5.1));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Simulate that Phase A already ran, we have a baseline, and we are now in DepositPending.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = Some(5.0);
        state.step = CexStep::DepositPending;

        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        // Baseline should remain unchanged, and we should advance to Trade when balance increased.
        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_stays_in_deposit_when_balance_unchanged() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Balance stays the same as baseline.
        backend
            .expect_get_balance()
            .returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Simulate Phase A done, baseline recorded, and we are waiting in DepositPending.
        state.deposit_txid = Some("tx-123".to_string());
        state.deposit_balance_before = Some(5.0);
        state.step = CexStep::DepositPending;

        finalizer
            .deposit(&mut state)
            .await
            .expect("deposit (phase B) should succeed");

        // Since balance did not increase enough, we should still be in DepositPending.
        assert_eq!(state.deposit_balance_before, Some(5.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_trade_skips_when_amount_in_zero_and_moves_to_withdraw() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // When amount_in <= 0, execute_swap must never be called.
        backend.expect_execute_swap().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Force amount_in to zero and move state into Trade step.
        state.size_in.value = Nat::from(0u32);
        state.step = CexStep::Trade;

        finalizer
            .trade(&mut state)
            .await
            .expect("trade should succeed even when skipped");

        // No size_out set and step advanced to Withdraw.
        assert!(state.size_out.is_none());
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_executes_swap_and_sets_size_out_and_step_withdraw() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend
            .expect_execute_swap()
            .times(1)
            .returning(|market, side, amount_in| {
                assert_eq!(market, "ckBTC_ckUSDT");
                assert_eq!(side, "sell");
                assert!(amount_in > 0.0);
                Ok(1.1)
            });

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Move directly into Trade step; size_in is taken from receipt and should be > 0.
        state.step = CexStep::Trade;

        finalizer
            .trade(&mut state)
            .await
            .expect("trade should succeed");

        let out = state.size_out.as_ref().expect("size_out should be set");
        assert_eq!(out.token, state.withdraw_asset);
        assert!((out.to_f64() - 1.1).abs() < 1e-9);
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_propagates_backend_errors() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend
            .expect_execute_swap()
            .times(1)
            .returning(|_market, _side, _amount_in| Err("boom".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        state.step = CexStep::Trade;

        let err = finalizer
            .trade(&mut state)
            .await
            .expect_err("trade should fail");

        assert_eq!(err, "boom");
        // On error we expect the step to remain Trade.
        assert!(matches!(state.step, CexStep::Trade));
    }

    #[tokio::test]
    async fn mexc_withdraw_is_idempotent_when_already_recorded() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // No withdraw should be executed if we already have identifiers.
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        state.step = CexStep::Withdraw;
        state.withdraw_id = Some("internal-1".to_string());
        state.withdraw_txid = Some("tx-1".to_string());

        finalizer
            .withdraw(&mut state)
            .await
            .expect("withdraw should succeed idempotently");

        assert!(matches!(state.step, CexStep::Completed));
    }

    #[tokio::test]
    async fn mexc_withdraw_fails_on_non_positive_amount() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        // Backend must not be called when amount <= 0.
        backend.expect_withdraw().times(0);

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(backend, transfer_service);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        state.step = CexStep::Withdraw;
        state.size_in.value = Nat::from(0u32);

        let err = finalizer
            .withdraw(&mut state)
            .await
            .expect_err("withdraw should fail for non-positive amount");

        assert!(err.contains("withdrawal amount is zero or negative"));
    }

    #[tokio::test]
    async fn mexc_finish_builds_synthetic_swap_execution_from_state() {
        let backend = Arc::new(MockCexBackend::new());
        let transfers = Arc::new(MockTransferActions::new());

        let finalizer = MexcFinalizer::new(backend, transfers);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer
            .prepare("42", &receipt)
            .await
            .expect("prepare should succeed");

        // Simulate that trade/withdraw legs have populated size_out.
        // Use a nice round native amount so we can reason about the price.
        state.size_out = Some(ChainTokenAmount::from_formatted(
            state.withdraw_asset.clone(),
            2.0,
        ));

        let swap = finalizer
            .finish(&receipt, &state)
            .await
            .expect("finish should succeed");

        // Pay leg comes from seized collateral (size_in).
        assert_eq!(swap.pay_asset, state.deposit_asset.asset_id());
        assert_eq!(swap.pay_amount, state.size_in.value);

        // Receive leg comes from size_out.
        let expected_out = state.size_out.as_ref().unwrap();
        assert_eq!(swap.receive_asset, state.withdraw_asset.asset_id());
        assert_eq!(swap.receive_amount, expected_out.value);

        // Price is computed as receive / pay in native units.
        let pay_native = state.size_in.to_f64();
        let recv_native = expected_out.to_f64();
        let expected_price = if pay_native > 0.0 {
            recv_native / pay_native
        } else {
            0.0
        };

        assert!((swap.exec_price - expected_price).abs() < 1e-9);
        assert!((swap.mid_price - expected_price).abs() < 1e-9);

        // Status and legs should reflect a single synthetic CEX hop.
        assert_eq!(swap.status, "completed".to_string());
        assert!(swap.legs.is_empty());
    }
}