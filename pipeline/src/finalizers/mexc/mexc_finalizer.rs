use std::sync::Arc;

use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, DepositAddress};
use liquidium_pipeline_core::{
    account::model::ChainAccount, tokens::chain_token_amount::ChainTokenAmount,
    transfer::transfer_service::TransferService,
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
    pub transfer_service: Arc<TransferService>,
}

impl<B> MexcFinalizer<B>
where
    B: CexBackend,
{
    pub fn new(backend: Arc<B>, transfer_service: Arc<TransferService>) -> Self {
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
                if current_bal - b0 > 0.00001 {
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

            let actions = self.transfer_service.actions();
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

    async fn finish(&self, receipt: &ExecutionReceipt, state: &CexState) -> Result<SwapExecution, String> {
        // We treat the CEX cycle as a single synthetic swap:
        //  - pay: seized collateral that was sent to MEXC (deposit leg)
        //  - receive: amount of debt that was actually repaid on-chain
        //
        // This keeps the semantics aligned with the DEX path, where SwapExecution
        // represents "how much did we pay vs how much did we receive" for
        // a given liquidation.

        let liq = receipt
            .liquidation_result
            .as_ref()
            .ok_or_else(|| "missing liquidation_result in receipt".to_string())?;

        // Receive side: whatever debt was repaid for this liquidation.
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
