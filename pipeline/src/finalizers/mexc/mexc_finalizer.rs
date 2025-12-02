use std::sync::Arc;

use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
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

        // Idempotency: if we already recorded a deposit txid, just move on.
        if state.deposit_txid.is_some() {
            debug!(
                "[mexc] liq_id={} deposit already recorded (txid={:?}), skipping",
                state.liq_id, state.deposit_txid
            );
            state.step = CexStep::Trade;
            return Ok(());
        }

        // Ask MEXC for the deposit address for this asset/network.
        let addr = self
            .backend
            .get_deposit_address(&state.deposit_asset.symbol(), &state.deposit_asset.chain())
            .await?;

        debug!(
            "[mexc] liq_id={} got deposit address={} tag={:?}",
            state.liq_id, addr.address, addr.tag
        );

        // Amount to send to MEXC (seized collateral).
        let amount = &state.size_in;

        assert_eq!(addr.asset, state.deposit_asset.symbol(), "deposit address mismatch");

        let actions = self.transfer_service.actions();
        let tx_id = actions
            .transfer(
                &state.deposit_asset,
                &ChainAccount::Icp(Account {
                    owner: Principal::from_text(addr.address).expect("could not decode deposit address"),
                    subaccount: None,
                }),
                amount.value.clone(),
            )
            .await?;

        // Record txid for idempotency and advance to the next step.
        state.deposit_txid = Some(tx_id);
        state.step = CexStep::Trade;

        Ok(())
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
        let receive_amount = liq.amounts.debt_repaid.clone();

        // Pay side: seized collateral we sent in, as recorded on the CEX state.
        let pay_amount = state.size_in.value.clone();

        // Compute an effective execution price in native units: receive / pay.
        let pay_f = pay_amount
            .0
            .to_f64()
            .ok_or_else(|| "could not convert pay amount to f64".to_string())?;
        let recv_f = receive_amount
            .0
            .to_f64()
            .ok_or_else(|| "could not convert receive amount to f64".to_string())?;

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
            pay_amount,
            receive_asset: state.withdraw_asset.asset_id(),
            receive_amount,
            mid_price: exec_price,
            exec_price,
            slippage: 0.0,
            legs: Vec::new(),
            ts: 0,
        };

        Ok(exec)
    }
}
