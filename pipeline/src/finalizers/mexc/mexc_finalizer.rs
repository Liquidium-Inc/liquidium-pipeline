use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::{
    account::model::ChainAccount,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
    transfer::actions::TransferActions,
};
use log::{debug, info, warn};

use crate::{
    finalizers::cex_finalizer::{CexFinalizerLogic, CexState, CexStep},
    stages::executor::ExecutionReceipt,
    swappers::model::SwapExecution,
};

// MEXC-specific implementation of the generic CEX finalizer logic.
//
// This is a thin state machine wrapper around the generic `CexState`:
// - `prepare` initializes the state from a liquidation id / receipt plus static config
// - `deposit` transitions Deposit -> Trade
// - `trade` transitions Trade -> Withdraw
// - `withdraw` transitions Withdraw -> Completed
pub struct MexcFinalizer<C>
where
    C: CexBackend,
{
    pub backend: Arc<C>,
    pub transfer_service: Arc<dyn TransferActions>,
    pub liquidator_principal: Principal,
    pub max_sell_slippage_bps: f64,
    approve_bumps: Mutex<HashMap<String, u8>>,
}

const DEFAULT_ORDERBOOK_LIMIT: u32 = 50;

#[derive(Debug, Clone)]
struct TradeLeg {
    market: String,
    side: String,
}

fn mexc_trade_legs(
    deposit_symbol: &str,
    withdraw_symbol: &str,
    default_market: &str,
    default_side: &str,
) -> Vec<TradeLeg> {
    let deposit = deposit_symbol.to_ascii_uppercase();
    let withdraw = withdraw_symbol.to_ascii_uppercase();

    if deposit == "CKBTC" && withdraw == "CKUSDT" {
        return vec![
            TradeLeg {
                market: "CKBTC_BTC".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "BTC_USDC".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "USDC_USDT".to_string(),
                side: "sell".to_string(),
            },
            TradeLeg {
                market: "CKUSDT_USDT".to_string(),
                side: "buy".to_string(),
            },
        ];
    }

    vec![TradeLeg {
        market: default_market.to_string(),
        side: default_side.to_string(),
    }]
}

impl<C> MexcFinalizer<C>
where
    C: CexBackend,
{
    pub fn new(
        backend: Arc<C>,
        transfer_service: Arc<dyn TransferActions>,
        liquidator_principal: Principal,
        max_sell_slippage_bps: f64,
    ) -> Self {
        Self {
            backend,
            transfer_service,
            liquidator_principal,
            max_sell_slippage_bps,
            approve_bumps: Mutex::new(HashMap::new()),
        }
    }

    pub async fn check_deposit(&self, state: &mut CexState) -> Result<(), String> {
        info!("Checking deposit {}", state.deposit_asset);
        let symbol = state.deposit_asset.symbol();
        let current_bal = self.backend.get_balance(&symbol).await?;

        match state.deposit_balance_before {
            Some(b0) => {
                let expected = state.size_in.to_f64();
                let delta = current_bal - b0;
                info!(
                    "[mexc] liq_id={} deposit check: before={} current={} delta={} expected={}",
                    state.liq_id, b0, current_bal, delta, expected
                );

                if delta >= expected - 0.00001 {
                    debug!(
                        "[mexc] liq_id={} deposit confirmed on CEX, before={} after={}",
                        state.liq_id, b0, current_bal
                    );
                    state.step = CexStep::Trade;
                    return Ok(());
                }
                debug!(
                    "[mexc] liq_id={} deposit not yet visible on CEX (before={}, after={}), staying in Deposit",
                    state.liq_id, b0, current_bal
                );
            }
            None => {
                // No baseline recorded yet (should normally be set when we send the transfer),
                // so record the current balance as the baseline and stay in Deposit.
                info!(
                    "[mexc] liq_id={} no baseline recorded, setting deposit_balance_before={} (current={})",
                    state.liq_id, current_bal, current_bal
                );
                state.deposit_balance_before = Some(current_bal);
            }
        }

        Ok(())
    }

    async fn check_sell_liquidity(&self, liq_id: &str, market: &str, amount_in: f64) -> Result<(), String> {
        if amount_in <= 0.0 {
            return Ok(());
        }

        let orderbook = self
            .backend
            .get_orderbook(market, Some(DEFAULT_ORDERBOOK_LIMIT))
            .await?;

        let best_bid = orderbook.bids.first().map(|level| level.price).unwrap_or(0.0);
        if best_bid <= 0.0 {
            return Err(format!("no bids available for market {}", market));
        }

        let mut remaining = amount_in;
        let mut proceeds = 0.0;

        for level in &orderbook.bids {
            if remaining <= 0.0 {
                break;
            }
            if level.quantity <= 0.0 || level.price <= 0.0 {
                continue;
            }

            let take = remaining.min(level.quantity);
            proceeds += take * level.price;
            remaining -= take;
        }

        if remaining > 0.0 {
            return Err(format!(
                "not enough bid liquidity for {} (needed {}, missing {})",
                market, amount_in, remaining
            ));
        }

        let avg_price = proceeds / amount_in;
        let slippage_bps = ((best_bid - avg_price) / best_bid) * 10_000.0;

        info!(
            "[mexc] liq_id={} liquidity check market={} amount_in={} best_bid={} avg_price={} slippage_bps={}",
            liq_id, market, amount_in, best_bid, avg_price, slippage_bps
        );

        if slippage_bps > self.max_sell_slippage_bps {
            warn!(
                "[mexc] liq_id={} slippage check failed: market={} amount_in={} best_bid={} avg_price={} slippage_bps={} max_bps={}",
                liq_id,
                market,
                amount_in,
                best_bid,
                avg_price,
                slippage_bps,
                self.max_sell_slippage_bps
            );
            return Err(format!(
                "sell slippage too high for {}: {:.2} bps > {:.2} bps",
                market, slippage_bps, self.max_sell_slippage_bps
            ));
        }

        Ok(())
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
            trade_leg_index: None,
            trade_leg_total: None,
            trade_last_market: None,
            trade_last_side: None,
            trade_last_amount_in: None,
            trade_last_amount_out: None,
            trade_next_amount_in: None,

            // withdraw leg
            withdraw_asset: receipt.request.debt_asset.clone(),
            withdraw_address: self.liquidator_principal.to_text(),
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
            let fee = state.deposit_asset.fee();
            let fee_amount = ChainTokenAmount::from_raw(state.deposit_asset.clone(), fee.clone());
            let total_amount = ChainTokenAmount::from_raw(state.deposit_asset.clone(), amount.value.clone() + fee);
            info!(
                "[mexc] liq_id={} deposit transfer amount={} fee={} total={}",
                state.liq_id,
                amount.formatted(),
                fee_amount.formatted(),
                total_amount.formatted()
            );

            let actions = &self.transfer_service;

            debug!(
                "[mexc] liq_id={} transferring {} address={}",
                state.liq_id, amount.value, addr.address
            );
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

            if matches!(state.deposit_asset, ChainToken::Icp { .. }) {
                let already_bumped = {
                    let approve_bumps = self.approve_bumps.lock().expect("approve_bumps mutex poisoned");
                    *approve_bumps.get(&state.liq_id).unwrap_or(&0)
                };

                if already_bumped >= 6 {
                    debug!(
                        "[mexc] liq_id={} deposit transfer: approve bump already done (count={})",
                        state.liq_id, already_bumped
                    );
                } else {
                    let approve_amount = Nat::from(1u8);
                    let spender = ChainAccount::Icp(Account {
                        owner: self.liquidator_principal,
                        subaccount: None,
                    });
                    info!(
                        "[mexc] liq_id={} deposit transfer: approve bump x6 amount={} asset={}",
                        state.liq_id, approve_amount, state.deposit_asset
                    );

                    let mut ok = true;
                    for _ in 0..6 {
                        if let Err(err) = self
                            .transfer_service
                            .approve(&state.deposit_asset, &spender, approve_amount.clone())
                            .await
                        {
                            ok = false;
                            info!("[mexc] liq_id={} approve bump failed: {}", state.liq_id, err);
                            break;
                        }
                    }

                    if ok {
                        let mut approve_bumps = self.approve_bumps.lock().expect("approve_bumps mutex poisoned");
                        approve_bumps.insert(state.liq_id.clone(), 6);
                    }
                }
            } else {
                debug!(
                    "[mexc] liq_id={} deposit transfer: approve bump skipped (non-ICP asset={})",
                    state.liq_id, state.deposit_asset
                );
            }

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

        let legs = mexc_trade_legs(
            &state.deposit_asset.symbol(),
            &state.withdraw_asset.symbol(),
            &state.market,
            &state.side,
        );
        state.trade_leg_total = Some(legs.len() as u32);
        if legs.len() > 1 {
            debug!("[mexc] liq_id={} multi-hop route: {:?}", state.liq_id, legs);
        }

        let idx = state.trade_leg_index.unwrap_or(0) as usize;
        if idx >= legs.len() {
            state.step = CexStep::Withdraw;
            if let Some(out_amt) = state.trade_next_amount_in {
                state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), out_amt));
            }
            return Ok(());
        }

        let amount_in = state.trade_next_amount_in.unwrap_or_else(|| state.size_in.to_f64());

        if amount_in <= 0.0 {
            debug!(
                "[mexc] liq_id={} trade skipped: non-positive amount_in={}",
                state.liq_id, amount_in
            );
            state.step = CexStep::Withdraw;
            return Ok(());
        }

        let leg = &legs[idx];

        state.trade_last_market = Some(leg.market.clone());
        state.trade_last_side = Some(leg.side.clone());
        state.trade_last_amount_in = Some(amount_in);
        state.trade_last_amount_out = None;
        state.last_error = None;

        debug!(
            "[mexc] liq_id={} trade leg {}/{} market={} side={} amount_in={}",
            state.liq_id,
            idx + 1,
            legs.len(),
            leg.market,
            leg.side,
            amount_in
        );

        if leg.side.eq_ignore_ascii_case("sell") {
            if let Err(err) = self.check_sell_liquidity(&state.liq_id, &leg.market, amount_in).await {
                state.last_error = Some(format!("liquidity check failed: {}", err));
                return Err(err);
            }
        }

        // Execute the spot trade on MEXC. The backend is responsible for:
        // - placing the order (market)
        // - handling partial fills / retries
        // - updating internal CEX balances
        let filled = match self.backend.execute_swap(&leg.market, &leg.side, amount_in).await {
            Ok(v) => v,
            Err(e) => {
                state.last_error = Some(format!(
                    "trade leg {}/{} {} {} failed: {}",
                    idx + 1,
                    legs.len(),
                    leg.market,
                    leg.side,
                    e
                ));
                return Err(e);
            }
        };

        debug!(
            "[mexc] liq_id={} trade leg {}/{} filled={}",
            state.liq_id,
            idx + 1,
            legs.len(),
            filled
        );

        state.trade_last_amount_out = Some(filled);
        state.trade_next_amount_in = Some(filled);
        state.trade_leg_index = Some((idx + 1) as u32);

        if idx + 1 < legs.len() {
            state.step = CexStep::TradePending;
            return Ok(());
        }

        // For now we don't feed `filled` back into on-chain amounts; withdraw leg
        // can either use backend-specific balance checks or later extend CexState
        // with an explicit post-trade amount.
        state.step = CexStep::Withdraw;

        state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), filled));
        Ok(())
    }

    async fn withdraw(&self, state: &mut CexState) -> Result<(), String> {
        let expected_address = self.liquidator_principal.to_text();
        if state.withdraw_address != expected_address {
            debug!(
                "[mexc] liq_id={} override withdraw_address {} -> {}",
                state.liq_id, state.withdraw_address, expected_address
            );
            state.withdraw_address = expected_address;
        }

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

        // Amount to withdraw: prefer post-trade output when available.
        let amount = state
            .size_out
            .as_ref()
            .map(|out| out.to_f64())
            .unwrap_or_else(|| state.size_in.to_f64());

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
    use liquidium_pipeline_connectors::backend::cex_backend::{
        DepositAddress, MockCexBackend, OrderBook, OrderBookLevel,
    };
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use liquidium_pipeline_core::transfer::actions::MockTransferActions;
    use liquidium_pipeline_core::types::protocol_types::{
        AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
        TxStatus,
    };

    use crate::executors::executor::ExecutorRequest;
    use crate::finalizers::cex_finalizer::{CexState, CexStep};
    use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};

    const TEST_MAX_SELL_SLIPPAGE_BPS: f64 = 200.0;

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
            timestamp: 0,
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
            ref_price: Nat::from(0u8),
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
        let liquidator = Principal::anonymous();

        let finalizer = MexcFinalizer::new(backend, transfer_service, liquidator, TEST_MAX_SELL_SLIPPAGE_BPS);

        let receipt = make_execution_receipt(42);
        let state: CexState = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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
            receipt.liquidation_result.as_ref().unwrap().amounts.collateral_received
        );

        // withdraw leg
        assert_eq!(state.withdraw_asset, receipt.request.debt_asset);
        assert_eq!(state.withdraw_address, liquidator.to_text());
        assert!(state.withdraw_id.is_none());
        assert!(state.withdraw_txid.is_none());
        assert!(state.size_out.is_none());
    }

    #[tokio::test]
    async fn mexc_deposit_phase_a_snapshots_baseline_and_sends_transfer() {
        let mut backend = MockCexBackend::new();
        let mut transfers = MockTransferActions::new();

        backend.expect_get_balance().returning(|_symbol| Ok(10.0));

        backend.expect_get_deposit_address().returning(|_symbol, _chain| {
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
        transfers
            .expect_approve()
            .times(6)
            .returning(|_token, _spender, _amount| Ok("approve-1".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Pre-conditions: no deposit has been sent yet
        assert!(state.deposit_txid.is_none());
        assert!(state.deposit_balance_before.is_none());
        assert!(matches!(state.step, CexStep::Deposit));

        // Phase A: snapshot baseline and send transfer
        finalizer.deposit(&mut state).await.expect("deposit should succeed");

        assert_eq!(state.deposit_balance_before, Some(10.0));
        assert_eq!(state.deposit_txid.as_deref(), Some("tx-123"));
        assert!(matches!(state.step, CexStep::DepositPending));
    }

    #[tokio::test]
    async fn mexc_deposit_phase_b_without_baseline_sets_baseline_and_keeps_step() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        backend.expect_get_balance().returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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

        backend.expect_get_balance().returning(|_symbol| Ok(5.1));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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
        backend.expect_get_balance().returning(|_symbol| Ok(5.0));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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

        let orderbook = OrderBook {
            bids: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1_000.0,
            }],
            asks: vec![OrderBookLevel {
                price: 101.0,
                quantity: 1_000.0,
            }],
        };
        backend
            .expect_get_orderbook()
            .times(3)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        let calls = std::sync::Arc::new(std::sync::Mutex::new(0usize));
        let calls_handle = calls.clone();
        backend
            .expect_execute_swap()
            .times(4)
            .returning(move |market, side, amount_in| {
                let mut idx = calls_handle.lock().unwrap();
                let cur = *idx;
                *idx += 1;

                match cur {
                    0 => {
                        assert_eq!(market, "CKBTC_BTC");
                        assert_eq!(side, "sell");
                        assert!(amount_in > 0.0);
                        Ok(0.005)
                    }
                    1 => {
                        assert_eq!(market, "BTC_USDC");
                        assert_eq!(side, "sell");
                        assert!((amount_in - 0.005).abs() < 1e-12);
                        Ok(12.34)
                    }
                    2 => {
                        assert_eq!(market, "USDC_USDT");
                        assert_eq!(side, "sell");
                        assert!((amount_in - 12.34).abs() < 1e-12);
                        Ok(12.33)
                    }
                    3 => {
                        assert_eq!(market, "CKUSDT_USDT");
                        assert_eq!(side, "buy");
                        assert!((amount_in - 12.33).abs() < 1e-12);
                        Ok(12.32)
                    }
                    _ => unreachable!("unexpected execute_swap call"),
                }
            });

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Move directly into Trade step; size_in is taken from receipt and should be > 0.
        state.step = CexStep::Trade;

        finalizer.trade(&mut state).await.expect("trade leg 1 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 2 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 3 should succeed");
        assert!(matches!(state.step, CexStep::TradePending));
        assert!(state.size_out.is_none());

        finalizer.trade(&mut state).await.expect("trade leg 4 should succeed");

        let out = state.size_out.as_ref().expect("size_out should be set");
        assert_eq!(out.token, state.withdraw_asset);
        assert!((out.to_f64() - 12.32).abs() < 1e-9);
        assert!(matches!(state.step, CexStep::Withdraw));
    }

    #[tokio::test]
    async fn mexc_trade_propagates_backend_errors() {
        let mut backend = MockCexBackend::new();
        let transfers = MockTransferActions::new();

        let orderbook = OrderBook {
            bids: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1_000.0,
            }],
            asks: vec![OrderBookLevel {
                price: 101.0,
                quantity: 1_000.0,
            }],
        };
        backend
            .expect_get_orderbook()
            .times(1)
            .returning(move |_market, _limit| Ok(orderbook.clone()));

        backend
            .expect_execute_swap()
            .times(1)
            .returning(|_market, _side, _amount_in| Err("boom".to_string()));

        let backend = Arc::new(backend);
        let transfer_service = Arc::new(transfers);

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        state.step = CexStep::Trade;

        let err = finalizer.trade(&mut state).await.expect_err("trade should fail");

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

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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

        let finalizer = MexcFinalizer::new(
            backend,
            transfer_service,
            Principal::anonymous(),
            TEST_MAX_SELL_SLIPPAGE_BPS,
        );

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

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

        let finalizer = MexcFinalizer::new(backend, transfers, Principal::anonymous(), TEST_MAX_SELL_SLIPPAGE_BPS);

        let receipt = make_execution_receipt(42);
        let mut state = finalizer.prepare("42", &receipt).await.expect("prepare should succeed");

        // Simulate that trade/withdraw legs have populated size_out.
        // Use a nice round native amount so we can reason about the price.
        state.size_out = Some(ChainTokenAmount::from_formatted(state.withdraw_asset.clone(), 2.0));

        let swap = finalizer.finish(&receipt, &state).await.expect("finish should succeed");

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
