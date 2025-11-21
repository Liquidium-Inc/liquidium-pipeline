use async_trait::async_trait;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::transfer::actions::TransferActions;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use liquidium_pipeline_core::tokens::token_registry::{TokenRegistry, TokenRegistryTrait};
use log::info;
use num_traits::ToPrimitive;

use crate::config::Config;
use crate::{
    persistance::{LiqResultRecord, ResultStatus, WalStore, now_secs},
    swappers::{
        model::{SwapExecution, SwapRequest},
        swap_interface::SwapInterface,
    },
};
use serde_json::json;
use tokio::time::sleep;

#[async_trait]
pub trait CexBridge: Send + Sync {
    async fn execute_roundtrip(&self, liq_id: &str, idx: i32, req: &SwapRequest) -> Result<(), String>;
}

pub struct MexcBridge<D, X>
where
    D: WalStore,
    X: CexBackend,
{
    pub config: Arc<Config>,
    pub db: Arc<D>,
    pub ccxt: Arc<X>,
    pub registry: Arc<TokenRegistry>,
    pub accounts: Arc<dyn TransferActions + Send + Sync>,
    pub swapper: Arc<dyn SwapInterface + Send + Sync>,
}

impl<D, X> MexcBridge<D, X>
where
    D: WalStore,
    X: CexBackend,
{
    pub fn new(
        config: Arc<Config>,
        db: Arc<D>,
        ccxt: Arc<X>,
        registry: Arc<TokenRegistry>,
        accounts: Arc<dyn TransferActions + Send + Sync>,
        swapper: Arc<dyn SwapInterface + Send + Sync>,
    ) -> Self {
        Self {
            config,
            db,
            ccxt,
            registry,
            accounts,
            swapper,
        }
    }

    async fn journal(
        &self,
        liq_id: &str,
        idx: i32,
        step: &str,
        status: ResultStatus,
        req: &SwapRequest,
        swap: Option<&SwapExecution>,
        last_error: Option<&str>,
    ) -> Result<(), String> {
        let existing = self
            .db
            .get_result(liq_id, idx)
            .await
            .map_err(|e| format!("wal get_result failed: {e}"))?;

        let created_at = existing.as_ref().map(|r| r.created_at).unwrap_or_else(now_secs);
        let attempt = existing.as_ref().map(|r| r.attempt).unwrap_or(0);

        let receive_amount = swap.map(|s| s.receive_amount.0.to_string());

        let meta = json!({
            "type": "mexc_cex_leg",
            "step": step,
            "pay_asset": {
                "chain": req.pay_asset.chain,
                "symbol": req.pay_asset.symbol,
            },
            "receive_asset": {
                "chain": req.receive_asset.chain,
                "symbol": req.receive_asset.symbol,
            },
            "pay_amount": req.pay_amount,
            "receive_amount": receive_amount,
            "last_error": last_error,
        });

        let row = LiqResultRecord {
            liq_id: liq_id.to_string(),
            idx,
            status,
            attempt,
            created_at,
            updated_at: now_secs(),
            meta_json: meta.to_string(),
        };

        self.db
            .upsert_result(row)
            .await
            .map_err(|e| format!("wal journal failed: {e}"))
    }

    async fn wait_for_credit(
        &self,
        liq_id: &str,
        idx: i32,
        req: &SwapRequest,
        asset_symbol: &str,
        start_balance: f64,
    ) -> Result<(), String> {
        let required = req.pay_amount.to_f64();

        let timeout = Duration::from_secs(6000);
        let wait_start = Instant::now();

        loop {
            let bal = self
                .ccxt
                .get_balance(asset_symbol)
                .await
                .map_err(|e| format!("failed to get MEXC balance: {}", e))?;

            info!(
                "MEXC bridge: current MEXC balance for {} is {} (required {})",
                asset_symbol, bal - start_balance, required
            );

            if bal - start_balance >= required - 0.000001 {
                let elapsed = wait_start.elapsed();
                info!(
                    "MEXC bridge: deposit credited on MEXC after {:?}, proceeding to swap",
                    elapsed
                );
                self.journal(liq_id, idx, "CexCredited", ResultStatus::InFlight, req, None, None)
                    .await?;
                break;
            }

            if Instant::now().duration_since(wait_start) > timeout {
                let msg = "timeout waiting for MEXC deposit credit".to_string();
                self.journal(
                    liq_id,
                    idx,
                    "CreditTimeout",
                    ResultStatus::FailedRetryable,
                    req,
                    None,
                    Some(&msg),
                )
                .await?;
                return Err(msg);
            }

            sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    async fn step_ic_deposit(
        &self,
        liq_id: &str,
        idx: i32,
        req: &SwapRequest,
        asset_in: &str,
        mexc_deposit_account: &str,
    ) -> Result<(), String> {
        let deposit_start = Instant::now();
        info!(
            "MEXC bridge: sending deposit of {} {} to {:?} (IC inbox)",
            req.pay_amount.formatted(),
            asset_in,
            mexc_deposit_account
        );

        let dep_account = ChainAccount::Evm(mexc_deposit_account.to_string());

        let transfer_in = self
            .registry
            .resolve(&req.pay_asset)
            .map_err(|e| format!("token registry resolve failed: {}", e))?;

        if let Err(e) = self
            .accounts
            .transfer(&transfer_in, &dep_account, req.pay_amount.value.clone())
            .await
        {
            let msg = format!("ICRC deposit failed: {}", e);
            self.journal(
                liq_id,
                idx,
                "DepositFailed",
                ResultStatus::FailedRetryable,
                req,
                None,
                Some(&msg),
            )
            .await?;
            return Err(msg);
        }

        let deposit_done = Instant::now();
        info!(
            "MEXC bridge: deposit step took {:?}",
            deposit_done.duration_since(deposit_start)
        );

        self.journal(liq_id, idx, "DepositSent", ResultStatus::InFlight, req, None, None)
            .await?;

        Ok(())
    }

    async fn step_swap(
        &self,
        liq_id: &str,
        idx: i32,
        req: &SwapRequest,
        asset_in: &str,
        asset_out: &str,
    ) -> Result<SwapExecution, String> {
        let swap_start = Instant::now();
        info!("MEXC bridge: executing swap on MEXC {} -> {}", asset_in, asset_out);

        let swap_reply = match self.swapper.execute(req).await {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("MEXC swap failed: {}", e);
                self.journal(
                    liq_id,
                    idx,
                    "SwapFailed",
                    ResultStatus::FailedRetryable,
                    req,
                    None,
                    Some(&msg),
                )
                .await?;
                return Err(msg);
            }
        };

        let swap_done = Instant::now();
        info!("MEXC bridge: swap step took {:?}", swap_done.duration_since(swap_start));

        self.journal(
            liq_id,
            idx,
            "SwapDone",
            ResultStatus::InFlight,
            req,
            Some(&swap_reply),
            None,
        )
        .await?;

        Ok(swap_reply)
    }

    async fn step_withdraw(
        &self,
        liq_id: &str,
        idx: i32,
        req: &SwapRequest,
        asset_out: &str,
        withdraw_target: &str,
        swap_reply: &SwapExecution,
    ) -> Result<(), String> {
        let out_amount_f64 = {
            let u = swap_reply
                .receive_amount
                .0
                .to_u128()
                .ok_or("receive_amount too large for f64")?;
            u as f64
        };

        let withdraw_start = Instant::now();
        info!(
            "MEXC bridge: withdrawing {} {} back to {}",
            out_amount_f64, asset_out, withdraw_target
        );

        if let Err(e) = self
            .ccxt
            .withdraw(
                &req.receive_asset.symbol,
                &req.receive_asset.chain,
                withdraw_target,
                out_amount_f64,
            )
            .await
        {
            let msg = format!("MEXC withdraw failed: {}", e);
            self.journal(
                liq_id,
                idx,
                "WithdrawFailed",
                ResultStatus::FailedRetryable,
                req,
                Some(swap_reply),
                Some(&msg),
            )
            .await?;
            return Err(msg);
        }

        let withdraw_done = Instant::now();
        info!(
            "MEXC bridge: withdraw step took {:?}",
            withdraw_done.duration_since(withdraw_start)
        );

        self.journal(
            liq_id,
            idx,
            "WithdrawSent",
            ResultStatus::Succeeded,
            req,
            Some(swap_reply),
            None,
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl<D, X> CexBridge for MexcBridge<D, X>
where
    D: WalStore,
    X: CexBackend,
{
    async fn execute_roundtrip(&self, liq_id: &str, idx: i32, req: &SwapRequest) -> Result<(), String> {
        let asset_in = req.pay_asset.clone();
        let asset_out = req.receive_asset.clone();

        self.journal(liq_id, idx, "Created", ResultStatus::Enqueued, req, None, None)
            .await?;

        info!(
            "MEXC bridge: starting cross-cex swap {} -> {} (amount {})",
            asset_in,
            asset_out,
            req.pay_amount.formatted()
        );
        let t0 = Instant::now();

        let dep = self
            .ccxt
            .get_deposit_address(&asset_in.symbol, &asset_in.chain)
            .await
            .map_err(|e| format!("failed to fetch MEXC deposit address: {}", e))?;

        info!("using deposit address {:?}", dep);

        let mexc_ic_deposit_account = dep.address.clone();

        self.step_ic_deposit(liq_id, idx, req, &asset_in.symbol, &mexc_ic_deposit_account)
            .await?;

        let bal = self
            .ccxt
            .get_balance(&asset_in.symbol)
            .await
            .map_err(|e| format!("failed to get MEXC balance: {}", e))?;

        self.wait_for_credit(liq_id, idx, req, &asset_in.symbol, bal).await?;

        // let swap_reply = self
        //     .step_swap(liq_id, idx, req, &asset_in.symbol, &asset_out.symbol)
        //     .await?;

        let withdraw_target = req.receive_address.clone().unwrap();

        info!("[] Sending to addr {} ", withdraw_target);

        // self.step_withdraw(liq_id, idx, req, &asset_in.symbol, &withdraw_target, &swap_reply)
        //     .await?;

        let _ = self
            .ccxt
            .withdraw(&req.receive_asset.symbol, "ARB", &withdraw_target, 1.0)
            .await
            .map_err(|e| e.to_string())?;

        let withdraw_done = Instant::now();
        info!(
            "MEXC bridge: total mexc leg took {:?}",
            withdraw_done.duration_since(t0)
        );

        Ok(())
    }
}

pub struct MexcFinalizer<B>
where
    B: CexBridge,
{
    pub bridge: Arc<B>,
}

impl<B> MexcFinalizer<B>
where
    B: CexBridge,
{
    pub fn new(bridge: Arc<B>) -> Self {
        Self { bridge }
    }

    // High-level flow delegating to the bridge
    pub async fn execute_mexc_swap(&self, liq_id: &str, idx: i32, swap_args: SwapRequest) -> Result<(), String> {
        self.bridge.execute_roundtrip(liq_id, idx, &swap_args).await
    }
}
