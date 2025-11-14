use std::sync::Arc;

use liquidium_pipeline_connectors::{backend::cex_backend::CexBackend, pipeline_agent::PipelineAgent};
use liquidium_pipeline_core::{account::actions::AccountActions, tokens::token_registry::TokenRegistry};
use log::info;
use num_traits::ToPrimitive;

use crate::{
    config::ConfigTrait,
    persistance::WalStore,
    swappers::{
        model::{SwapExecution, SwapRequest},
        swap_interface::SwapInterface,
    },
};

pub struct MexcFinalizer<D, C, P, X>
where
    D: WalStore,
    C: ConfigTrait,
    P: PipelineAgent,
    X: CexBackend,
{
    pub config: Arc<C>,
    pub db: Arc<D>,
    pub agent: Arc<P>,
    pub ccxt: Arc<X>,

    pub registry: Arc<TokenRegistry>,
    pub accounts: Arc<dyn AccountActions + Send + Sync>,
    pub swapper: Arc<dyn SwapInterface + Send + Sync>,
}

impl<D, C, P, X> MexcFinalizer<D, C, P, X>
where
    D: WalStore,
    C: ConfigTrait,
    P: PipelineAgent,
    X: CexBackend,
{
    pub fn new(
        config: Arc<C>,
        db: Arc<D>,
        agent: Arc<P>,
        ccxt: Arc<X>,
        registry: Arc<TokenRegistry>,
        accounts: Arc<dyn AccountActions + Send + Sync>,
        swapper: Arc<dyn SwapInterface + Send + Sync>,
    ) -> Self {
        Self {
            config,
            db,
            agent,
            ccxt,
            registry,
            accounts,
            swapper,
        }
    }

    // High-level flow: deposit -> swap -> withdraw back
    pub async fn execute_mexc_swap(&self, swap_args: SwapRequest) -> Result<SwapExecution, String> {
        let asset_in = swap_args.pay_asset.clone();
        let asset_out = swap_args.receive_asset.clone();

        info!(
            "MEXC finalizer: starting cross-cex swap {} -> {} (amount {})",
            asset_in, asset_out, swap_args.pay_amount
        );

        // 1. Determine the IC deposit account used as MEXC inbox for this asset.
        // The off-chain / bridge side is responsible for forwarding from that inbox
        // to the actual MEXC deposit address.
        let mexc_ic_deposit_account = "";

        info!(
            "MEXC finalizer: sending deposit of {} {} to {:?} (IC inbox)",
            swap_args.pay_amount, asset_in, mexc_ic_deposit_account
        );

        let transfer_in = self.registry.resolve(&asset_in)?;
        self.accounts
            .transfer(&transfer_in, mexc_ic_deposit_account, swap_args.pay_amount.clone(), false)
            .await
            .map_err(|e| format!("ICRC deposit failed: {}", e))?;

        // TODO: persist in WAL: deposit_sent

        // 3. Wait for deposit credited on MEXC
        // In practice: off-chain watcher polls balances / deposit history.
        // Here we assume funds are available by the time we call swap.

        // 4. Execute swap on MEXC via ccxt-backed swapper (implements IcrcSwapInterface)
        info!("MEXC finalizer: executing swap on MEXC {} -> {}", asset_in, asset_out);
        let swap_reply = self
            .swapper
            .execute(&swap_args)
            .await
            .map_err(|e| format!("MEXC swap failed: {}", e))?;

        // TODO: persist in WAL: swap_done

        // 5. Withdraw back from MEXC to IC address for the output asset
        let withdraw_target = "";

        let out_amount_f64 = {
            let u = swap_reply
                .receive_amount
                .0
                .to_u128()
                .ok_or("receive_amount too large for f64")?;
            u as f64
        };

        info!(
            "MEXC finalizer: withdrawing {} {} back to {} (network {:?})",
            out_amount_f64, asset_out, withdraw_target, asset_out
        );

        self.ccxt
            .withdraw(
                &asset_out.symbol,
                &asset_out.chain,
                &withdraw_target,
                None,
                out_amount_f64,
            )
            .await
            .map_err(|e| format!("MEXC withdraw failed: {}", e))?;

        // TODO: persist in WAL: withdraw_sent

        Ok(swap_reply)
    }
}
