use std::sync::Arc;

use log::{info, warn};

use crate::{
    account::account::IcrcAccountActions,
    config::ConfigTrait,
    persistance::WalStore,
    pipeline_agent::PipelineAgent,
    swappers::{
        kong_types::{SwapArgs, SwapReply},
        swap_interface::IcrcSwapInterface,
    },
};

use super::ccxt_client::CcxtClient;

pub struct MexcFinalizer<
    D: WalStore,
    S: IcrcSwapInterface,
    A: IcrcAccountActions,
    C: ConfigTrait,
    P: PipelineAgent,
    X: CcxtClient,
> {
    pub config: Arc<C>,
    pub db: Arc<D>,
    pub swapper: Arc<S>, // MixcCcxtSwapper<X> typically
    pub account: Arc<A>,
    pub agent: Arc<P>,
    pub ccxt: Arc<X>,
}

impl<D: WalStore, S: IcrcSwapInterface, A: IcrcAccountActions, C: ConfigTrait, P: PipelineAgent, X: CcxtClient>
    MexcFinalizer<D, S, A, C, P, X>
{
    pub fn new(db: Arc<D>, swapper: Arc<S>, account: Arc<A>, config: Arc<C>, agent: Arc<P>, ccxt: Arc<X>) -> Self {
        Self {
            db,
            swapper,
            account,
            config,
            agent,
            ccxt,
        }
    }

    // High-level flow: deposit -> swap -> withdraw back
    pub async fn execute_mexc_swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String> {
        let token_in = &swap_args.token_in;
        let token_out = &swap_args.token_out;

        let asset_in = token_in.symbol.clone(); // map ICRC symbol -> CEX asset
        let asset_out = token_out.symbol.clone(); // same for out side

        let network_in = Some("ICP"); // TODO: from config
        let network_out = Some("ETH"); // TODO: from config

        info!("MEXC finalizer: starting cross-cex swap {} -> {}", asset_in, asset_out);

        // 1. Get MEXC deposit address
        let dep = self.ccxt.get_deposit_address(&asset_in, network_in.as_deref()).await?;

        // 2. Send funds from IC to that deposit address
        // This uses your IcrcAccountActions implementation.
        // Adjust method name / params to your real API.
        info!(
            "MEXC finalizer: sending deposit to {} (network {:?})",
            dep.address, dep.network
        );

        // Example, you likely have something like:
        // self.account.send_icrc_deposit(&asset_in, &dep.address, swap_args.amount_in.value.clone()).await?;
        self.account
            .send_cex_deposit(&asset_in, &dep.address, swap_args.amount_in.value.clone())
            .await
            .map_err(|e| format!("ICRC deposit failed: {}", e))?;

        // TODO: mark in WAL: deposit_sent

        // 3. Wait for deposit credited on MEXC
        // In practice this is usually done by an off-chain watcher, not this function.
        // Here we just assume funds are there or you call ccxt to poll balances.

        // 4. Execute swap on MEXC via ccxt-backed swapper
        info!("MEXC finalizer: executing ccxt swap on MEXC");
        let swap_reply = self.swapper.swap(swap_args).await?;

        // TODO: mark in WAL: swap_done

        // 5. Withdraw back from MEXC to IC address
        let withdraw_target = self
            .config
            .mexc_withdraw_address_for(&asset_out)
            .map_err(|e| format!("No withdraw address for {}: {}", asset_out, e))?;

        let out_amount_f64 = {
            // you will want proper Nat -> f64 conversion with decimals
            let u = swap_reply
                .amount_out
                .value
                .0
                .to_u128()
                .ok_or("swap_out too large for f64")?;
            u as f64
        };

        info!(
            "MEXC finalizer: withdrawing {} {} back to {}",
            out_amount_f64, asset_out, withdraw_target
        );

        let _receipt = self
            .ccxt
            .withdraw(
                &asset_out,
                network_out.as_deref(),
                &withdraw_target,
                None,
                out_amount_f64,
            )
            .await
            .map_err(|e| format!("MEXC withdraw failed: {}", e))?;

        // TODO: mark in WAL: withdraw_sent

        Ok(swap_reply)
    }
}
