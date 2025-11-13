use std::sync::Arc;

use log::info;
use num_traits::ToPrimitive;

use crate::{
    account::{actions::AccountActions, model::ChainToken},
    config::ConfigTrait,
    connectors::ccxt_client::CcxtClient,
    persistance::WalStore,
    pipeline_agent::PipelineAgent,
    swappers::{
        model::{SwapExecution, SwapRequest},
        swap_interface::SwapInterface,
    },
};

pub struct MexcFinalizer<
    D: WalStore,
    S: SwapInterface,
    A: AccountActions,
    C: ConfigTrait,
    P: PipelineAgent,
    X: CcxtClient,
> {
    pub config: Arc<C>,
    pub db: Arc<D>,
    pub swapper: Arc<S>, // typically a MexcSwapSwapper<X> implementing IcrcSwapInterface
    pub account: Arc<A>,
    pub agent: Arc<P>,
    pub ccxt: Arc<X>,
}

impl<D, S, A, C, P, X> MexcFinalizer<D, S, A, C, P, X>
where
    D: WalStore,
    S: SwapInterface,
    A: AccountActions,
    C: ConfigTrait,
    P: PipelineAgent,
    X: CcxtClient,
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

        self.account
            .transfer(
                &asset_in,
                mexc_ic_deposit_account,
                swap_args.pay_amount.0.to_u128().unwrap(),
                false,
            )
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
                &asset_out.symbol(),
                &asset_out.chain(),
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
