use std::sync::Arc;

use crate::executors::executor::{ExecutorRequest, IcrcSwapExecutor};
use crate::executors::kong_swap::types::SwapArgs;
use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;
use crate::{config::Config, stage::PipelineStage};
use async_trait::async_trait;
use candid::Nat;
use lending::{
    interface::liquidation::LiquidationRequest, liquidation::liquidation::LiquidateblePosition,
};
use lending_utils::types::pool::AssetType;
use log::info;

pub struct IcrcLiquidationStrategy<T: IcrcSwapExecutor + Send + Sync> {
    pub config: Arc<Config>,
    pub executor: Arc<T>,
}

impl<T: IcrcSwapExecutor + Send + Sync> IcrcLiquidationStrategy<T> {
    pub fn new(config: Arc<Config>, executor: Arc<T>) -> Self {
        Self { config, executor }
    }
}

#[async_trait]
pub trait Strategy: Send + Sync {
    async fn calculate_received_collateral() -> Nat;
}

#[async_trait]
impl<T: IcrcSwapExecutor + Send + Sync> Strategy for IcrcLiquidationStrategy<T> {
    async fn calculate_received_collateral() -> Nat {
        todo!()
    }
}

#[async_trait]
impl<T: IcrcSwapExecutor + Send + Sync> PipelineStage<Vec<LiquidateblePosition>, ExecutorRequest>
    for IcrcLiquidationStrategy<T>
{
    async fn process(
        &self,
        positions: Vec<LiquidateblePosition>,
    ) -> Result<ExecutorRequest, String> {
        // Find the biggest debt
        let debt_position = positions
            .iter()
            .max_by(|a, b| a.debt_amount.cmp(&b.debt_amount))
            .unwrap();

        // Find the biggest collateral
        let collateral_position = positions
            .iter()
            .max_by(|a, b| a.collateral_amount.cmp(&b.collateral_amount))
            .unwrap();

        let debt_asset_principal = match debt_position.asset_type {
            AssetType::CkAsset(principal) => Ok(principal),
            _ => Err("invalid asset type"),
        }
        .unwrap();

        let collateral_asset_principal = match debt_position.asset_type {
            AssetType::CkAsset(principal) => Ok(principal),
            _ => Err("invalid asset type"),
        }
        .unwrap();

        let token_in = self
            .config
            .collateral_assets
            .iter()
            .find(|item| *item.0 == collateral_asset_principal)
            .expect("invalid asset principal")
            .1;

        let token_out = self
            .config
            .collateral_assets
            .iter()
            .find(|item| *item.0 == debt_asset_principal)
            .expect("invalid asset principal")
            .1;

        let amount = IcrcTokenAmount {
            token: token_in.clone(),
            value: Self::calculate_received_collateral().await,
        };

        let swap_info = self
            .executor
            .get_swap_info(token_in, token_out, &amount)
            .await
            .expect("could not get swap info");

        info!("Got swap info {:#?}", swap_info);

        Ok(ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: debt_position.account,
                debt_pool_id: debt_position.pool_id,
                collateral_pool_id: collateral_position.pool_id,
                debt_amount: None,
            },
            swap_args: SwapArgs {
                pay_token: token_in.symbol.clone(),
                pay_amount: amount.value,
                pay_tx_id: None,
                receive_token: swap_info.receive_symbol,
                receive_amount: Some(swap_info.receive_amount),
                receive_address: None,
                max_slippage: Some(swap_info.slippage),
                referred_by: None,
            },
        })
    }
}
