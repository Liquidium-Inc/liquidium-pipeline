use std::sync::Arc;

use ic_agent::Agent;
use log::info;

use crate::{
    account::account::LiquidatorAccount,
    bridge::{
        evm_client_wrapper::init_evm_client,
        evm_core_bridge::HyperliquidEvmCoreBridge,
        evm_to_icp_bridge::{EvmBridge, HyperliquidToIcpBridge},
    },
    config::{Config, ConfigTrait},
    finalizers::{
        hyperliquid::hyperliquid_finalizer_v2::HyperliquidFinalizerV2,
        kong_swap::kong_swap_finalizer::KongSwapFinalizer,
    },
    persistance::{WalStore, sqlite::SqliteWalStore},
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    stages::{
        bridge::{BridgeToHyperliquidStage, BridgeToIcStage}, executor::ExecutionReceipt, finalize::LiquidationOutcome, swap::HyperliquidSwapStage
    },
    swappers::{hyperliquid_swapper::HyperliquidSwapper, kong_swap_swapper::KongSwapSwapper},
};

// Enum to handle different finalizer pipelines
pub enum FinalizerPipeline {
    Kong(Arc<KongSwapFinalizer<SqliteWalStore, KongSwapSwapper<Agent>, LiquidatorAccount<Agent>, Config, Agent>>),
    Hyperliquid {
        bridge_to_hl: Arc<
            BridgeToHyperliquidStage<HyperliquidToIcpBridge<Agent, Config>, HyperliquidEvmCoreBridge<Config>, Config>,
        >,
        swap: Arc<HyperliquidSwapStage<HyperliquidSwapper, Config>>,
        bridge_to_ic:
            Arc<BridgeToIcStage<HyperliquidToIcpBridge<Agent, Config>, HyperliquidEvmCoreBridge<Config>, Config>>,
        finalizer: Arc<HyperliquidFinalizerV2<SqliteWalStore, Config, Agent>>,
    },
}

impl FinalizerPipeline {
    pub async fn initialize_hyperliquid(config: Arc<Config>, agent: Arc<Agent>, db: Arc<SqliteWalStore>) -> Self {
        info!("Initializing Hyperliquid multi-stage pipeline...");

        let evm_client = init_evm_client(
            config.get_hyperliquid_rpc_url().clone().unwrap(),
            config.get_hyperliquid_chain_id().unwrap(),
            config.get_hyperliquid_wallet_key().unwrap(),
        )
        .await
        .expect("Failed to initialize EVM client");

        //  Create IC to EVM bridge
        let mut ic_to_evm_bridge = HyperliquidToIcpBridge::new(agent.clone(), evm_client.clone(), config.clone())
            .expect("Failed to create EVM bridge");

        let ic_to_evm_bridge = Arc::new(ic_to_evm_bridge);

        // Create EVM to Core bridge
        let mut evm_to_core_bridge = Arc::new(
            HyperliquidEvmCoreBridge::new(config.clone(), evm_client.clone())
                .await
                .expect("Failed to create EVM bridge"),
        );

        // Create Hyperliquid swapper
        let hl_swapper = Arc::new(
            HyperliquidSwapper::new(config.get_hyperliquid_wallet_key().unwrap())
                .expect("Failed to create Hyperliquid swapper"),
        );

        // Create pipeline stages
        let bridge_to_hl = Arc::new(BridgeToHyperliquidStage::new(
            ic_to_evm_bridge.clone(),
            evm_to_core_bridge.clone(),
            config.clone(),
        ));

        let swap_stage = Arc::new(HyperliquidSwapStage::new(hl_swapper, config.clone()));
        let bridge_to_ic = Arc::new(BridgeToIcStage::new(
            ic_to_evm_bridge.clone(),
            evm_to_core_bridge.clone(),
            config.clone(),
        ));
        let hl_finalizer = Arc::new(HyperliquidFinalizerV2::new(db, config.clone(), agent.clone()));

        info!("Hyperliquid pipeline initialized");

        FinalizerPipeline::Hyperliquid {
            bridge_to_hl,
            swap: swap_stage,
            bridge_to_ic,
            finalizer: hl_finalizer,
        }
    }

    pub async fn process(
        &self,
        receipts: &Vec<ExecutionReceipt>,
    ) -> Result<Vec<LiquidationOutcome>, String> {
        match self {
            FinalizerPipeline::Kong(finalizer) => finalizer.process(receipts).await,
            FinalizerPipeline::Hyperliquid {
                bridge_to_hl,
                swap,
                bridge_to_ic,
                finalizer,
            } => {
                // Multi-stage pipeline: executor -> bridge_to_hl -> swap -> bridge_to_ic -> finalizer
                log::info!("Processing through Hyperliquid multi-stage pipeline");

                let bridge_receipts = bridge_to_hl.process(receipts).await?;
                let swap_receipts = swap.process(&bridge_receipts).await?;
                let ic_receipts = bridge_to_ic.process(&swap_receipts).await?;
                finalizer.process(&ic_receipts).await
            }
        }
    }
}
