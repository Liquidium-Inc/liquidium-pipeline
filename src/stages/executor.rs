use async_trait::async_trait;
use candid::{Decode, Encode};
use lending::interface::liquidation::LiquidationResult;
use log::info;

use crate::{
    executors::{
        executor::{ExecutorRequest, IcrcSwapExecutor},
        kong_swap::kong_swap::KongSwapExecutor,
    },
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
};

#[async_trait]
impl<A: PipelineAgent> PipelineStage<Vec<ExecutorRequest>, ()> for KongSwapExecutor<A> {
    async fn process(&self, executor_requests: Vec<ExecutorRequest>) -> Result<(), String> {
        for executor_request in executor_requests {
            let args = Encode!(&self.account_id.owner, &executor_request.liquidation).map_err(|e| e.to_string())?;

            // Make the update call to the canister
            let response = self
                .agent
                .call_update(&self.lending_canister, "liquidate", args)
                .await
                .map_err(|e| format!("Agent update error: {e}"))?;

            // Decode the candid response
            let result: Result<LiquidationResult, String> = Decode!(&response, Result<LiquidationResult, String>)
                .map_err(|e| format!("Candid decode error: {e}"))?;

            info!("Executed liquidation {:?}", result);

            if executor_request.swap_args.is_some() {
                let _ = self.swap(executor_request.swap_args.unwrap()).await;
            }
        }
        // TODO: Log each execution
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{
//         executors::kong_swap::{
//             kong_swap::KongSwapExecutor,
//             types::{SwapArgs, SwapReply},
//         },
//         pipeline_agent::MockPipelineAgent,
//     };

//     use candid::{Encode, Nat, Principal};
//     use icrc_ledger_types::icrc1::account::Account;
//     use lending::interface::liquidation::{LiquidationAmounts, LiquidationRequest, LiquidationResult};
//     use lending_utils::types::pool::AssetType;
//     use std::{collections::HashMap, sync::Arc};

//     #[tokio::test]
//     async fn test_kong_executor_process_success() {
//         let mut mock_agent = MockPipelineAgent::new();

//         let principal = Principal::anonymous();

//         let liquidation_result = LiquidationResult {
//             amounts: LiquidationAmounts {
//                 collateral_received: Nat::from(10_000u64),
//                 debt_repaid: Nat::from(9_500u64),
//                 bonus_earned: Nat::from(500u64),
//             },
//             tx_id: "tx123".to_string(),
//             collateral_asset: AssetType::CkAsset(Principal::anonymous()),
//             debt_asset: AssetType::CkAsset(Principal::management_canister()),
//         };

//         let swap_result = SwapReply {
//             tx_id: 1,
//             request_id: 42,
//             status: "Success".to_string(),
//             pay_chain: "ICP".to_string(),
//             pay_symbol: "ckUSDC".to_string(),
//             pay_amount: Nat::from(100_000u64),
//             receive_chain: "ICP".to_string(),
//             receive_symbol: "ckBTC".to_string(),
//             receive_amount: Nat::from(2_000u64),
//             mid_price: 0.0002,
//             price: 0.000195,
//             slippage: 0.0025,
//             txs: vec![],
//             transfer_ids: vec![],
//             claim_ids: vec![888],
//             ts: 1_717_178_000, // example UNIX timestamp
//         };

//         let liquidation_result_response = Encode!(&Ok::<_, String>(liquidation_result.clone())).unwrap();

//         let swap_result_response = Encode!(&Ok::<_, String>(swap_result.clone())).unwrap();

//         mock_agent.expect_call_update().returning(move |_, method, _| {
//             println!("Method {method}",);
//             match method {
//                 "swap" => Ok(swap_result_response.clone()),
//                 _ => Ok(liquidation_result_response.clone()),
//             }
//         });

//         let executor = KongSwapExecutor {
//             agent: Arc::new(mock_agent),
//             account_id: Account {
//                 owner: Principal::anonymous(),
//                 subaccount: None,
//             },
//             allowances: HashMap::new(),
//             lending_canister: Principal::anonymous(),
//             dex_account: Account {
//                 owner: Principal::anonymous(),
//                 subaccount: None,
//             },
//         };

//         let request = ExecutorRequest {
//             liquidation: LiquidationRequest {
//                 borrower: principal,
//                 debt_pool_id: principal,
//                 collateral_pool_id: principal,
//                 debt_amount: None,
//             },
//             swap_args: SwapArgs {
//                 pay_token: "USDC".to_string(),
//                 pay_amount: Nat::from(1000u32),
//                 pay_tx_id: None,
//                 receive_token: "USDT".to_string(),
//                 receive_amount: Some(Nat::from(1000u32)),
//                 receive_address: None,
//                 max_slippage: Some(3.0),
//                 referred_by: None,
//             },
//         };

//         let result = executor.process(request).await.unwrap();
//         assert_eq!(result.received_asset, swap_result.receive_symbol);
//         assert_eq!(result.received_amount, swap_result.receive_amount);
//     }
// }
