use async_trait::async_trait;
use candid::Encode;
use lending::interface::liquidation::LiquidationResult;
use log::info;

use crate::{
    executors::{
        executor::{ExecutorRequest, IcrcSwapExecutor},
        kong_swap::{kong_swap::KongSwapExecutor, types::SwapReply},
    },
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
};

#[derive(Debug, Clone)]
pub enum ExecutionStatus {
    Success,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ExecutionReceipt {
    pub liquidation_result: Option<LiquidationResult>,
    pub swap_result: Option<SwapReply>,
    pub status: ExecutionStatus,
}

#[async_trait]
impl<A: PipelineAgent> PipelineStage<Vec<ExecutorRequest>, Vec<ExecutionReceipt>> for KongSwapExecutor<A> {
    async fn process(&self, executor_requests: Vec<ExecutorRequest>) -> Result<Vec<ExecutionReceipt>, String> {
        let mut execution_receipts: Vec<ExecutionReceipt> = vec![];
        for executor_request in executor_requests {
            let args = Encode!(&self.account_id.owner, &executor_request.liquidation).map_err(|e| e.to_string())?;

            // Make the update call to the canister
            let liquidation_result = self
                .agent
                .call_update::<LiquidationResult>(&self.lending_canister, "liquidate", args)
                .await;

            // Check that the liquidation was executed successfully
            if liquidation_result.is_err() {
                execution_receipts.push(ExecutionReceipt {
                    liquidation_result: None,
                    swap_result: None,
                    status: ExecutionStatus::Error(format!(
                        "Could not execute liquidation {:?} {:?}",
                        executor_request,
                        liquidation_result.err()
                    )),
                });

                continue;
            }

            info!("Executed liquidation {:?}", liquidation_result);

            execution_receipts.push(ExecutionReceipt {
                liquidation_result: liquidation_result.ok(),
                swap_result: None,
                status: ExecutionStatus::Success,
            });

            if executor_request.swap_args.is_some() {
                let result = self
                    .swap(executor_request.swap_args.unwrap())
                    .await
                    .unwrap_or_else(|_| panic!("Could not execute swap"));

                let len = execution_receipts.len();
                execution_receipts[len - 1].swap_result = Some(result);
            }
        }

        Ok(execution_receipts)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use lending::interface::liquidation::{LiquidationAmounts, LiquidationRequest};
    use lending_utils::types::pool::AssetType;

    use crate::{
        executors::kong_swap::types::{SwapArgs, SwapReply},
        pipeline_agent::MockPipelineAgent,
    };

    use super::*;

    #[tokio::test]
    async fn test_kong_executor_process_success() {
        let mut mock_agent = MockPipelineAgent::new();

        let principal = Principal::from_text("aaaaa-aa").unwrap();

        let liquidation_result = LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(10_000u64),
                debt_repaid: Nat::from(9_500u64),
                bonus_earned: Nat::from(500u64),
            },
            tx_id: "tx123".to_string(),
            collateral_asset: AssetType::CkAsset(Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap()),
            debt_asset: AssetType::CkAsset(Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap()),
        };

        let swap_result = SwapReply {
            tx_id: 1,
            request_id: 42,
            status: "Success".to_string(),
            pay_chain: "ICP".to_string(),
            pay_symbol: "ckUSDC".to_string(),
            pay_amount: Nat::from(100_000u64),
            receive_chain: "ICP".to_string(),
            receive_symbol: "ckBTC".to_string(),
            receive_amount: Nat::from(2_000u64),
            mid_price: 0.0002,
            price: 0.000195,
            slippage: 0.0025,
            txs: vec![],
            transfer_ids: vec![],
            claim_ids: vec![888],
            ts: 1_717_178_000,
        };

        mock_agent
            .expect_call_update::<SwapReply>()
            .returning(move |_, method, _| match method {
                "swap" => Ok(swap_result.clone()),
                _ => panic!("Unexpected method"),
            });

        mock_agent
            .expect_call_update::<LiquidationResult>()
            .returning(move |_, method, _| match method {
                "liquidate" => Ok(liquidation_result.clone()),
                _ => panic!("Unexpected method"),
            });

        let executor = KongSwapExecutor {
            agent: Arc::new(mock_agent),
            account_id: Account {
                owner: principal,
                subaccount: None,
            },
            allowances: HashMap::new(),
            lending_canister: principal,
            dex_account: Account {
                owner: principal,
                subaccount: None,
            },
        };

        let request = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: None,
            },
            swap_args: Some(SwapArgs {
                pay_token: "ckUSDC".to_string(),
                pay_amount: Nat::from(1000u64),
                pay_tx_id: None,
                receive_token: "ckBTC".to_string(),
                receive_amount: Some(Nat::from(1000u64)),
                receive_address: None,
                max_slippage: Some(3.0),
                referred_by: None,
            }),
        };

        let result = executor.process(vec![request]).await;
        assert!(result.is_ok(), "Expected process to succeed");
    }

    #[tokio::test]
    async fn test_kong_executor_process_liquidation_only() {
        let mut mock_agent = MockPipelineAgent::new();

        let principal = Principal::from_text("aaaaa-aa").unwrap();

        let liquidation_result = LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(20_000u64),
                debt_repaid: Nat::from(18_000u64),
                bonus_earned: Nat::from(2_000u64),
            },
            tx_id: "tx456".to_string(),
            collateral_asset: AssetType::CkAsset(Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap()),
            debt_asset: AssetType::CkAsset(Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap()),
        };

        mock_agent.expect_call_update().returning(move |_, method, _| {
            match method {
                "liquidate" => Ok(liquidation_result.clone()),
                other => panic!("Unexpected method {other}"), // no swap expected
            }
        });

        let executor = KongSwapExecutor {
            agent: Arc::new(mock_agent),
            account_id: Account {
                owner: principal,
                subaccount: None,
            },
            allowances: HashMap::new(),
            lending_canister: principal,
            dex_account: Account {
                owner: principal,
                subaccount: None,
            },
        };

        let request = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: Some(Nat::from(18_000u64)),
            },
            swap_args: None,
        };

        let result = executor.process(vec![request]).await;
        assert!(result.is_ok(), "Expected liquidation-only process to succeed");
    }

    #[tokio::test]
    async fn test_kong_executor_process_liquidation_failure() {
        let mut mock_agent = MockPipelineAgent::new();
        let principal = Principal::from_text("aaaaa-aa").unwrap();

        // Simulate liquidation failure
        mock_agent
            .expect_call_update::<LiquidationResult>()
            .returning(|_, method, _| {
                assert_eq!(method, "liquidate");
                Err("liquidation failed".to_string())
            });

        let executor = KongSwapExecutor {
            agent: Arc::new(mock_agent),
            account_id: Account {
                owner: principal,
                subaccount: None,
            },
            allowances: HashMap::new(),
            lending_canister: principal,
            dex_account: Account {
                owner: principal,
                subaccount: None,
            },
        };

        let request = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: Some(Nat::from(1_000u64)),
            },
            swap_args: Some(SwapArgs {
                pay_token: "ckUSDC".to_string(),
                pay_amount: Nat::from(1000u64),
                pay_tx_id: None,
                receive_token: "ckBTC".to_string(),
                receive_amount: Some(Nat::from(1000u64)),
                receive_address: None,
                max_slippage: Some(3.0),
                referred_by: None,
            }),
        };

        let result = executor.process(vec![request]).await.expect("process failed");

        assert_eq!(result.len(), 1);
        let receipt = &result[0];
        matches!(receipt.status, ExecutionStatus::Error(_));
        assert!(receipt.liquidation_result.is_none());
        assert!(receipt.swap_result.is_none());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_kong_executor_process_swap_failure() {
        let mut mock_agent = MockPipelineAgent::new();
        let principal = Principal::from_text("aaaaa-aa").unwrap();

        // Mock a successful liquidation result
        let liquidation_result = LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(10_000u64),
                debt_repaid: Nat::from(9_000u64),
                bonus_earned: Nat::from(1_000u64),
            },
            tx_id: "tx789".to_string(),
            collateral_asset: AssetType::CkAsset(Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap()),
            debt_asset: AssetType::CkAsset(Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap()),
        };

        // Simulate a successful liquidation update call
        mock_agent
            .expect_call_update::<LiquidationResult>()
            .returning(move |_, method, _| {
                assert_eq!(method, "liquidate");
                Ok(liquidation_result.clone())
            });

        // Simulate a failed swap call
        mock_agent
            .expect_call_update::<SwapReply>()
            .returning(move |_, method, _| {
                assert_eq!(method, "swap");
                Err("swap execution failed".into())
            });

        // Build the executor with the mocked agent
        let executor = KongSwapExecutor {
            agent: Arc::new(mock_agent),
            account_id: Account {
                owner: principal,
                subaccount: None,
            },
            allowances: HashMap::new(),
            lending_canister: principal,
            dex_account: Account {
                owner: principal,
                subaccount: None,
            },
        };

        // Build a request with both liquidation and swap
        let request = ExecutorRequest {
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: None,
            },
            swap_args: Some(SwapArgs {
                pay_token: "ckUSDC".to_string(),
                pay_amount: Nat::from(1000u64),
                pay_tx_id: None,
                receive_token: "ckBTC".to_string(),
                receive_amount: Some(Nat::from(1000u64)),
                receive_address: None,
                max_slippage: Some(3.0),
                referred_by: None,
            }),
        };

        // Run the executor
        let result = executor.process(vec![request]).await;
    }
}
