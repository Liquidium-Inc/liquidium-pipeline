use async_trait::async_trait;
use candid::Encode;
use log::debug;
use num_traits::ToPrimitive;

use crate::{
    executors::{
        executor::{ExecutorRequest, IcrcSwapExecutor},
        kong_swap::{kong_swap::KongSwapExecutor, types::SwapReply},
    },
    pipeline_agent::PipelineAgent,
    stage::PipelineStage,
    types::protocol_types::{LiquidationResult, LiquidationStatus},
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ExecutionStatus {
    Success,
    Error(String),
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ExecutionReceipt {
    pub request: ExecutorRequest,
    pub liquidation_result: Option<LiquidationResult>,
    pub swap_result: Option<SwapReply>,
    pub status: ExecutionStatus,
    pub expected_profit: i128,
    pub realized_profit: i128,
}

impl ExecutionReceipt {
    pub fn formatted_debt_repaid(&self) -> String {
        if let Some(result) = &self.liquidation_result {
            let amount =
                result.amounts.debt_repaid.0.to_f64().unwrap() / 10f64.powi(self.request.debt_asset.decimals as i32);
            return format!("{amount} {}", self.request.debt_asset.symbol);
        }
        format!("0 {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_received_collateral(&self) -> String {
        if let Some(result) = &self.liquidation_result {
            let amount = result.amounts.collateral_received.0.to_f64().unwrap()
                / 10f64.powi(self.request.collateral_asset.decimals as i32);
            return format!("{amount} {}", self.request.collateral_asset.symbol);
        }
        format!("0 {}", self.request.collateral_asset.symbol)
    }

    pub fn formatted_swap_output(&self) -> String {
        if let Some(result) = &self.swap_result {
            let amount =
                result.receive_amount.0.to_f64().unwrap() / 10f64.powi(self.request.debt_asset.decimals as i32);
            return format!("{amount} {}", self.request.debt_asset.symbol);
        }
        format!("0 {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_realized_profit(&self) -> String {
        let amount = self.realized_profit as f64 / 10f64.powi(self.request.debt_asset.decimals as i32);
        format!("{amount} {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_expected_profit(&self) -> String {
        let amount = self.expected_profit as f64 / 10f64.powi(self.request.debt_asset.decimals as i32);
        format!("{amount} {}", self.request.debt_asset.symbol)
    }

    pub fn formatted_profit_delta(&self) -> String {
        let delta = self.realized_profit - self.expected_profit;
        let decimals = self.request.debt_asset.decimals as i32;
        let abs_amount = (delta.abs() as f64) / 10f64.powi(decimals);
        let prefix = if delta >= 0 { "+" } else { "-" };
        format!("{prefix}{:.3}", abs_amount)
    }
}

#[async_trait]
impl<'a, A: PipelineAgent> PipelineStage<'a, Vec<ExecutorRequest>, Vec<ExecutionReceipt>> for KongSwapExecutor<A> {
    async fn process(&self, executor_requests: &'a Vec<ExecutorRequest>) -> Result<Vec<ExecutionReceipt>, String> {
        let mut execution_receipts: Vec<ExecutionReceipt> = vec![];
        for executor_request in executor_requests {
            let args = Encode!(&self.account_id.owner, &executor_request.liquidation).map_err(|e| e.to_string())?;

            // Make the update call to the canister
            let liquidation_result = self
                .agent
                .call_update::<LiquidationResult>(&self.lending_canister, "liquidate", args)
                .await;

            let liquidation_result = liquidation_result.and_then(|result| match result.status {
                LiquidationStatus::Success => Ok(result),
                LiquidationStatus::Failed(err) => Err(err),
            });

            // Check that the liquidation was executed successfully
            if liquidation_result.is_err() {
                execution_receipts.push(ExecutionReceipt {
                    request: executor_request.clone(),
                    liquidation_result: None,
                    swap_result: None,
                    expected_profit: executor_request.expected_profit,
                    realized_profit: 0,
                    status: ExecutionStatus::Error(format!(
                        "Could not execute liquidation {:?} {:?}",
                        executor_request,
                        liquidation_result.err()
                    )),
                });

                continue;
            }

            debug!("Executed liquidation {:?}", liquidation_result);
            execution_receipts.push(ExecutionReceipt {
                request: executor_request.clone(),
                liquidation_result: liquidation_result.clone().ok(),
                swap_result: None,
                status: ExecutionStatus::Success,
                expected_profit: executor_request.expected_profit,
                realized_profit: 0,
            });

            let collateral_received = liquidation_result.unwrap().amounts.collateral_received.clone();
            if let Some(mut swap_args) = executor_request.swap_args.clone() {
                swap_args.pay_amount = collateral_received;
                let max_retries = 3;
                let mut attempt = 0;
                let result;
                loop {
                    match self.swap(swap_args.clone()).await {
                        Ok(res) => {
                            result = res;
                            break;
                        }
                        Err(e) => {
                            attempt += 1;
                            if attempt >= max_retries {
                                panic!("Swap failed after {max_retries} attempts: {e}");
                            }

                            swap_args.max_slippage = Some(swap_args.max_slippage.unwrap() + 0.25); // Increase slippage for next attempt
                            if swap_args.pay_amount > executor_request.collateral_asset.fee.clone() {
                                swap_args.pay_amount -= executor_request.collateral_asset.fee.clone();
                            } else {
                                panic!("Swap failed after {max_retries} attempts: Insufficient pay amount");
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100 * 2u64.pow(attempt))).await;
                        }
                    }
                }

                debug!("Executed swap {:?}", result);
                let len = execution_receipts.len();
                let realized_profit =
                    result.receive_amount.clone() - executor_request.liquidation.debt_amount.clone().unwrap();
                execution_receipts[len - 1].swap_result = Some(result);
                execution_receipts[len - 1].realized_profit = realized_profit.0.to_i128().unwrap();
            }
        }

        Ok(execution_receipts)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        executors::kong_swap::types::{SwapArgs, SwapReply, SwapResult},
        icrc_token::icrc_token::IcrcToken,
        pipeline_agent::MockPipelineAgent,
        types::protocol_types::{AssetType, LiquidationAmounts, LiquidationRequest},
    };
    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;

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
            status: LiquidationStatus::Success,
        };

        let swap_result = SwapResult::Ok(SwapReply {
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
        });

        mock_agent
            .expect_call_update::<SwapResult>()
            .withf(|_, method, _| method == "swap")
            .return_const(Ok(swap_result.clone()));

        mock_agent
            .expect_call_update::<LiquidationResult>()
            .withf(|_, method, _| method == "liquidate")
            .returning(move |_, method, _| {
                println!("{}", method);
                Ok(liquidation_result.clone())
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
            debt_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            collateral_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: Some(Nat::from(500u64)),
                min_collateral_amount: None,
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
            expected_profit: 0,
        };

        let result = executor.process(&vec![request]).await;
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
            status: LiquidationStatus::Success,
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
            debt_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            collateral_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: Some(Nat::from(18_000u64)),
                min_collateral_amount: None,
            },
            swap_args: None,
            expected_profit: 0,
        };

        let result = executor.process(&vec![request]).await;
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
            debt_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            collateral_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: Some(Nat::from(1_000u64)),
                min_collateral_amount: None,
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
            expected_profit: 0,
        };

        let result = executor.process(&vec![request]).await.expect("process failed");

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
            status: LiquidationStatus::Success,
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
            debt_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            collateral_asset: IcrcToken {
                ledger: Principal::anonymous(),
                decimals: 8,
                name: "Dummy Token".to_string(),
                symbol: "DUM".to_string(),
                fee: Nat::from(0u8), // example fee in smallest units
            },
            liquidation: LiquidationRequest {
                borrower: principal,
                debt_pool_id: principal,
                collateral_pool_id: principal,
                debt_amount: None,
                min_collateral_amount: None,
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
            expected_profit: 0,
        };

        // Run the executor
        let _ = executor.process(&vec![request]).await;
    }
}
