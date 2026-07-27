use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use candid::{Nat, Principal};
use futures::future::join_all;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};

use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest},
    router::SwapVenue,
};

use super::{
    client::{IcpswapManualClient, IcpswapReadClient},
    execution::IcpswapExecutionStateStore,
    manual::{deposit_step, operator_step, recover_step, trade_step, transfer_step, withdraw_step},
    plan::{amount_out_minimum, nat_to_decimal_text},
    state::validate_execution_state,
    types::{
        IcpswapExecutionPlan, IcpswapQuoteError, IcpswapRoutePreview, IcpswapState, IcpswapSwapArgs, IcpswapToken,
        IcpswapTokenMetadata,
    },
};

pub struct IcpswapVenue<C: IcpswapReadClient> {
    client: Arc<C>,
    tokens: HashMap<AssetId, IcpswapTokenMetadata>,
    fee_tiers: Vec<Nat>,
    default_max_slippage_bps: u32,
}

#[async_trait]
pub trait IcpswapFinalizerLogic: Send + Sync {
    async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError>;

    fn prepare(&self, execution_id: &str, route: IcpswapExecutionPlan, owner: Account) -> IcpswapState {
        IcpswapState::prepare(execution_id, route, owner)
    }

    async fn transfer(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String>;

    async fn deposit(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String>;

    async fn trade(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String>;

    async fn withdraw(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String>;

    async fn recover(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String>;

    async fn reconcile_operator(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
    ) -> Result<(), String>;

    async fn advance(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        owner: Account,
        now_nanos: u64,
    ) -> Result<IcpswapState, String> {
        let state = store
            .load(execution_id)
            .await?
            .ok_or_else(|| format!("missing persisted ICPSwap state for {execution_id}"))?;
        self.advance_loaded(store, execution_id, owner, now_nanos, state).await
    }

    async fn advance_loaded(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        owner: Account,
        now_nanos: u64,
        mut state: IcpswapState,
    ) -> Result<IcpswapState, String> {
        validate_execution_state(&state, execution_id, owner)?;

        match state.step {
            super::types::IcpswapStep::Transfer | super::types::IcpswapStep::TransferPending => {
                self.transfer(store, execution_id, &mut state, now_nanos).await?
            }
            super::types::IcpswapStep::Deposit | super::types::IcpswapStep::DepositPending => {
                self.deposit(store, execution_id, &mut state, now_nanos).await?
            }
            super::types::IcpswapStep::Trade | super::types::IcpswapStep::TradePending => {
                self.trade(store, execution_id, &mut state, now_nanos).await?
            }
            super::types::IcpswapStep::Withdraw | super::types::IcpswapStep::WithdrawPending => {
                self.withdraw(store, execution_id, &mut state, now_nanos).await?
            }
            super::types::IcpswapStep::Recover | super::types::IcpswapStep::RecoverPending => {
                self.recover(store, execution_id, &mut state, now_nanos).await?
            }
            super::types::IcpswapStep::OperatorRequired => {
                self.reconcile_operator(store, execution_id, &mut state).await?
            }
            super::types::IcpswapStep::Completed
            | super::types::IcpswapStep::Refunded
            | super::types::IcpswapStep::Failed => {}
        }
        Ok(state)
    }

    async fn finish(
        &self,
        request: &SwapRequest,
        state: &IcpswapState,
        now_nanos: u64,
    ) -> Result<SwapExecution, String> {
        completed_execution(request, state, now_nanos)
    }
}

impl<C: IcpswapReadClient> IcpswapVenue<C> {
    pub fn new(
        client: Arc<C>,
        tokens: Vec<IcpswapTokenMetadata>,
        mut fee_tiers: Vec<Nat>,
        default_max_slippage_bps: u32,
    ) -> Result<Self, IcpswapQuoteError> {
        if fee_tiers.is_empty() {
            return Err(IcpswapQuoteError::EmptyFeeTiers);
        }
        amount_out_minimum(&Nat::from(0u8), default_max_slippage_bps)
            .map_err(|_| IcpswapQuoteError::InvalidDefaultSlippage(default_max_slippage_bps))?;

        fee_tiers.sort();
        fee_tiers.dedup();
        let tokens = tokens
            .into_iter()
            .map(|metadata| (metadata.token.asset_id(), metadata))
            .collect();

        Ok(Self {
            client,
            tokens,
            fee_tiers,
            default_max_slippage_bps,
        })
    }

    pub async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err(IcpswapQuoteError::PayAssetMismatch);
        }

        let input = self
            .tokens
            .get(&request.pay_asset)
            .ok_or_else(|| IcpswapQuoteError::MissingToken(request.pay_asset.to_string()))?;
        let output = self
            .tokens
            .get(&request.receive_asset)
            .ok_or_else(|| IcpswapQuoteError::MissingToken(request.receive_asset.to_string()))?;
        let token_in =
            icp_ledger(&input.token).ok_or_else(|| IcpswapQuoteError::MissingToken(request.pay_asset.to_string()))?;
        let token_out = icp_ledger(&output.token)
            .ok_or_else(|| IcpswapQuoteError::MissingToken(request.receive_asset.to_string()))?;
        let (input_fee, output_fee) =
            futures::join!(self.client.ledger_fee(token_in), self.client.ledger_fee(token_out));
        let input_fee = input_fee.map_err(IcpswapQuoteError::LedgerFee)?;
        let output_fee = output_fee.map_err(IcpswapQuoteError::LedgerFee)?;
        // `SwapRequest::pay_amount` is the total spend budget. The ICRC-1
        // workflow charges once for the transfer into the pool subaccount and
        // once for the pool's deposit sweep.
        let input_operation_fees = input_fee.clone() * Nat::from(2u8);
        if request.pay_amount.value <= input_operation_fees {
            return Err(IcpswapQuoteError::InputFeesExceedBudget {
                budget: request.pay_amount.value.clone(),
                fees: input_operation_fees,
            });
        }
        let amount_in = ChainTokenAmount::from_raw(
            request.pay_amount.token.clone(),
            request.pay_amount.value.clone() - input_operation_fees,
        );
        let max_slippage_bps = request.max_slippage_bps.unwrap_or(self.default_max_slippage_bps);
        amount_out_minimum(&Nat::from(0u8), max_slippage_bps)?;

        let input_descriptor = IcpswapToken {
            address: token_in.to_text(),
            standard: input.standard.clone(),
        };
        let output_descriptor = IcpswapToken {
            address: token_out.to_text(),
            standard: output.standard.clone(),
        };

        let attempts = self.fee_tiers.iter().cloned().map(|fee_tier| {
            let input_descriptor = input_descriptor.clone();
            let output_descriptor = output_descriptor.clone();
            let output_token = output.token.clone();
            let input_token = input.token.clone();
            let amount_in = amount_in.clone();
            let input_fee = input_fee.clone();
            let output_fee = output_fee.clone();
            async move {
                let pool = self
                    .client
                    .get_pool(&input_descriptor, &output_descriptor, &fee_tier)
                    .await
                    .map_err(|error| format!("fee {fee_tier}: {error}"))?;
                if pool.fee != fee_tier {
                    return Err(IcpswapQuoteError::PoolFeeMismatch {
                        pool: pool.canister_id,
                        expected: fee_tier,
                        actual: pool.fee,
                    }
                    .to_string());
                }

                let token0 = parse_pool_principal("token0", &pool.token0.address).map_err(|error| error.to_string())?;
                let token1 = parse_pool_principal("token1", &pool.token1.address).map_err(|error| error.to_string())?;
                let zero_for_one = if token_in == token0 && token_out == token1 {
                    true
                } else if token_in == token1 && token_out == token0 {
                    false
                } else {
                    return Err(format!(
                        "pool {} token pair does not match {} -> {}",
                        pool.canister_id, token_in, token_out
                    ));
                };
                let gross = self
                    .client
                    .quote(
                        pool.canister_id,
                        &IcpswapSwapArgs {
                            zero_for_one,
                            amount_in: nat_to_decimal_text(&amount_in.value),
                            amount_out_minimum: "0".to_string(),
                        },
                    )
                    .await
                    .map_err(|error| format!("pool {}: {error}", pool.canister_id))?;

                IcpswapExecutionPlan::new(
                    pool.canister_id,
                    token0,
                    token1,
                    fee_tier,
                    amount_in,
                    ChainTokenAmount::from_raw(input_token, input_fee),
                    ChainTokenAmount::from_raw(output_token.clone(), gross),
                    ChainTokenAmount::from_raw(output_token, output_fee),
                    max_slippage_bps,
                )
                .map_err(|error| format!("pool {}: {error}", pool.canister_id))
            }
        });

        let mut candidates = Vec::new();
        let mut failures = Vec::new();
        for attempt in join_all(attempts).await {
            match attempt {
                Ok(plan) => candidates.push(plan),
                Err(error) => failures.push(error),
            }
        }
        if candidates.is_empty() {
            return Err(IcpswapQuoteError::NoUsablePools { failures });
        }
        candidates.sort_by(|left, right| {
            right
                .net_expected_output()
                .value
                .cmp(&left.net_expected_output().value)
                .then_with(|| left.fee_tier.cmp(&right.fee_tier))
                .then_with(|| left.pool.to_text().cmp(&right.pool.to_text()))
        });
        let plan = candidates.remove(0);

        Ok(IcpswapRoutePreview {
            quote: common_quote(request, &plan),
            route: plan,
        })
    }
}

#[async_trait]
impl<C> IcpswapFinalizerLogic for IcpswapVenue<C>
where
    C: IcpswapReadClient + IcpswapManualClient + 'static,
{
    async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        IcpswapVenue::preview_route(self, request).await
    }

    async fn transfer(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String> {
        transfer_step(self.client.as_ref(), store, execution_id, state, now_nanos).await
    }

    async fn deposit(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String> {
        deposit_step(self.client.as_ref(), store, execution_id, state, now_nanos).await
    }

    async fn trade(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String> {
        trade_step(self.client.as_ref(), store, execution_id, state, now_nanos).await
    }

    async fn withdraw(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String> {
        withdraw_step(self.client.as_ref(), store, execution_id, state, now_nanos).await
    }

    async fn recover(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
        now_nanos: u64,
    ) -> Result<(), String> {
        recover_step(self.client.as_ref(), store, execution_id, state, now_nanos).await
    }

    async fn reconcile_operator(
        &self,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        state: &mut IcpswapState,
    ) -> Result<(), String> {
        operator_step(self.client.as_ref(), store, execution_id, state).await
    }
}

#[async_trait]
impl<C: IcpswapReadClient + 'static> SwapVenue for IcpswapVenue<C> {
    fn venue_name(&self) -> &'static str {
        "icpswap"
    }

    async fn init(&self) -> Result<(), String> {
        Ok(())
    }

    async fn quote(&self, request: &SwapRequest) -> Result<SwapQuote, String> {
        self.preview_route(request)
            .await
            .map(|result| result.quote)
            .map_err(|error| error.to_string())
    }
}

fn completed_execution(request: &SwapRequest, state: &IcpswapState, now_nanos: u64) -> Result<SwapExecution, String> {
    let gross = state
        .trade
        .gross_output_amount
        .as_ref()
        .ok_or_else(|| "completed manual swap has no gross output".to_string())?;
    if gross <= &state.plan.output_ledger_fee.value {
        return Err("completed manual output cannot cover its ledger fee".to_string());
    }
    let net = state
        .withdraw
        .wallet_credited_amount
        .as_ref()
        .ok_or_else(|| "completed manual swap has no confirmed wallet credit".to_string())?;
    let pay = state.plan.amount_in.to_f64();
    let expected_output = state.plan.net_expected_output();
    let receive_token = expected_output.token.clone();
    let receive_amount = ChainTokenAmount::from_raw(receive_token.clone(), net.clone());
    let receive = receive_amount.to_f64();
    let expected = expected_output.to_f64();
    let exec_price = if pay > 0.0 { receive / pay } else { 0.0 };
    let mid_price = if pay > 0.0 { expected / pay } else { 0.0 };
    let realized_slippage_bps = execution_slippage_bps(expected, receive);

    Ok(SwapExecution {
        swap_id: 0,
        request_id: 0,
        status: "filled".to_string(),
        pay_asset: state.plan.amount_in.token.asset_id(),
        pay_amount: state.plan.amount_in.value.clone(),
        receive_asset: receive_token.asset_id(),
        receive_amount: net.clone(),
        mid_price,
        exec_price,
        realized_slippage_bps,
        legs: vec![SwapQuoteLeg {
            venue: "icpswap".to_string(),
            route_id: format!("{}:manual={}", state.plan.pool, state.execution_id),
            pay_chain: request.pay_asset.chain.clone(),
            pay_symbol: state.plan.amount_in.token.symbol(),
            pay_amount: state.plan.amount_in.value.clone(),
            receive_chain: request.receive_asset.chain.clone(),
            receive_symbol: receive_token.symbol(),
            receive_amount: net.clone(),
            price: exec_price,
            lp_fee: Nat::from(0u8),
            gas_fee: state.plan.output_ledger_fee.value.clone(),
        }],
        approval_count: Some(0),
        ts: now_nanos / 1_000_000_000,
    })
}

fn execution_slippage_bps(expected: f64, receive: f64) -> f64 {
    if expected > 0.0 {
        ((expected - receive) / expected).max(0.0) * 10_000.0
    } else {
        0.0
    }
}

fn common_quote(request: &SwapRequest, plan: &IcpswapExecutionPlan) -> SwapQuote {
    let pay_symbol = plan.amount_in.token.symbol();
    let expected_output = plan.net_expected_output();
    let receive_symbol = expected_output.token.symbol();
    SwapQuote {
        pay_asset: request.pay_asset.clone(),
        pay_amount: plan.amount_in.value.clone(),
        receive_asset: request.receive_asset.clone(),
        receive_amount: expected_output.value.clone(),
        mid_price: 0.0,
        exec_price: 0.0,
        estimated_slippage_bps: 0.0,
        legs: vec![SwapQuoteLeg {
            venue: "icpswap".to_string(),
            route_id: plan.pool.to_text(),
            pay_chain: "ICP".to_string(),
            pay_symbol,
            pay_amount: plan.amount_in.value.clone(),
            receive_chain: "ICP".to_string(),
            receive_symbol,
            receive_amount: expected_output.value,
            price: 0.0,
            lp_fee: Nat::from(0u8),
            gas_fee: plan.output_ledger_fee.value.clone(),
        }],
    }
}

fn parse_pool_principal(field: &'static str, address: &str) -> Result<Principal, IcpswapQuoteError> {
    Principal::from_text(address).map_err(|error| IcpswapQuoteError::InvalidPoolPrincipal {
        field,
        address: address.to_string(),
        message: error.to_string(),
    })
}

fn icp_ledger(token: &ChainToken) -> Option<Principal> {
    match token {
        ChainToken::Icp { ledger, .. } => Some(*ledger),
        _ => None,
    }
}

#[cfg(test)]
mod unit_tests {
    use super::execution_slippage_bps;

    #[test]
    fn execution_ratio_is_normalized_to_basis_points() {
        assert!((execution_slippage_bps(100.0, 99.0) - 100.0).abs() < f64::EPSILON);
        assert_eq!(execution_slippage_bps(100.0, 101.0), 0.0);
        assert_eq!(execution_slippage_bps(0.0, 0.0), 0.0);
    }
}
