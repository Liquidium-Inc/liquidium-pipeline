use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use candid::{Nat, Principal};
use futures::future::join_all;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};
use num_traits::ToPrimitive;

use crate::swappers::icpswap::supports_pair;
use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest},
    venue::SwapVenue,
};

use super::{
    client::{IcpswapManualClient, IcpswapReadClient},
    execution::IcpswapExecutionStateStore,
    ledger_transfers::{forward_step, funding_step},
    manual::{deposit_step, operator_step, recover_step, trade_step, transfer_step, withdraw_step},
    plan::{amount_out_minimum, nat_to_decimal_text},
    session::IcpswapExecutionSession,
    state::validate_execution_state,
    types::{
        IcpswapExecutionPlan, IcpswapPoolData, IcpswapPoolMetadata, IcpswapQuoteError, IcpswapRoutePreview,
        IcpswapState, IcpswapSwapArgs, IcpswapToken, IcpswapTokenMetadata,
    },
};

pub struct IcpswapVenue<C: IcpswapReadClient> {
    client: Arc<C>,
    tokens: HashMap<AssetId, IcpswapTokenMetadata>,
    fee_tiers: Vec<Nat>,
    default_max_slippage_bps: u32,
}

/// Request-scoped values shared by every fee-tier preview. Owning the token
/// values keeps concurrent pool attempts independent and easy to audit.
struct IcpswapQuoteContext {
    token_in: Principal,
    token_out: Principal,
    input_token: ChainToken,
    output_token: ChainToken,
    input_descriptor: IcpswapToken,
    output_descriptor: IcpswapToken,
    input_fee: Nat,
    output_fee: Nat,
    amount_in: ChainTokenAmount,
    max_slippage_bps: u32,
}

struct IcpswapQuoteCandidate {
    plan: IcpswapExecutionPlan,
    estimated_price_impact_bps: f64,
    spot_output: Nat,
}

#[async_trait]
pub trait IcpswapFinalizerLogic: Send + Sync {
    async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError>;

    async fn advance_loaded(
        &self,
        session: &IcpswapExecutionSession,
        store: &dyn IcpswapExecutionStateStore,
        execution_id: &str,
        now_nanos: u64,
        mut state: IcpswapState,
    ) -> Result<IcpswapState, String> {
        validate_execution_state(&state, execution_id)?;

        const MAX_IMMEDIATE_TRANSITIONS: usize = 16;
        for _ in 0..MAX_IMMEDIATE_TRANSITIONS {
            if matches!(
                state.step,
                super::types::IcpswapStep::Completed
                    | super::types::IcpswapStep::Refunded
                    | super::types::IcpswapStep::Failed
            ) || state.next_attempt_at_nanos.is_some_and(|ready_at| now_nanos < ready_at)
            {
                return Ok(state);
            }

            let before = state.clone();
            match state.step {
                super::types::IcpswapStep::Funding
                | super::types::IcpswapStep::FundingPending
                | super::types::IcpswapStep::FundingSurplusPending => {
                    funding_step(
                        session.funder.as_ref(),
                        session.child_ledger.as_ref(),
                        store,
                        execution_id,
                        &mut state,
                        now_nanos,
                    )
                    .await?
                }
                super::types::IcpswapStep::Transfer | super::types::IcpswapStep::TransferPending => {
                    transfer_step(session.child.as_ref(), store, execution_id, &mut state, now_nanos).await?
                }
                super::types::IcpswapStep::Deposit | super::types::IcpswapStep::DepositPending => {
                    deposit_step(session.child.as_ref(), store, execution_id, &mut state, now_nanos).await?
                }
                super::types::IcpswapStep::Trade | super::types::IcpswapStep::TradePending => {
                    trade_step(session.child.as_ref(), store, execution_id, &mut state, now_nanos).await?
                }
                super::types::IcpswapStep::Withdraw | super::types::IcpswapStep::WithdrawPending => {
                    withdraw_step(session.child.as_ref(), store, execution_id, &mut state, now_nanos).await?
                }
                super::types::IcpswapStep::Recover | super::types::IcpswapStep::RecoverPending => {
                    recover_step(session.child.as_ref(), store, execution_id, &mut state, now_nanos).await?
                }
                super::types::IcpswapStep::Forward | super::types::IcpswapStep::ForwardPending => {
                    forward_step(
                        session.child_ledger.as_ref(),
                        store,
                        execution_id,
                        &mut state,
                        now_nanos,
                    )
                    .await?
                }
                super::types::IcpswapStep::OperatorRequired => {
                    operator_step(session.child.as_ref(), store, execution_id, &mut state).await?
                }
                super::types::IcpswapStep::Completed
                | super::types::IcpswapStep::Refunded
                | super::types::IcpswapStep::Failed => return Ok(state),
            }

            if state == before {
                return Ok(state);
            }
        }

        Err(format!(
            "ICPSwap exceeded {MAX_IMMEDIATE_TRANSITIONS} immediately runnable transitions"
        ))
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

    /// Orchestrates a read-only ICPSwap preview from request preparation through
    /// deterministic pool selection; each detailed stage lives below.
    pub async fn preview_route(&self, request: &SwapRequest) -> Result<IcpswapRoutePreview, IcpswapQuoteError> {
        let context = self.prepare_quote_context(request).await?;
        let candidate = self.preview_best_pool(&context).await?;

        Ok(IcpswapRoutePreview {
            quote: common_quote(
                request,
                &candidate.plan,
                candidate.estimated_price_impact_bps,
                &candidate.spot_output,
            ),
            route: candidate.plan,
        })
    }

    /// Validates the request and resolves the token, fee, and spend values that
    /// are identical for every pool fee-tier attempt.
    async fn prepare_quote_context(&self, request: &SwapRequest) -> Result<IcpswapQuoteContext, IcpswapQuoteError> {
        if request.pay_amount.token.asset_id() != request.pay_asset {
            return Err(IcpswapQuoteError::PayAssetMismatch);
        }

        validate_supported_pair(request)?;

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
        let amount_in = executable_input_amount(request, &input_fee)?;
        let max_slippage_bps = request.max_slippage_bps.unwrap_or(self.default_max_slippage_bps);
        amount_out_minimum(&Nat::from(0u8), max_slippage_bps)?;

        Ok(IcpswapQuoteContext {
            token_in,
            token_out,
            input_token: input.token.clone(),
            output_token: output.token.clone(),
            input_descriptor: IcpswapToken {
                address: token_in.to_text(),
                standard: input.standard.clone(),
            },
            output_descriptor: IcpswapToken {
                address: token_out.to_text(),
                standard: output.standard.clone(),
            },
            input_fee,
            output_fee,
            amount_in,
            max_slippage_bps,
        })
    }

    /// Previews every configured fee tier concurrently, then selects the route
    /// with the greatest net output using stable tie-breakers.
    async fn preview_best_pool(
        &self,
        context: &IcpswapQuoteContext,
    ) -> Result<IcpswapQuoteCandidate, IcpswapQuoteError> {
        let attempts = self
            .fee_tiers
            .iter()
            .cloned()
            .map(|fee_tier| self.preview_pool(context, fee_tier));

        let mut candidates = Vec::new();
        let mut failures = Vec::new();
        for attempt in join_all(attempts).await {
            match attempt {
                Ok(candidate) => candidates.push(candidate),
                Err(error) => failures.push(error),
            }
        }
        if candidates.is_empty() {
            return Err(IcpswapQuoteError::NoUsablePools { failures });
        }
        candidates.sort_by(|left, right| {
            right
                .plan
                .net_expected_output()
                .value
                .cmp(&left.plan.net_expected_output().value)
                .then_with(|| left.plan.fee_tier.cmp(&right.plan.fee_tier))
                .then_with(|| left.plan.pool.to_text().cmp(&right.plan.pool.to_text()))
        });
        Ok(candidates.remove(0))
    }

    /// Builds one complete pool candidate. Quote output and pool metadata are
    /// queried concurrently because neither depends on the other.
    async fn preview_pool(
        &self,
        context: &IcpswapQuoteContext,
        fee_tier: Nat,
    ) -> Result<IcpswapQuoteCandidate, String> {
        let pool = self
            .client
            .get_pool(&context.input_descriptor, &context.output_descriptor, &fee_tier)
            .await
            .map_err(|error| format!("fee {fee_tier}: {error}"))?;

        validate_pool_fee(&pool, &fee_tier)?;

        let (token0, token1, zero_for_one) = resolve_pool_direction(&pool, context.token_in, context.token_out)?;
        let quote_args = IcpswapSwapArgs {
            zero_for_one,
            amount_in: nat_to_decimal_text(&context.amount_in.value),
            amount_out_minimum: "0".to_string(),
        };
        let (gross, metadata) = futures::join!(
            self.client.quote(pool.canister_id, &quote_args),
            self.client.pool_metadata(pool.canister_id),
        );
        let gross = gross.map_err(|error| format!("pool {}: {error}", pool.canister_id))?;
        let metadata = metadata.map_err(|error| format!("pool {} metadata: {error}", pool.canister_id))?;
        validate_pool_metadata(&pool, token0, token1, &metadata)?;

        let spot_output = spot_output_amount(
            &context.amount_in.value,
            &metadata.sqrt_price_x96,
            zero_for_one,
            pool.canister_id,
        )?;
        let estimated_price_impact_bps = quoted_price_impact_bps(&spot_output, &gross);
        let plan = IcpswapExecutionPlan::new(
            pool.canister_id,
            token0,
            token1,
            fee_tier,
            context.amount_in.clone(),
            ChainTokenAmount::from_raw(context.input_token.clone(), context.input_fee.clone()),
            ChainTokenAmount::from_raw(context.output_token.clone(), gross),
            ChainTokenAmount::from_raw(context.output_token.clone(), context.output_fee.clone()),
            context.max_slippage_bps,
        )
        .map_err(|error| format!("pool {}: {error}", pool.canister_id))?;

        Ok(IcpswapQuoteCandidate {
            plan,
            estimated_price_impact_bps,
            spot_output,
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
        .settlement
        .transfer
        .credited_amount
        .as_ref()
        .ok_or_else(|| "completed manual swap has no confirmed destination credit".to_string())?;
    let pay = request.pay_amount.to_f64();
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
        pay_asset: request.pay_asset.clone(),
        pay_amount: request.pay_amount.value.clone(),
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
            gas_fee: state.plan.output_ledger_fee.value.clone() * Nat::from(2u8),
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

/// Converts an ICPSwap pool plan into the venue-neutral quote consumed by the
/// multi-venue planner.
///
/// The top-level quote describes the complete venue allocation: its pay amount
/// includes the three reserved input-ledger fees, and its receive amount is the
/// expected destination credit after both output-ledger fees. The nested route
/// leg intentionally records only the amount that reaches the pool so the WAL
/// preserves the distinction between total spend and executable swap input.
/// Pool price impact remains based on the pool's spot and quoted gross outputs;
/// `exec_price` instead reflects the final expected output against the all-in
/// allocation.
fn common_quote(
    request: &SwapRequest,
    plan: &IcpswapExecutionPlan,
    estimated_price_impact_bps: f64,
    spot_output: &Nat,
) -> SwapQuote {
    let pay_symbol = plan.amount_in.token.symbol();
    let expected_output = plan.net_expected_output();
    let receive_symbol = expected_output.token.symbol();
    let all_in_pay = request.pay_amount.value.0.to_f64().unwrap_or(0.0);
    let pool_pay = plan.amount_in.value.0.to_f64().unwrap_or(0.0);
    let spot_receive = spot_output.0.to_f64().unwrap_or(0.0);
    let forwarded_receive = expected_output.value.0.to_f64().unwrap_or(0.0);
    let mid_price = if pool_pay > 0.0 { spot_receive / pool_pay } else { 0.0 };
    let exec_price = if all_in_pay > 0.0 {
        forwarded_receive / all_in_pay
    } else {
        0.0
    };
    SwapQuote {
        pay_asset: request.pay_asset.clone(),
        // The venue-level quote consumes the committed all-in allocation. The
        // smaller amount that reaches the pool remains visible on the route leg.
        pay_amount: request.pay_amount.value.clone(),
        receive_asset: request.receive_asset.clone(),
        receive_amount: expected_output.value.clone(),
        mid_price,
        exec_price,
        estimated_price_impact_bps,
        legs: vec![SwapQuoteLeg {
            venue: "icpswap".to_string(),
            route_id: plan.pool.to_text(),
            pay_chain: "ICP".to_string(),
            pay_symbol,
            pay_amount: plan.amount_in.value.clone(),
            receive_chain: "ICP".to_string(),
            receive_symbol,
            receive_amount: expected_output.value,
            price: exec_price,
            lp_fee: Nat::from(0u8),
            gas_fee: plan.output_ledger_fee.value.clone() * Nat::from(2u8),
        }],
    }
}

/// Converts the pool's Q96 square-root price into the raw output amount for
/// this direction. Raw ledger units are intentional: ICPSwap's quote uses the
/// same units, so token decimals cancel when the two outputs are compared.
fn spot_output_amount(
    amount_in: &Nat,
    sqrt_price_x96: &Nat,
    zero_for_one: bool,
    pool: Principal,
) -> Result<Nat, String> {
    if sqrt_price_x96 == &Nat::from(0u8) {
        return Err(IcpswapQuoteError::InvalidPoolPrice { pool }.to_string());
    }
    let price_x192 = &sqrt_price_x96.0 * &sqrt_price_x96.0;
    let q192 = Nat::from(1u8).0 << 192usize;
    let output = if zero_for_one {
        (&amount_in.0 * &price_x192) / &q192
    } else {
        (&amount_in.0 * &q192) / &price_x192
    };
    Ok(Nat(output))
}

/// Measures planning-time price impact against pool spot. This is deliberately
/// independent from `amount_out_minimum`, which protects execution-time drift.
fn quoted_price_impact_bps(spot_output: &Nat, gross_quote: &Nat) -> f64 {
    let spot = spot_output.0.to_f64().unwrap_or(f64::INFINITY);
    let quoted = gross_quote.0.to_f64().unwrap_or(0.0);
    if !spot.is_finite() || spot <= 0.0 || !quoted.is_finite() {
        return f64::INFINITY;
    }
    ((spot - quoted) / spot).max(0.0) * 10_000.0
}

fn validate_supported_pair(request: &SwapRequest) -> Result<(), IcpswapQuoteError> {
    if supports_pair(&request.pay_amount.token, &request.receive_asset) {
        Ok(())
    } else {
        Err(IcpswapQuoteError::UnsupportedPair {
            pay_asset: request.pay_asset.to_string(),
            receive_asset: request.receive_asset.to_string(),
        })
    }
}

/// Converts the total allocation into the amount sent through the pool after
/// reserving the wallet transfer and pool deposit-sweep ledger fees.
fn executable_input_amount(request: &SwapRequest, input_fee: &Nat) -> Result<ChainTokenAmount, IcpswapQuoteError> {
    // Funding trader -> child, child -> pool deposit account, and the pool's
    // deposit sweep each consume one input-ledger fee.
    let input_operation_fees = input_fee.clone() * Nat::from(3u8);
    if request.pay_amount.value <= input_operation_fees {
        return Err(IcpswapQuoteError::InputFeesExceedBudget {
            budget: request.pay_amount.value.clone(),
            fees: input_operation_fees,
        });
    }
    Ok(ChainTokenAmount::from_raw(
        request.pay_amount.token.clone(),
        request.pay_amount.value.clone() - input_operation_fees,
    ))
}

fn validate_pool_fee(pool: &IcpswapPoolData, expected: &Nat) -> Result<(), String> {
    if &pool.fee == expected {
        Ok(())
    } else {
        Err(IcpswapQuoteError::PoolFeeMismatch {
            pool: pool.canister_id,
            expected: expected.clone(),
            actual: pool.fee.clone(),
        }
        .to_string())
    }
}

fn resolve_pool_direction(
    pool: &IcpswapPoolData,
    token_in: Principal,
    token_out: Principal,
) -> Result<(Principal, Principal, bool), String> {
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
    Ok((token0, token1, zero_for_one))
}

fn validate_pool_metadata(
    pool: &IcpswapPoolData,
    token0: Principal,
    token1: Principal,
    metadata: &IcpswapPoolMetadata,
) -> Result<(), String> {
    let metadata_token0 =
        parse_pool_principal("metadata.token0", &metadata.token0.address).map_err(|error| error.to_string())?;
    let metadata_token1 =
        parse_pool_principal("metadata.token1", &metadata.token1.address).map_err(|error| error.to_string())?;
    if metadata_token0 == token0 && metadata_token1 == token1 {
        Ok(())
    } else {
        Err(format!(
            "pool {} metadata token pair does not match discovered pool",
            pool.canister_id
        ))
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
