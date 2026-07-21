use std::{collections::HashMap, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use candid::{Nat, Principal};
use futures::future::join_all;
use liquidium_pipeline_core::tokens::{
    asset_id::AssetId, chain_token::ChainToken, chain_token_amount::ChainTokenAmount,
};

use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest},
    router::SwapVenue,
};

use super::{
    client::IcpswapReadClient,
    plan::{amount_out_minimum, nat_to_decimal_text},
    types::{
        IcpswapExecutionPlan, IcpswapQuoteError, IcpswapQuoteResult, IcpswapSwapArgs, IcpswapToken,
        IcpswapTokenMetadata,
    },
};

pub struct IcpswapVenue<C: IcpswapReadClient> {
    client: Arc<C>,
    tokens: HashMap<AssetId, IcpswapTokenMetadata>,
    fee_tiers: Vec<Nat>,
    default_max_slippage_bps: u32,
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

    pub async fn quote_with_plan(&self, request: &SwapRequest) -> Result<IcpswapQuoteResult, IcpswapQuoteError> {
        let quoted_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.quote_with_plan_at(request, quoted_at).await
    }

    pub async fn quote_with_plan_at(
        &self,
        request: &SwapRequest,
        quoted_at: u64,
    ) -> Result<IcpswapQuoteResult, IcpswapQuoteError> {
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
            let amount_in = request.pay_amount.clone();
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
                    quoted_at,
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
                .net_expected_output
                .value
                .cmp(&left.net_expected_output.value)
                .then_with(|| left.fee_tier.cmp(&right.fee_tier))
                .then_with(|| left.pool.to_text().cmp(&right.pool.to_text()))
        });
        let plan = candidates.remove(0);

        Ok(IcpswapQuoteResult {
            quote: common_quote(request, &plan),
            plan,
        })
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
        self.quote_with_plan(request)
            .await
            .map(|result| result.quote)
            .map_err(|error| error.to_string())
    }

    async fn execute(&self, _request: &SwapRequest) -> Result<SwapExecution, String> {
        Err("ICPSwap execution is disabled until durable settlement and recovery are implemented".to_string())
    }
}

fn common_quote(request: &SwapRequest, plan: &IcpswapExecutionPlan) -> SwapQuote {
    let pay_symbol = plan.amount_in.token.symbol();
    let receive_symbol = plan.net_expected_output.token.symbol();
    SwapQuote {
        pay_asset: request.pay_asset.clone(),
        pay_amount: plan.amount_in.value.clone(),
        receive_asset: request.receive_asset.clone(),
        receive_amount: plan.net_expected_output.value.clone(),
        mid_price: 0.0,
        exec_price: 0.0,
        slippage: 0.0,
        legs: vec![SwapQuoteLeg {
            venue: "icpswap".to_string(),
            route_id: plan.pool.to_text(),
            pay_chain: "ICP".to_string(),
            pay_symbol,
            pay_amount: plan.amount_in.value.clone(),
            receive_chain: "ICP".to_string(),
            receive_symbol,
            receive_amount: plan.net_expected_output.value.clone(),
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
