use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::types::{IcpswapExecutionPlan, IcpswapPlanError};

const BASIS_POINTS_DENOMINATOR: u32 = 10_000;

pub fn nat_to_decimal_text(value: &Nat) -> String {
    value.0.to_str_radix(10)
}

impl IcpswapExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: Principal,
        token0: Principal,
        token1: Principal,
        fee_tier: Nat,
        amount_in: ChainTokenAmount,
        input_ledger_fee: ChainTokenAmount,
        gross_quoted_out: ChainTokenAmount,
        output_ledger_fee: ChainTokenAmount,
        max_slippage_bps: u32,
        quoted_at: u64,
    ) -> Result<Self, IcpswapPlanError> {
        let token_in = icp_ledger(&amount_in.token)?;
        let token_out = icp_ledger(&gross_quoted_out.token)?;
        if input_ledger_fee.token != amount_in.token {
            return Err(IcpswapPlanError::InputTokenMismatch {
                field: "input_ledger_fee",
            });
        }
        if output_ledger_fee.token != gross_quoted_out.token {
            return Err(IcpswapPlanError::OutputTokenMismatch {
                field: "output_ledger_fee",
            });
        }

        let zero_for_one = resolve_direction(token_in, token_out, token0, token1)?;
        let minimum_value = amount_out_minimum(&gross_quoted_out.value, max_slippage_bps)?;
        let net_value = net_expected_output(&gross_quoted_out.value, &output_ledger_fee.value)?;
        let output_token = gross_quoted_out.token.clone();

        Ok(Self {
            pool,
            token_in,
            token_out,
            token0,
            token1,
            zero_for_one,
            fee_tier,
            amount_in,
            input_ledger_fee,
            gross_quoted_out,
            output_ledger_fee,
            net_expected_output: ChainTokenAmount::from_raw(output_token.clone(), net_value),
            max_slippage_bps,
            amount_out_minimum: ChainTokenAmount::from_raw(output_token, minimum_value),
            quoted_at,
        })
    }
}

/// ICRC-2 charges the transfer fee to the allowance in addition to the
/// transfer amount. `Nat` is arbitrary precision, so this addition cannot
/// overflow.
pub fn required_allowance(plan: &IcpswapExecutionPlan) -> Nat {
    plan.amount_in.value.clone() + plan.input_ledger_fee.value.clone()
}

pub fn resolve_direction(
    token_in: Principal,
    token_out: Principal,
    token0: Principal,
    token1: Principal,
) -> Result<bool, IcpswapPlanError> {
    if token_in == token0 && token_out == token1 {
        Ok(true)
    } else if token_in == token1 && token_out == token0 {
        Ok(false)
    } else {
        Err(IcpswapPlanError::UnsupportedTokenPair {
            token_in,
            token_out,
            token0,
            token1,
        })
    }
}

pub fn amount_out_minimum(gross_quote: &Nat, max_slippage_bps: u32) -> Result<Nat, IcpswapPlanError> {
    let retained_bps = BASIS_POINTS_DENOMINATOR
        .checked_sub(max_slippage_bps)
        .ok_or(IcpswapPlanError::InvalidSlippage(max_slippage_bps))?;
    Ok((gross_quote.clone() * Nat::from(retained_bps)) / Nat::from(BASIS_POINTS_DENOMINATOR))
}

pub fn net_expected_output(gross_quote: &Nat, output_ledger_fee: &Nat) -> Result<Nat, IcpswapPlanError> {
    if output_ledger_fee > gross_quote {
        return Err(IcpswapPlanError::OutputFeeExceedsQuote {
            gross: gross_quote.clone(),
            fee: output_ledger_fee.clone(),
        });
    }
    Ok(gross_quote.clone() - output_ledger_fee.clone())
}

fn icp_ledger(token: &ChainToken) -> Result<Principal, IcpswapPlanError> {
    match token {
        ChainToken::Icp { ledger, .. } => Ok(*ledger),
        other => Err(IcpswapPlanError::NonIcpToken(other.to_string())),
    }
}
