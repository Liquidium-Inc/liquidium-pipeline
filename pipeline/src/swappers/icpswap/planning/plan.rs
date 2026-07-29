use candid::{Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::types::{IcpswapExecutionPlan, IcpswapPlanError};
use crate::swappers::model::amount_after_bps_haircut;

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
        net_forwarded_output(&gross_quoted_out.value, &output_ledger_fee.value)?;
        let output_token = gross_quoted_out.token.clone();

        Ok(Self {
            pool,
            token_in,
            token_out,
            zero_for_one,
            fee_tier,
            amount_in,
            input_ledger_fee,
            gross_quoted_out,
            output_ledger_fee,
            max_slippage_bps,
            amount_out_minimum: ChainTokenAmount::from_raw(output_token, minimum_value),
        })
    }

    /// `gross >= fee` is enforced in [`Self::new`], but the plan derives
    /// `Deserialize` and is reloaded from the WAL without revalidating, so a
    /// corrupted record would reach an unchecked `Nat` subtraction -- which
    /// panics on underflow. Saturating keeps a bad record from taking down the
    /// whole finalize cycle; `new` remains the place the invariant is enforced.
    pub fn net_expected_output(&self) -> ChainTokenAmount {
        let net = net_forwarded_output(&self.gross_quoted_out.value, &self.output_ledger_fee.value)
            .unwrap_or_else(|_| Nat::from(0u8));
        ChainTokenAmount::from_raw(self.gross_quoted_out.token.clone(), net)
    }
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
    amount_after_bps_haircut(gross_quote, max_slippage_bps)
        .map_err(|_| IcpswapPlanError::InvalidSlippage(max_slippage_bps))
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

/// Net credit after the pool withdrawal and the child-to-recipient forwarding
/// transfer each consume one output-ledger fee.
pub fn net_forwarded_output(gross_quote: &Nat, output_ledger_fee: &Nat) -> Result<Nat, IcpswapPlanError> {
    net_expected_output(gross_quote, &(output_ledger_fee.clone() * Nat::from(2u8)))
}

fn icp_ledger(token: &ChainToken) -> Result<Principal, IcpswapPlanError> {
    match token {
        ChainToken::Icp { ledger, .. } => Ok(*ledger),
        other => Err(IcpswapPlanError::NonIcpToken(other.to_string())),
    }
}
