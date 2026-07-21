use candid::{CandidType, Decode, Encode, Int, Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use serde::Deserialize;

use super::{
    plan::{amount_out_minimum, net_expected_output, resolve_direction},
    types::{
        IcpswapError, IcpswapExecutionPlan, IcpswapPlanError, IcpswapPoolData, IcpswapResult, IcpswapSwapArgs,
        IcpswapToken,
    },
};

fn principal(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn icp_token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

#[test]
fn resolves_both_pool_directions() {
    let token0 = principal(1);
    let token1 = principal(2);

    assert_eq!(resolve_direction(token0, token1, token0, token1), Ok(true));
    assert_eq!(resolve_direction(token1, token0, token0, token1), Ok(false));
}

#[test]
fn rejects_pair_not_matching_pool_tokens() {
    let err = resolve_direction(principal(1), principal(3), principal(1), principal(2)).unwrap_err();
    assert!(matches!(err, IcpswapPlanError::UnsupportedTokenPair { .. }));
}

#[test]
fn calculates_minimum_output_with_integer_flooring() {
    assert_eq!(amount_out_minimum(&Nat::from(1_001u64), 100), Ok(Nat::from(990u64)));
    assert_eq!(amount_out_minimum(&Nat::from(1_001u64), 0), Ok(Nat::from(1_001u64)));
    assert_eq!(amount_out_minimum(&Nat::from(1_001u64), 10_000), Ok(Nat::from(0u8)));
    assert_eq!(
        amount_out_minimum(&Nat::from(1_001u64), 10_001),
        Err(IcpswapPlanError::InvalidSlippage(10_001))
    );
}

#[test]
fn slippage_math_supports_values_larger_than_u128() {
    let gross: Nat = "100000000000000000000000000000000000000000000000000"
        .parse()
        .expect("valid Nat");
    let expected: Nat = "97500000000000000000000000000000000000000000000000"
        .parse()
        .expect("valid Nat");
    assert_eq!(amount_out_minimum(&gross, 250), Ok(expected));
}

#[test]
fn deducts_output_fee_and_prevents_underflow() {
    assert_eq!(
        net_expected_output(&Nat::from(1_000u64), &Nat::from(10u64)),
        Ok(Nat::from(990u64))
    );
    assert_eq!(
        net_expected_output(&Nat::from(10u64), &Nat::from(10u64)),
        Ok(Nat::from(0u8))
    );
    assert!(matches!(
        net_expected_output(&Nat::from(9u64), &Nat::from(10u64)),
        Err(IcpswapPlanError::OutputFeeExceedsQuote { .. })
    ));
}

#[test]
fn builds_complete_plan_for_reversed_direction() {
    let token0 = principal(1);
    let token1 = principal(2);
    let input = icp_token(token1, "IN", 10);
    let output = icp_token(token0, "OUT", 5);

    let plan = IcpswapExecutionPlan::new(
        principal(9),
        token0,
        token1,
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input, Nat::from(50_000u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(10_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("valid plan");

    assert!(!plan.zero_for_one);
    assert_eq!(plan.amount_out_minimum.value, Nat::from(9_900u64));
    assert_eq!(plan.net_expected_output.value, Nat::from(9_995u64));
}

#[test]
fn rejects_fee_denominated_in_the_wrong_token() {
    let token0 = principal(1);
    let token1 = principal(2);
    let input = icp_token(token0, "IN", 10);
    let output = icp_token(token1, "OUT", 5);

    let err = IcpswapExecutionPlan::new(
        principal(9),
        token0,
        token1,
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(50_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(10_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(5u64)),
        100,
        123,
    )
    .unwrap_err();

    assert_eq!(
        err,
        IcpswapPlanError::OutputTokenMismatch {
            field: "output_ledger_fee"
        }
    );
}

#[allow(non_snake_case)]
#[derive(CandidType, Deserialize)]
struct OfficialSwapArgs {
    zeroForOne: bool,
    amountIn: String,
    amountOutMinimum: String,
}

#[test]
fn swap_args_encode_with_official_candid_field_names() {
    let encoded = Encode!(&IcpswapSwapArgs {
        zero_for_one: true,
        amount_in: "100000000".to_string(),
        amount_out_minimum: "99000000".to_string(),
    })
    .expect("encode");
    let decoded = Decode!(&encoded, OfficialSwapArgs).expect("official Candid shape");

    assert!(decoded.zeroForOne);
    assert_eq!(decoded.amountIn, "100000000");
    assert_eq!(decoded.amountOutMinimum, "99000000");
}

#[allow(non_snake_case)]
#[derive(CandidType, Deserialize)]
struct OfficialPoolData {
    key: String,
    token0: IcpswapToken,
    token1: IcpswapToken,
    fee: Nat,
    tickSpacing: Int,
    canisterId: Principal,
}

#[test]
fn pool_data_encodes_with_official_candid_field_names() {
    let encoded = Encode!(&IcpswapPoolData {
        key: "token0_token1_3000".to_string(),
        token0: IcpswapToken {
            address: principal(1).to_text(),
            standard: "ICRC2".to_string(),
        },
        token1: IcpswapToken {
            address: principal(2).to_text(),
            standard: "ICRC2".to_string(),
        },
        fee: Nat::from(3_000u64),
        tick_spacing: Int::from(60),
        canister_id: principal(9),
    })
    .expect("encode");
    let decoded = Decode!(&encoded, OfficialPoolData).expect("official Candid shape");

    assert_eq!(decoded.key, "token0_token1_3000");
    assert_eq!(decoded.token0.address, principal(1).to_text());
    assert_eq!(decoded.token1.address, principal(2).to_text());
    assert_eq!(decoded.fee, Nat::from(3_000u64));
    assert_eq!(decoded.tickSpacing, Int::from(60));
    assert_eq!(decoded.canisterId, principal(9));
}

#[allow(non_camel_case_types)]
#[derive(CandidType, Deserialize)]
enum OfficialNatResult {
    ok(Nat),
    err(IcpswapError),
}

#[test]
fn result_encodes_with_lowercase_official_variant_labels() {
    let encoded = Encode!(&IcpswapResult::Ok(Nat::from(42u8))).expect("encode");
    let decoded = Decode!(&encoded, OfficialNatResult).expect("official Candid result shape");

    assert!(matches!(decoded, OfficialNatResult::ok(value) if value == Nat::from(42u8)));
}
