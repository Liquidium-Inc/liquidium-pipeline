use std::sync::Arc;

use candid::{CandidType, Decode, Encode, Int, Nat, Principal};
use icrc_ledger_types::icrc1::{
    account::Account,
    transfer::{TransferArg, TransferError},
};
use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use serde::Deserialize;

use super::{
    client::{
        IcpswapClient, IcpswapExecutionClient, IcpswapReadClient, IcpswapRecoveryTransferClient,
        MockIcpswapLedgerClient,
    },
    plan::{amount_out_minimum, nat_to_decimal_text, net_expected_output, resolve_direction},
    types::{
        IcpswapClientError, IcpswapDepositAndSwapArgs, IcpswapError, IcpswapExecutionClientError,
        IcpswapExecutionPhase, IcpswapExecutionPlan, IcpswapExecutionState, IcpswapGetPoolArgs, IcpswapPlanError,
        IcpswapPoolData, IcpswapRecoveryTransferOutcome, IcpswapRecoveryTransferRequest, IcpswapResult,
        IcpswapSwapArgs, IcpswapToken,
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
fn nat_decimal_text_has_no_display_separators() {
    assert_eq!(nat_to_decimal_text(&Nat::from(100_000u64)), "100000");
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
        ChainTokenAmount::from_raw(input.clone(), Nat::from(50_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
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
fn planned_state_starts_before_any_external_side_effect() {
    let token0 = principal(1);
    let token1 = principal(2);
    let input = icp_token(token0, "IN", 10);
    let output = icp_token(token1, "OUT", 5);
    let plan = IcpswapExecutionPlan::new(
        principal(9),
        token0,
        token1,
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(50_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(10_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("valid plan");

    let state = IcpswapExecutionState::planned(plan.clone());

    assert_eq!(state.plan, plan);
    assert_eq!(state.phase, IcpswapExecutionPhase::Planned);
    assert!(state.gross_swap_output.is_none());
    assert!(state.input_balance_before.is_none());
    assert!(state.output_balance_before.is_none());
    assert!(state.approval_block_index.is_none());
    assert!(state.approval_created_at.is_none());
    assert!(state.submitted_at.is_none());
    assert!(!state.recovery_attempted);
    assert!(state.last_error.is_none());
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
        ChainTokenAmount::from_raw(input.clone(), Nat::from(10u64)),
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

fn pool_data(pool: Principal, token0: &IcpswapToken, token1: &IcpswapToken, fee: Nat) -> IcpswapPoolData {
    IcpswapPoolData {
        key: format!("{}_{}_{}", token0.address, token1.address, fee),
        token0: token0.clone(),
        token1: token1.clone(),
        fee,
        tick_spacing: Int::from(60),
        canister_id: pool,
    }
}

#[tokio::test]
async fn client_discovers_pool_with_exact_factory_arguments() {
    let factory = principal(7);
    let pool = principal(8);
    let token0 = IcpswapToken {
        address: principal(1).to_text(),
        standard: "ICRC2".to_string(),
    };
    let token1 = IcpswapToken {
        address: principal(2).to_text(),
        standard: "ICRC2".to_string(),
    };
    let fee = Nat::from(3_000u64);
    let expected = pool_data(pool, &token0, &token1, fee.clone());
    let response = expected.clone();
    let expected_token0 = token0.clone();
    let expected_token1 = token1.clone();
    let expected_fee = fee.clone();

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<IcpswapResult<IcpswapPoolData>>()
        .times(1)
        .withf(move |canister, method, encoded| {
            if *canister != factory || method != "getPool" {
                return false;
            }
            let Ok(args) = Decode!(encoded, IcpswapGetPoolArgs) else {
                return false;
            };
            args.token0 == expected_token0 && args.token1 == expected_token1 && args.fee == expected_fee
        })
        .return_once(move |_, _, _| Ok(IcpswapResult::Ok(response)));

    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);
    let actual = client.get_pool(&token0, &token1, &fee).await.expect("pool");

    assert_eq!(actual, expected);
    assert_eq!(client.factory(), factory);
}

#[tokio::test]
async fn client_surfaces_factory_protocol_error() {
    let factory = principal(7);
    let token0 = IcpswapToken {
        address: principal(1).to_text(),
        standard: "ICRC2".to_string(),
    };
    let token1 = IcpswapToken {
        address: principal(2).to_text(),
        standard: "ICRC2".to_string(),
    };

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<IcpswapResult<IcpswapPoolData>>()
        .return_once(|_, _, _| Ok(IcpswapResult::Err(IcpswapError::CommonError)));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    let error = client.get_pool(&token0, &token1, &Nat::from(500u64)).await.unwrap_err();
    assert_eq!(
        error,
        IcpswapClientError::Protocol {
            method: "getPool",
            error: IcpswapError::CommonError,
        }
    );
}

#[tokio::test]
async fn client_surfaces_malformed_factory_response() {
    let factory = principal(7);
    let token0 = IcpswapToken {
        address: principal(1).to_text(),
        standard: "ICRC2".to_string(),
    };
    let token1 = IcpswapToken {
        address: principal(2).to_text(),
        standard: "ICRC2".to_string(),
    };

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<IcpswapResult<IcpswapPoolData>>()
        .return_once(|_, _, _| Err("Candid decode error: unexpected record field".to_string()));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(
        client.get_pool(&token0, &token1, &Nat::from(500u64)).await,
        Err(IcpswapClientError::Transport {
            canister: factory,
            method: "getPool",
            message: "Candid decode error: unexpected record field".to_string(),
        })
    );
}

#[tokio::test]
async fn client_quotes_pool_with_native_integer_strings() {
    let factory = principal(7);
    let pool = principal(8);
    let args = IcpswapSwapArgs {
        zero_for_one: false,
        amount_in: "100000000".to_string(),
        amount_out_minimum: "0".to_string(),
    };
    let expected_args = args.clone();

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<IcpswapResult<Nat>>()
        .times(1)
        .withf(move |canister, method, encoded| {
            if *canister != pool || method != "quote" {
                return false;
            }
            Decode!(encoded, IcpswapSwapArgs).is_ok_and(|decoded| decoded == expected_args)
        })
        .return_once(|_, _, _| Ok(IcpswapResult::Ok(Nat::from(42_000u64))));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(client.quote(pool, &args).await, Ok(Nat::from(42_000u64)));
}

#[tokio::test]
async fn client_surfaces_quote_transport_error() {
    let factory = principal(7);
    let pool = principal(8);
    let args = IcpswapSwapArgs {
        zero_for_one: true,
        amount_in: "1".to_string(),
        amount_out_minimum: "0".to_string(),
    };

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<IcpswapResult<Nat>>()
        .return_once(|_, _, _| Err("replica unavailable".to_string()));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    let error = client.quote(pool, &args).await.unwrap_err();
    assert_eq!(
        error,
        IcpswapClientError::Transport {
            canister: pool,
            method: "quote",
            message: "replica unavailable".to_string(),
        }
    );
}

#[tokio::test]
async fn client_surfaces_quote_protocol_error() {
    let factory = principal(7);
    let pool = principal(8);
    let args = IcpswapSwapArgs {
        zero_for_one: true,
        amount_in: "1".to_string(),
        amount_out_minimum: "0".to_string(),
    };

    let mut agent = MockPipelineAgent::new();
    agent.expect_call_query::<IcpswapResult<Nat>>().return_once(|_, _, _| {
        Ok(IcpswapResult::Err(IcpswapError::InternalError(
            "pool unavailable".to_string(),
        )))
    });
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(
        client.quote(pool, &args).await,
        Err(IcpswapClientError::Protocol {
            method: "quote",
            error: IcpswapError::InternalError("pool unavailable".to_string()),
        })
    );
}

#[tokio::test]
async fn client_reuses_icp_backend_for_ledger_fee() {
    let factory = principal(7);
    let ledger = principal(3);
    let agent = MockPipelineAgent::new();
    let mut backend = MockIcpswapLedgerClient::new();
    backend
        .expect_ledger_fee()
        .withf(move |actual| *actual == ledger)
        .times(1)
        .return_once(|_| Ok(Nat::from(10u64)));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(backend), factory);

    assert_eq!(client.ledger_fee(ledger).await, Ok(Nat::from(10u64)));
}

#[tokio::test]
async fn client_adds_ledger_context_to_fee_error() {
    let factory = principal(7);
    let ledger = principal(3);
    let agent = MockPipelineAgent::new();
    let mut backend = MockIcpswapLedgerClient::new();
    backend
        .expect_ledger_fee()
        .return_once(|_| Err("fee query rejected".to_string()));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(backend), factory);

    assert_eq!(
        client.ledger_fee(ledger).await,
        Err(IcpswapClientError::LedgerFee {
            ledger,
            message: "fee query rejected".to_string(),
        })
    );
}

#[tokio::test]
async fn client_submits_exact_deposit_from_and_swap_call() {
    let factory = principal(7);
    let pool = principal(8);
    let args = IcpswapDepositAndSwapArgs {
        zero_for_one: false,
        token_in_fee: Nat::from(10u64),
        token_out_fee: Nat::from(5u64),
        amount_in: "100000".to_string(),
        amount_out_minimum: "99000".to_string(),
    };
    let expected_args = args.clone();
    let response = Encode!(&IcpswapResult::Ok(Nat::from(99_500u64))).expect("response");

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update_raw()
        .times(1)
        .withf(move |canister, method, encoded| {
            *canister == pool
                && method == "depositFromAndSwap"
                && Decode!(encoded, IcpswapDepositAndSwapArgs).is_ok_and(|decoded| decoded == expected_args)
        })
        .return_once(move |_, _, _| Ok(response));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(
        client.deposit_from_and_swap(pool, &args).await,
        Ok(Nat::from(99_500u64))
    );
}

#[tokio::test]
async fn malformed_update_response_is_treated_as_ambiguous() {
    let factory = principal(7);
    let pool = principal(8);
    let args = IcpswapDepositAndSwapArgs {
        zero_for_one: true,
        token_in_fee: Nat::from(10u64),
        token_out_fee: Nat::from(5u64),
        amount_in: "100000".to_string(),
        amount_out_minimum: "99000".to_string(),
    };
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update_raw()
        .times(1)
        .return_once(|_, _, _| Ok(vec![0xde, 0xad, 0xbe, 0xef]));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert!(matches!(
        client.deposit_from_and_swap(pool, &args).await,
        Err(IcpswapExecutionClientError::SubmissionUnknown {
            pool: actual_pool,
            method: "depositFromAndSwap",
            ..
        }) if actual_pool == pool
    ));
}

#[tokio::test]
async fn client_submits_exact_deduplicated_recovery_transfer() {
    let factory = principal(7);
    let ledger = principal(1);
    let destination = Account {
        owner: principal(4),
        subaccount: Some([7; 32]),
    };
    let request = IcpswapRecoveryTransferRequest {
        ledger,
        from: Account {
            owner: principal(4),
            subaccount: None,
        },
        to: destination,
        amount: Nat::from(99_980u64),
        fee: Nat::from(10u64),
        created_at_time: 600,
    };
    let response = Encode!(&Result::<Nat, TransferError>::Ok(Nat::from(902u64))).expect("response");

    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update_raw()
        .times(1)
        .withf(move |canister, method, encoded| {
            *canister == ledger
                && method == "icrc1_transfer"
                && Decode!(encoded, TransferArg).is_ok_and(|args| {
                    args.from_subaccount.is_none()
                        && args.to == destination
                        && args.amount == Nat::from(99_980u64)
                        && args.fee == Some(Nat::from(10u64))
                        && args.memo.is_none()
                        && args.created_at_time == Some(600)
                })
        })
        .return_once(move |_, _, _| Ok(response));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(
        client.transfer_recovered_funds(&request).await,
        Ok(IcpswapRecoveryTransferOutcome::Completed(Nat::from(902u64)))
    );
}

#[tokio::test]
async fn client_preserves_duplicate_recovery_transfer_block() {
    let factory = principal(7);
    let ledger = principal(1);
    let request = IcpswapRecoveryTransferRequest {
        ledger,
        from: Account {
            owner: principal(4),
            subaccount: None,
        },
        to: Account {
            owner: principal(4),
            subaccount: Some([7; 32]),
        },
        amount: Nat::from(99_980u64),
        fee: Nat::from(10u64),
        created_at_time: 600,
    };
    let response = Encode!(&Result::<Nat, TransferError>::Err(TransferError::Duplicate {
        duplicate_of: Nat::from(902u64),
    }))
    .expect("response");
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update_raw()
        .times(1)
        .return_once(move |_, _, _| Ok(response));
    let client = IcpswapClient::new(Arc::new(agent), Arc::new(MockIcpswapLedgerClient::new()), factory);

    assert_eq!(
        client.transfer_recovered_funds(&request).await,
        Ok(IcpswapRecoveryTransferOutcome::Duplicate(Nat::from(902u64)))
    );
}
