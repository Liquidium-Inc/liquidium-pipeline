use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use crate::swappers::icpswap::{
    identity::IcpswapExecutionIdentity,
    state::validate_execution_state,
    transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementKind, IcpswapSettlementState},
    types::{ICPSWAP_STATE_VERSION, IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep},
};

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

fn principal(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn plan() -> IcpswapExecutionPlan {
    let input = token(principal(1), "ICP", 10);
    let output = token(principal(2), "ckUSDC", 5);
    IcpswapExecutionPlan::new(
        principal(9),
        principal(1),
        principal(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(50_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(10_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
    )
    .expect("valid plan")
}

fn funding_trader() -> Account {
    Account {
        owner: principal(4),
        subaccount: None,
    }
}

fn state() -> IcpswapExecutionState {
    let plan = plan();
    let (identity, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "1536").expect("execution identity");
    let child = Account {
        owner: identity.principal,
        subaccount: None,
    };
    let funding = IcpswapFundingState::new(funding_trader(), child, plan.input_ledger_fee.clone());
    let settlement = IcpswapSettlementState {
        kind: None,
        destination: funding_trader(),
        fee: plan.output_ledger_fee.clone(),
        transfer: IcpswapLedgerTransferState::default(),
        interrupted_transfer: None,
        interrupted_observed_debit: None,
        recovery_credit: None,
        residual_dust: None,
    };
    IcpswapExecutionState::prepare("run", plan, identity, funding, settlement).expect("state")
}

#[test]
fn state_starts_at_funding_with_an_isolated_owner() {
    let state = state();
    let identity = &state.identity;

    assert_eq!(state.schema_version, ICPSWAP_STATE_VERSION);
    assert_eq!(state.step, IcpswapStep::Funding);
    assert_eq!(state.owner.owner, identity.principal);
    assert_eq!(state.funding.destination, state.owner);
    validate_execution_state(&state, "run").expect("valid state");
}

#[test]
fn transfer_evidence_round_trips_without_private_material() {
    let mut state = state();
    state.settlement = IcpswapSettlementState {
        kind: Some(IcpswapSettlementKind::Output),
        destination: funding_trader(),
        fee: state.plan.output_ledger_fee.clone(),
        transfer: IcpswapLedgerTransferState {
            source_balance_before: Some(Nat::from(100u8)),
            destination_balance_before: Some(Nat::from(200u8)),
            credited_amount: Some(Nat::from(95u8)),
            ..Default::default()
        },
        interrupted_transfer: None,
        interrupted_observed_debit: None,
        recovery_credit: None,
        residual_dust: None,
    };

    let json = serde_json::to_value(&state).expect("serialize state");
    assert_eq!(json["settlement"]["kind"], "output");
    assert!(!json.to_string().contains(TEST_MNEMONIC));
    let decoded: IcpswapExecutionState = serde_json::from_value(json).expect("decode state");
    assert_eq!(decoded, state);
}

#[test]
fn validation_rejects_identity_mismatch_and_unsupported_versions() {
    let mut mismatched_identity = state();
    mismatched_identity.identity.principal = Principal::anonymous();
    assert!(validate_execution_state(&mismatched_identity, "run").is_err());

    let mut state = state();
    state.schema_version = ICPSWAP_STATE_VERSION + 1;
    assert!(
        validate_execution_state(&state, "run")
            .expect_err("unsupported version")
            .contains("unsupported ICPSwap state version")
    );
}

#[test]
fn output_recovery_must_keep_the_original_committed_receiver() {
    let mut state = state();
    let receiver = Account {
        owner: principal(5),
        subaccount: None,
    };
    state.settlement.kind = Some(IcpswapSettlementKind::OutputRecovery);
    state.settlement.destination = receiver;
    state.settlement.recovery_credit = Some(Nat::from(100u64));
    state.settlement.interrupted_transfer = Some(IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: state.owner.subaccount,
            to: receiver,
            amount: Nat::from(95u64),
            fee: Some(Nat::from(5u64)),
            memo: None,
            created_at_time: Some(1_000),
        }),
        ..Default::default()
    });

    validate_execution_state(&state, "run").expect("original receiver remains valid");

    state.settlement.destination = state.funding.surplus_destination;
    assert!(
        validate_execution_state(&state, "run")
            .expect_err("output recovery cannot redirect funds")
            .contains("changed the committed receiver")
    );
}
