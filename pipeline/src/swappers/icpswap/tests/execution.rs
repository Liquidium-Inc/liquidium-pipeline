use std::sync::Mutex;

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{
    client::MockIcpswapExecutionClient,
    execution::{IcpswapExecutionStateStore, approve_and_submit},
    types::{
        IcpswapExecutionClientError, IcpswapExecutionError, IcpswapExecutionPhase, IcpswapExecutionPlan,
        IcpswapExecutionState,
    },
};

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
    let input = token(principal(1), "INPUT", 10);
    let output = token(principal(2), "OUTPUT", 5);
    IcpswapExecutionPlan::new(
        principal(9),
        principal(1),
        principal(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan")
}

#[derive(Default)]
struct MemoryStateStore {
    state: Mutex<Option<IcpswapExecutionState>>,
    phases: Mutex<Vec<IcpswapExecutionPhase>>,
}

#[async_trait]
impl IcpswapExecutionStateStore for MemoryStateStore {
    async fn load(&self, _liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String> {
        Ok(self.state.lock().expect("state lock").clone())
    }

    async fn persist(&self, _liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        self.phases.lock().expect("phases lock").push(state.phase);
        *self.state.lock().expect("state lock") = Some(state.clone());
        Ok(())
    }
}

fn owner() -> Account {
    Account {
        owner: principal(4),
        subaccount: None,
    }
}

fn expect_transaction_cursor(client: &mut MockIcpswapExecutionClient) {
    client
        .expect_latest_transaction_id()
        .times(1)
        .withf(|pool, transaction_owner| *pool == principal(9) && *transaction_owner == principal(4))
        .return_once(|_, _| Ok(Some(Nat::from(41u64))));
}

#[tokio::test]
async fn approves_exact_pool_for_amount_plus_input_fee_then_submits_exact_args() {
    let mut client = MockIcpswapExecutionClient::new();
    expect_transaction_cursor(&mut client);
    client
        .expect_allowance()
        .times(1)
        .withf(|ledger, actual_owner, spender| {
            *ledger == principal(1)
                && *actual_owner == owner()
                && spender.owner == principal(9)
                && spender.subaccount.is_none()
        })
        .return_once(|_, _, _| Ok(Nat::from(20u64)));
    client
        .expect_approve()
        .times(1)
        .withf(|request| {
            request.ledger == principal(1)
                && request.owner == owner()
                && request.spender.owner == principal(9)
                && request.spender.subaccount.is_none()
                && request.current_allowance == Nat::from(20u64)
                && request.required_allowance == Nat::from(100_010u64)
                && request.created_at_time == 456
        })
        .return_once(|_| Ok(Nat::from(77u64)));
    client
        .expect_deposit_from_and_swap()
        .times(1)
        .withf(|pool, args| {
            *pool == principal(9)
                && args.zero_for_one
                && args.token_in_fee == Nat::from(10u64)
                && args.token_out_fee == Nat::from(5u64)
                && args.amount_in == "100000"
                && args.amount_out_minimum == "118800"
        })
        .return_once(|_, _| Ok(Nat::from(119_500u64)));
    let store = MemoryStateStore::default();

    let state = approve_and_submit(&client, &store, "liq-1", owner(), Some(plan()), 456)
        .await
        .expect("submission");

    assert_eq!(state.phase, IcpswapExecutionPhase::AwaitingOutput);
    assert_eq!(state.approval_block_index, Some(Nat::from(77u64)));
    assert_eq!(
        state.gross_swap_output.expect("gross output").value,
        Nat::from(119_500u64)
    );
    assert_eq!(
        *store.phases.lock().expect("phases lock"),
        vec![
            IcpswapExecutionPhase::Planned,
            IcpswapExecutionPhase::Planned,
            IcpswapExecutionPhase::Approved,
            IcpswapExecutionPhase::Approved,
            IcpswapExecutionPhase::SubmissionUnknown,
            IcpswapExecutionPhase::AwaitingOutput,
        ]
    );
}

#[tokio::test]
async fn sufficient_allowance_skips_approval() {
    let mut client = MockIcpswapExecutionClient::new();
    expect_transaction_cursor(&mut client);
    client
        .expect_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(100_010u64)));
    client.expect_approve().times(0);
    client
        .expect_deposit_from_and_swap()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(119_500u64)));
    let store = MemoryStateStore::default();

    let state = approve_and_submit(&client, &store, "liq-2", owner(), Some(plan()), 456)
        .await
        .expect("submission");

    assert_eq!(state.phase, IcpswapExecutionPhase::AwaitingOutput);
    assert!(state.approval_block_index.is_none());
}

#[tokio::test]
async fn approval_failure_stops_before_swap_submission() {
    let mut client = MockIcpswapExecutionClient::new();
    client
        .expect_allowance()
        .times(2)
        .returning(|_, _, _| Ok(Nat::from(0u8)));
    client.expect_approve().times(1).return_once(|request| {
        Err(IcpswapExecutionClientError::Approval {
            ledger: request.ledger,
            message: "ledger rejected approval".to_string(),
        })
    });
    client.expect_deposit_from_and_swap().times(0);
    let store = MemoryStateStore::default();

    let error = approve_and_submit(&client, &store, "liq-3", owner(), Some(plan()), 456)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IcpswapExecutionError::Client(IcpswapExecutionClientError::Approval { .. })
    ));
    assert_eq!(
        *store.phases.lock().expect("phases lock"),
        vec![IcpswapExecutionPhase::Planned, IcpswapExecutionPhase::Planned]
    );
}

#[tokio::test]
async fn ambiguous_approval_rechecks_allowance_before_submission() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let allowance_calls = calls.clone();
    let mut client = MockIcpswapExecutionClient::new();
    expect_transaction_cursor(&mut client);
    client.expect_allowance().times(2).returning(move |_, _, _| {
        let call = allowance_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(if call == 0 {
            Nat::from(0u8)
        } else {
            Nat::from(100_010u64)
        })
    });
    client.expect_approve().times(1).return_once(|request| {
        Err(IcpswapExecutionClientError::Approval {
            ledger: request.ledger,
            message: "response timeout".to_string(),
        })
    });
    client
        .expect_deposit_from_and_swap()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(119_500u64)));
    let store = MemoryStateStore::default();

    let state = approve_and_submit(&client, &store, "liq-approval-unknown", owner(), Some(plan()), 456)
        .await
        .expect("refreshed allowance proves approval succeeded");

    assert_eq!(state.phase, IcpswapExecutionPhase::AwaitingOutput);
    assert_eq!(state.approval_created_at, Some(456));
    assert!(state.approval_block_index.is_none());
}

#[tokio::test]
async fn ambiguous_submission_is_checkpointed_and_never_resubmitted() {
    let mut client = MockIcpswapExecutionClient::new();
    expect_transaction_cursor(&mut client);
    client
        .expect_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(100_010u64)));
    client.expect_approve().times(0);
    client.expect_deposit_from_and_swap().times(1).return_once(|pool, _| {
        Err(IcpswapExecutionClientError::SubmissionUnknown {
            pool,
            method: "depositFromAndSwap",
            message: "timeout".to_string(),
        })
    });
    let store = MemoryStateStore::default();

    let first = approve_and_submit(&client, &store, "liq-4", owner(), Some(plan()), 456).await;
    assert!(matches!(
        first,
        Err(IcpswapExecutionError::Client(
            IcpswapExecutionClientError::SubmissionUnknown { .. }
        ))
    ));
    assert_eq!(
        store.state.lock().expect("state lock").as_ref().expect("state").phase,
        IcpswapExecutionPhase::SubmissionUnknown
    );

    let retry = approve_and_submit(&client, &store, "liq-4", owner(), None, 789).await;
    assert_eq!(
        retry,
        Err(IcpswapExecutionError::SubmissionAlreadyStarted(
            IcpswapExecutionPhase::SubmissionUnknown
        ))
    );
}

#[tokio::test]
async fn protocol_error_moves_to_refund_pending() {
    let mut client = MockIcpswapExecutionClient::new();
    expect_transaction_cursor(&mut client);
    client
        .expect_allowance()
        .times(1)
        .return_once(|_, _, _| Ok(Nat::from(100_010u64)));
    client.expect_approve().times(0);
    client.expect_deposit_from_and_swap().times(1).return_once(|_, _| {
        Err(IcpswapExecutionClientError::Protocol {
            method: "depositFromAndSwap",
            error: super::types::IcpswapError::InternalError("slippage check failed".to_string()),
        })
    });
    let store = MemoryStateStore::default();

    let result = approve_and_submit(&client, &store, "liq-5", owner(), Some(plan()), 456).await;

    assert!(result.is_err());
    assert_eq!(
        store.state.lock().expect("state lock").as_ref().expect("state").phase,
        IcpswapExecutionPhase::RefundPending
    );
}
