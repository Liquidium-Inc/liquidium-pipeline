use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use super::{
    client::MockIcpswapRecoveryTransferClient,
    execution::IcpswapExecutionStateStore,
    recovery_transfer::transfer_to_recovery,
    types::{
        IcpswapExecutionPhase, IcpswapExecutionPlan, IcpswapExecutionState, IcpswapRecoveryTransferClientError,
        IcpswapRecoveryTransferError, IcpswapRecoveryTransferOutcome,
    },
};

fn p(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn account(owner: Principal) -> Account {
    Account {
        owner,
        subaccount: None,
    }
}

fn destination() -> Account {
    Account {
        owner: p(4),
        subaccount: Some([7; 32]),
    }
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.into(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn refunded_state(gross: u64) -> IcpswapExecutionState {
    let input = token(p(1), "IN", 10);
    let output = token(p(2), "OUT", 5);
    let plan = IcpswapExecutionPlan::new(
        p(9),
        p(1),
        p(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
        123,
    )
    .expect("plan");
    let mut state = IcpswapExecutionState::planned(plan);
    state.phase = IcpswapExecutionPhase::Refunded;
    state.returned_gross_amount = Some(ChainTokenAmount::from_raw(input, Nat::from(gross)));
    state.refund_transaction_id = Some(Nat::from(43u64));
    state.refund_ledger_block_index = Some(Nat::from(901u64));
    state
}

struct Store(Mutex<IcpswapExecutionState>);

#[async_trait]
impl IcpswapExecutionStateStore for Store {
    async fn load(&self, _: &str) -> Result<Option<IcpswapExecutionState>, String> {
        Ok(Some(self.0.lock().unwrap().clone()))
    }

    async fn persist(&self, _: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        *self.0.lock().unwrap() = state.clone();
        Ok(())
    }
}

#[tokio::test]
async fn subtracts_both_fees_and_persists_intent_before_call() {
    let store = Arc::new(Store(Mutex::new(refunded_state(100_000))));
    let observed_store = store.clone();
    let expected_destination = destination();
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(move |request| {
            let persisted = observed_store.0.lock().unwrap().clone();
            assert_eq!(persisted.phase, IcpswapExecutionPhase::RecoveryTransferSubmitted);
            assert_eq!(persisted.recovery_destination, Some(expected_destination));
            assert_eq!(persisted.recovery_transfer_created_at, Some(600));
            assert_eq!(request.ledger, p(1));
            assert_eq!(request.from, account(p(4)));
            assert_eq!(request.to, expected_destination);
            assert_eq!(request.amount, Nat::from(99_980u64));
            assert_eq!(request.fee, Nat::from(10u64));
            assert_eq!(request.created_at_time, 600);
            Ok(IcpswapRecoveryTransferOutcome::Completed(Nat::from(902u64)))
        });

    let result = transfer_to_recovery(&client, store.as_ref(), "liq", account(p(4)), destination(), 600, 300)
        .await
        .expect("recovered");

    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
    assert_eq!(result.recovery_transfer_block_index, Some(Nat::from(902u64)));
}

#[tokio::test]
async fn duplicate_ledger_result_is_success() {
    let store = Store(Mutex::new(refunded_state(100_000)));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(|_| Ok(IcpswapRecoveryTransferOutcome::Duplicate(Nat::from(777u64))));

    let result = transfer_to_recovery(&client, &store, "liq", account(p(4)), destination(), 600, 300)
        .await
        .expect("duplicate confirms transfer");

    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
    assert_eq!(result.recovery_transfer_block_index, Some(Nat::from(777u64)));
}

#[tokio::test]
async fn ambiguous_response_retries_the_same_deterministic_transfer() {
    let store = Store(Mutex::new(refunded_state(100_000)));
    let mut first_client = MockIcpswapRecoveryTransferClient::new();
    first_client
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(|request| {
            assert_eq!(request.created_at_time, 600);
            Err(IcpswapRecoveryTransferClientError::SubmissionUnknown {
                ledger: request.ledger,
                message: "timeout".into(),
            })
        });

    transfer_to_recovery(&first_client, &store, "liq", account(p(4)), destination(), 600, 300)
        .await
        .expect_err("ambiguous response");
    assert_eq!(
        store.0.lock().unwrap().phase,
        IcpswapExecutionPhase::RecoveryTransferSubmitted
    );

    let mut retry_client = MockIcpswapRecoveryTransferClient::new();
    retry_client
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(|request| {
            assert_eq!(request.created_at_time, 600);
            assert_eq!(request.amount, Nat::from(99_980u64));
            Ok(IcpswapRecoveryTransferOutcome::Duplicate(Nat::from(902u64)))
        });

    let result = transfer_to_recovery(&retry_client, &store, "liq", account(p(4)), destination(), 700, 300)
        .await
        .expect("deduplicated retry");
    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
}

#[tokio::test]
async fn zero_transferable_amount_recovers_without_calling_ledger() {
    let store = Store(Mutex::new(refunded_state(20)));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client.expect_transfer_recovered_funds().times(0);

    let result = transfer_to_recovery(&client, &store, "liq", account(p(4)), destination(), 600, 300)
        .await
        .expect("nothing transferable");

    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
    assert_eq!(result.recovery_transfer_amount.unwrap().value, Nat::from(0u8));
    assert!(result.recovery_transfer_block_index.is_none());
}

#[tokio::test]
async fn restart_from_pending_submits_the_persisted_transfer() {
    let mut state = refunded_state(100_000);
    state.phase = IcpswapExecutionPhase::RecoveryTransferPending;
    state.recovery_destination = Some(destination());
    state.recovery_transfer_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        Nat::from(99_980u64),
    ));
    state.recovery_transfer_created_at = Some(600);
    let store = Store(Mutex::new(state));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client
        .expect_transfer_recovered_funds()
        .times(1)
        .return_once(|request| {
            assert_eq!(request.created_at_time, 600);
            Ok(IcpswapRecoveryTransferOutcome::Completed(Nat::from(902u64)))
        });

    let result = transfer_to_recovery(&client, &store, "liq", account(p(4)), destination(), 700, 300)
        .await
        .expect("pending restart");
    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
}

#[tokio::test]
async fn unconfirmed_transfer_times_out_without_another_call() {
    let mut state = refunded_state(100_000);
    state.phase = IcpswapExecutionPhase::RecoveryTransferSubmitted;
    state.recovery_destination = Some(destination());
    state.recovery_transfer_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        Nat::from(99_980u64),
    ));
    state.recovery_transfer_created_at = Some(600);
    let store = Store(Mutex::new(state));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client.expect_transfer_recovered_funds().times(0);

    let result = transfer_to_recovery(&client, &store, "liq", account(p(4)), destination(), 900, 300)
        .await
        .expect("terminal timeout");
    assert_eq!(result.phase, IcpswapExecutionPhase::FailedTerminal);
    assert!(result.last_error.unwrap().contains("before timeout"));
}

#[tokio::test]
async fn persisted_destination_cannot_be_replaced() {
    let mut state = refunded_state(100_000);
    state.phase = IcpswapExecutionPhase::RecoveryTransferPending;
    state.recovery_destination = Some(destination());
    state.recovery_transfer_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        Nat::from(99_980u64),
    ));
    state.recovery_transfer_created_at = Some(600);
    let store = Store(Mutex::new(state));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client.expect_transfer_recovered_funds().times(0);
    let changed_destination = Account {
        owner: p(4),
        subaccount: Some([8; 32]),
    };

    let error = transfer_to_recovery(&client, &store, "liq", account(p(4)), changed_destination, 700, 300)
        .await
        .expect_err("destination mismatch");

    assert!(matches!(
        error,
        IcpswapRecoveryTransferError::DestinationMismatch { .. }
    ));
    assert_eq!(store.0.lock().unwrap().recovery_destination, Some(destination()));
}

#[tokio::test]
async fn recovered_phase_is_idempotent() {
    let mut state = refunded_state(100_000);
    state.phase = IcpswapExecutionPhase::Recovered;
    state.recovery_destination = Some(destination());
    state.recovery_transfer_amount = Some(ChainTokenAmount::from_raw(
        state.plan.amount_in.token.clone(),
        Nat::from(99_980u64),
    ));
    state.recovery_transfer_created_at = Some(600);
    state.recovery_transfer_block_index = Some(Nat::from(902u64));
    let store = Store(Mutex::new(state));
    let mut client = MockIcpswapRecoveryTransferClient::new();
    client.expect_transfer_recovered_funds().times(0);

    let result = transfer_to_recovery(&client, &store, "liq", account(p(4)), destination(), 700, 300)
        .await
        .expect("already recovered");
    assert_eq!(result.phase, IcpswapExecutionPhase::Recovered);
    assert_eq!(result.recovery_transfer_block_index, Some(Nat::from(902u64)));
}
