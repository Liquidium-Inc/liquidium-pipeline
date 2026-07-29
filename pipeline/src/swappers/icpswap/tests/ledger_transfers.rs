use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::{account::Account, transfer::TransferArg};
use icrc_ledger_types::icrc2::approve::ApproveArgs;
use liquidium_pipeline_connectors::backend::icp_backend::{IcpBackend, IcrcTransferError};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use mockall::Sequence;

use super::{
    execution::IcpswapExecutionStateStore,
    identity::IcpswapExecutionIdentity,
    ledger_transfers::{forward_step, funding_step},
    transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementKind, IcpswapSettlementState},
    types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep},
};
use crate::utils::ICP_LEDGER_PRINCIPAL;

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

mockall::mock! {
    IcpBackend {}

    #[async_trait]
    impl IcpBackend for IcpBackend {
        async fn icrc1_balance(&self, ledger: Principal, account: &Account) -> Result<Nat, String>;
        async fn icp_account_balance(&self, ledger: Principal, account_id_hex: &str) -> Result<Nat, String>;
        async fn icrc1_transfer(
            &self,
            ledger: Principal,
            from: &Account,
            to: &Account,
            amount: Nat,
        ) -> Result<Nat, String>;
        async fn icrc1_transfer_with_args(
            &self,
            ledger: Principal,
            args: TransferArg,
        ) -> Result<Nat, IcrcTransferError>;
        async fn icp_transfer(
            &self,
            ledger: Principal,
            to_account_id_hex: &str,
            amount_e8s: Nat,
        ) -> Result<u64, String>;
        async fn icrc1_decimals(&self, ledger: Principal) -> Result<u8, String>;
        async fn icrc1_fee(&self, ledger: Principal) -> Result<Nat, String>;
        async fn icrc2_allowance(
            &self,
            ledger: Principal,
            account: &Account,
            spender: &Account,
        ) -> Result<Nat, String>;
        async fn icrc2_approve(&self, ledger: Principal, args: ApproveArgs) -> Result<Nat, String>;
    }
}

fn principal(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn account(id: u8) -> Account {
    Account {
        owner: principal(id),
        subaccount: None,
    }
}

fn token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn state() -> IcpswapExecutionState {
    let input_ledger = Principal::from_text(ICP_LEDGER_PRINCIPAL).expect("native ICP ledger");
    let input = token(input_ledger, "ICP", 10);
    let output = token(principal(2), "ckUSDC", 5);
    let plan = IcpswapExecutionPlan::new(
        principal(9),
        input_ledger,
        principal(2),
        Nat::from(3_000u64),
        ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        ChainTokenAmount::from_raw(input, Nat::from(10u64)),
        ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
        ChainTokenAmount::from_raw(output, Nat::from(5u64)),
        100,
    )
    .expect("plan");
    let (identity, _) = IcpswapExecutionIdentity::derive(TEST_MNEMONIC, "42").expect("identity");
    let child = Account {
        owner: identity.principal,
        subaccount: None,
    };
    IcpswapExecutionState::prepare(
        "run",
        plan.clone(),
        identity,
        IcpswapFundingState::new(account(4), child, plan.input_ledger_fee.clone()),
        IcpswapSettlementState {
            kind: None,
            destination: account(5),
            fee: plan.output_ledger_fee.clone(),
            transfer: IcpswapLedgerTransferState::default(),
            interrupted_transfer: None,
            interrupted_observed_debit: None,
            recovery_credit: None,
            residual_dust: None,
        },
    )
    .expect("state")
}

struct Store(Mutex<IcpswapExecutionState>);

impl Store {
    fn new(state: IcpswapExecutionState) -> Self {
        Self(Mutex::new(state))
    }

    fn state(&self) -> IcpswapExecutionState {
        self.0.lock().expect("store lock").clone()
    }
}

/// Serializes and decodes the state exactly as a daemon restart would through
/// the WAL JSON envelope.
fn restart(state: &IcpswapExecutionState) -> IcpswapExecutionState {
    serde_json::from_value(serde_json::to_value(state).expect("encode restart state")).expect("decode restart state")
}

#[async_trait]
impl IcpswapExecutionStateStore for Store {
    async fn load(&self, _: &str) -> Result<Option<IcpswapExecutionState>, String> {
        Ok(Some(self.state()))
    }

    async fn persist(&self, _: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        *self.0.lock().expect("store lock") = state.clone();
        Ok(())
    }
}

#[tokio::test]
async fn funding_intent_and_balance_baselines_are_durable_before_submission() {
    let store = Arc::new(Store::new(state()));
    let observed = store.clone();
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // Funding first observes the isolated child, then the shared source.
    for value in [7u64, 1_000_000u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(move |ledger, args| {
            let persisted = observed.state();
            let transfer = &persisted.funding.transfer;
            assert_eq!(persisted.step, IcpswapStep::FundingPending);
            assert_eq!(transfer.args.as_ref(), Some(&args));
            assert_eq!(transfer.source_balance_before, Some(Nat::from(1_000_000u64)));
            assert_eq!(transfer.destination_balance_before, Some(Nat::from(7u64)));
            assert_eq!(ledger, Principal::from_text(ICP_LEDGER_PRINCIPAL).unwrap());
            assert_eq!(args.amount, Nat::from(100_013u64));
            assert_eq!(args.created_at_time, Some(1_000));
            Ok(Nat::from(77u64))
        });

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("funding");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Transfer);
    assert_eq!(persisted.funding.transfer.block_index, Some(Nat::from(77u64)));
}

#[tokio::test]
async fn restart_resumes_the_persisted_funding_intent_without_repreparing_it() {
    let mut initial = state();
    initial.step = IcpswapStep::FundingPending;
    initial.funding.transfer = IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: initial.funding.source.subaccount,
            to: initial.funding.destination,
            amount: Nat::from(100_020u64),
            fee: Some(Nat::from(10u64)),
            memo: None,
            created_at_time: Some(1_000),
        }),
        source_balance_before: Some(Nat::from(1_000_000u64)),
        destination_balance_before: Some(Nat::from(0u8)),
        ..Default::default()
    };
    let persisted_args = initial.funding.transfer.args.clone().expect("funding intent");
    let store = Store::new(restart(&initial));
    let mut client = MockIcpBackend::new();
    client.expect_icrc1_balance().times(0);
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .withf(move |_, args| args == &persisted_args)
        .return_once(|_, _| Ok(Nat::from(77u64)));

    let mut current = store.state();
    funding_step(&client, &store, "run", &mut current, 9_999)
        .await
        .expect("resume funding intent");

    assert_eq!(store.state().step, IcpswapStep::Transfer);
    assert_eq!(store.state().funding.transfer.block_index, Some(Nat::from(77u64)));
}

#[tokio::test]
async fn restart_resumes_the_persisted_surplus_recovery_without_repreparing_it() {
    let mut initial = state();
    initial.step = IcpswapStep::FundingSurplusPending;
    initial.funding.surplus_transfer = IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: initial.funding.destination.subaccount,
            to: initial.funding.surplus_destination,
            amount: Nat::from(90u64),
            fee: Some(Nat::from(10u64)),
            memo: None,
            created_at_time: Some(1_000),
        }),
        source_balance_before: Some(Nat::from(100_120u64)),
        destination_balance_before: Some(Nat::from(50u64)),
        ..Default::default()
    };
    let persisted_args = initial
        .funding
        .surplus_transfer
        .args
        .clone()
        .expect("surplus recovery intent");
    let store = Store::new(restart(&initial));
    let mut client = MockIcpBackend::new();
    client.expect_icrc1_balance().times(0);
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .withf(move |_, args| args == &persisted_args)
        .return_once(|_, _| Ok(Nat::from(91u64)));

    let mut current = store.state();
    funding_step(&client, &store, "run", &mut current, 9_999)
        .await
        .expect("resume surplus recovery intent");

    assert_eq!(store.state().step, IcpswapStep::Funding);
    assert_eq!(
        store.state().funding.surplus_transfer.block_index,
        Some(Nat::from(91u64))
    );
}

#[tokio::test]
async fn restart_resumes_the_persisted_output_recovery_without_repreparing_it() {
    let mut initial = state();
    initial.step = IcpswapStep::ForwardPending;
    initial.settlement.kind = Some(IcpswapSettlementKind::OutputRecovery);
    initial.settlement.destination = initial.funding.surplus_destination;
    initial.settlement.recovery_credit = Some(Nat::from(50u64));
    initial.settlement.transfer = IcpswapLedgerTransferState {
        args: Some(TransferArg {
            from_subaccount: initial.owner.subaccount,
            to: initial.settlement.destination,
            amount: Nat::from(45u64),
            fee: Some(Nat::from(5u64)),
            memo: None,
            created_at_time: Some(2_000),
        }),
        source_balance_before: Some(Nat::from(950u64)),
        destination_balance_before: Some(Nat::from(5u64)),
        ..Default::default()
    };
    let persisted_args = initial
        .settlement
        .transfer
        .args
        .clone()
        .expect("output recovery intent");
    let store = Store::new(restart(&initial));
    let mut client = MockIcpBackend::new();
    client.expect_icrc1_balance().times(0);
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .withf(move |ledger, args| *ledger == principal(2) && args == &persisted_args)
        .return_once(|_, _| Ok(Nat::from(99u64)));

    let mut current = store.state();
    forward_step(&client, &store, "run", &mut current, 9_999)
        .await
        .expect("resume output recovery intent");

    assert_eq!(store.state().step, IcpswapStep::Refunded);
    assert_eq!(store.state().settlement.transfer.block_index, Some(Nat::from(99u64)));
}

#[tokio::test]
async fn forwarding_sends_only_the_recorded_execution_credit() {
    let mut initial = state();
    initial.step = IcpswapStep::Forward;
    initial.withdraw.wallet_credited_amount = Some(Nat::from(100u64));
    initial.settlement.kind = Some(IcpswapSettlementKind::Output);
    let store = Arc::new(Store::new(initial));
    let observed = store.clone();
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The child owns 1,000 units, but only this execution's 100-unit credit is
    // eligible for forwarding.
    for value in [1_000u64, 20u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(move |ledger, args| {
            let persisted = observed.state();
            assert_eq!(persisted.step, IcpswapStep::ForwardPending);
            assert_eq!(persisted.settlement.transfer.args, Some(args.clone()));
            assert_eq!(ledger, principal(2));
            assert_eq!(args.to, account(5));
            assert_eq!(args.amount, Nat::from(95u64));
            Ok(Nat::from(88u64))
        });

    let mut current = store.state();
    forward_step(&client, store.as_ref(), "run", &mut current, 2_000)
        .await
        .expect("forward");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Completed);
    assert_eq!(persisted.settlement.transfer.credited_amount, Some(Nat::from(95u64)));
}

#[tokio::test]
async fn too_old_funding_transfer_is_retried_when_the_isolated_child_is_unchanged() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The shared trader may fund unrelated liquidations while this transfer
    // ages out. Only the isolated child's unchanged balance decides the retry.
    for value in [7u64, 1_000_000u64, 900_000u64, 7u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("unchanged balances permit a fresh attempt");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Funding);
    assert_eq!(persisted.funding.transfer.args.expect("args").created_at_time, None);
}

#[tokio::test]
async fn too_old_funding_transfer_with_expected_deltas_is_confirmed() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The persisted top-up sends 100,013, bringing the child from 7 to its exact
    // 100,020 target. An unrelated trader debit must not invalidate that credit.
    for value in [7u64, 1_000_000u64, 850_000u64, 100_020u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("balance deltas prove the aged transfer succeeded");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Transfer);
    assert_eq!(persisted.funding.transfer.credited_amount, Some(Nat::from(100_013u64)));
}

#[tokio::test]
async fn future_dated_funding_transfer_is_reset_with_a_retry_delay() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    for value in [7u64, 1_000_000u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::CreatedInFuture { ledger_time: 900 }));

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect_err("future timestamp must pause this attempt");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Funding);
    assert_eq!(persisted.funding.transfer.args.expect("args").created_at_time, None);
    assert_eq!(persisted.next_attempt_at_nanos, Some(2_000_001_000));
}

#[tokio::test]
async fn too_old_output_forward_is_confirmed_from_the_isolated_child_debit() {
    let mut initial = state();
    initial.step = IcpswapStep::Forward;
    initial.withdraw.wallet_credited_amount = Some(Nat::from(100u64));
    initial.settlement.kind = Some(IcpswapSettlementKind::Output);
    let store = Arc::new(Store::new(initial));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The child debits 95 plus a 5-unit fee. Unrelated credits into the shared
    // receiver are diagnostic only and do not make this transfer ambiguous.
    for value in [1_000u64, 20u64, 900u64, 500u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));

    let mut current = store.state();
    forward_step(&client, store.as_ref(), "run", &mut current, 2_000)
        .await
        .expect("isolated source debit proves settlement succeeded");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Completed);
    assert_eq!(persisted.settlement.transfer.credited_amount, Some(Nat::from(95u64)));
}

#[tokio::test]
async fn recovery_forwards_the_recorded_input_credit_to_the_funding_trader() {
    let mut initial = state();
    initial.step = IcpswapStep::Forward;
    initial.recovery.wallet_credited_amount = Some(Nat::from(100u64));
    initial.settlement.kind = Some(IcpswapSettlementKind::Recovery);
    initial.settlement.destination = initial.funding.source;
    initial.settlement.fee = initial.plan.input_ledger_fee.clone();
    let expected_destination = initial.funding.source;
    let expected_ledger = initial.plan.token_in;
    let store = Arc::new(Store::new(initial));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    for value in [1_000u64, 20u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(move |ledger, args| {
            assert_eq!(ledger, expected_ledger);
            assert_eq!(args.to, expected_destination);
            assert_eq!(args.amount, Nat::from(90u64));
            Ok(Nat::from(89u64))
        });

    let mut current = store.state();
    forward_step(&client, store.as_ref(), "run", &mut current, 2_000)
        .await
        .expect("recovery settlement");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Refunded);
    assert_eq!(persisted.settlement.transfer.credited_amount, Some(Nat::from(90u64)));
}

#[tokio::test]
async fn unexpected_aged_funding_delta_reenters_balance_normalization() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    for value in [7u64, 1_000_000u64, 999_000u64, 8u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("isolated child balance can be normalized on the next advance");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Funding);
    assert!(
        persisted
            .last_error
            .expect("reconciliation note")
            .contains("re-normalizing")
    );
}

#[tokio::test]
async fn funding_sweeps_surplus_to_recovery_before_pool_transfer() {
    let store = Arc::new(Store::new(state()));
    let expected_recovery = store.state().funding.surplus_destination;
    let observed = store.clone();
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The exact child target is 100,020. Its extra 100 units can forward 90
    // after paying the 10-unit recovery transfer fee.
    for value in [100_120u64, 50u64, 100_020u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(move |ledger, args| {
            let persisted = observed.state();
            assert_eq!(persisted.step, IcpswapStep::FundingSurplusPending);
            assert_eq!(persisted.funding.surplus_transfer.args, Some(args.clone()));
            assert_eq!(ledger, Principal::from_text(ICP_LEDGER_PRINCIPAL).unwrap());
            assert_eq!(args.to, expected_recovery);
            assert_eq!(args.amount, Nat::from(90u64));
            assert_eq!(args.fee, Some(Nat::from(10u64)));
            Ok(Nat::from(91u64))
        });

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("surplus recovery");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Funding);
    assert_eq!(
        persisted.funding.surplus_transfer.credited_amount,
        Some(Nat::from(90u64))
    );

    let mut current = persisted;
    funding_step(&client, store.as_ref(), "run", &mut current, 1_001)
        .await
        .expect("normalized balance advances to the pool transfer");
    assert_eq!(store.state().step, IcpswapStep::Transfer);
}

#[tokio::test]
async fn funding_records_surplus_too_small_to_sweep() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    client
        .expect_icrc1_balance()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(100_025u64)));
    client.expect_icrc1_transfer_with_args().times(0);

    let mut current = store.state();
    funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect("unsweepable dust does not block the trade");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Transfer);
    assert_eq!(persisted.funding.residual_dust, Some(Nat::from(5u64)));
}

#[tokio::test]
async fn unexpected_aged_settlement_delta_recovers_the_remaining_output() {
    let mut initial = state();
    initial.step = IcpswapStep::Forward;
    initial.withdraw.wallet_credited_amount = Some(Nat::from(100u64));
    initial.settlement.kind = Some(IcpswapSettlementKind::Output);
    let store = Arc::new(Store::new(initial));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The forward should debit 100 total, but the isolated child only moved by
    // 50. Settlement cannot safely infer where the execution output went.
    for value in [1_000u64, 20u64, 950u64, 500u64, 950u64, 5u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    let mut transfers = Sequence::new();
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .in_sequence(&mut transfers)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));
    let recovery_destination = store.state().funding.surplus_destination;
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .in_sequence(&mut transfers)
        .return_once(move |ledger, args| {
            assert_eq!(ledger, principal(2));
            assert_eq!(args.to, recovery_destination);
            assert_eq!(args.amount, Nat::from(45u64));
            assert_eq!(args.fee, Some(Nat::from(5u64)));
            Ok(Nat::from(99u64))
        });

    let mut current = store.state();
    forward_step(&client, store.as_ref(), "run", &mut current, 2_000)
        .await
        .expect("unexpected output movement enters recovery");

    let recovered = store.state();
    assert_eq!(recovered.step, IcpswapStep::Forward);
    assert_eq!(recovered.settlement.kind, Some(IcpswapSettlementKind::OutputRecovery));
    assert_eq!(recovered.settlement.destination, recovery_destination);
    assert_eq!(recovered.settlement.interrupted_observed_debit, Some(Nat::from(50u64)));
    assert_eq!(recovered.settlement.recovery_credit, Some(Nat::from(50u64)));
    assert_eq!(
        recovered
            .settlement
            .interrupted_transfer
            .as_ref()
            .and_then(|transfer| transfer.args.as_ref())
            .map(|args| args.to),
        Some(account(5))
    );

    let mut current = recovered;
    forward_step(&client, store.as_ref(), "run", &mut current, 2_001)
        .await
        .expect("remaining output recovery transfer");
    assert_eq!(store.state().step, IcpswapStep::Refunded);
}

#[tokio::test]
async fn unexpected_aged_settlement_records_an_unsweepable_remainder_as_dust() {
    let mut initial = state();
    initial.step = IcpswapStep::Forward;
    initial.withdraw.wallet_credited_amount = Some(Nat::from(100u64));
    initial.settlement.kind = Some(IcpswapSettlementKind::Output);
    let store = Arc::new(Store::new(initial));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    // The expected total debit is 100. Observing 96 leaves only four units,
    // which cannot pay the output ledger's five-unit forwarding fee.
    for value in [1_000u64, 20u64, 904u64, 500u64] {
        client
            .expect_icrc1_balance()
            .times(1)
            .in_sequence(&mut balances)
            .return_once(move |_, _| Ok(Nat::from(value)));
    }
    client
        .expect_icrc1_transfer_with_args()
        .times(1)
        .return_once(|_, _| Err(IcrcTransferError::TooOld));

    let mut current = store.state();
    forward_step(&client, store.as_ref(), "run", &mut current, 2_000)
        .await
        .expect("unsweepable settlement remainder is recorded without parking");

    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::Refunded);
    assert_eq!(persisted.settlement.recovery_credit, Some(Nat::from(4u64)));
    assert_eq!(persisted.settlement.residual_dust, Some(Nat::from(4u64)));
    assert_eq!(persisted.operator_pending_step, None);
}
