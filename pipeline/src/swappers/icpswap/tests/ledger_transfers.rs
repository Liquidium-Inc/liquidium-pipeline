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
        IcpswapFundingState {
            source: account(4),
            destination: child,
            fee: plan.input_ledger_fee.clone(),
            transfer: IcpswapLedgerTransferState {
                args: Some(TransferArg {
                    from_subaccount: None,
                    to: child,
                    amount: Nat::from(100_020u64),
                    fee: Some(Nat::from(10u64)),
                    memo: None,
                    created_at_time: None,
                }),
                ..Default::default()
            },
        },
        IcpswapSettlementState {
            kind: None,
            destination: account(5),
            fee: plan.output_ledger_fee.clone(),
            transfer: IcpswapLedgerTransferState::default(),
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
    for value in [1_000_000u64, 7u64] {
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
async fn too_old_unchanged_funding_transfer_is_safely_prepared_again() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    for value in [1_000_000u64, 7u64, 1_000_000u64, 7u64] {
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
async fn inconsistent_aged_transfer_deltas_require_an_operator() {
    let store = Arc::new(Store::new(state()));
    let mut client = MockIcpBackend::new();
    let mut balances = Sequence::new();
    for value in [1_000_000u64, 7u64, 999_000u64, 8u64] {
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
    let error = funding_step(&client, store.as_ref(), "run", &mut current, 1_000)
        .await
        .expect_err("inconsistent movement is ambiguous");

    assert!(error.contains("inconsistent balance deltas"));
    let persisted = store.state();
    assert_eq!(persisted.step, IcpswapStep::OperatorRequired);
    assert_eq!(persisted.operator_pending_step, Some(IcpswapStep::FundingPending));
}
