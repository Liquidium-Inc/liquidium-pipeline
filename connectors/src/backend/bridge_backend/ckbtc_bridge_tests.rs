//! The ckBTC minter bridge: what it checks before the burn, how it reads the
//! minter's answers, and how a lost answer is found again.
use std::sync::Arc;

use candid::{Nat, Principal};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc3::{
        archive::{ArchivedRange, QueryTxArchiveFn},
        transactions::{Burn, GetTransactionsRequest, GetTransactionsResponse, Transaction, TransactionRange},
    },
};

use super::{BridgeBackend, BridgeDestination, BridgeError, BridgeRequest, BridgeStatus, CkBtcBridgeBackend};
use crate::{
    backend::{
        ckbtc_minter::{
            CkBtcMinterInfo, ReimbursedDeposit, ReimbursementReason, ReimbursementRequest, RetrieveBtcOk,
            RetrieveBtcStatusEntry, RetrieveBtcStatusV2, RetrieveBtcWithApprovalArgs, RetrieveBtcWithApprovalError,
            RetrieveBtcWithApprovalRet, WithdrawalFee,
        },
        icp_backend::MockIcpBackend,
    },
    pipeline_agent::MockPipelineAgent,
};

const OWNER: &str = "aaaaa-aa";
const MINTER: &str = "mqygn-kiaaa-aaaar-qaadq-cai";
const LEDGER: &str = "mxzaz-hqaaa-aaaar-qaada-cai";
const MAINNET_ADDRESS: &str = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq";
const TESTNET_ADDRESS: &str = "tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx";

fn owner() -> Principal {
    Principal::from_text(OWNER).unwrap()
}

fn source() -> Account {
    Account {
        owner: owner(),
        subaccount: None,
    }
}

fn request(amount: f64) -> BridgeRequest {
    BridgeRequest {
        asset: "ckBTC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: OWNER.to_string(),
        target_asset: "BTC".to_string(),
        destination: BridgeDestination::BtcAddress(MAINNET_ADDRESS.to_string()),
        amount,
        provider_fee_budget_native_units: None,
    }
}

fn backend(agent: MockPipelineAgent, icp: MockIcpBackend) -> CkBtcBridgeBackend<MockPipelineAgent, MockIcpBackend> {
    CkBtcBridgeBackend::new(
        Arc::new(agent),
        Arc::new(icp),
        Principal::from_text(MINTER).unwrap(),
        Principal::from_text(LEDGER).unwrap(),
        owner(),
    )
}

fn minter_info(agent: &mut MockPipelineAgent, minimum: u64) {
    agent
        .expect_call_query::<CkBtcMinterInfo>()
        .returning(move |_, method, _| {
            assert_eq!(method, "get_minter_info");
            Ok(CkBtcMinterInfo {
                retrieve_btc_min_amount: minimum,
            })
        });
}

/// A funded bridge account whose allowance to the minter is `allowance`.
fn funded_account(icp: &mut MockIcpBackend, balance: u64, allowance: u64) {
    icp.expect_icrc1_fee().returning(|_| Ok(Nat::from(10u64)));
    icp.expect_icrc1_balance().returning(move |_, _| Ok(Nat::from(balance)));
    icp.expect_icrc2_allowance()
        .returning(move |_, _, _| Ok(Nat::from(allowance)));
}

#[tokio::test]
async fn a_retrieval_approves_the_minter_and_returns_the_burn_index() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 50_000);
    agent
        .expect_call_update::<RetrieveBtcWithApprovalRet>()
        .times(1)
        .returning(|_, method, arg| {
            assert_eq!(method, "retrieve_btc_with_approval");
            let args: RetrieveBtcWithApprovalArgs = candid::decode_one(&arg).unwrap();
            assert_eq!(args.address, MAINNET_ADDRESS);
            assert_eq!(args.amount, 100_000);
            assert!(args.from_subaccount.is_none());
            Ok(RetrieveBtcWithApprovalRet::Ok(RetrieveBtcOk { block_index: 77 }))
        });
    let mut icp = MockIcpBackend::new();
    funded_account(&mut icp, 100_010, 0);
    icp.expect_icrc2_approve().times(1).returning(|_, args| {
        assert_eq!(args.spender.owner, Principal::from_text(MINTER).unwrap());
        assert_eq!(args.amount, Nat::from(100_000u64));
        assert_eq!(args.expected_allowance, Some(Nat::from(0u8)));
        Ok(Nat::from(3u8))
    });

    let submission = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap();

    assert_eq!(submission.bridge_id, "ckbtc-retrieve:77");
}

#[tokio::test]
async fn an_allowance_that_already_covers_the_burn_is_not_set_again() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 50_000);
    agent
        .expect_call_update::<RetrieveBtcWithApprovalRet>()
        .times(1)
        .returning(|_, _, _| Ok(RetrieveBtcWithApprovalRet::Ok(RetrieveBtcOk { block_index: 78 })));
    let mut icp = MockIcpBackend::new();
    funded_account(&mut icp, 100_010, 100_000);
    icp.expect_icrc2_approve().times(0);

    let submission = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap();

    assert_eq!(submission.bridge_id, "ckbtc-retrieve:78");
}

#[tokio::test]
async fn an_amount_under_the_minter_minimum_is_refused_before_the_approve() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 500_000);
    agent.expect_call_update::<RetrieveBtcWithApprovalRet>().times(0);
    let mut icp = MockIcpBackend::new();
    icp.expect_icrc2_approve().times(0);

    let error = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap_err();

    assert!(matches!(error, BridgeError::BelowMinimum(_)), "{error:?}");
}

#[tokio::test]
async fn a_short_balance_is_refused_before_the_approve() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 50_000);
    let mut icp = MockIcpBackend::new();
    // The burn plus the approve fee is one unit more than the balance.
    funded_account(&mut icp, 100_009, 0);
    icp.expect_icrc2_approve().times(0);

    let error = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap_err();

    assert!(matches!(error, BridgeError::Refused(_)), "{error:?}");
    assert!(
        error.to_string().contains("below the burn and approve budget"),
        "{error}"
    );
}

/// Nothing has burned before the minter call, so a failed read is a refusal
/// and the next cycle sends a new request.
#[tokio::test]
async fn a_failed_read_before_the_burn_is_a_refusal() {
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<CkBtcMinterInfo>()
        .returning(|_, _, _| Err("replica timeout".to_string()));
    agent.expect_call_update::<RetrieveBtcWithApprovalRet>().times(0);

    let error = backend(agent, MockIcpBackend::new())
        .submit_bridge(request(0.001))
        .await
        .unwrap_err();

    assert!(matches!(error, BridgeError::Refused(_)), "{error:?}");
}

#[tokio::test]
async fn the_destination_must_be_a_mainnet_bitcoin_address() {
    let mut agent = MockPipelineAgent::new();
    agent.expect_call_query::<CkBtcMinterInfo>().times(0);
    let mut request = request(0.001);
    request.destination = BridgeDestination::BtcAddress(TESTNET_ADDRESS.to_string());

    let error = backend(agent, MockIcpBackend::new())
        .submit_bridge(request)
        .await
        .unwrap_err();

    assert!(matches!(error, BridgeError::Refused(_)), "{error:?}");
    assert!(error.to_string().contains("not mainnet"), "{error}");
}

/// Each minter refusal says whether the burn happened. A ledger call the
/// minter could not complete may have burned, so it stays ambiguous.
#[tokio::test]
async fn minter_refusals_keep_whether_the_burn_happened() {
    let cases = [
        (RetrieveBtcWithApprovalError::AlreadyProcessing, "deferred"),
        (RetrieveBtcWithApprovalError::AmountTooLow(50_000), "below minimum"),
        (RetrieveBtcWithApprovalError::MalformedAddress("bad".into()), "refused"),
        (
            RetrieveBtcWithApprovalError::InsufficientAllowance { allowance: 1 },
            "refused",
        ),
        (
            RetrieveBtcWithApprovalError::InsufficientFunds { balance: 1 },
            "refused",
        ),
        (
            RetrieveBtcWithApprovalError::TemporarilyUnavailable("ledger call failed".into()),
            "ambiguous",
        ),
        (
            RetrieveBtcWithApprovalError::GenericError {
                error_message: "unexpected".into(),
                error_code: 1,
            },
            "ambiguous",
        ),
    ];
    for (minter_error, expected) in cases {
        let mut agent = MockPipelineAgent::new();
        minter_info(&mut agent, 50_000);
        let returned = minter_error.clone();
        agent
            .expect_call_update::<RetrieveBtcWithApprovalRet>()
            .returning(move |_, _, _| Ok(RetrieveBtcWithApprovalRet::Err(returned.clone())));
        let mut icp = MockIcpBackend::new();
        funded_account(&mut icp, 1_000_000, 1_000_000);

        let error = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap_err();

        let kind = match error {
            BridgeError::Deferred(_) => "deferred",
            BridgeError::BelowMinimum(_) => "below minimum",
            BridgeError::Refused(_) => "refused",
            BridgeError::Ambiguous(_) => "ambiguous",
            other => panic!("{minter_error:?} mapped to {other:?}"),
        };
        assert_eq!(kind, expected, "{minter_error:?}");
    }
}

/// A call without an answer may have burned, so it stays open for the lookup.
#[tokio::test]
async fn a_failed_minter_call_is_left_open() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 50_000);
    agent
        .expect_call_update::<RetrieveBtcWithApprovalRet>()
        .returning(|_, _, _| Err("replica timeout".to_string()));
    let mut icp = MockIcpBackend::new();
    funded_account(&mut icp, 1_000_000, 1_000_000);

    let error = backend(agent, icp).submit_bridge(request(0.001)).await.unwrap_err();

    assert!(matches!(error, BridgeError::Other(_)), "{error:?}");
}

fn status_entries(agent: &mut MockPipelineAgent, certified: bool, indexes: Vec<u64>) {
    let entries: Vec<RetrieveBtcStatusEntry> = indexes
        .into_iter()
        .map(|block_index| RetrieveBtcStatusEntry {
            block_index,
            status_v2: Some(RetrieveBtcStatusV2::Pending),
        })
        .collect();
    let answer = move |_: &Principal, method: &str, _: Vec<u8>| {
        assert_eq!(method, "retrieve_btc_status_v2_by_account");
        Ok(entries.clone())
    };
    if certified {
        agent
            .expect_call_update::<Vec<RetrieveBtcStatusEntry>>()
            .times(1)
            .returning(answer);
    } else {
        agent
            .expect_call_query::<Vec<RetrieveBtcStatusEntry>>()
            .times(1)
            .returning(answer);
    }
}

fn burn(from: Account, amount: u64) -> Transaction {
    Transaction {
        kind: "burn".to_string(),
        mint: None,
        burn: Some(Burn {
            from,
            amount: Nat::from(amount),
            spender: None,
            memo: None,
            created_at_time: None,
        }),
        transfer: None,
        approve: None,
        timestamp: 1,
    }
}

fn block_start(arg: &[u8]) -> u64 {
    let req: GetTransactionsRequest = candid::decode_one(arg).unwrap();
    req.start.0.try_into().unwrap()
}

#[tokio::test]
async fn the_history_position_is_the_highest_burn_index_of_the_account() {
    let mut agent = MockPipelineAgent::new();
    status_entries(&mut agent, false, vec![3, 9, 5]);

    let cursor = backend(agent, MockIcpBackend::new())
        .prepare_submission(&request(0.001))
        .await
        .unwrap();

    assert_eq!(cursor, Some(9));
}

#[tokio::test]
async fn an_account_without_retrievals_starts_at_zero() {
    let mut agent = MockPipelineAgent::new();
    status_entries(&mut agent, false, Vec::new());

    let cursor = backend(agent, MockIcpBackend::new())
        .prepare_submission(&request(0.001))
        .await
        .unwrap();

    assert_eq!(cursor, Some(0));
}

/// The minter names only the burn index, so the ledger burn proves which
/// retrieval is this request: the same account and the same amount.
#[tokio::test]
async fn a_lost_answer_is_found_from_the_certified_history_and_the_ledger_burn() {
    let mut agent = MockPipelineAgent::new();
    status_entries(&mut agent, true, vec![3, 5, 9, 12, 14]);
    let other = Account {
        owner: Principal::management_canister(),
        subaccount: Some([1u8; 32]),
    };
    agent
        .expect_call_query::<GetTransactionsResponse>()
        .times(3)
        .returning(move |_, method, arg| {
            assert_eq!(method, "get_transactions");
            let start = block_start(&arg);
            let transaction = match start {
                9 => burn(source(), 100_000),
                12 => burn(source(), 99_999),
                14 => burn(other, 100_000),
                _ => panic!("unexpected block {start}"),
            };
            Ok(GetTransactionsResponse {
                first_index: Nat::from(start),
                log_length: Nat::from(20u8),
                transactions: vec![transaction],
                archived_transactions: Vec::new(),
            })
        });

    // Index 5 is the cursor itself and index 3 is claimed; neither is read.
    let candidates = backend(agent, MockIcpBackend::new())
        .find_submissions(&request(0.001), 5, &["ckbtc-retrieve:3".to_string()])
        .await
        .unwrap();

    let ids: Vec<&str> = candidates.iter().map(|c| c.bridge_id.as_str()).collect();
    assert_eq!(ids, vec!["ckbtc-retrieve:9"]);
    assert_eq!(candidates[0].sequence, 9);
}

/// An old burn lives in the ledger's archive. It is still the proof.
#[tokio::test]
async fn an_archived_burn_is_read_from_the_archive() {
    let mut agent = MockPipelineAgent::new();
    status_entries(&mut agent, true, vec![9]);
    let archive = Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap();
    agent
        .expect_call_query::<GetTransactionsResponse>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(GetTransactionsResponse {
                first_index: Nat::from(500u64),
                log_length: Nat::from(600u64),
                transactions: Vec::new(),
                archived_transactions: vec![ArchivedRange {
                    start: Nat::from(9u64),
                    length: Nat::from(1u8),
                    callback: QueryTxArchiveFn::new(archive, "get_transactions"),
                }],
            })
        });
    agent
        .expect_call_query::<TransactionRange>()
        .times(1)
        .returning(move |canister, _, arg| {
            assert_eq!(*canister, archive);
            assert_eq!(block_start(&arg), 9);
            Ok(TransactionRange {
                transactions: vec![burn(source(), 100_000)],
            })
        });

    let candidates = backend(agent, MockIcpBackend::new())
        .find_submissions(&request(0.001), 0, &[])
        .await
        .unwrap();

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].bridge_id, "ckbtc-retrieve:9");
}

/// A ledger that cannot be read leaves the lookup undecided; nothing is sent again.
#[tokio::test]
async fn a_failed_ledger_read_fails_the_lookup() {
    let mut agent = MockPipelineAgent::new();
    status_entries(&mut agent, true, vec![9]);
    agent
        .expect_call_query::<GetTransactionsResponse>()
        .returning(|_, _, _| Err("ledger unavailable".to_string()));

    let error = backend(agent, MockIcpBackend::new())
        .find_submissions(&request(0.001), 0, &[])
        .await
        .unwrap_err();

    assert!(error.contains("ledger unavailable"), "{error}");
}

#[tokio::test]
async fn the_lookup_refuses_another_source_account() {
    let mut request = request(0.001);
    request.source_address = Principal::anonymous().to_text();

    let error = backend(MockPipelineAgent::new(), MockIcpBackend::new())
        .find_submissions(&request, 0, &[])
        .await
        .unwrap_err();

    assert!(error.contains("does not match configured bridge owner"), "{error}");
}

/// Only a confirmed Bitcoin transaction completes. A reimbursement that is
/// still under way holds the funds, so it waits like any other state.
#[tokio::test]
async fn the_status_maps_every_minter_state() {
    let reimbursement = ReimbursementRequest {
        account: source(),
        amount: 100_000,
        reason: ReimbursementReason::CallFailed,
    };
    let cases = vec![
        (RetrieveBtcStatusV2::Pending, BridgeStatus::Pending),
        (RetrieveBtcStatusV2::Signing, BridgeStatus::Pending),
        (RetrieveBtcStatusV2::Sending { txid: vec![1] }, BridgeStatus::Pending),
        (RetrieveBtcStatusV2::Submitted { txid: vec![1] }, BridgeStatus::Pending),
        (RetrieveBtcStatusV2::AmountTooLow, BridgeStatus::Pending),
        (RetrieveBtcStatusV2::WillReimburse(reimbursement), BridgeStatus::Pending),
        (
            RetrieveBtcStatusV2::Confirmed { txid: vec![1] },
            BridgeStatus::Completed,
        ),
        (RetrieveBtcStatusV2::Unknown, BridgeStatus::Unknown),
    ];
    for (minter_status, expected) in cases {
        let mut agent = MockPipelineAgent::new();
        let returned = minter_status.clone();
        agent
            .expect_call_query::<RetrieveBtcStatusV2>()
            .returning(move |_, method, _| {
                assert_eq!(method, "retrieve_btc_status_v2");
                Ok(returned.clone())
            });
        let status = backend(agent, MockIcpBackend::new())
            .get_bridge_status("ckbtc-retrieve:77")
            .await
            .unwrap();
        assert_eq!(status, expected, "{minter_status:?}");
    }

    // The ckBTC is back on the account, so the retrieval is over.
    let mut agent = MockPipelineAgent::new();
    agent.expect_call_query::<RetrieveBtcStatusV2>().returning(|_, _, _| {
        Ok(RetrieveBtcStatusV2::Reimbursed(ReimbursedDeposit {
            account: source(),
            mint_block_index: 81,
            amount: 100_000,
            reason: ReimbursementReason::CallFailed,
        }))
    });
    let status = backend(agent, MockIcpBackend::new())
        .get_bridge_status("ckbtc-retrieve:77")
        .await
        .unwrap();
    match status {
        BridgeStatus::Canceled { reason } => assert!(reason.unwrap().contains("block 81")),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn an_unreadable_status_says_so() {
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_query::<RetrieveBtcStatusV2>()
        .returning(|_, _, _| Err("replica unavailable".to_string()));

    let error = backend(agent, MockIcpBackend::new())
        .get_bridge_status("ckbtc-retrieve:77")
        .await
        .unwrap_err();

    assert!(matches!(error, BridgeError::Unreadable(_)), "{error:?}");
}

#[tokio::test]
async fn the_fee_budget_and_the_minimum_come_from_the_minter() {
    let mut agent = MockPipelineAgent::new();
    minter_info(&mut agent, 50_000);
    agent.expect_call_query::<WithdrawalFee>().returning(|_, method, _| {
        assert_eq!(method, "estimate_withdrawal_fee");
        Ok(WithdrawalFee {
            minter_fee: 10,
            bitcoin_fee: 1_490,
        })
    });
    let mut icp = MockIcpBackend::new();
    icp.expect_icrc1_fee().returning(|_| Ok(Nat::from(10u64)));
    let backend = backend(agent, icp);

    let budget = backend.get_fee_budget("ckBTC", "ICP", "BTC").await.unwrap();
    assert_eq!(budget.source_fee_budget, 0.000_000_1);
    assert_eq!(budget.destination_fee_budget, 0.000_015);
    assert!(budget.side_costs.is_empty());

    let minimum = backend.get_minimum_bridge_amount("ckBTC", "ICP", "BTC").await.unwrap();
    assert_eq!(minimum, 0.000_5);

    // Other routes are not this backend's business.
    let none = backend.get_fee_budget("ckUSDC", "ICP", "USDC").await.unwrap();
    assert_eq!(none.destination_fee_budget, 0.0);
    assert_eq!(
        backend.get_minimum_bridge_amount("BTC", "BTC", "ckBTC").await.unwrap(),
        0.0
    );
}

#[tokio::test]
async fn the_source_balance_is_the_ledger_balance() {
    let mut icp = MockIcpBackend::new();
    icp.expect_icrc1_balance().returning(|_, account| {
        assert_eq!(*account, source());
        Ok(Nat::from(250_000u64))
    });
    let backend = backend(MockPipelineAgent::new(), icp);

    assert_eq!(
        backend.get_source_balance("ckBTC", "ICP", OWNER).await.unwrap(),
        0.002_5
    );
    assert!(backend.get_source_balance("ckUSDC", "ICP", OWNER).await.is_err());
}
