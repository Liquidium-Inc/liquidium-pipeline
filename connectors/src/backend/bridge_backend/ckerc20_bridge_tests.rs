use alloy::primitives::{Address, TxHash, U256};
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};
use mockall::predicate::eq;
use std::sync::Arc;

use crate::backend::{
    bridge_backend::types::{RetrieveErc20Request, RetrieveEthRequest},
    bridge_backend::{BridgeBackend, BridgeDestination, BridgeRequest},
    icp_backend::MockIcpBackend,
};
use crate::pipeline_agent::MockPipelineAgent;

use super::{
    CkErc20BridgeBackend, CkEthMinterInfo, Eip1559TransactionPrice, FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX,
    MIN_ETH_FORWARD_GAS_RESERVE_WEI, MockBridgeEvmBackend, WithdrawErc20Ret, WithdrawalRet, destination_to_bytes32,
    ensure_source_matches_bridge_owner, ensure_source_matches_signer, eth_forward_gas_reserve_from_price,
    parse_source_icp_account, resolve_cketh_route_for_request,
};

#[test]
fn destination_account_encodes_principal_and_default_subaccount() {
    let account = Account {
        owner: Principal::management_canister(),
        subaccount: None,
    };
    let (principal, subaccount) = destination_to_bytes32(&account);
    let expected_principal = principal_to_subaccount(Principal::management_canister());
    assert_eq!(principal.as_slice(), expected_principal);
    assert_eq!(subaccount.as_slice(), [0u8; 32]);
}

#[test]
fn destination_account_encodes_explicit_subaccount() {
    let account = Account {
        owner: Principal::management_canister(),
        subaccount: Some([7u8; 32]),
    };
    let (principal, subaccount) = destination_to_bytes32(&account);
    let expected_principal = principal_to_subaccount(account.owner);
    assert_eq!(principal.as_slice(), expected_principal);
    assert_eq!(subaccount.as_slice(), [7u8; 32]);
}

#[test]
fn validate_source_matches_signer() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    ensure_source_matches_signer("0x1111111111111111111111111111111111111111", signer).expect("must pass");
    let err =
        ensure_source_matches_signer("0x2222222222222222222222222222222222222222", signer).expect_err("must fail");
    assert!(err.contains("does not match signer"));
}

#[test]
fn route_validation_enforces_icp_destination_for_forward_route() {
    let request = crate::backend::bridge_backend::BridgeRequest {
        asset: "USDC".to_string(),
        source_chain: "ETH".to_string(),
        source_address: "0x1111111111111111111111111111111111111111".to_string(),
        target_asset: "ckUSDC".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };
    resolve_cketh_route_for_request(&request).expect("route must validate");

    let mut bad_destination = request;
    bad_destination.destination = BridgeDestination::EvmAddress(
        "0x1111111111111111111111111111111111111111"
            .parse::<Address>()
            .expect("address"),
    );
    let err = resolve_cketh_route_for_request(&bad_destination).expect_err("must fail");
    assert!(err.contains("invalid destination type"));
}

#[test]
fn eth_forward_gas_reserve_uses_floor_when_gas_is_cheap() {
    let reserve = eth_forward_gas_reserve_from_price(1_000_000_000, 150_000);
    assert_eq!(reserve, U256::from(MIN_ETH_FORWARD_GAS_RESERVE_WEI));
}

#[test]
fn eth_forward_gas_reserve_scales_when_gas_is_expensive() {
    let reserve = eth_forward_gas_reserve_from_price(100_000_000_000, 150_000);
    assert_eq!(reserve, U256::from(30_000_000_000_000_000u128));
}

#[test]
fn route_validation_rejects_non_forward_route_kind() {
    let request = crate::backend::bridge_backend::BridgeRequest {
        asset: "ckBTC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: "0x1111111111111111111111111111111111111111".to_string(),
        target_asset: "BTC".to_string(),
        destination: BridgeDestination::BtcAddress("1BoatSLRHtKNngkdXEeobR76b53LETtpyT".to_string()),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };
    let err = resolve_cketh_route_for_request(&request).expect_err("must fail");
    assert!(err.contains("not supported by CkErc20BridgeBackend"));
}

#[test]
fn route_validation_enforces_evm_destination_for_reverse_route() {
    let request = crate::backend::bridge_backend::BridgeRequest {
        asset: "ckUSDC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: Principal::management_canister().to_text(),
        target_asset: "USDC".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };
    resolve_cketh_route_for_request(&request).expect("route must validate");

    let mut bad_destination = request;
    bad_destination.destination = BridgeDestination::IcpAccount(Account {
        owner: Principal::management_canister(),
        subaccount: None,
    });
    let err = resolve_cketh_route_for_request(&bad_destination).expect_err("must fail");
    assert!(err.contains("invalid destination type"));
}

#[test]
fn parse_source_icp_account_accepts_principal_and_account() {
    let principal_text = Principal::management_canister().to_text();
    let account_from_principal = parse_source_icp_account(&principal_text).expect("principal parse");
    assert_eq!(account_from_principal.owner, Principal::management_canister());
    assert!(account_from_principal.subaccount.is_none());

    let account_text = Account {
        owner: Principal::management_canister(),
        subaccount: Some([9u8; 32]),
    }
    .to_string();
    let account_from_account = parse_source_icp_account(&account_text).expect("account parse");
    assert_eq!(account_from_account.owner, Principal::management_canister());
    assert_eq!(account_from_account.subaccount, Some([9u8; 32]));
}

#[test]
fn reverse_source_must_match_bridge_owner_and_be_owner_only() {
    let source = Account {
        owner: Principal::management_canister(),
        subaccount: None,
    };
    ensure_source_matches_bridge_owner(&source, Principal::management_canister()).expect("should pass");

    let wrong_owner = Account {
        owner: Principal::anonymous(),
        subaccount: None,
    };
    let err =
        ensure_source_matches_bridge_owner(&wrong_owner, Principal::management_canister()).expect_err("must fail");
    assert!(err.contains("does not match configured bridge owner"));

    let with_subaccount = Account {
        owner: Principal::management_canister(),
        subaccount: Some([1u8; 32]),
    };
    let err =
        ensure_source_matches_bridge_owner(&with_subaccount, Principal::management_canister()).expect_err("must fail");
    assert!(err.contains("subaccount must be None"));
}

#[tokio::test]
async fn forward_bridge_rejects_subaccount_destination_when_native_helper_is_resolved() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let native_helper = "0x2222222222222222222222222222222222222222".to_string();

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: Some(native_helper.clone()),
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm.expect_erc20_decimals_of().times(0);
    mock_evm.expect_erc20_approve_and_wait().times(0);
    mock_evm.expect_helper_deposit_native().times(0);
    mock_evm.expect_helper_deposit_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let mock_icp = MockIcpBackend::new();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "USDC".to_string(),
        source_chain: "ETH".to_string(),
        source_address: "0x1111111111111111111111111111111111111111".to_string(),
        target_asset: "ckUSDC".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: Some([7u8; 32]),
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let err = backend.submit_bridge(request).await.expect_err("must fail");
    assert!(err.contains("does not support subaccount destinations"));
}

#[tokio::test]
async fn forward_bridge_skips_approve_when_allowance_is_sufficient() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222"
        .parse::<Address>()
        .expect("address");
    let token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        .parse::<Address>()
        .expect("address");
    let amount_base_units = U256::from(1_000_000u64);
    let tx_hash = TxHash::from([0x11u8; 32]);

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: Some(helper.to_string()),
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_erc20_decimals_of()
        .with(eq(token))
        .times(1)
        .returning(|_| Ok(6));
    mock_evm
        .expect_erc20_balance_of()
        .with(eq(token), eq(signer))
        .times(1)
        .returning(|_, _| Ok(U256::from(1_000_000u64)));
    mock_evm
        .expect_erc20_allowance_of()
        .with(eq(token), eq(signer), eq(helper))
        .times(1)
        .returning(|_, _, _| Ok(U256::from(1_000_000u64)));
    mock_evm.expect_erc20_approve_and_wait().times(0);
    mock_evm
        .expect_helper_deposit_native()
        .withf(move |seen_helper, seen_token, seen_amount, _| {
            *seen_helper == helper && *seen_token == token && *seen_amount == amount_base_units
        })
        .times(1)
        .returning(move |_, _, _, _| Ok(tx_hash));
    mock_evm.expect_helper_deposit_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let mock_icp = MockIcpBackend::new();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "USDC".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckUSDC".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, format!("{:#x}", tx_hash));
}

#[tokio::test]
async fn forward_bridge_approves_when_allowance_is_insufficient() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222"
        .parse::<Address>()
        .expect("address");
    let token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        .parse::<Address>()
        .expect("address");
    let amount_base_units = U256::from(1_000_000u64);
    let tx_hash = TxHash::from([0x22u8; 32]);

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: Some(helper.to_string()),
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_erc20_decimals_of()
        .with(eq(token))
        .times(1)
        .returning(|_| Ok(6));
    mock_evm
        .expect_erc20_balance_of()
        .with(eq(token), eq(signer))
        .times(1)
        .returning(|_, _| Ok(U256::from(1_000_000u64)));
    mock_evm
        .expect_erc20_allowance_of()
        .with(eq(token), eq(signer), eq(helper))
        .times(1)
        .returning(|_, _, _| Ok(U256::from(999_999u64)));
    mock_evm
        .expect_erc20_approve_and_wait()
        .with(eq(token), eq(helper), eq(amount_base_units))
        .times(1)
        .returning(|_, _, _| Ok(TxHash::from([0x33u8; 32])));
    mock_evm
        .expect_helper_deposit_native()
        .withf(move |seen_helper, seen_token, seen_amount, _| {
            *seen_helper == helper && *seen_token == token && *seen_amount == amount_base_units
        })
        .times(1)
        .returning(move |_, _, _, _| Ok(tx_hash));
    mock_evm.expect_helper_deposit_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let mock_icp = MockIcpBackend::new();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "USDC".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckUSDC".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, format!("{:#x}", tx_hash));
}

#[tokio::test]
async fn forward_bridge_fails_preflight_when_balance_is_insufficient() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222"
        .parse::<Address>()
        .expect("address");
    let token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        .parse::<Address>()
        .expect("address");

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: Some(helper.to_string()),
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_erc20_decimals_of()
        .with(eq(token))
        .times(1)
        .returning(|_| Ok(6));
    mock_evm
        .expect_erc20_balance_of()
        .with(eq(token), eq(signer))
        .times(1)
        .returning(|_, _| Ok(U256::from(999_999u64)));
    mock_evm.expect_erc20_allowance_of().times(0);
    mock_evm.expect_erc20_approve_and_wait().times(0);
    mock_evm.expect_helper_deposit_native().times(0);
    mock_evm.expect_helper_deposit_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let mock_icp = MockIcpBackend::new();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "USDC".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckUSDC".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let err = backend
        .submit_bridge(request)
        .await
        .expect_err("bridge must fail preflight");
    assert!(err.contains("bridge amount preflight failed"));
    assert!(err.contains("available=999999"));
    assert!(err.contains("required=1000000"));
}

#[tokio::test]
async fn native_forward_bridge_uses_deposit_eth_with_subaccount_helper() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222"
        .parse::<Address>()
        .expect("address");
    let amount_wei = U256::from(1_000_000_000_000_000_000u128);
    let gas_reserve = U256::from(12_000_000_000_000_000u128);
    let tx_hash = TxHash::from([0x44u8; 32]);

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: Some(helper.to_string()),
                eth_helper_contract_address: None,
                erc20_helper_contract_address: None,
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(move |_| Ok(gas_reserve));
    mock_evm
        .expect_native_balance_of()
        .with(eq(signer))
        .times(1)
        .returning(move |_| Ok(amount_wei + gas_reserve));
    mock_evm
        .expect_helper_deposit_eth_with_subaccount()
        .withf(move |seen_helper, seen_amount, _principal, subaccount| {
            *seen_helper == helper && *seen_amount == amount_wei && subaccount.as_slice() == [7u8; 32]
        })
        .times(1)
        .returning(move |_, _, _, _| Ok(tx_hash));
    mock_evm.expect_helper_deposit_eth_native().times(0);
    mock_evm.expect_erc20_decimals_of().times(0);
    mock_evm.expect_erc20_balance_of().times(0);
    mock_evm.expect_erc20_allowance_of().times(0);
    mock_evm.expect_erc20_approve_and_wait().times(0);
    mock_evm.expect_helper_deposit_native().times(0);
    mock_evm.expect_helper_deposit_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "ETH".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckETH".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: Some([7u8; 32]),
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, format!("{:#x}", tx_hash));
}

#[tokio::test]
async fn native_forward_bridge_falls_back_to_legacy_eth_helper_for_default_subaccount() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222"
        .parse::<Address>()
        .expect("address");
    let amount_wei = U256::from(1_000_000_000_000_000_000u128);
    let gas_reserve = U256::from(MIN_ETH_FORWARD_GAS_RESERVE_WEI);
    let tx_hash = TxHash::from([0x45u8; 32]);

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: Some(helper.to_string()),
                erc20_helper_contract_address: None,
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(move |_| Ok(gas_reserve));
    mock_evm
        .expect_native_balance_of()
        .with(eq(signer))
        .times(1)
        .returning(move |_| Ok(amount_wei + gas_reserve));
    mock_evm
        .expect_helper_deposit_eth_native()
        .withf(move |seen_helper, seen_amount, _principal| *seen_helper == helper && *seen_amount == amount_wei)
        .times(1)
        .returning(move |_, _, _| Ok(tx_hash));
    mock_evm.expect_helper_deposit_eth_with_subaccount().times(0);
    mock_evm.expect_receipt_status().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "ETH".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckETH".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, format!("{:#x}", tx_hash));
}

#[tokio::test]
async fn native_forward_bridge_rejects_insufficient_eth_after_gas_reserve() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222".to_string();
    let gas_reserve = U256::from(15_000_000_000_000_000u128);

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: Some(helper.clone()),
                eth_helper_contract_address: None,
                erc20_helper_contract_address: None,
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(move |_| Ok(gas_reserve));
    mock_evm
        .expect_native_balance_of()
        .with(eq(signer))
        .times(1)
        .returning(move |_| Ok(U256::from(1_000_000_000_000_000_000u128) + gas_reserve - U256::from(1u8)));
    mock_evm.expect_helper_deposit_eth_native().times(0);
    mock_evm.expect_helper_deposit_eth_with_subaccount().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "ETH".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckETH".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let err = backend
        .submit_bridge(request)
        .await
        .expect_err("bridge must fail preflight");
    assert!(err.contains("ETH balance is below requested deposit amount plus gas reserve"));
}

#[tokio::test]
async fn native_forward_bridge_propagates_dynamic_gas_reserve_errors() {
    let signer = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let helper = "0x2222222222222222222222222222222222222222".to_string();

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: Some(helper.clone()),
                eth_helper_contract_address: None,
                erc20_helper_contract_address: None,
                cketh_ledger_id: None,
                minimum_withdrawal_amount: None,
            })
        });

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm.expect_signer_address().times(1).returning(move || signer);
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(|_| Err("native gas price fetch failed: rpc unavailable".to_string()));
    mock_evm.expect_native_balance_of().times(0);
    mock_evm.expect_helper_deposit_eth_native().times(0);
    mock_evm.expect_helper_deposit_eth_with_subaccount().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let request = BridgeRequest {
        asset: "ETH".to_string(),
        source_chain: "ETH".to_string(),
        source_address: signer.to_string(),
        target_asset: "ckETH".to_string(),
        destination: BridgeDestination::IcpAccount(Account {
            owner: Principal::management_canister(),
            subaccount: None,
        }),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let err = backend
        .submit_bridge(request)
        .await
        .expect_err("gas reserve failure should stop native forward bridge");
    assert!(err.contains("native gas price fetch failed"));
}

#[tokio::test]
async fn get_source_balance_eth_forward_subtracts_dynamic_gas_reserve() {
    let owner = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");
    let gas_reserve = U256::from(30_000_000_000_000_000u128);

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm
        .expect_native_balance_of()
        .with(eq(owner))
        .times(1)
        .returning(move |_| Ok(U256::from(1_000_000_000_000_000_000u128) + gas_reserve));
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(move |_| Ok(gas_reserve));

    let backend = CkErc20BridgeBackend::new(
        Arc::new(MockPipelineAgent::new()),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let bridgeable = backend
        .get_source_balance("ETH", "ETH", &owner.to_string())
        .await
        .expect("source balance should resolve");
    assert!((bridgeable - 1.0).abs() < 1e-18);
}

#[tokio::test]
async fn get_source_balance_eth_forward_propagates_dynamic_gas_reserve_errors() {
    let owner = "0x1111111111111111111111111111111111111111"
        .parse::<Address>()
        .expect("address");

    let mut mock_evm = MockBridgeEvmBackend::new();
    mock_evm
        .expect_native_balance_of()
        .with(eq(owner))
        .times(1)
        .returning(|_| Ok(U256::from(1_000_000_000_000_000_000u128)));
    mock_evm
        .expect_native_tx_gas_reserve()
        .with(eq(150_000u128))
        .times(1)
        .returning(|_| Err("native gas price fetch failed: rpc unavailable".to_string()));

    let backend = CkErc20BridgeBackend::new(
        Arc::new(MockPipelineAgent::new()),
        Arc::new(MockIcpBackend::new()),
        Arc::new(mock_evm),
        Principal::management_canister(),
        Principal::management_canister(),
    );

    let err = backend
        .get_source_balance("ETH", "ETH", &owner.to_string())
        .await
        .expect_err("gas reserve failure should stop source balance sizing");
    assert!(err.contains("native gas price fetch failed"));
}

#[tokio::test]
async fn reverse_bridge_skips_approvals_when_icrc2_allowances_are_sufficient() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let cketh_ledger = Principal::from_slice(&[1u8; 29]);
    let ckusdc_ledger = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").expect("ledger");
    let source_account = Account {
        owner: bridge_owner,
        subaccount: None,
    };

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: None,
                cketh_ledger_id: Some(cketh_ledger),
                minimum_withdrawal_amount: None,
            })
        });
    mock_agent
        .expect_call_query::<Eip1559TransactionPrice>()
        .times(1)
        .returning(|_, _, _| {
            Ok(Eip1559TransactionPrice {
                max_priority_fee_per_gas: Nat::from(1u8),
                max_fee_per_gas: Nat::from(2u8),
                max_transaction_fee: Nat::from(100_000u64),
                timestamp: Some(1),
                gas_limit: Nat::from(21_000u64),
            })
        });
    mock_agent
        .expect_call_update::<WithdrawErc20Ret>()
        .times(1)
        .returning(|_, _, _| {
            Ok(WithdrawErc20Ret::Ok(RetrieveErc20Request {
                ckerc20_block_index: Nat::from(7u8),
                cketh_block_index: Nat::from(8u8),
            }))
        });

    let mut mock_icp = MockIcpBackend::new();
    let ckusdc_ledger_for_decimals = ckusdc_ledger;
    mock_icp.expect_icrc1_decimals().times(1).returning(move |ledger| {
        if ledger == ckusdc_ledger_for_decimals {
            Ok(6)
        } else {
            Err(format!("unexpected decimals ledger {ledger}"))
        }
    });
    let ckusdc_ledger_for_fee = ckusdc_ledger;
    mock_icp.expect_icrc1_fee().times(1).returning(move |ledger| {
        if ledger == ckusdc_ledger_for_fee {
            Ok(Nat::from(10_000u64))
        } else {
            Err(format!("unexpected fee ledger {ledger}"))
        }
    });
    let ckusdc_ledger_for_balance = ckusdc_ledger;
    let cketh_ledger_for_balance = cketh_ledger;
    let source_account_for_balance = source_account;
    mock_icp
        .expect_icrc1_balance()
        .times(2)
        .returning(move |ledger, account| {
            if *account != source_account_for_balance {
                return Err("unexpected account".to_string());
            }
            if ledger == ckusdc_ledger_for_balance {
                return Ok(Nat::from(2_000_000u64));
            }
            if ledger == cketh_ledger_for_balance {
                return Ok(Nat::from(500_000u64));
            }
            Err(format!("unexpected balance ledger {ledger}"))
        });
    let ckusdc_ledger_for_allowance = ckusdc_ledger;
    let cketh_ledger_for_allowance = cketh_ledger;
    let source_account_for_allowance = source_account;
    mock_icp
        .expect_icrc2_allowance()
        .times(2)
        .returning(move |ledger, account, spender| {
            if *account != source_account_for_allowance {
                return Err("unexpected allowance account".to_string());
            }
            if spender.owner != minter || spender.subaccount.is_some() {
                return Err("unexpected spender".to_string());
            }
            if ledger == ckusdc_ledger_for_allowance {
                return Ok(Nat::from(1_000_000u64));
            }
            if ledger == cketh_ledger_for_allowance {
                return Ok(Nat::from(120_000u64));
            }
            Err(format!("unexpected allowance ledger {ledger}"))
        });
    mock_icp.expect_icrc2_approve().times(0);

    let mock_evm = MockBridgeEvmBackend::new();
    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        minter,
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckUSDC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: bridge_owner.to_text(),
        target_asset: "USDC".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, "ic-withdraw:7:8");
}

#[tokio::test]
async fn reverse_bridge_approves_only_missing_icrc2_allowance() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let cketh_ledger = Principal::from_slice(&[2u8; 29]);
    let ckusdc_ledger = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").expect("ledger");
    let source_account = Account {
        owner: bridge_owner,
        subaccount: None,
    };

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<CkEthMinterInfo>()
        .times(1)
        .returning(move |_, _, _| {
            Ok(CkEthMinterInfo {
                deposit_with_subaccount_helper_contract_address: None,
                eth_helper_contract_address: None,
                erc20_helper_contract_address: None,
                cketh_ledger_id: Some(cketh_ledger),
                minimum_withdrawal_amount: None,
            })
        });
    mock_agent
        .expect_call_query::<Eip1559TransactionPrice>()
        .times(1)
        .returning(|_, _, _| {
            Ok(Eip1559TransactionPrice {
                max_priority_fee_per_gas: Nat::from(1u8),
                max_fee_per_gas: Nat::from(2u8),
                max_transaction_fee: Nat::from(100_000u64),
                timestamp: Some(1),
                gas_limit: Nat::from(21_000u64),
            })
        });
    mock_agent
        .expect_call_update::<WithdrawErc20Ret>()
        .times(1)
        .returning(|_, _, _| {
            Ok(WithdrawErc20Ret::Ok(RetrieveErc20Request {
                ckerc20_block_index: Nat::from(9u8),
                cketh_block_index: Nat::from(10u8),
            }))
        });

    let mut mock_icp = MockIcpBackend::new();
    let ckusdc_ledger_for_decimals = ckusdc_ledger;
    mock_icp.expect_icrc1_decimals().times(1).returning(move |ledger| {
        if ledger == ckusdc_ledger_for_decimals {
            Ok(6)
        } else {
            Err(format!("unexpected decimals ledger {ledger}"))
        }
    });
    let ckusdc_ledger_for_fee = ckusdc_ledger;
    mock_icp.expect_icrc1_fee().times(1).returning(move |ledger| {
        if ledger == ckusdc_ledger_for_fee {
            Ok(Nat::from(10_000u64))
        } else {
            Err(format!("unexpected fee ledger {ledger}"))
        }
    });
    let ckusdc_ledger_for_balance = ckusdc_ledger;
    let cketh_ledger_for_balance = cketh_ledger;
    let source_account_for_balance = source_account;
    mock_icp
        .expect_icrc1_balance()
        .times(2)
        .returning(move |ledger, account| {
            if *account != source_account_for_balance {
                return Err("unexpected account".to_string());
            }
            if ledger == ckusdc_ledger_for_balance {
                return Ok(Nat::from(2_000_000u64));
            }
            if ledger == cketh_ledger_for_balance {
                return Ok(Nat::from(500_000u64));
            }
            Err(format!("unexpected balance ledger {ledger}"))
        });
    let ckusdc_ledger_for_allowance = ckusdc_ledger;
    let cketh_ledger_for_allowance = cketh_ledger;
    let source_account_for_allowance = source_account;
    mock_icp
        .expect_icrc2_allowance()
        .times(2)
        .returning(move |ledger, account, spender| {
            if *account != source_account_for_allowance {
                return Err("unexpected allowance account".to_string());
            }
            if spender.owner != minter || spender.subaccount.is_some() {
                return Err("unexpected spender".to_string());
            }
            if ledger == ckusdc_ledger_for_allowance {
                return Ok(Nat::from(1_000_000u64));
            }
            if ledger == cketh_ledger_for_allowance {
                return Ok(Nat::from(119_999u64));
            }
            Err(format!("unexpected allowance ledger {ledger}"))
        });
    mock_icp
        .expect_icrc2_approve()
        .withf(move |ledger, args| {
            *ledger == cketh_ledger
                && args.amount == Nat::from(120_000u64)
                && args.spender.owner == minter
                && args.spender.subaccount.is_none()
        })
        .times(1)
        .returning(|_, _| Ok(Nat::from(1u8)));

    let mock_evm = MockBridgeEvmBackend::new();
    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(mock_evm),
        minter,
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckUSDC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: bridge_owner.to_text(),
        target_asset: "USDC".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, "ic-withdraw:9:10");
}

#[tokio::test]
async fn native_reverse_bridge_approves_cketh_and_calls_withdraw_eth() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let cketh_ledger = Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").expect("ledger");
    let source_account = Account {
        owner: bridge_owner,
        subaccount: None,
    };

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<Eip1559TransactionPrice>()
        .times(1)
        .returning(|_, _, _| {
            Ok(Eip1559TransactionPrice {
                max_priority_fee_per_gas: Nat::from(1u8),
                max_fee_per_gas: Nat::from(2u8),
                max_transaction_fee: Nat::from(100_000u64),
                timestamp: Some(1),
                gas_limit: Nat::from(21_000u64),
            })
        });
    mock_agent
        .expect_call_update::<WithdrawalRet>()
        .times(1)
        .returning(|_, _, _| {
            Ok(WithdrawalRet::Ok(RetrieveEthRequest {
                block_index: Nat::from(77u8),
            }))
        });

    let mut mock_icp = MockIcpBackend::new();
    mock_icp.expect_icrc1_fee().times(1).returning(move |ledger| {
        if ledger == cketh_ledger {
            Ok(Nat::from(2_000_000_000_000u64))
        } else {
            Err(format!("unexpected fee ledger {ledger}"))
        }
    });
    let source_account_for_balance = source_account;
    mock_icp
        .expect_icrc1_balance()
        .times(1)
        .returning(move |ledger, account| {
            if ledger != cketh_ledger {
                return Err(format!("unexpected balance ledger {ledger}"));
            }
            if *account != source_account_for_balance {
                return Err("unexpected balance account".to_string());
            }
            Ok(Nat::from(2_000_000_000_000_000_000u128))
        });
    let source_account_for_allowance = source_account;
    mock_icp
        .expect_icrc2_allowance()
        .times(1)
        .returning(move |ledger, account, spender| {
            if ledger != cketh_ledger {
                return Err(format!("unexpected allowance ledger {ledger}"));
            }
            if *account != source_account_for_allowance {
                return Err("unexpected allowance account".to_string());
            }
            if spender.owner != minter || spender.subaccount.is_some() {
                return Err("unexpected spender".to_string());
            }
            Ok(Nat::from(999_999_999_999_999_999u128))
        });
    mock_icp
        .expect_icrc2_approve()
        .withf(move |ledger, args| {
            *ledger == cketh_ledger
                && args.amount == Nat::from(1_000_000_000_000_120_000u128)
                && args.spender.owner == minter
                && args.spender.subaccount.is_none()
        })
        .times(1)
        .returning(|_, _| Ok(Nat::from(1u8)));

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckETH".to_string(),
        source_chain: "ICP".to_string(),
        source_address: bridge_owner.to_text(),
        target_asset: "ETH".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let submission = backend.submit_bridge(request).await.expect("bridge must submit");
    assert_eq!(submission.bridge_id, "ic-withdraw-eth:77");
}

#[tokio::test]
async fn native_reverse_bridge_rejects_below_minimum_before_allowance_or_withdraw() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent.expect_call_query::<Eip1559TransactionPrice>().times(0);
    mock_agent.expect_call_update::<WithdrawalRet>().times(0);

    let mut mock_icp = MockIcpBackend::new();
    mock_icp.expect_icrc1_fee().times(0);
    mock_icp.expect_icrc1_balance().times(0);
    mock_icp.expect_icrc2_allowance().times(0);
    mock_icp.expect_icrc2_approve().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckETH".to_string(),
        source_chain: "ICP".to_string(),
        source_address: bridge_owner.to_text(),
        target_asset: "ETH".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 0.004_999,
        provider_fee_budget_native_units: None,
    };

    let err = backend
        .submit_bridge(request)
        .await
        .expect_err("below minimum must fail");
    assert!(err.starts_with(FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX));
}

#[tokio::test]
async fn native_reverse_bridge_allows_exact_minimum() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let cketh_ledger = Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").expect("ledger");
    let source_account = Account {
        owner: bridge_owner,
        subaccount: None,
    };

    let mut mock_agent = MockPipelineAgent::new();
    mock_agent.expect_call_query::<Eip1559TransactionPrice>().times(0);
    mock_agent
        .expect_call_update::<WithdrawalRet>()
        .times(1)
        .returning(|_, _, _| {
            Ok(WithdrawalRet::Ok(RetrieveEthRequest {
                block_index: Nat::from(78u8),
            }))
        });

    let mut mock_icp = MockIcpBackend::new();
    mock_icp.expect_icrc1_fee().times(1).returning(move |ledger| {
        if ledger == cketh_ledger {
            Ok(Nat::from(2_000_000_000_000u64))
        } else {
            Err(format!("unexpected fee ledger {ledger}"))
        }
    });
    let source_account_for_balance = source_account;
    mock_icp
        .expect_icrc1_balance()
        .times(1)
        .returning(move |ledger, account| {
            if ledger != cketh_ledger {
                return Err(format!("unexpected balance ledger {ledger}"));
            }
            if *account != source_account_for_balance {
                return Err("unexpected balance account".to_string());
            }
            Ok(Nat::from(10_000_000_000_000_000u128))
        });
    let source_account_for_allowance = source_account;
    mock_icp
        .expect_icrc2_allowance()
        .times(1)
        .returning(move |ledger, account, spender| {
            if ledger != cketh_ledger {
                return Err(format!("unexpected allowance ledger {ledger}"));
            }
            if *account != source_account_for_allowance {
                return Err("unexpected allowance account".to_string());
            }
            if spender.owner != minter || spender.subaccount.is_some() {
                return Err("unexpected spender".to_string());
            }
            Ok(Nat::from(5_000_000_000_120_000u128))
        });
    mock_icp.expect_icrc2_approve().times(0);

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckETH".to_string(),
        source_chain: "ICP".to_string(),
        source_address: bridge_owner.to_text(),
        target_asset: "ETH".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 0.005,
        provider_fee_budget_native_units: Some(Nat::from(120_000u64)),
    };

    let submission = backend.submit_bridge(request).await.expect("minimum should submit");
    assert_eq!(submission.bridge_id, "ic-withdraw-eth:78");
}

#[tokio::test]
async fn native_reverse_bridge_minimum_uses_default_without_minter_lookup() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(MockPipelineAgent::new()),
        Arc::new(MockIcpBackend::new()),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let minimum = backend
        .get_minimum_bridge_amount("ckETH", "ICP", "ETH")
        .await
        .expect("minimum should resolve");
    assert_eq!(minimum, 0.005);
}

#[tokio::test]
async fn native_reverse_bridge_destination_fee_budget_uses_quote() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<Eip1559TransactionPrice>()
        .times(1)
        .returning(|_, _, _| {
            Ok(Eip1559TransactionPrice {
                max_priority_fee_per_gas: Nat::from(1u8),
                max_fee_per_gas: Nat::from(2u8),
                max_transaction_fee: Nat::from(100_000_000_000_000u64),
                timestamp: Some(1),
                gas_limit: Nat::from(21_000u64),
            })
        });

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(MockIcpBackend::new()),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let fee_budget = backend
        .get_destination_fee_budget("ckETH", "ICP", "ETH")
        .await
        .expect("destination fee budget should resolve");
    assert!((fee_budget - 0.00012).abs() < 1e-18);
}

#[tokio::test]
async fn native_reverse_bridge_fee_budget_reuses_one_provider_quote() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();
    let cketh_ledger = Principal::from_text("ss2fx-dyaaa-aaaar-qacoq-cai").expect("ledger");
    let mut mock_agent = MockPipelineAgent::new();
    mock_agent
        .expect_call_query::<Eip1559TransactionPrice>()
        .times(1)
        .returning(|_, _, _| {
            Ok(Eip1559TransactionPrice {
                max_priority_fee_per_gas: Nat::from(1u8),
                max_fee_per_gas: Nat::from(2u8),
                max_transaction_fee: Nat::from(100_000_000_000_000u64),
                timestamp: Some(1),
                gas_limit: Nat::from(21_000u64),
            })
        });

    let mut mock_icp = MockIcpBackend::new();
    mock_icp.expect_icrc1_fee().times(1).returning(move |ledger| {
        if ledger == cketh_ledger {
            Ok(Nat::from(2_000_000_000_000u64))
        } else {
            Err(format!("unexpected fee ledger {ledger}"))
        }
    });

    let backend = CkErc20BridgeBackend::new(
        Arc::new(mock_agent),
        Arc::new(mock_icp),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let fee_budget = backend
        .get_fee_budget("ckETH", "ICP", "ETH")
        .await
        .expect("fee budget should resolve");
    assert!((fee_budget.source_fee_budget - 0.000122).abs() < 1e-18);
    assert!((fee_budget.destination_fee_budget - 0.00012).abs() < 1e-18);
    assert_eq!(
        fee_budget.provider_fee_budget_native_units,
        Some(Nat::from(120_000_000_000_000u64))
    );
}

#[tokio::test]
async fn erc20_reverse_bridge_destination_fee_budget_is_zero() {
    let bridge_owner = Principal::management_canister();
    let minter = Principal::anonymous();

    let backend = CkErc20BridgeBackend::new(
        Arc::new(MockPipelineAgent::new()),
        Arc::new(MockIcpBackend::new()),
        Arc::new(MockBridgeEvmBackend::new()),
        minter,
        bridge_owner,
    );

    let fee_budget = backend
        .get_destination_fee_budget("ckUSDC", "ICP", "USDC")
        .await
        .expect("destination fee budget should resolve");
    assert_eq!(fee_budget, 0.0);
}

#[tokio::test]
async fn native_reverse_bridge_rejects_wrong_bridge_owner() {
    let bridge_owner = Principal::management_canister();
    let wrong_owner = Principal::anonymous();
    let backend = CkErc20BridgeBackend::new(
        Arc::new(MockPipelineAgent::new()),
        Arc::new(MockIcpBackend::new()),
        Arc::new(MockBridgeEvmBackend::new()),
        Principal::anonymous(),
        bridge_owner,
    );

    let request = BridgeRequest {
        asset: "ckETH".to_string(),
        source_chain: "ICP".to_string(),
        source_address: wrong_owner.to_text(),
        target_asset: "ETH".to_string(),
        destination: BridgeDestination::EvmAddress(
            "0x1111111111111111111111111111111111111111"
                .parse::<Address>()
                .expect("address"),
        ),
        amount: 1.0,
        provider_fee_budget_native_units: None,
    };

    let err = backend.submit_bridge(request).await.expect_err("must fail");
    assert!(err.contains("does not match configured bridge owner"));
}
