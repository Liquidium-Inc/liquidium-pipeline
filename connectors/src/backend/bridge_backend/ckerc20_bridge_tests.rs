use alloy::primitives::Address;
use candid::Principal;
use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};
use std::sync::Arc;

use crate::backend::{
    bridge_backend::{BridgeBackend, BridgeDestination, BridgeRequest},
    icp_backend::MockIcpBackend,
};
use crate::pipeline_agent::MockPipelineAgent;

use super::{
    CkErc20BridgeBackend, CkEthMinterInfo, MockBridgeEvmBackend, destination_to_bytes32,
    ensure_source_matches_bridge_owner, ensure_source_matches_signer, parse_source_icp_account,
    resolve_cketh_route_for_request,
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
fn route_validation_rejects_non_forward_route_kind() {
    let request = crate::backend::bridge_backend::BridgeRequest {
        asset: "ckBTC".to_string(),
        source_chain: "ICP".to_string(),
        source_address: "0x1111111111111111111111111111111111111111".to_string(),
        target_asset: "BTC".to_string(),
        destination: BridgeDestination::BtcAddress("1BoatSLRHtKNngkdXEeobR76b53LETtpyT".to_string()),
        amount: 1.0,
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
                erc20_helper_contract_address: Some(native_helper.clone()),
                cketh_ledger_id: None,
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
    };

    let err = backend.submit_bridge(request).await.expect_err("must fail");
    assert!(err.contains("does not support subaccount destinations"));
}
