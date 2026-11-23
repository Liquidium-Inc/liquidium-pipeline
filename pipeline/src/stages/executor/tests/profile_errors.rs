use super::*;

#[tokio::test]
async fn profile_refusals_stop_future_submissions_without_parsing_messages() {
    for error in [ProtocolError::ProfileNotFound, ProtocolError::InvalidTargetPrincipal] {
        let mut agent = MockPipelineAgent::new();
        let refusal = error.clone();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(move |_, _, _| Ok(Err(refusal.clone())));
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents.expect_mark_failed().times(1).returning(|_, _, _, _| Ok(()));
        let executor = test_executor(agent, intents);
        let mut request = icp_request();
        request.liquidation.collateral_share_receiver = Some(Principal::management_canister());
        let receipts = executor.process(&vec![request.clone()]).await.unwrap();
        assert!(
            matches!(&receipts[0].status, ExecutionStatus::ShareProfileSetupError(ShareProfileError::Refused(reason)) if reason == &error)
        );
        assert!(executor.process(&vec![request]).await.is_err());
    }
}

#[tokio::test]
async fn ownership_change_after_repayment_stops_submissions_and_keeps_refund_result() {
    let mut agent = MockPipelineAgent::new();
    agent
        .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
        .times(1)
        .returning(|_, _, _| {
            let mut result = make_liquidation_result(42);
            result.status = LiquidationStatus::FailedLiquidation("arbitrary human wording".to_string());
            Ok(Ok(result))
        });
    agent
        .expect_call_query::<Option<candid::Principal>>()
        .times(1)
        .returning(|_, method, _| {
            assert_eq!(method, "get_wallet_profile");
            Ok(Some(Principal::self_authenticating(b"changed owner")))
        });
    let mut intents = MockLiquidationIntentStore::new();
    intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
    intents.expect_mark_failed().times(1).returning(|_, id, receipt, _| {
        assert_eq!(id.as_deref(), Some("42"));
        assert!(receipt.as_ref().unwrap().liquidation_result.is_some());
        Ok(())
    });
    let executor = test_executor(agent, intents);
    let mut request = icp_request();
    request.liquidation.collateral_share_receiver = Some(Principal::management_canister());
    let receipts = executor.process(&vec![request.clone()]).await.unwrap();
    assert!(matches!(
        receipts[0].status,
        ExecutionStatus::ShareProfileSetupError(ShareProfileError::OwnershipChanged { .. })
    ));
    assert!(executor.process(&vec![request]).await.is_err());
}

#[tokio::test]
async fn profile_rechecks_keep_local_failures_typed_and_preserve_refunds() {
    let expected = Principal::management_canister();
    let cases = [
        (Ok(None), Some(ShareProfileError::ProfileNotFound)),
        (
            Err("query unavailable".to_string()),
            Some(ShareProfileError::Call("query unavailable".to_string())),
        ),
        (Ok(Some(expected)), None),
    ];
    for (profile_result, expected_error) in cases {
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update::<Result<LiquidationResult, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| {
                let mut result = make_liquidation_result(42);
                result.status = LiquidationStatus::FailedLiquidation("any failure wording".to_string());
                Ok(Ok(result))
            });
        agent
            .expect_call_query::<Option<Principal>>()
            .times(1)
            .return_once(move |_, _, _| profile_result);
        let mut intents = MockLiquidationIntentStore::new();
        intents.expect_create_submitting().times(1).returning(|_, _| Ok(()));
        intents.expect_mark_failed().times(1).returning(|_, id, receipt, _| {
            assert_eq!(id.as_deref(), Some("42"));
            assert!(receipt.unwrap().liquidation_result.is_some());
            Ok(())
        });
        let executor = test_executor(agent, intents);
        let mut request = icp_request();
        request.liquidation.collateral_share_receiver = Some(expected);
        let receipts = executor.process(&vec![request]).await.unwrap();
        match expected_error {
            Some(expected_error) => {
                assert!(
                    matches!(&receipts[0].status, ExecutionStatus::ShareProfileSetupError(error) if error == &expected_error)
                );
                assert!(executor.process(&vec![]).await.is_err());
            }
            None => {
                assert!(matches!(&receipts[0].status, ExecutionStatus::FailedLiquidation(_)));
                assert!(executor.process(&vec![]).await.is_ok());
            }
        }
    }
}

#[test]
fn profile_errors_are_specific_to_share_requests() {
    assert_eq!(liquidation_profile_error(None, &ProtocolError::ProfileNotFound), None);
    assert_eq!(
        liquidation_profile_error(Some(Principal::management_canister()), &ProtocolError::NoLiquidity),
        None
    );
}
