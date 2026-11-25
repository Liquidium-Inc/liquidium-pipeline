//! Resolves the existing lending profile of the liquidator's default ICP account.
use candid::{Encode, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::types::protocol_types::{
    IcpCallerAuth, InitializeAccountRequest, LendingSignature, ProtocolError, SignedInitializeAccountRequest,
};
use serde::{Deserialize, Serialize};

use crate::utils::now_ts;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ShareProfileError {
    #[error("share payout setup requires an authenticated calling principal")]
    AnonymousCaller,
    #[error("share payout profile call failed: {0}")]
    Call(String),
    #[error("share payout profile operation refused: {0:?}")]
    Refused(ProtocolError),
    #[error("the default ICP account has no lending profile")]
    ProfileNotFound,
    #[error("the default ICP account owns profile {actual}, but the request named {expected}")]
    OwnershipChanged { expected: Principal, actual: Principal },
    #[error("default ICP account is linked but its lending profile could not be found")]
    LinkedProfileMissing,
}

/// Returns the profile linked to the caller's canonical default ICP account address.
pub async fn get_share_profile<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    caller: Principal,
) -> Result<Option<Principal>, ShareProfileError> {
    let address = Account {
        owner: caller,
        subaccount: None,
    }
    .to_string();
    agent
        .call_query(
            lending_canister,
            "get_wallet_profile",
            Encode!(&address).map_err(|error| ShareProfileError::Call(error.to_string()))?,
        )
        .await
        .map_err(ShareProfileError::Call)
}

/// Returns the setup error when a share request was refused for profile or authentication reasons.
pub fn liquidation_profile_error(
    requested_profile: Option<Principal>,
    error: &ProtocolError,
) -> Option<ShareProfileError> {
    if requested_profile.is_some()
        && matches!(
            error,
            ProtocolError::ProfileNotFound
                | ProtocolError::InvalidTargetPrincipal
                | ProtocolError::AccountNotFound
                | ProtocolError::SignatureError(_)
        )
    {
        Some(ShareProfileError::Refused(error.clone()))
    } else {
        None
    }
}

/// Verifies that the caller's default ICP account still owns the expected lending profile.
pub async fn verify_share_profile<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    caller: Principal,
    expected: Principal,
) -> Result<(), ShareProfileError> {
    match get_share_profile(agent, lending_canister, caller).await? {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(ShareProfileError::OwnershipChanged { expected, actual }),
        None => Err(ShareProfileError::ProfileNotFound),
    }
}

/// Returns the default ICP account's profile, registering it only when missing.
pub async fn resolve_share_profile<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    caller: Principal,
) -> Result<Principal, ShareProfileError> {
    if caller == Principal::anonymous() {
        return Err(ShareProfileError::AnonymousCaller);
    }
    let existing = get_share_profile(agent, lending_canister, caller).await?;
    if let Some(profile) = existing {
        return Ok(profile);
    }
    let request = SignedInitializeAccountRequest {
        data: InitializeAccountRequest {
            expiry_timestamp: (now_ts() + 60) as u64,
        },
        signature_info: LendingSignature::IcpCaller(IcpCallerAuth {
            principal: caller,
            subaccount: None,
        }),
    };
    let registration = agent
        .call_update::<Result<Principal, ProtocolError>>(
            lending_canister,
            "register_profile",
            Encode!(&request).map_err(|error| ShareProfileError::Call(error.to_string()))?,
        )
        .await
        .map_err(ShareProfileError::Call)?;
    match registration {
        Ok(profile) => Ok(profile),
        Err(ProtocolError::AccountAlreadyLinked) => get_share_profile(agent, lending_canister, caller)
            .await?
            .ok_or(ShareProfileError::LinkedProfileMissing),
        Err(error) => Err(ShareProfileError::Refused(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Decode;
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;

    fn caller() -> Principal {
        Principal::self_authenticating(b"liquidator")
    }
    fn profile() -> Principal {
        Principal::self_authenticating(b"profile")
    }
    fn missing_agent() -> MockPipelineAgent {
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Option<Principal>>()
            .times(1)
            .returning(|_, method, arg| {
                assert_eq!(method, "get_wallet_profile");
                assert_eq!(
                    Decode!(&arg, String).unwrap(),
                    Account {
                        owner: caller(),
                        subaccount: None
                    }
                    .to_string()
                );
                Ok(None)
            });
        agent
    }

    #[tokio::test]
    async fn reuses_existing_profile_without_registration() {
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Option<Principal>>()
            .times(1)
            .returning(|_, method, arg| {
                assert_eq!(method, "get_wallet_profile");
                assert_eq!(Decode!(&arg, String).unwrap(), caller().to_text());
                Ok(Some(profile()))
            });
        agent.expect_call_update::<Result<Principal, ProtocolError>>().never();
        assert_eq!(
            resolve_share_profile(&agent, &Principal::management_canister(), caller())
                .await
                .unwrap(),
            profile()
        );
    }

    #[tokio::test]
    async fn registers_with_actual_caller_and_default_subaccount() {
        let mut agent = missing_agent();
        agent
            .expect_call_update::<Result<Principal, ProtocolError>>()
            .times(1)
            .returning(|_, method, arg| {
                assert_eq!(method, "register_profile");
                let request = Decode!(&arg, SignedInitializeAccountRequest).unwrap();
                assert!(request.data.expiry_timestamp > now_ts() as u64);
                assert!(request.data.expiry_timestamp <= (now_ts() + 300) as u64);
                assert_eq!(
                    request.signature_info,
                    LendingSignature::IcpCaller(IcpCallerAuth {
                        principal: caller(),
                        subaccount: None
                    })
                );
                Ok(Ok(profile()))
            });
        assert_eq!(
            resolve_share_profile(&agent, &Principal::management_canister(), caller())
                .await
                .unwrap(),
            profile()
        );
    }

    #[tokio::test]
    async fn registration_race_queries_profile_again() {
        let mut agent = missing_agent();
        agent
            .expect_call_update::<Result<Principal, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Ok(Err(ProtocolError::AccountAlreadyLinked)));
        agent
            .expect_call_query::<Option<Principal>>()
            .times(1)
            .returning(|_, _, _| Ok(Some(profile())));
        assert_eq!(
            resolve_share_profile(&agent, &Principal::management_canister(), caller())
                .await
                .unwrap(),
            profile()
        );
    }

    #[tokio::test]
    async fn linked_but_missing_profile_stops_setup() {
        let mut agent = missing_agent();
        agent
            .expect_call_update::<Result<Principal, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| Ok(Err(ProtocolError::AccountAlreadyLinked)));
        agent
            .expect_call_query::<Option<Principal>>()
            .times(1)
            .returning(|_, _, _| Ok(None));
        assert!(matches!(
            resolve_share_profile(&agent, &Principal::management_canister(), caller()).await,
            Err(ShareProfileError::LinkedProfileMissing)
        ));
    }

    #[tokio::test]
    async fn registration_errors_stop_setup() {
        let mut agent = missing_agent();
        agent
            .expect_call_update::<Result<Principal, ProtocolError>>()
            .times(1)
            .returning(|_, _, _| {
                Ok(Err(ProtocolError::SignatureError(
                    liquidium_pipeline_core::types::protocol_types::SignatureVerificationError::IcpCallerMismatch,
                )))
            });
        assert!(matches!(
            resolve_share_profile(&agent, &Principal::management_canister(), caller()).await,
            Err(ShareProfileError::Refused(_))
        ));
    }

    #[tokio::test]
    async fn anonymous_caller_stops_before_any_call() {
        assert!(matches!(
            resolve_share_profile(
                &MockPipelineAgent::new(),
                &Principal::management_canister(),
                Principal::anonymous()
            )
            .await,
            Err(ShareProfileError::AnonymousCaller)
        ));
    }
}
