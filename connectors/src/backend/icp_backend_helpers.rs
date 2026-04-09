use candid::{Nat, Principal};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::approve::ApproveArgs,
};

use crate::backend::icp_backend::IcpBackend;

/// Reads an ICRC-1 balance and enriches failures with call-site context.
pub async fn icrc1_balance_with_context<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    account: &Account,
    context: &str,
) -> Result<Nat, String> {
    backend
        .icrc1_balance(ledger, account)
        .await
        .map_err(|e| format!("{context}: icrc1_balance_of failed on ledger {ledger}: {e}"))
}

/// Reads ICRC-1 decimals and enriches failures with call-site context.
pub async fn icrc1_decimals_with_context<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    context: &str,
) -> Result<u8, String> {
    backend
        .icrc1_decimals(ledger)
        .await
        .map_err(|e| format!("{context}: icrc1_decimals failed on ledger {ledger}: {e}"))
}

/// Sends ICRC-2 approve and enriches failures with call-site context.
pub async fn icrc2_approve_with_context<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    args: ApproveArgs,
    context: &str,
) -> Result<Nat, String> {
    backend
        .icrc2_approve(ledger, args)
        .await
        .map_err(|e| format!("{context}: icrc2_approve failed on ledger {ledger}: {e}"))
}

#[cfg(test)]
mod tests {
    use candid::{Nat, Principal};
    use icrc_ledger_types::{
        icrc1::account::Account,
        icrc2::approve::ApproveArgs,
    };

    use super::{icrc1_balance_with_context, icrc1_decimals_with_context, icrc2_approve_with_context};
    use crate::backend::icp_backend::MockIcpBackend;

    #[tokio::test]
    async fn balance_wrapper_adds_context_and_ledger_on_error() {
        let ledger = Principal::management_canister();
        let account = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };

        let mut backend = MockIcpBackend::new();
        backend
            .expect_icrc1_balance()
            .returning(|_, _| Err("boom".to_string()));

        let err = icrc1_balance_with_context(&backend, ledger, &account, "bridge").await.expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc1_balance_of failed on ledger"));
        assert!(err.contains("boom"));
    }

    #[tokio::test]
    async fn decimals_wrapper_passes_success_value() {
        let ledger = Principal::management_canister();
        let mut backend = MockIcpBackend::new();
        backend.expect_icrc1_decimals().returning(|_| Ok(6));

        let decimals = icrc1_decimals_with_context(&backend, ledger, "bridge").await.expect("must pass");
        assert_eq!(decimals, 6);
    }

    #[tokio::test]
    async fn approve_wrapper_adds_context_and_ledger_on_error() {
        let ledger = Principal::management_canister();
        let args = ApproveArgs {
            from_subaccount: None,
            spender: Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            amount: Nat::from(123u8),
            expected_allowance: None,
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };

        let mut backend = MockIcpBackend::new();
        backend
            .expect_icrc2_approve()
            .returning(|_, _| Err("approval boom".to_string()));

        let err = icrc2_approve_with_context(&backend, ledger, args, "bridge").await.expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc2_approve failed on ledger"));
        assert!(err.contains("approval boom"));
    }
}
