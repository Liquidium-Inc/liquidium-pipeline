use candid::{Nat, Principal};
use icrc_ledger_types::{icrc1::account::Account, icrc2::allowance::AllowanceArgs, icrc2::approve::ApproveArgs};

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

/// Reads ICRC-1 fee and enriches failures with call-site context.
pub async fn icrc1_fee_with_context<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    context: &str,
) -> Result<Nat, String> {
    backend
        .icrc1_fee(ledger)
        .await
        .map_err(|e| format!("{context}: icrc1_fee failed on ledger {ledger}: {e}"))
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

/// Reads ICRC-2 allowance and enriches failures with call-site context.
pub async fn icrc2_allowance_with_context<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    args: AllowanceArgs,
    context: &str,
) -> Result<Nat, String> {
    backend
        .icrc2_allowance(ledger, &args.account, &args.spender)
        .await
        .map_err(|e| format!("{context}: icrc2_allowance failed on ledger {ledger}: {e}"))
}

/// A burn through a minter: the account that burns on one ledger, and the
/// minter that spends its allowance.
pub struct MinterBurn<'a, B: IcpBackend + ?Sized> {
    pub backend: &'a B,
    pub ledger: Principal,
    pub account: Account,
    pub minter: Principal,
    pub context: &'a str,
}

impl<B: IcpBackend + ?Sized> MinterBurn<'_, B> {
    fn spender(&self) -> Account {
        Account {
            owner: self.minter,
            subaccount: None,
        }
    }

    async fn allowance(&self) -> Result<Nat, String> {
        let args = AllowanceArgs {
            account: self.account,
            spender: self.spender(),
        };
        icrc2_allowance_with_context(self.backend, self.ledger, args, self.context).await
    }

    /// An allowance that already covers the burn needs no second approve.
    async fn fee_after(&self, allowance: &Nat, amount: &Nat) -> Result<Nat, String> {
        if allowance >= amount {
            return Ok(Nat::from(0u8));
        }
        icrc1_fee_with_context(self.backend, self.ledger, self.context).await
    }

    /// The ledger fee a burn of `amount` still pays for its approve.
    pub async fn approve_fee_due(&self, amount: &Nat) -> Result<Nat, String> {
        self.fee_after(&self.allowance().await?, amount).await
    }

    /// Checks that the balance covers the burn and the approve it still
    /// needs, then gives the minter the allowance.
    pub async fn fund(&self, amount: &Nat) -> Result<(), String> {
        let allowance = self.allowance().await?;
        let required = amount.clone() + self.fee_after(&allowance, amount).await?;
        let balance = icrc1_balance_with_context(self.backend, self.ledger, &self.account, self.context).await?;
        if balance < required {
            return Err(format!(
                "bridge amount preflight failed: balance {balance} is below the burn and approve budget {required} (ledger={} source={})",
                self.ledger, self.account
            ));
        }
        if allowance >= *amount {
            return Ok(());
        }
        let approve = ApproveArgs {
            from_subaccount: None,
            spender: self.spender(),
            amount: amount.clone(),
            expected_allowance: Some(allowance),
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };
        icrc2_approve_with_context(self.backend, self.ledger, approve, self.context)
            .await
            .map(|_| ())
    }
}

/// Raises the allowance of `spender` on `account` to `required` when it is
/// lower. `expected_allowance` makes the ledger refuse the approve when the
/// allowance changed since it was read, instead of overwriting it.
pub async fn ensure_icrc2_allowance<B: IcpBackend + ?Sized>(
    backend: &B,
    ledger: Principal,
    account: &Account,
    spender: Account,
    required: Nat,
    context: &str,
) -> Result<(), String> {
    let current = icrc2_allowance_with_context(
        backend,
        ledger,
        AllowanceArgs {
            account: *account,
            spender,
        },
        context,
    )
    .await?;
    if current >= required {
        return Ok(());
    }
    icrc2_approve_with_context(
        backend,
        ledger,
        ApproveArgs {
            from_subaccount: None,
            spender,
            amount: required,
            expected_allowance: Some(current),
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        },
        context,
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use candid::{Nat, Principal};
    use icrc_ledger_types::{icrc1::account::Account, icrc2::allowance::AllowanceArgs, icrc2::approve::ApproveArgs};

    use super::{
        icrc1_balance_with_context, icrc1_decimals_with_context, icrc1_fee_with_context, icrc2_allowance_with_context,
        icrc2_approve_with_context,
    };
    use crate::backend::icp_backend::MockIcpBackend;

    #[tokio::test]
    async fn balance_wrapper_adds_context_and_ledger_on_error() {
        let ledger = Principal::management_canister();
        let account = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };

        let mut backend = MockIcpBackend::new();
        backend.expect_icrc1_balance().returning(|_, _| Err("boom".to_string()));

        let err = icrc1_balance_with_context(&backend, ledger, &account, "bridge")
            .await
            .expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc1_balance_of failed on ledger"));
        assert!(err.contains("boom"));
    }

    #[tokio::test]
    async fn decimals_wrapper_passes_success_value() {
        let ledger = Principal::management_canister();
        let mut backend = MockIcpBackend::new();
        backend.expect_icrc1_decimals().returning(|_| Ok(6));

        let decimals = icrc1_decimals_with_context(&backend, ledger, "bridge")
            .await
            .expect("must pass");
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

        let err = icrc2_approve_with_context(&backend, ledger, args, "bridge")
            .await
            .expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc2_approve failed on ledger"));
        assert!(err.contains("approval boom"));
    }

    #[tokio::test]
    async fn fee_wrapper_adds_context_and_ledger_on_error() {
        let ledger = Principal::management_canister();
        let mut backend = MockIcpBackend::new();
        backend.expect_icrc1_fee().returning(|_| Err("fee boom".to_string()));

        let err = icrc1_fee_with_context(&backend, ledger, "bridge")
            .await
            .expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc1_fee failed on ledger"));
        assert!(err.contains("fee boom"));
    }

    #[tokio::test]
    async fn allowance_wrapper_adds_context_and_ledger_on_error() {
        let ledger = Principal::management_canister();
        let args = AllowanceArgs {
            account: Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            spender: Account {
                owner: Principal::management_canister(),
                subaccount: None,
            },
        };

        let mut backend = MockIcpBackend::new();
        backend
            .expect_icrc2_allowance()
            .returning(|_, _, _| Err("allowance boom".to_string()));

        let err = icrc2_allowance_with_context(&backend, ledger, args, "bridge")
            .await
            .expect_err("must fail");
        assert!(err.contains("bridge"));
        assert!(err.contains("icrc2_allowance failed on ledger"));
        assert!(err.contains("allowance boom"));
    }
}
