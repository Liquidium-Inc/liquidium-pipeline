use std::sync::Arc;

use candid::{Encode, Nat, Principal};
use futures::future::join_all;
use icrc_ledger_types::{
    icrc1::account::Account as IcrcAccount,
    icrc1::account::Account,
    icrc2::{
        allowance::{Allowance, AllowanceArgs},
        approve::{ApproveArgs, ApproveError},
    },
};
use log::{debug, info, warn};

use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use crate::{approval_state::ApprovalState, persistance::WalStore, utils::max_for_ledger};

#[derive(Clone, Debug)]
struct EnsureAllowanceResult {
    allowance_before: Nat,
    allowance_after: Nat,
    threshold: Nat,
    approval_attempted: bool,
}

#[derive(Debug, Default)]
struct InitAllowanceStats {
    checked: usize,
    approved: usize,
    failed: usize,
}

pub struct BasicExecutor<A: PipelineAgent, D: WalStore + Sync + Send> {
    pub agent: Arc<A>,
    pub account_id: Account,
    pub lending_canister: Principal,
    pub wal: Arc<D>,
    pub approval_state: Arc<ApprovalState>,
}

impl<A: PipelineAgent, D: WalStore> BasicExecutor<A, D> {
    pub fn new(
        agent: Arc<A>,
        account_id: Account,
        lending_canister: Principal,
        wal: Arc<D>,
        approval_state: Arc<ApprovalState>,
    ) -> Self {
        Self {
            agent,
            account_id,
            lending_canister,
            wal,
            approval_state,
        }
    }

    pub async fn init(&mut self, tokens: &[Principal]) -> Result<(), String> {
        let results = self.collect_allowance_results(tokens).await;
        let mut stats = InitAllowanceStats::default();
        for (ledger, result) in results {
            self.handle_startup_allowance_result(ledger, result, &mut stats);
        }
        self.log_startup_allowance_summary(&stats);
        Ok(())
    }

    pub async fn refresh_allowances(&self, tokens: &[Principal]) -> Result<(), String> {
        for (ledger, result) in self.collect_allowance_results(tokens).await {
            self.log_refresh_allowance_result(ledger, result);
        }

        Ok(())
    }

    async fn ensure_lending_allowance_for_ledger(&self, ledger: Principal) -> Result<EnsureAllowanceResult, String> {
        let spender = self.lending_spender();
        let spender_account = self.lending_spender_account();
        let threshold = self.allowance_threshold(&ledger);
        let allowance_before = self.allowance(&ledger, spender_account).await?;

        let mut allowance_after = allowance_before.clone();
        let mut approval_attempted = false;

        if allowance_before < threshold {
            approval_attempted = true;
            let approved_amount = max_for_ledger(&ledger);
            let approve_result = self
                .approve(&ledger, spender_account, approved_amount.clone())
                .await?;
            debug!(
                "[executor] allowance approve | ledger={} account={} spender={} approved_amount={} block_index={}",
                ledger.to_text(),
                self.source_account_text(),
                self.spender_account_text(),
                approved_amount,
                approve_result
            );
            allowance_after = approved_amount;
        }

        self.approval_state
            .set_allowance(ledger, spender, allowance_after.clone());

        Ok(EnsureAllowanceResult {
            allowance_before,
            allowance_after,
            threshold,
            approval_attempted,
        })
    }

    async fn approve(&self, ledger: &Principal, spender: Account, amount: Nat) -> Result<Nat, String> {
        let args = ApproveArgs {
            from_subaccount: None,
            spender,
            amount,
            expected_allowance: None,
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };

        let args = Encode!(&args).map_err(|e| format!("Encode error: {}", e))?;

        let result = self
            .agent
            .call_update::<Result<Nat, ApproveError>>(ledger, "icrc2_approve", args)
            .await
            .map_err(|e| format!("Approve call error: {}", e))?
            .map_err(|e| format!("Approve call canister error: {}", e))?;

        Ok(result)
    }

    async fn allowance(&self, ledger: &Principal, spender: Account) -> Result<Nat, String> {
        let blob = Encode!(&AllowanceArgs {
            account: self.account_id,
            spender,
        })
        .map_err(|err| format!("Failed to encode allowance args for {}: {}", ledger, err))?;

        let result = self
            .agent
            .call_query::<Allowance>(ledger, "icrc2_allowance", blob)
            .await
            .map_err(|err| format!("Allowance query failed for {}: {}", ledger, err))?;

        Ok(result.allowance)
    }
}

impl<A: PipelineAgent, D: WalStore> BasicExecutor<A, D> {
    async fn collect_allowance_results(
        &self,
        tokens: &[Principal],
    ) -> Vec<(Principal, Result<EnsureAllowanceResult, String>)> {
        let allowance_futures = tokens.iter().copied().map(|token| async move {
            (token, self.ensure_lending_allowance_for_ledger(token).await)
        });
        join_all(allowance_futures).await
    }

    fn lending_spender(&self) -> Principal {
        self.lending_canister
    }

    fn lending_spender_account(&self) -> IcrcAccount {
        IcrcAccount {
            owner: self.lending_spender(),
            subaccount: None,
        }
    }

    fn source_account_text(&self) -> String {
        Self::format_account(&self.account_id)
    }

    fn spender_account_text(&self) -> String {
        Self::format_account(&self.lending_spender_account())
    }

    fn format_account(account: &Account) -> String {
        format!(
            "{} (subaccount_present={})",
            account.owner.to_text(),
            account.subaccount.is_some()
        )
    }

    fn allowance_threshold(&self, ledger: &Principal) -> Nat {
        max_for_ledger(ledger) / Nat::from(2u8)
    }

    fn handle_startup_allowance_result(
        &mut self,
        ledger: Principal,
        result: Result<EnsureAllowanceResult, String>,
        stats: &mut InitAllowanceStats,
    ) {
        stats.checked += 1;
        match result {
            Ok(res) => {
                if res.approval_attempted {
                    stats.approved += 1;
                }
                self.log_startup_allowance_success(ledger, &res);
            }
            Err(err) => {
                stats.failed += 1;
                self.log_startup_allowance_failure(ledger, &err);
                self.set_zero_allowance_for_startup_failure(ledger);
            }
        }
    }

    fn log_refresh_allowance_result(&self, ledger: Principal, result: Result<EnsureAllowanceResult, String>) {
        match result {
            Ok(res) => self.log_refresh_allowance_success(ledger, &res),
            Err(err) => self.log_refresh_allowance_failure(ledger, &err),
        }
    }

    fn set_zero_allowance_for_startup_failure(&mut self, ledger: Principal) {
        let spender = self.lending_spender();
        self.approval_state.set_allowance(ledger, spender, Nat::from(0u8));
    }

    fn log_startup_allowance_success(&self, ledger: Principal, res: &EnsureAllowanceResult) {
        info!(
            "[executor] startup allowance | ledger={} account={} spender={} before={} threshold={} approval_attempted={} after={}",
            ledger.to_text(),
            self.source_account_text(),
            self.spender_account_text(),
            res.allowance_before,
            res.threshold,
            res.approval_attempted,
            res.allowance_after
        );
    }

    fn log_startup_allowance_failure(&self, ledger: Principal, err: &str) {
        warn!(
            "[executor] startup allowance ensure failed | ledger={} account={} spender={} err={}",
            ledger.to_text(),
            self.source_account_text(),
            self.spender_account_text(),
            err
        );
    }

    fn log_startup_allowance_summary(&self, stats: &InitAllowanceStats) {
        info!(
            "[executor] startup allowance summary | account={} spender={} checked={} approvals_attempted={} failed={}",
            self.source_account_text(),
            self.spender_account_text(),
            stats.checked,
            stats.approved,
            stats.failed
        );
    }

    fn log_refresh_allowance_success(&self, ledger: Principal, res: &EnsureAllowanceResult) {
        debug!(
            "[executor] allowance refresh | ledger={} account={} spender={} before={} threshold={} approval_attempted={} after={}",
            ledger.to_text(),
            self.source_account_text(),
            self.spender_account_text(),
            res.allowance_before,
            res.threshold,
            res.approval_attempted,
            res.allowance_after
        );
    }

    fn log_refresh_allowance_failure(&self, ledger: Principal, err: &str) {
        warn!(
            "[executor] allowance refresh failed | ledger={} account={} spender={} err={}",
            ledger.to_text(),
            self.source_account_text(),
            self.spender_account_text(),
            err
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;

    use crate::persistance::MockWalStore;

    fn p(text: &str) -> Principal {
        Principal::from_text(text).expect("invalid principal")
    }

    fn make_executor(agent: MockPipelineAgent) -> BasicExecutor<MockPipelineAgent, MockWalStore> {
        let wal = MockWalStore::new();
        BasicExecutor::new(
            Arc::new(agent),
            Account {
                owner: p("2vxsx-fae"),
                subaccount: None,
            },
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Arc::new(wal),
            Arc::new(ApprovalState::new()),
        )
    }

    #[tokio::test]
    async fn refresh_allowances_reapproves_when_below_threshold_and_updates_state() {
        let ledger = p("mxzaz-hqaaa-aaaar-qaada-cai"); // ckBTC
        let spender = p("nja4y-2yaaa-aaaae-qddxa-cai");

        let mut agent = MockPipelineAgent::new();

        agent
            .expect_call_query::<Allowance>()
            .withf(move |canister, method, _| *canister == ledger && method == "icrc2_allowance")
            .times(1)
            .returning(move |_, _, _| {
                Ok(Allowance {
                    allowance: Nat::from(0u8),
                    expires_at: None,
                })
            });

        agent
            .expect_call_update::<Result<Nat, ApproveError>>()
            .withf(move |canister, method, _| *canister == ledger && method == "icrc2_approve")
            .times(1)
            .returning(move |_, _, _| Ok(Ok(Nat::from(u64::MAX))));

        let executor = make_executor(agent);
        executor
            .refresh_allowances(&[ledger])
            .await
            .expect("refresh_allowances should not fail");

        let got = executor
            .approval_state
            .get_allowance(ledger, spender)
            .expect("allowance state should be set");
        assert_eq!(got, Nat::from(u64::MAX));
    }

    #[tokio::test]
    async fn refresh_allowances_continues_when_one_token_fails() {
        let failing_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let healthy_ledger = p("ryjl3-tyaaa-aaaaa-aaaba-cai");
        let spender = p("nja4y-2yaaa-aaaae-qddxa-cai");

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Allowance>()
            .times(2)
            .returning(move |canister, method, _| {
                if method != "icrc2_allowance" {
                    return Err("unexpected method".to_string());
                }
                if *canister == failing_ledger {
                    Err("simulated allowance query failure".to_string())
                } else if *canister == healthy_ledger {
                    Ok(Allowance {
                        allowance: Nat::from(u64::MAX),
                        expires_at: None,
                    })
                } else {
                    Err("unexpected canister".to_string())
                }
            });

        let executor = make_executor(agent);
        executor
            .refresh_allowances(&[failing_ledger, healthy_ledger])
            .await
            .expect("refresh_allowances should continue over individual failures");

        assert!(
            executor.approval_state.get_allowance(failing_ledger, spender).is_none(),
            "failing token should not update allowance cache"
        );
        assert_eq!(
            executor
                .approval_state
                .get_allowance(healthy_ledger, spender)
                .expect("healthy token allowance should be set"),
            Nat::from(u64::MAX)
        );
    }

    #[tokio::test]
    async fn refresh_allowances_caches_approved_amount_not_approve_block_index() {
        let ledger = p("mxzaz-hqaaa-aaaar-qaada-cai"); // ckBTC
        let spender = p("nja4y-2yaaa-aaaae-qddxa-cai");

        let mut agent = MockPipelineAgent::new();

        agent
            .expect_call_query::<Allowance>()
            .withf(move |canister, method, _| *canister == ledger && method == "icrc2_allowance")
            .times(1)
            .returning(move |_, _, _| {
                Ok(Allowance {
                    allowance: Nat::from(0u8),
                    expires_at: None,
                })
            });

        agent
            .expect_call_update::<Result<Nat, ApproveError>>()
            .withf(move |canister, method, _| *canister == ledger && method == "icrc2_approve")
            .times(1)
            .returning(move |_, _, _| Ok(Ok(Nat::from(42u8))));

        let executor = make_executor(agent);
        executor
            .refresh_allowances(&[ledger])
            .await
            .expect("refresh_allowances should cache approved amount");

        let got = executor
            .approval_state
            .get_allowance(ledger, spender)
            .expect("allowance state should be set from approved amount");
        assert_eq!(got, Nat::from(u64::MAX));
    }

    #[tokio::test]
    async fn init_logs_failures_but_does_not_fail_startup() {
        let failing_ledger = p("mxzaz-hqaaa-aaaar-qaada-cai");
        let healthy_ledger = p("ryjl3-tyaaa-aaaaa-aaaba-cai");
        let spender = p("nja4y-2yaaa-aaaae-qddxa-cai");

        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_query::<Allowance>()
            .times(2)
            .returning(move |canister, method, _| {
                if method != "icrc2_allowance" {
                    return Err("unexpected method".to_string());
                }
                if *canister == failing_ledger {
                    Err("simulated startup allowance failure".to_string())
                } else if *canister == healthy_ledger {
                    Ok(Allowance {
                        allowance: Nat::from(u64::MAX),
                        expires_at: None,
                    })
                } else {
                    Err("unexpected canister".to_string())
                }
            });

        let mut executor = make_executor(agent);
        executor
            .init(&[failing_ledger, healthy_ledger])
            .await
            .expect("startup should continue even when one token allowance ensure fails");

        assert_eq!(
            executor.approval_state.get_allowance(failing_ledger, spender).as_ref(),
            Some(&Nat::from(0u8)),
            "failed token should set approval_state to zero so strategy keeps requiring approval"
        );
        assert_eq!(
            executor.approval_state.get_allowance(healthy_ledger, spender).as_ref(),
            Some(&Nat::from(u64::MAX)),
            "healthy token should have its resolved allowance in approval_state"
        );
    }
}
