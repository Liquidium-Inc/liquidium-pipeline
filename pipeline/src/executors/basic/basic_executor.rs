use std::{collections::HashMap, sync::Arc};

use candid::{Encode, Nat, Principal};
use futures::future::join_all;
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::{
        allowance::{Allowance, AllowanceArgs},
        approve::{ApproveArgs, ApproveError},
    },
};
use log::{debug, info, warn};

use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use crate::{persistance::WalStore, utils::max_for_ledger};

pub struct BasicExecutor<A: PipelineAgent, D: WalStore + Sync + Send> {
    pub agent: Arc<A>,
    pub account_id: Account,
    pub lending_canister: Principal,
    pub wal: Arc<D>,
    pub allowances: HashMap<(Principal, Principal), Nat>,
}

impl<A: PipelineAgent, D: WalStore> BasicExecutor<A, D> {
    pub fn new(agent: Arc<A>, account_id: Account, lending_canister: Principal, wal: Arc<D>) -> Self {
        Self {
            agent,
            account_id,
            lending_canister,
            wal,
            allowances: HashMap::new(),
        }
    }

    pub async fn init(&mut self, tokens: &[Principal]) -> Result<(), String> {
        let spender = self.lending_canister;
        let this = &*self;

        // Build futures without mutating self inside the loop
        let futures = tokens.iter().map(|token| {
            let token = *token;

            async move {
                let lending_allowance = this.check_allowance(&token, &spender).await;
                (token, spender, lending_allowance)
            }
        });

        // Run all allowance checks in parallel
        let results = join_all(futures).await;

        // Now safely update the local allowance cache
        for (token, spender, lending_allowance) in results {
            self.allowances.insert((token, spender), lending_allowance);
        }

        Ok(())
    }

    async fn check_allowance(&self, token: &Principal, spender: &Principal) -> Nat {
        let mut allowance = self
            .allowance(
                token,
                Account {
                    owner: *spender,
                    subaccount: None,
                },
            )
            .await;
        debug!("Current allowance for {}: {}", token, allowance);

        if allowance < max_for_ledger(token) / Nat::from(2u8) {
            info!("Allowance low for {}, re-approving…", token);
            allowance = match self
                .approve(
                    token,
                    Account {
                        owner: *spender,
                        subaccount: None,
                    },
                )
                .await
            {
                Ok(a) => {
                    debug!("Approved {} => {}", token, a);
                    a
                }
                Err(e) => {
                    warn!("Could not set allowance for {}: {}", token, e);
                    Nat::from(0u8)
                }
            };
        }
        allowance
    }

    async fn approve(&self, ledger: &Principal, spender: Account) -> Result<Nat, String> {
        let args = ApproveArgs {
            from_subaccount: None,
            spender,
            amount: max_for_ledger(ledger),
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

    async fn allowance(&self, ledger: &Principal, spender: Account) -> Nat {
        let blob = match Encode!(&AllowanceArgs {
            account: self.account_id,
            spender,
        }) {
            Ok(blob) => blob,
            Err(err) => {
                warn!("Failed to encode allowance args for {}: {}", ledger, err);
                return Nat::from(0u8);
            }
        };

        let result = match self
            .agent
            .call_query::<Allowance>(ledger, "icrc2_allowance", blob)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                warn!("Allowance query failed for {}: {}", ledger, err);
                return Nat::from(0u8);
            }
        };

        result.allowance
    }
}
