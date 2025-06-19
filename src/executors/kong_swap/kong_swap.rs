use std::{collections::HashMap, str::FromStr, sync::Arc, u128::MAX};

use async_trait::async_trait;
use candid::{Encode, Nat, Principal, encode_args};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::{
        allowance::{Allowance, AllowanceArgs},
        approve::{ApproveArgs, ApproveError},
    },
};
use log::{debug, info, warn};

use crate::{
    executors::executor::IcrcSwapExecutor,
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    pipeline_agent::PipelineAgent,
};

use super::types::{SwapAmountsReply, SwapArgs, SwapReply};

static DEX_PRINCIPAL: &str = "2ipq2-uqaaa-aaaar-qailq-cai";

pub struct KongSwapExecutor<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub account_id: Account,
    pub lending_canister: Principal,
    pub dex_account: Account,
    pub allowances: HashMap<Principal, Nat>,
}

impl<A: PipelineAgent> KongSwapExecutor<A> {
    pub fn new(agent: Arc<A>, account_id: Account, lending_canister: Principal) -> Self {
        Self {
            agent,
            account_id,
            lending_canister,
            dex_account: Account {
                owner: DEX_PRINCIPAL.parse().unwrap(),
                subaccount: None,
            },
            allowances: HashMap::new(),
        }
    }

    pub async fn init(&mut self, tokens: Vec<Principal>) -> Result<(), String> {
        info!("Starting DEX token approval process");
        for token in tokens {
            let mut allowance = self.allowance(&token).await;
            debug!("Current allowance for {}: {}", token, allowance);

            if allowance < MAX / 2 {
                info!("Allowance low for {}, re-approving…", token);
                allowance = match self.approve(&token).await {
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

            self.allowances.insert(token, allowance);
        }
        info!("DEX token approval complete");
        Ok(())
    }
}

#[async_trait]
impl<A: PipelineAgent> IcrcSwapExecutor for KongSwapExecutor<A> {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String> {
        let dex_principal = Principal::from_str(DEX_PRINCIPAL).unwrap();

        info!(
            "Fetching swap info for {} {} {}",
            token_in.symbol, token_out.symbol, amount.value
        );

        let result = self
            .agent
            .call_query::<Result<SwapAmountsReply, String>>(
                &dex_principal,
                "swap_amounts",
                encode_args((token_in.symbol.clone(), amount.value.clone(), token_out.symbol.clone())).unwrap(),
            )
            .await
            .map_err(|e| format!("Swap call error: {}", e))?
            .map_err(|e| format!("Swap call error: {}", e))?;

        Ok(result)
    }

    async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String> {
        let dex_principal = Principal::from_str(DEX_PRINCIPAL).unwrap();
        let result = self
            .agent
            .call_update::<SwapReply>(&dex_principal, "swap", Encode!(&swap_args).unwrap())
            .await
            .map_err(|e| format!("Swap call error: {}", e))?;

        Ok(result)
    }
}

impl<A: PipelineAgent> KongSwapExecutor<A> {
    async fn approve(&self, ledger: &Principal) -> Result<Nat, String> {
        let args = ApproveArgs {
            from_subaccount: None,
            spender: self.dex_account,
            amount: Nat::from(MAX),
            expected_allowance: None,
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };
        let args = Encode!(&args).map_err(|e| format!("Encode error: {}", e))?;

        info!("Approving {} on spender {}", ledger, self.dex_account.owner);
        let result = self
            .agent
            .call_update::<Result<Nat, ApproveError>>(ledger, "icrc2_approve", args)
            .await
            .map_err(|e| format!("Approve call error: {}", e))?
            .map_err(|e| format!("Approve call canister error: {}", e))?;

        Ok(result)
    }

    async fn allowance(&self, ledger: &Principal) -> Nat {
        let blob = Encode!(&AllowanceArgs {
            account: self.account_id,
            spender: self.dex_account,
        })
        .unwrap();

        let result = self
            .agent
            .call_query::<Allowance>(ledger, "icrc2_allowance", blob)
            .await
            .expect("could not fetch allowance");

        info!(
            "Allowance for {} on {} = {}",
            ledger, self.dex_account.owner, result.allowance
        );

        result.allowance
    }
}
