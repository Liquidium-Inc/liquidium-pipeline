use std::{collections::HashMap, str::FromStr, sync::Arc};

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
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    pipeline_agent::PipelineAgent,
    swappers::{
        kong::kong_types::{SwapAmountsReply, SwapArgs, SwapReply, SwapResult},
        swap_interface::IcrcSwapInterface,
    },
};

static DEX_PRINCIPAL: &str = "2ipq2-uqaaa-aaaar-qailq-cai";

pub struct KongSwapSwapper<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub account_id: Account,
    pub dex_account: Account,
    pub allowances: HashMap<(Principal, Principal), Nat>,
}

impl<A: PipelineAgent> KongSwapSwapper<A> {
    pub fn new(agent: Arc<A>, account_id: Account) -> Self {
        Self {
            agent,
            account_id,
            dex_account: Account {
                owner: DEX_PRINCIPAL.parse().unwrap(),
                subaccount: None,
            },
            allowances: HashMap::new(),
        }
    }

    pub async fn init(&mut self, tokens: &Vec<Principal>) -> Result<(), String> {
        info!("Starting DEX token approval process");
        for token in tokens {
            let swap_allowance = self.check_allowance(token, &self.dex_account.owner).await;
            self.allowances.insert((*token, self.dex_account.owner), swap_allowance);
        }
        info!("DEX token approval complete");
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
}

#[async_trait]
impl<A: PipelineAgent> IcrcSwapInterface for KongSwapSwapper<A> {
    async fn get_swap_info(
        &self,
        token_in: &IcrcToken,
        token_out: &IcrcToken,
        amount: &IcrcTokenAmount,
    ) -> Result<SwapAmountsReply, String> {
        let dex_principal = Principal::from_str(DEX_PRINCIPAL).unwrap();

        info!(
            "Fetching swap info for {} {} -> {} ",
            amount.value, token_in.symbol, token_out.symbol,
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
            .call_update::<SwapResult>(&dex_principal, "swap", swap_args.into())
            .await
            .map_err(|e| format!("Swap call error: {}", e))?;

        match result {
            SwapResult::Ok(res) => Ok(res),
            SwapResult::Err(e) => return Err(format!("Could not execute swap {e}")),
        }
    }
}

impl<A: PipelineAgent> KongSwapSwapper<A> {
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

        info!("Approving {} on spender {}", ledger, self.dex_account.owner);
        let result = self
            .agent
            .call_update::<Result<Nat, ApproveError>>(ledger, "icrc2_approve", args)
            .await
            .map_err(|e| format!("Approve call error: {}", e))?
            .map_err(|e| format!("Approve call canister error: {}", e))?;

        Ok(result)
    }

    async fn allowance(&self, ledger: &Principal, spender: Account) -> Nat {
        let blob = Encode!(&AllowanceArgs {
            account: self.account_id,
            spender,
        })
        .unwrap();

        let result = self
            .agent
            .call_query::<Allowance>(ledger, "icrc2_allowance", blob)
            .await
            .expect("could not fetch allowance");

        debug!(
            "Allowance for {} on {} = {}",
            ledger, self.dex_account.owner, result.allowance
        );

        result.allowance
    }
}
fn max_for_ledger(token: &Principal) -> Nat {
    if *token == Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap() {
        return Nat::from(u64::MAX);
    }

    if *token == Principal::from_text("cngnf-vqaaa-aaaar-qag4q-cai").unwrap() {
        return Nat::from(340_282_366_920_938_463_463_374_607_431_768_211_455u128);
    }

    if *token == Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap() {
        return Nat::from(u64::MAX);
    }

    Nat::from(0u8)
}
