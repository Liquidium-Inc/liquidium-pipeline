use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use candid::{Encode, Nat, Principal, encode_args};
use futures::future::join_all;
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::{
        allowance::{Allowance, AllowanceArgs},
        approve::{ApproveArgs, ApproveError},
    },
};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
use log::{debug, info, warn};

use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;

use crate::swappers::kong::kong_types::{SwapAmountsReply, SwapArgs, SwapReply, SwapResult};

static DEX_PRINCIPAL: &str = "2ipq2-uqaaa-aaaar-qailq-cai";

pub struct KongSwapSwapper<A: PipelineAgent> {
    pub agent: Arc<A>,
    pub account_id: Account,
    pub dex_account: Account,
    pub allowances: Mutex<HashMap<(Principal, Principal), Nat>>,
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
            allowances: Mutex::new(HashMap::new()),
        }
    }

    pub async fn ensure_allowance(&self, token: &ChainToken) -> Result<bool, String> {
        let ledger = match token {
            ChainToken::Icp { ledger, .. } => *ledger,
            _ => return Ok(false),
        };

        let owner = self.dex_account.owner;
        let threshold = max_for_ledger(&ledger) / Nat::from(2u8);

        {
            let map = self.allowances.lock().expect("allowances mutex poisoned");
            if let Some(cached) = map.get(&(ledger, owner))
                && *cached >= threshold
            {
                return Ok(false);
            }
        }

        let allowance = self
            .allowance(
                &ledger,
                Account {
                    owner,
                    subaccount: None,
                },
            )
            .await;

        if allowance >= threshold {
            let mut map = self.allowances.lock().expect("allowances mutex poisoned");
            map.insert((ledger, owner), allowance);
            return Ok(false);
        }

        info!("Allowance low for {}, re-approving…", ledger);
        let approved = self
            .approve(
                &ledger,
                Account {
                    owner,
                    subaccount: None,
                },
            )
            .await?;

        let mut map = self.allowances.lock().expect("allowances mutex poisoned");
        map.insert((ledger, owner), approved);
        Ok(true)
    }

    pub async fn init(&self, tokens: &[Principal]) -> Result<(), String> {
        let owner = self.dex_account.owner;
        let this = self;

        let futures = tokens.iter().map(|token| {
            let token = *token;

            async move {
                let allowance = this.check_allowance(&token, &owner).await;
                (token, owner, allowance)
            }
        });

        let results = join_all(futures).await;

        let mut map = this.allowances.lock().expect("allowances mutex poisoned");
        for (token, owner, allowance) in results {
            map.insert((token, owner), allowance);
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
        debug!(
            "Current allowance for {}: {} on {}",
            token,
            allowance,
            self.agent
                .agent()
                .get_principal()
                .map(|p| p.to_text())
                .unwrap_or_else(|_| "<unknown>".to_string())
        );

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

impl<A: PipelineAgent> KongSwapSwapper<A> {
    pub async fn get_swap_info(
        &self,
        token_in: &ChainToken,
        token_out: &ChainToken,
        amount: &ChainTokenAmount,
    ) -> Result<SwapAmountsReply, String> {
        let dex_principal = Principal::from_str(DEX_PRINCIPAL).unwrap();

        info!(
            "Fetching swap info for {} {} -> {} ",
            amount.value,
            token_in.symbol(),
            token_out.symbol(),
        );

        let result = self
            .agent
            .call_query::<Result<SwapAmountsReply, String>>(
                &dex_principal,
                "swap_amounts",
                encode_args((token_in.symbol(), amount.value.clone(), token_out.symbol()))
                    .map_err(|e| format!("Swap args encode error: {}", e))?,
            )
            .await
            .map_err(|e| {
                warn!(
                    "[kong] quote call failed {} {} -> {} err={}",
                    amount.value,
                    token_in.symbol(),
                    token_out.symbol(),
                    e
                );
                format!("Swap call error: {}", e)
            })?
            .map_err(|e| {
                warn!(
                    "[kong] quote response error {} {} -> {} err={}",
                    amount.value,
                    token_in.symbol(),
                    token_out.symbol(),
                    e
                );
                format!("Swap call error: {}", e)
            })?;

        Ok(result)
    }

    pub async fn balance_of(&self, token: &ChainToken) -> Result<Nat, String> {
        let ledger = match token {
            ChainToken::Icp { ledger, .. } => *ledger,
            _ => return Err("unsupported token type for balance query".to_string()),
        };

        let args = Encode!(&Account {
            owner: self.account_id.owner,
            subaccount: self.account_id.subaccount,
        })
        .map_err(|e| format!("balance_of encode error: {}", e))?;

        let result = self
            .agent
            .call_query::<Nat>(&ledger, "icrc1_balance_of", args)
            .await
            .map_err(|e| {
                warn!("[kong] balance query failed ledger={} err={}", ledger, e);
                format!("balance query failed: {}", e)
            })?;

        Ok(result)
    }

    pub async fn swap(&self, swap_args: SwapArgs) -> Result<SwapReply, String> {
        debug!("Swap args {:#?}", swap_args);
        let pay_amount = swap_args.pay_amount.clone();
        let pay_token = swap_args.pay_token.clone();
        let receive_token = swap_args.receive_token.clone();
        let receive_address = swap_args.receive_address.clone();
        let max_slippage = swap_args.max_slippage;

        info!(
            "[kong] swap request pay={} {} -> {} recv_addr={:?} max_slip={:?}",
            pay_amount, pay_token, receive_token, receive_address, max_slippage
        );
        let dex_principal = Principal::from_str(DEX_PRINCIPAL).unwrap();
        let result = self
            .agent
            .call_update::<SwapResult>(&dex_principal, "swap", swap_args.into())
            .await
            .map_err(|e| {
                warn!(
                    "[kong] swap call failed pay={} {} -> {} err={}",
                    pay_amount, pay_token, receive_token,
                    e
                );
                format!("Swap call error: {}", e)
            })?;

        match result {
            SwapResult::Ok(res) => Ok(res),
            SwapResult::Err(e) => {
                warn!(
                    "[kong] swap rejected pay={} {} -> {} err={}",
                    pay_amount, pay_token, receive_token, e
                );
                Err(format!("Could not execute swap {e}"))
            }
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
