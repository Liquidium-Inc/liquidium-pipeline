pub mod account {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use candid::{Encode, Nat, Principal};
    use icrc_ledger_agent::Icrc1Agent;
    use icrc_ledger_types::icrc1::{account::Subaccount, transfer::TransferArg};

    use icrc_ledger_types::icrc1::account::Account;
    use log::debug;

    use crate::{
        icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
        pipeline_agent::PipelineAgent,
    };

    pub const RECOVERY_ACCOUNT: &Subaccount = &[
        0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[cfg_attr(test, mockall::automock)]
    #[async_trait]
    pub trait IcrcAccountInfo: Send + Sync {
        async fn get_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String>;
        async fn sync_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String>;
        fn get_cached_balance(&self, ledger_id: Principal, account: Account) -> Option<IcrcTokenAmount>;
    }

    pub struct LiquidatorAccount<A: PipelineAgent> {
        pub agent: Arc<A>,
        pub cache: Arc<Mutex<HashMap<(Principal, String), IcrcTokenAmount>>>, // (ledger_id, account) -> (balance, )
        pub sub_account: Option<Subaccount>,
    }

    impl<A: PipelineAgent> LiquidatorAccount<A> {
        pub fn new(agent: Arc<A>) -> Self {
            Self {
                agent,
                cache: Arc::new(Mutex::new(HashMap::new())),
                sub_account: None,
            }
        }

        pub fn set_sub_account(&mut self, account: Subaccount) {
            self.sub_account = Some(account);
        }
    }

    #[async_trait]
    impl<A> IcrcAccountInfo for LiquidatorAccount<A>
    where
        A: PipelineAgent,
    {
        async fn get_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String> {
            let icrc_token = IcrcToken::from_principal(ledger_id, Arc::new(self.agent.agent())).await;

            let balance = self
                .agent
                .call_query::<Nat>(&ledger_id, "icrc1_balance_of", Encode!(&account).unwrap())
                .await?;

            Ok(IcrcTokenAmount {
                token: icrc_token,
                value: balance,
            })
        }

        async fn sync_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String> {
            let balance = self.get_balance(ledger_id, account).await?;
            self.cache
                .lock()
                .unwrap()
                .insert((ledger_id, account.to_string()), balance.clone());

            debug!(
                "Balance synced for ledger {}. Balance: {}",
                ledger_id,
                balance.formatted()
            );
            Ok(balance)
        }

        fn get_cached_balance(&self, ledger_id: Principal, account: Account) -> Option<IcrcTokenAmount> {
            let balance = self
                .cache
                .lock()
                .unwrap()
                .get(&(ledger_id, account.to_string()))
                .cloned();
            balance
        }
    }

    #[cfg_attr(test, mockall::automock)]
    #[async_trait]
    pub trait IcrcAccountActions: Send + Sync {
        async fn transfer(&self, amount: IcrcTokenAmount, to: Account) -> Result<String, String>;
        async fn transfer_from_subaccount(&self, amount: IcrcTokenAmount, to: Account) -> Result<String, String>;
    }

    #[async_trait]
    impl<A: PipelineAgent> IcrcAccountActions for LiquidatorAccount<A> {
        async fn transfer_from_subaccount(&self, token_amount: IcrcTokenAmount, to: Account) -> Result<String, String> {
            let icrc_agent = Icrc1Agent {
                agent: self.agent.agent(),
                ledger_canister_id: token_amount.token.ledger,
            };
            icrc_agent
                .transfer(TransferArg {
                    from_subaccount: self.sub_account,
                    to,
                    fee: None,
                    created_at_time: None,
                    memo: None,
                    amount: token_amount.value - token_amount.token.fee,
                })
                .await
                .map_err(|e| format!("Withdraw error {:?}", e))?
                .map_err(|e| format!("Withdraw error {:?}", e))
                .map(|res| res.to_string().replace("_", ""))
        }

        async fn transfer(&self, token_amount: IcrcTokenAmount, to: Account) -> Result<String, String> {
            let icrc_agent = Icrc1Agent {
                agent: self.agent.agent(),
                ledger_canister_id: token_amount.token.ledger,
            };
            icrc_agent
                .transfer(TransferArg {
                    from_subaccount: None,
                    to,
                    fee: None,
                    created_at_time: None,
                    memo: None,
                    amount: token_amount.value - token_amount.token.fee,
                })
                .await
                .map_err(|e| format!("Withdraw error {:?}", e))?
                .map_err(|e| format!("Withdraw error {:?}", e))
                .map(|res| res.to_string().replace("_", ""))
        }
    }
}
