pub mod account {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use candid::{Encode, Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use log::debug;

    use crate::{
        icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
        pipeline_agent::PipelineAgent,
    };

    #[cfg_attr(test, mockall::automock)]
    #[async_trait]
    pub trait IcrcAccountInfo: Send + Sync {
        async fn get_balance(&self, ledger_id: Principal, account: Principal) -> Result<IcrcTokenAmount, String>;
        async fn sync_balance(&self, ledger_id: Principal, account: Principal) -> Result<IcrcTokenAmount, String>;
        fn get_cached_balance(&self, ledger_id: Principal, account: Principal) -> Option<IcrcTokenAmount>;
    }

    pub struct LiquidatorAccount<A: PipelineAgent> {
        pub agent: Arc<A>,
        pub cache: Arc<Mutex<HashMap<(Principal, Principal), IcrcTokenAmount>>>, // (ledger_id, account) -> (balance, )
    }

    impl<A: PipelineAgent> LiquidatorAccount<A> {
        pub fn new(agent: Arc<A>) -> Self {
            Self {
                agent,
                cache: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl<A> IcrcAccountInfo for LiquidatorAccount<A>
    where
        A: PipelineAgent,
    {
        async fn get_balance(&self, ledger_id: Principal, account: Principal) -> Result<IcrcTokenAmount, String> {
            let account = Account {
                owner: account,
                subaccount: None,
            };

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

        async fn sync_balance(&self, ledger_id: Principal, account: Principal) -> Result<IcrcTokenAmount, String> {
            let balance = self.get_balance(ledger_id, account).await?;
            self.cache.lock().unwrap().insert((ledger_id, account), balance.clone());
            debug!("Balance synced for ledger {}. Balance: {}", ledger_id, balance.formatted());
            Ok(balance)
        }

        fn get_cached_balance(&self, ledger_id: Principal, account: Principal) -> Option<IcrcTokenAmount> {
            self.cache.lock().unwrap().get(&(ledger_id, account)).cloned()
        }
    }
}
