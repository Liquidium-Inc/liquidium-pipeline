pub mod account {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use candid::{Encode, Nat, Principal};

    use crate::pipeline_agent::PipelineAgent;

    #[cfg_attr(test, mockall::automock)]
    #[async_trait]
    pub trait IcrcAccountInfo: Send + Sync {
        async fn get_balance(&self, ledger_id: Principal, account: Principal) -> Result<Nat, String>;
        async fn sync_balance(&self, ledger_id: Principal, account: Principal) -> Result<(), String>;
        fn get_cached_balance(&self, ledger_id: Principal, account: Principal) -> Option<Nat>;
    }

    pub struct LiquidatorAccount<A: PipelineAgent> {
        pub agent: Arc<A>,
        pub cache: Arc<Mutex<HashMap<(Principal, Principal), Nat>>>, // (ledger_id, account) -> balance
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
        async fn get_balance(&self, ledger_id: Principal, account: Principal) -> Result<Nat, String> {
            let balance = self
                .agent
                .call_query::<Nat>(&ledger_id, "icrc1_balance_of", Encode!(&account).unwrap())
                .await?;
            Ok(balance)
        }

        async fn sync_balance(&self, ledger_id: Principal, account: Principal) -> Result<(), String> {
            let balance = self.get_balance(ledger_id, account).await?;
            self.cache.lock().unwrap().insert((ledger_id, account), balance.clone());
            Ok(())
        }

        fn get_cached_balance(&self, ledger_id: Principal, account: Principal) -> Option<Nat> {
            self.cache.lock().unwrap().get(&(ledger_id, account)).cloned()
        }
    }
}
