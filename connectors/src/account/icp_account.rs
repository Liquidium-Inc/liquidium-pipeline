
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::backend::icp_backend::IcpBackend;
use crate::crypto::derivation::derive_evm_private_key;
use async_trait::async_trait;
use liquidium_pipeline_core::{
    account::{
        actions::AccountInfo,
        model::{Chain, ChainBalance},
    },
    tokens::chain_token::ChainToken,
};

use candid::Principal;
use ic_agent::Identity;
use ic_agent::identity::{BasicIdentity, Secp256k1Identity};
use icrc_ledger_types::icrc1::account::Account;

use icrc_ledger_types::icrc1::account::Subaccount;

pub fn create_identity_from_pem_file(pem_file: &str) -> Result<Box<dyn Identity>, String> {
    match BasicIdentity::from_pem_file(pem_file) {
        Ok(basic_identity) => Ok(Box::new(basic_identity)),
        Err(_) => match Secp256k1Identity::from_pem_file(pem_file) {
            Ok(secp256k1_identity) => Ok(Box::new(secp256k1_identity)),
            Err(err) => Err(format!(
                "Failed to create identity from pem file at {}. Unknown identity format. {}",
                pem_file, err
            )),
        },
    }
}

pub fn derive_icp_identity(mnemonic: &str, account: u32, index: u32) -> Result<Secp256k1Identity, String> {
    let sk = derive_evm_private_key(mnemonic, account, index)?;
    Ok(Secp256k1Identity::from_private_key(sk))
}

pub fn derive_icp_principal(mnemonic: &str, account: u32, index: u32) -> Result<Principal, String> {
    let id = derive_icp_identity(mnemonic, account, index)?;
    id.sender().map_err(|e| format!("could not decode principal: {e}"))
}

pub fn derive_icp_account(
    mnemonic: &str,
    account: u32,
    index: u32,
    subaccount: Option<[u8; 32]>,
) -> Result<Account, String> {
    let owner = derive_icp_principal(mnemonic, account, index)?;
    Ok(Account { owner, subaccount })
}

pub const RECOVERY_ACCOUNT: &Subaccount = &[
    0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

// pub struct LiquidatorAccount<A: PipelineAgent> {
//     pub agent: Arc<A>,
//     pub cache: Arc<Mutex<HashMap<(Principal, String), IcrcTokenAmount>>>, // (ledger_id, account) -> (balance, )
//     pub sub_account: Option<Subaccount>,
// }

// impl<A: PipelineAgent> LiquidatorAccount<A> {
//     pub fn new(agent: Arc<A>) -> Self {
//         Self {
//             agent,
//             cache: Arc::new(Mutex::new(HashMap::new())),
//             sub_account: None,
//         }
//     }

//     pub fn set_sub_account(&mut self, account: Subaccount) {
//         self.sub_account = Some(account);
//     }
// }

// #[async_trait]
// impl<A> IcrcAccountInfo for LiquidatorAccount<A>
// where
//     A: PipelineAgent,
// {
//     async fn get_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String> {
//         let icrc_token = ChainToken::from_principal(ledger_id, Arc::new(self.agent.agent())).await;

//         let balance = self
//             .agent
//             .call_query::<Nat>(&ledger_id, "icrc1_balance_of", Encode!(&account).unwrap())
//             .await?;

//         Ok(IcrcTokenAmount {
//             token: icrc_token,
//             value: balance,
//         })
//     }

//     async fn sync_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String> {
//         let balance = self.get_balance(ledger_id, account).await?;
//         self.cache
//             .lock()
//             .unwrap()
//             .insert((ledger_id, account.to_string()), balance.clone());

//         debug!(
//             "Balance synced for ledger {}. Balance: {}",
//             ledger_id,
//             balance.formatted()
//         );
//         Ok(balance)
//     }

//     fn get_cached_balance(&self, ledger_id: Principal, account: Account) -> Option<IcrcTokenAmount> {
//         let balance = self
//             .cache
//             .lock()
//             .unwrap()
//             .get(&(ledger_id, account.to_string()))
//             .cloned();
//         balance
//     }
// }

// #[cfg_attr(test, mockall::automock)]
// #[async_trait]
// pub trait IcrcAccountActions: Send + Sync {
//     async fn transfer(&self, amount: IcrcTokenAmount, to: Account) -> Result<String, String>;
//     async fn transfer_from_subaccount(&self, amount: IcrcTokenAmount, to: Account) -> Result<String, String>;
// }

// #[async_trait]
// impl<A: PipelineAgent> IcrcAccountActions for LiquidatorAccount<A> {
//     async fn transfer_from_subaccount(&self, token_amount: IcrcTokenAmount, to: Account) -> Result<String, String> {
//         let icrc_agent = Icrc1Agent {
//             agent: self.agent.agent(),
//             ledger_canister_id: token_amount.token.ledger,
//         };
//         icrc_agent
//             .transfer(TransferArg {
//                 from_subaccount: self.sub_account,
//                 to,
//                 fee: None,
//                 created_at_time: None,
//                 memo: None,
//                 amount: token_amount.value - token_amount.token.fee,
//             })
//             .await
//             .map_err(|e| format!("Withdraw error {:?}", e))?
//             .map_err(|e| format!("Withdraw error {:?}", e))
//             .map(|res| res.to_string().replace("_", ""))
//     }

//     async fn transfer(&self, token_amount: IcrcTokenAmount, to: Account) -> Result<String, String> {
//         let icrc_agent = Icrc1Agent {
//             agent: self.agent.agent(),
//             ledger_canister_id: token_amount.token.ledger,
//         };
//         icrc_agent
//             .transfer(TransferArg {
//                 from_subaccount: None,
//                 to,
//                 fee: None,
//                 created_at_time: None,
//                 memo: None,
//                 amount: token_amount.value - token_amount.token.fee,
//             })
//             .await
//             .map_err(|e| format!("Withdraw error {:?}", e))?
//             .map_err(|e| format!("Withdraw error {:?}", e))
//             .map(|res| res.to_string().replace("_", ""))
//     }
// }

// Adapter that turns an IcpBackend + Account into a core::AccountInfo implementation.
pub struct IcpAccountInfoAdapter<B: IcpBackend> {
    backend: Arc<B>,
    account: Account,
    cache: Arc<Mutex<HashMap<(Principal, String), ChainBalance>>>,
}

impl<B: IcpBackend> IcpAccountInfoAdapter<B> {
    pub fn new(backend: Arc<B>, account: Account) -> Self {
        Self {
            backend,
            account,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn account(&self) -> &Account {
        &self.account
    }
}

#[async_trait]
impl<B> AccountInfo for IcpAccountInfoAdapter<B>
where
    B: IcpBackend + Send + Sync,
{
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        if let Some(cached) = self.get_cached_balance(token) {
            return Ok(cached);
        }
        self.sync_balance(token).await
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainBalance, String> {
        match token {
            ChainToken::Icp {
                ledger,
                symbol,
                decimals,
            } => {
                let amount = self
                    .backend
                    .icrc1_balance(*ledger, self.account())
                    .await
                    .map_err(|e| format!("icp get_balance failed: {e}"))?;

                let balance = ChainBalance {
                    chain: Chain::Icp,
                    amount_native: amount,
                    decimals: *decimals,
                    symbol: symbol.clone(),
                };

                // Cache key: (ledger principal, symbol)
                {
                    let mut cache = self.cache.lock().expect("icp balance cache poisoned");
                    cache.insert((*ledger, symbol.clone()), balance.clone());
                }

                Ok(balance)
            }
            _ => Err("IcpAccountInfoAdapter only supports ChainToken::Icp".to_string()),
        }
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainBalance> {
        match token {
            ChainToken::Icp { ledger, symbol, .. } => {
                let cache = self.cache.lock().ok()?;
                cache.get(&(*ledger, symbol.clone())).cloned()
            }
            _ => None,
        }
    }
}
