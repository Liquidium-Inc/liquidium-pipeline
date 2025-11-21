use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::backend::icp_backend::IcpBackend;
use crate::crypto::derivation::derive_evm_private_key;
use async_trait::async_trait;
use liquidium_pipeline_core::{
    account::actions::AccountInfo,
    tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
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

// Adapter that turns an IcpBackend + Account into a core::AccountInfo implementation.
pub struct IcpAccountInfoAdapter<B: IcpBackend> {
    backend: Arc<B>,
    account: Account,
    cache: Arc<Mutex<HashMap<(Principal, String), ChainTokenAmount>>>,
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
    async fn get_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, String> {
        if let Some(cached) = self.get_cached_balance(token) {
            return Ok(cached);
        }
        self.sync_balance(token).await
    }

    async fn sync_balance(&self, token: &ChainToken) -> Result<ChainTokenAmount, String> {
        match token {
            ChainToken::Icp { ledger, symbol, .. } => {
                let amount = self
                    .backend
                    .icrc1_balance(*ledger, self.account())
                    .await
                    .map_err(|e| format!("icp get_balance failed: {e}"))?;

                let balance = ChainTokenAmount {
                    token: token.clone(),
                    value: amount,
                };

                let mut cache = self.cache.lock().expect("icp balance cache poisoned");
                cache.insert((*ledger, symbol.clone()), balance.clone());

                Ok(balance)
            }
            _ => Err("IcpAccountInfoAdapter only supports ChainToken::Icp".to_string()),
        }
    }

    fn get_cached_balance(&self, token: &ChainToken) -> Option<ChainTokenAmount> {
        match token {
            ChainToken::Icp { ledger, symbol, .. } => {
                let cache = self.cache.lock().ok()?;
                cache.get(&(*ledger, symbol.clone())).cloned()
            }
            _ => None,
        }
    }
}
