use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bip32::{DerivationPath, Seed, XPrv};
use bip39::{Language, Mnemonic};
use candid::Principal;
use ic_agent::Identity;
use ic_agent::identity::{BasicIdentity, Secp256k1Identity};
use icrc_ledger_types::icrc1::account::Account;
use k256::SecretKey;

use async_trait::async_trait;
use candid::{Encode, Nat};
use icrc_ledger_agent::Icrc1Agent;
use icrc_ledger_types::icrc1::{account::Subaccount, transfer::TransferArg};

use log::debug;

use crate::account::icp::model::IcrcAccountInfo;
use crate::{
    icrc_token::{icrc_token::IcrcToken, icrc_token_amount::IcrcTokenAmount},
    pipeline_agent::PipelineAgent,
};

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
    // Parse mnemonic
    let mnemonic = Mnemonic::parse_in(Language::English, mnemonic).map_err(|e| format!("invalid mnemonic: {e}"))?;

    // Seed
    let seed = mnemonic.to_seed("");

    // Master XPrv
    let master = XPrv::new(seed).map_err(|e| format!("xprv error: {e}"))?;

    // ICP-ish path
    let path_str = format!("m/44'/223'/{}'/0/{}", account, index);
    let derivation = DerivationPath::from_str(&path_str).map_err(|e| format!("path error: {e}"))?;

    // Walk path via derive_child
    let mut node = master;
    for cn in derivation.into_iter() {
        node = node.derive_child(cn).map_err(|e| format!("derive_child error: {e}"))?;
    }

    let raw = node.private_key().to_bytes();
    let sk = SecretKey::from_bytes(&raw).map_err(|e| format!("secret key error: {e}"))?;

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
