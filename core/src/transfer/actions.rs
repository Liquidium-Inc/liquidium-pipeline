use async_trait::async_trait;
use candid::Nat;

use crate::account::model::ChainAccount;
use crate::tokens::chain_token::ChainToken;

#[mockall::automock]
#[async_trait]
pub trait TransferActions: Send + Sync {
    async fn transfer(&self, token: &ChainToken, to: &ChainAccount, amount_native: Nat) -> Result<String, String>; // return tx hash/id

    // Optional ICRC-2-style approval for the current account (used to bump ledger activity).
    async fn approve(&self, token: &ChainToken, spender: &ChainAccount, amount_native: Nat) -> Result<String, String>;
}
