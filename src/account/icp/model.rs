use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;

use crate::icrc_token::icrc_token_amount::IcrcTokenAmount;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcrcAccountInfo: Send + Sync {
    async fn get_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String>;
    async fn sync_balance(&self, ledger_id: Principal, account: Account) -> Result<IcrcTokenAmount, String>;
    fn get_cached_balance(&self, ledger_id: Principal, account: Account) -> Option<IcrcTokenAmount>;
}
