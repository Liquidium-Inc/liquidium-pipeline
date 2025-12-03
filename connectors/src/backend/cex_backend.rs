use alloy::providers::mock;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct DepositAddress {
    pub asset: String,
    pub network: String,
    pub address: String,
    pub tag: Option<String>, // for exchanges that need memo/tag
}

#[derive(Debug, Clone)]
pub struct WithdrawalReceipt {
    pub asset: String,
    pub network: String,
    pub amount: f64,
    pub txid: Option<String>,
    pub internal_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithdrawStatus {
    Pending,
    Completed,
    Failed,
    Canceled,
    Unknown,
}

#[mockall::automock]
#[async_trait]
pub trait CexBackend: Send + Sync {
    // trading
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String>;

    // deposits
    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String>;

    // withdrawals
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String>;

    // balance
    async fn get_balance(&self, asset: &str) -> Result<f64, String>;

    // withdrawal status
    async fn get_withdraw_status_by_id(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatus, String>;
}
