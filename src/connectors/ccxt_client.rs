use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct DepositAddress {
    pub asset: String,
    pub network: Option<String>,
    pub address: String,
    pub tag: Option<String>, // for exchanges that need memo/tag
}

#[derive(Debug, Clone)]
pub struct WithdrawalReceipt {
    pub asset: String,
    pub network: Option<String>,
    pub amount: f64,
    pub txid: Option<String>,
    pub internal_id: Option<String>,
}

#[async_trait]
pub trait CcxtClient: Send + Sync {
    // trading
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String>;

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String>;

    // deposits
    async fn get_deposit_address(&self, asset: &str, network: Option<&str>) -> Result<DepositAddress, String>;

    // withdrawals
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        tag: Option<&str>,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String>;
}
