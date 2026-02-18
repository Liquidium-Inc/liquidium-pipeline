use async_trait::async_trait;
use liquidium_pipeline_core::error::AppError;

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

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

#[derive(Debug, Clone)]
pub struct SwapFillReport {
    /// Actual input amount consumed by the exchange for this order.
    pub input_consumed: f64,
    /// Actual output amount received from the exchange for this order.
    pub output_received: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyOrderInputMode {
    Auto,
    QuoteOrderQty,
    BaseQuantity,
}

#[derive(Debug, Clone)]
pub struct SwapExecutionOptions {
    /// Optional deterministic client order id for retry-safe submissions.
    pub client_order_id: Option<String>,
    /// Buy-side input mode selection.
    pub buy_mode: BuyOrderInputMode,
    /// Optional quote overspend cap (in bps) used by base-quantity buy mode.
    pub max_quote_overspend_bps: Option<f64>,
}

impl Default for SwapExecutionOptions {
    fn default() -> Self {
        Self {
            client_order_id: None,
            buy_mode: BuyOrderInputMode::Auto,
            max_quote_overspend_bps: None,
        }
    }
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
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, AppError>;

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, AppError>;

    async fn execute_swap_detailed(&self, market: &str, side: &str, amount_in: f64)
    -> Result<SwapFillReport, AppError>;

    async fn execute_swap_detailed_with_options(
        &self,
        market: &str,
        side: &str,
        amount_in: f64,
        options: SwapExecutionOptions,
    ) -> Result<SwapFillReport, AppError>;

    async fn get_orderbook(&self, market: &str, limit: Option<u32>) -> Result<OrderBook, AppError>;

    // deposits
    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, AppError>;

    // withdrawals
    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, AppError>;

    // balance
    async fn get_balance(&self, asset: &str) -> Result<f64, AppError>;

    // withdrawal status
    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, AppError>;
}
