use ethers::types::{Address, H256, U256};
use serde::{Deserialize, Serialize};

// Represents a single swap leg in a multi-hop swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapLeg {
    // Token being sold
    pub token_in: Address,
    // Token being bought
    pub token_out: Address,
    // Amount of token_in to swap
    pub amount_in: U256,
    // Minimum amount of token_out to receive
    pub min_amount_out: U256,
    // Pool or path identifier
    pub pool_id: Option<String>,
}

// Multi-hop swap arguments for Hyperliquid
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiHopSwapArgs {
    // First swap: BTC -> USDC
    pub first_leg: SwapLeg,
    // Second swap: USDC -> USDT
    pub second_leg: SwapLeg,
    // Maximum slippage tolerance (basis points, e.g., 100 = 1%)
    pub max_slippage_bps: u16,
    // Deadline for the swap (unix timestamp)
    pub deadline: u64,
}

// Result of a single swap leg
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapLegResult {
    // Transaction hash on Hyperliquid
    pub tx_hash: H256,
    // Token that was sold
    pub token_in: Address,
    // Amount sold
    pub amount_in: U256,
    // Token that was bought
    pub token_out: Address,
    // Amount received
    pub amount_out: U256,
    // Effective price
    pub price: f64,
    // Gas used
    pub gas_used: U256,
    // Block number
    pub block_number: u64,
    // Timestamp
    pub timestamp: u64,
}

// Complete result of a multi-hop swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiHopSwapResult {
    // First swap result: BTC -> USDC
    pub first_leg: SwapLegResult,
    // Second swap result: USDC -> USDT
    pub second_leg: SwapLegResult,
    // Total gas used
    pub total_gas_used: U256,
    // Initial amount (BTC)
    pub initial_amount: U256,
    // Final amount (USDT)
    pub final_amount: U256,
    // Overall conversion rate (USDT per BTC)
    pub overall_rate: f64,
}

// Quote for a single swap leg
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapQuote {
    // Token being sold
    pub token_in: Address,
    // Token being bought
    pub token_out: Address,
    // Amount of token_in
    pub amount_in: U256,
    // Expected amount of token_out
    pub amount_out: U256,
    // Price impact (basis points)
    pub price_impact_bps: u16,
    // Estimated gas
    pub estimated_gas: U256,
    // Quote timestamp
    pub timestamp: u64,
}

// Quote for the full multi-hop swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiHopSwapQuote {
    // Quote for BTC -> USDC
    pub first_leg: SwapQuote,
    // Quote for USDC -> USDT
    pub second_leg: SwapQuote,
    // Initial BTC amount
    pub initial_amount: U256,
    // Expected final USDT amount
    pub final_amount: U256,
    // Overall price impact
    pub total_price_impact_bps: u16,
    // Total estimated gas
    pub total_estimated_gas: U256,
    // Quote valid until (unix timestamp)
    pub valid_until: u64,
}

// Token configuration for Hyperliquid
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidToken {
    // Token symbol (e.g., "BTC", "USDC", "USDT")
    pub symbol: String,
    // EVM address on Hyperliquid
    pub address: Address,
    // Number of decimals
    pub decimals: u8,
}

impl HyperliquidToken {
    pub fn new(symbol: String, address: Address, decimals: u8) -> Self {
        Self {
            symbol,
            address,
            decimals,
        }
    }

    // Convert a human-readable amount to raw token units
    pub fn to_raw_amount(&self, amount: f64) -> U256 {
        let raw = amount * 10_f64.powi(self.decimals as i32);
        U256::from(raw as u128)
    }

    // Convert raw token units to human-readable amount
    pub fn from_raw_amount(&self, raw: U256) -> f64 {
        let divisor = 10_f64.powi(self.decimals as i32);
        raw.as_u128() as f64 / divisor
    }
}

// Error types for Hyperliquid swaps
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HyperliquidSwapError {
    // Insufficient liquidity for the swap
    InsufficientLiquidity { token: Address, required: U256 },
    // Slippage exceeded maximum tolerance
    SlippageExceeded { expected: U256, received: U256 },
    // Deadline passed
    DeadlineExceeded { deadline: u64, current_time: u64 },
    // Transaction reverted
    TransactionReverted { tx_hash: H256, reason: String },
    // Network error
    NetworkError(String),
    // Invalid parameters
    InvalidParameters(String),
    // Approval failed
    ApprovalFailed { token: Address, spender: Address },
}

impl std::fmt::Display for HyperliquidSwapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HyperliquidSwapError::InsufficientLiquidity { token, required } => {
                write!(f, "Insufficient liquidity for token {:?}: required {}", token, required)
            }
            HyperliquidSwapError::SlippageExceeded { expected, received } => {
                write!(f, "Slippage exceeded: expected {}, received {}", expected, received)
            }
            HyperliquidSwapError::DeadlineExceeded { deadline, current_time } => {
                write!(f, "Deadline {} exceeded at {}", deadline, current_time)
            }
            HyperliquidSwapError::TransactionReverted { tx_hash, reason } => {
                write!(f, "Transaction {:?} reverted: {}", tx_hash, reason)
            }
            HyperliquidSwapError::NetworkError(e) => write!(f, "Network error: {}", e),
            HyperliquidSwapError::InvalidParameters(e) => write!(f, "Invalid parameters: {}", e),
            HyperliquidSwapError::ApprovalFailed { token, spender } => {
                write!(f, "Approval failed for token {:?} to spender {:?}", token, spender)
            }
        }
    }
}

impl std::error::Error for HyperliquidSwapError {}
