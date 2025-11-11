use async_trait::async_trait;
use ethers::prelude::*;
use std::sync::Arc;

use super::hyperliquid_types::{
    HyperliquidSwapError, HyperliquidToken, MultiHopSwapArgs, MultiHopSwapQuote, MultiHopSwapResult, SwapLeg,
    SwapLegResult, SwapQuote,
};

// Trait for executing multi-hop swaps on Hyperliquid
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait HyperliquidSwapInterface: Send + Sync {
    // Get a quote for a multi-hop swap (BTC -> USDC -> USDT)
    async fn get_multi_hop_quote(
        &self,
        btc_amount: U256,
        max_slippage_bps: u16,
    ) -> Result<MultiHopSwapQuote, HyperliquidSwapError>;

    // Execute a multi-hop swap (BTC -> USDC -> USDT)
    async fn execute_multi_hop_swap(
        &self,
        swap_args: MultiHopSwapArgs,
    ) -> Result<MultiHopSwapResult, HyperliquidSwapError>;

    // Get a quote for a single swap leg
    async fn get_swap_quote(
        &self,
        token_in: &HyperliquidToken,
        token_out: &HyperliquidToken,
        amount_in: U256,
    ) -> Result<SwapQuote, HyperliquidSwapError>;

    // Execute a single swap leg
    async fn execute_swap(&self, swap_leg: &SwapLeg) -> Result<SwapLegResult, HyperliquidSwapError>;
}

// Implementation of Hyperliquid swapper
pub struct HyperliquidSwapper {
    // EVM provider for Hyperliquid
    provider: Arc<Provider<Http>>,
    // Wallet for signing transactions
    wallet: LocalWallet,
    // BTC token configuration
    btc_token: HyperliquidToken,
    // USDC token configuration
    usdc_token: HyperliquidToken,
    // USDT token configuration
    usdt_token: HyperliquidToken,
    // DEX contract address (e.g., Uniswap V2/V3 router)
    dex_router: Address,
    // Chain ID
    chain_id: u64,
}

impl HyperliquidSwapper {
    // Create a new Hyperliquid swapper
    pub fn new(
        rpc_url: String,
        wallet_private_key: String,
        btc_token: HyperliquidToken,
        usdc_token: HyperliquidToken,
        usdt_token: HyperliquidToken,
        dex_router: Address,
        chain_id: u64,
    ) -> Result<Self, HyperliquidSwapError> {
        let provider = Provider::<Http>::try_from(rpc_url)
            .map_err(|e| HyperliquidSwapError::NetworkError(format!("Failed to create provider: {}", e)))?;

        let wallet: LocalWallet = wallet_private_key
            .parse()
            .map_err(|e| HyperliquidSwapError::InvalidParameters(format!("Invalid private key: {}", e)))?;

        Ok(Self {
            provider: Arc::new(provider),
            wallet,
            btc_token,
            usdc_token,
            usdt_token,
            dex_router,
            chain_id,
        })
    }

    // Approve a token for the DEX router
    async fn approve_token(&self, token_address: Address, amount: U256) -> Result<(), HyperliquidSwapError> {
        log::info!("Approving token {:?} for amount {}", token_address, amount);

        // TODO: Implement actual ERC20 approve call
        // This would use ethers to call the approve function on the token contract

        Ok(())
    }

    // Wait for a transaction to be confirmed
    async fn wait_for_tx(&self, tx_hash: H256) -> Result<TransactionReceipt, HyperliquidSwapError> {
        log::info!("Waiting for transaction {:?}", tx_hash);

        // TODO: Implement actual transaction waiting logic
        // This would use the provider to wait for transaction confirmation

        Err(HyperliquidSwapError::NetworkError(
            "Transaction waiting not yet implemented".to_string(),
        ))
    }

    // Calculate the minimum amount out based on slippage tolerance
    fn calculate_min_amount_out(&self, expected_amount: U256, slippage_bps: u16) -> U256 {
        let slippage_factor = U256::from(10000 - slippage_bps);
        expected_amount * slippage_factor / U256::from(10000)
    }
}

#[async_trait]
impl HyperliquidSwapInterface for HyperliquidSwapper {
    async fn get_multi_hop_quote(
        &self,
        btc_amount: U256,
        _max_slippage_bps: u16,
    ) -> Result<MultiHopSwapQuote, HyperliquidSwapError> {
        log::info!(
            "Getting multi-hop quote for {} BTC",
            self.btc_token.from_raw_amount(btc_amount)
        );

        // Get quote for first leg: BTC -> USDC
        let first_leg_quote = self
            .get_swap_quote(&self.btc_token, &self.usdc_token, btc_amount)
            .await?;

        // Get quote for second leg: USDC -> USDT
        let second_leg_quote = self
            .get_swap_quote(&self.usdc_token, &self.usdt_token, first_leg_quote.amount_out)
            .await?;

        let total_price_impact_bps = first_leg_quote.price_impact_bps + second_leg_quote.price_impact_bps.clone();
        let total_estimated_gas = first_leg_quote.estimated_gas + second_leg_quote.estimated_gas.clone();

        Ok(MultiHopSwapQuote {
            first_leg: first_leg_quote,
            second_leg: second_leg_quote.clone(),
            initial_amount: btc_amount,
            final_amount: second_leg_quote.amount_out,
            total_price_impact_bps,
            total_estimated_gas,
            valid_until: chrono::Utc::now().timestamp() as u64 + 300, // 5 minutes
        })
    }

    async fn execute_multi_hop_swap(
        &self,
        swap_args: MultiHopSwapArgs,
    ) -> Result<MultiHopSwapResult, HyperliquidSwapError> {
        log::info!("Executing multi-hop swap: BTC -> USDC -> USDT");

        // Check deadline
        let current_time = chrono::Utc::now().timestamp() as u64;
        if current_time > swap_args.deadline {
            return Err(HyperliquidSwapError::DeadlineExceeded {
                deadline: swap_args.deadline,
                current_time,
            });
        }

        // Execute first leg: BTC -> USDC
        log::info!("Executing first leg: BTC -> USDC");
        let first_leg_result = self.execute_swap(&swap_args.first_leg).await?;

        // Verify we got enough USDC
        if first_leg_result.amount_out < swap_args.second_leg.amount_in {
            return Err(HyperliquidSwapError::InsufficientLiquidity {
                token: self.usdc_token.address,
                required: swap_args.second_leg.amount_in,
            });
        }

        // Execute second leg: USDC -> USDT
        log::info!("Executing second leg: USDC -> USDT");
        let mut second_leg = swap_args.second_leg;
        // Update the amount_in based on what we actually received
        second_leg.amount_in = first_leg_result.amount_out;

        let second_leg_result = self.execute_swap(&second_leg).await?;

        let total_gas_used = first_leg_result.gas_used + second_leg_result.gas_used;
        let overall_rate = if swap_args.first_leg.amount_in > U256::zero() {
            second_leg_result.amount_out.as_u128() as f64 / swap_args.first_leg.amount_in.as_u128() as f64
        } else {
            0.0
        };

        Ok(MultiHopSwapResult {
            first_leg: first_leg_result,
            second_leg: second_leg_result.clone(),
            total_gas_used,
            initial_amount: swap_args.first_leg.amount_in,
            final_amount: second_leg_result.amount_out,
            overall_rate,
        })
    }

    async fn get_swap_quote(
        &self,
        token_in: &HyperliquidToken,
        token_out: &HyperliquidToken,
        amount_in: U256,
    ) -> Result<SwapQuote, HyperliquidSwapError> {
        log::info!(
            "Getting quote: {} {} -> {}",
            token_in.from_raw_amount(amount_in),
            token_in.symbol,
            token_out.symbol
        );

        // TODO: Implement actual quote fetching from Hyperliquid DEX
        // This would call the DEX router's getAmountsOut function or equivalent
        // For now, return a placeholder error

        Err(HyperliquidSwapError::NetworkError(
            "Quote fetching not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected implementation:
        // 1. Call DEX router to get expected output amount
        // 2. Calculate price impact
        // 3. Estimate gas
        // 4. Return SwapQuote
    }

    async fn execute_swap(&self, swap_leg: &SwapLeg) -> Result<SwapLegResult, HyperliquidSwapError> {
        log::info!("Executing swap: {:?} -> {:?}", swap_leg.token_in, swap_leg.token_out);

        // Step 1: Approve token
        self.approve_token(swap_leg.token_in, swap_leg.amount_in).await?;

        // TODO: Implement actual swap execution via Hyperliquid DEX
        // This would:
        // 1. Build the swap transaction
        // 2. Sign it with the wallet
        // 3. Send to the network
        // 4. Wait for confirmation
        // 5. Parse the result

        Err(HyperliquidSwapError::NetworkError(
            "Swap execution not yet implemented - requires Hyperliquid SDK integration".to_string(),
        ))

        // Expected implementation:
        // 1. Call DEX router's swap function
        // 2. Wait for transaction to be mined
        // 3. Parse logs to get actual amounts
        // 4. Return SwapLegResult
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_swapper_creation() {
        // TODO: Add tests when implementation is complete
    }

    #[test]
    fn test_min_amount_calculation() {
        let swapper = create_test_swapper();

        // Test 1% slippage (100 bps)
        let amount = U256::from(1000000u64);
        let min_amount = swapper.calculate_min_amount_out(amount, 100);
        assert_eq!(min_amount, U256::from(990000u64)); // 99% of original

        // Test 0.5% slippage (50 bps)
        let min_amount = swapper.calculate_min_amount_out(amount, 50);
        assert_eq!(min_amount, U256::from(995000u64)); // 99.5% of original
    }

    fn create_test_swapper() -> HyperliquidSwapper {
        let btc_token = HyperliquidToken::new(
            "BTC".to_string(),
            "0x0000000000000000000000000000000000000001".parse().unwrap(),
            8,
        );
        let usdc_token = HyperliquidToken::new(
            "USDC".to_string(),
            "0x0000000000000000000000000000000000000002".parse().unwrap(),
            6,
        );
        let usdt_token = HyperliquidToken::new(
            "USDT".to_string(),
            "0x0000000000000000000000000000000000000003".parse().unwrap(),
            6,
        );

        HyperliquidSwapper {
            provider: Arc::new(Provider::<Http>::try_from("http://localhost:8545").unwrap()),
            wallet: "0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap(),
            btc_token,
            usdc_token,
            usdt_token,
            dex_router: "0x0000000000000000000000000000000000000004".parse().unwrap(),
            chain_id: 1,
        }
    }
}
