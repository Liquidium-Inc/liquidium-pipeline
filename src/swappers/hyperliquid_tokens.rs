use alloy::primitives::Address;
use std::str::FromStr;

use crate::config::ConfigTrait;
use crate::swappers::hyperliquid_types::HyperliquidToken;

/// Creates standard Hyperliquid token definitions from configuration
pub struct HyperliquidTokenFactory;

impl HyperliquidTokenFactory {
    /// Create BTC token from config
    pub fn create_btc<C: ConfigTrait>(config: &C) -> Result<HyperliquidToken, String> {
        let address = config
            .get_hyperliquid_btc_address()
            .ok_or("Missing HYPERLIQUID_BTC_ADDRESS")?;

        let address = Address::from_str(&address)
            .map_err(|e| format!("Invalid BTC address: {}", e))?;

        Ok(HyperliquidToken::new("BTC".to_string(), address, 8))
    }

    /// Create USDC token from config
    pub fn create_usdc<C: ConfigTrait>(config: &C) -> Result<HyperliquidToken, String> {
        let address = config
            .get_hyperliquid_usdc_address()
            .ok_or("Missing HYPERLIQUID_USDC_ADDRESS")?;

        let address = Address::from_str(&address)
            .map_err(|e| format!("Invalid USDC address: {}", e))?;

        Ok(HyperliquidToken::new("USDC".to_string(), address, 6))
    }

    /// Create USDT token from config
    pub fn create_usdt<C: ConfigTrait>(config: &C) -> Result<HyperliquidToken, String> {
        let address = config
            .get_hyperliquid_usdt_address()
            .ok_or("Missing HYPERLIQUID_USDT_ADDRESS")?;

        let address = Address::from_str(&address)
            .map_err(|e| format!("Invalid USDT address: {}", e))?;

        Ok(HyperliquidToken::new("USDT".to_string(), address, 6))
    }

    /// Create all three tokens (BTC, USDC, USDT) from config
    pub fn create_all<C: ConfigTrait>(
        config: &C,
    ) -> Result<(HyperliquidToken, HyperliquidToken, HyperliquidToken), String> {
        let btc = Self::create_btc(config)?;
        let usdc = Self::create_usdc(config)?;
        let usdt = Self::create_usdt(config)?;

        Ok((btc, usdc, usdt))
    }
}