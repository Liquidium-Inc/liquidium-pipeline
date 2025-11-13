use alloy::primitives::Address;
use candid::Principal;

use crate::config::ConfigTrait;
use crate::swappers::hyperliquid_types::HyperliquidToken;

// Creates standard Hyperliquid token definitions from configuration
pub struct HyperliquidTokenFactory;

impl HyperliquidTokenFactory {
    // Create a token from config using its ledger canister ID
    pub fn create_token<C: ConfigTrait>(
        config: &C,
        ledger_id: &Principal,
        symbol: String,
        decimals: u8,
    ) -> Result<HyperliquidToken, String> {
        let address = config.get_erc20_address_by_ledger(ledger_id)?;

        Ok(HyperliquidToken::new(symbol, address, decimals))
    }
}
