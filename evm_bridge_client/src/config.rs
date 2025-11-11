// Configuration structures for EVM client

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub rpc_url: String,
    pub chain_id: u64,
}

#[derive(Debug, Clone)]
pub struct TxPolicyConfig {
    // Max fee per gas in wei (None = fetch from network)
    pub max_fee_per_gas_wei: Option<u128>,
    // Max priority fee per gas in wei (None = fetch from network)
    pub max_priority_fee_per_gas_wei: Option<u128>,
    // Gas limit multiplier in basis points (10000 = 100%)
    pub gas_limit_multiplier_bps: u32,
    // Maximum transaction retry attempts
    pub max_retries: u32,
    // Number of blocks to wait for confirmation
    pub confirm_blocks: u64,
    // Timeout for receipt polling in seconds
    pub receipt_timeout_secs: u64,
}

impl Default for TxPolicyConfig {
    fn default() -> Self {
        Self {
            max_fee_per_gas_wei: None,
            max_priority_fee_per_gas_wei: None,
            gas_limit_multiplier_bps: 12000, // 120%
            max_retries: 3,
            confirm_blocks: 1,
            receipt_timeout_secs: 300,
        }
    }
}
