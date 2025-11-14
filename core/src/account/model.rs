#[derive(Clone, Debug)]
pub enum Chain {
    Icp,
    Evm { chain: String }, // "ETH", "ARB", ...
}

#[derive(Clone, Debug)]
pub struct ChainBalance {
    pub chain: String,
    pub symbol: String,
    pub amount_native: u128,
    pub decimals: u8,
}

#[derive(Clone, Debug)]
pub enum TxRef {
    IcpBlockIndex(String),
    EvmTxHash(String),
}
