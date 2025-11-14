#[derive(Clone, Debug)]
pub enum Chain {
    Icp,
    Evm { chain: String }, // "ETH", "ARB", ...
}
