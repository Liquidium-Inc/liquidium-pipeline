use candid::Nat;

#[derive(Debug, Clone)]
pub struct SwapResult {
    pub received_asset: String,
    pub received_amount: Nat,
}
