use async_trait::async_trait;
use candid::Nat;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CollateralServiceTrait: Send + Sync {
    async fn calculate_received_collateral(&self) -> Nat;
}

#[derive(Default)]
pub struct CollateralService;

#[async_trait]
impl CollateralServiceTrait for CollateralService {
    async fn calculate_received_collateral(&self) -> Nat {
        todo!()
    }
}
