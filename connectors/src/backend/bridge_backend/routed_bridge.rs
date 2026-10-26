use std::sync::Arc;

use async_trait::async_trait;

use liquidium_pipeline_core::outbound::OutboundIntent;

use super::{
    BridgeBackend, BridgeDestination, BridgeFeeBudget, BridgeRequest, BridgeStatus, BridgeSubmission,
    BridgeSubmissionCandidate, DepositNotice,
};

/// Keeps provider-specific submission handles with the backend that issued them.
pub struct RoutedBridgeBackend {
    pub evm: Arc<dyn BridgeBackend>,
    pub solana: Arc<dyn BridgeBackend>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::bridge_backend::MockBridgeBackend;

    #[tokio::test]
    async fn persisted_sol_handle_and_evm_handle_use_their_own_backend() {
        let mut solana = MockBridgeBackend::new();
        solana.expect_get_bridge_status().with(mockall::predicate::eq("cksol-withdraw:8"))
            .times(1).returning(|_| Ok(BridgeStatus::Completed));
        solana.expect_get_source_balance().with(
            mockall::predicate::eq("ckSOL"), mockall::predicate::eq("ICP"), mockall::predicate::eq("wallet"),
        ).times(1).returning(|_, _, _| Ok(1.5));
        let mut evm = MockBridgeBackend::new();
        evm.expect_get_bridge_status().with(mockall::predicate::eq("legacy-evm-handle"))
            .times(1).returning(|_| Ok(BridgeStatus::Pending));
        let backend = RoutedBridgeBackend { evm: Arc::new(evm), solana: Arc::new(solana) };
        assert_eq!(backend.get_bridge_status("cksol-withdraw:8").await.unwrap(), BridgeStatus::Completed);
        assert_eq!(backend.get_bridge_status("legacy-evm-handle").await.unwrap(), BridgeStatus::Pending);
        assert_eq!(backend.get_source_balance("ckSOL", "ICP", "wallet").await.unwrap(), 1.5);
    }
}

impl RoutedBridgeBackend {
    fn source(&self, asset: &str, chain: &str) -> &dyn BridgeBackend {
        if (asset.eq_ignore_ascii_case("ckSOL") && chain.eq_ignore_ascii_case("ICP"))
            || (asset.eq_ignore_ascii_case("SOL") && chain.eq_ignore_ascii_case("SOL"))
        {
            self.solana.as_ref()
        } else {
            self.evm.as_ref()
        }
    }
}

#[async_trait]
impl BridgeBackend for RoutedBridgeBackend {
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        self.source(asset, chain).get_source_balance(asset, chain, address).await
    }

    async fn get_source_fee_budget(&self, asset: &str, chain: &str, target: &str) -> Result<f64, String> {
        self.source(asset, chain).get_source_fee_budget(asset, chain, target).await
    }

    async fn get_destination_fee_budget(&self, asset: &str, chain: &str, target: &str) -> Result<f64, String> {
        self.source(asset, chain).get_destination_fee_budget(asset, chain, target).await
    }

    async fn get_fee_budget(&self, asset: &str, chain: &str, target: &str) -> Result<BridgeFeeBudget, String> {
        self.source(asset, chain).get_fee_budget(asset, chain, target).await
    }

    async fn get_minimum_bridge_amount(&self, asset: &str, chain: &str, target: &str) -> Result<f64, String> {
        self.source(asset, chain).get_minimum_bridge_amount(asset, chain, target).await
    }

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        self.source(&request.asset, &request.source_chain).submit_bridge(request).await
    }

    async fn deposit_send(&self, request: &BridgeRequest) -> Result<OutboundIntent, String> {
        self.source(&request.asset, &request.source_chain).deposit_send(request).await
    }

    async fn notify_deposit(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
        funding_receipt: &str,
    ) -> Result<DepositNotice, String> {
        self.source(asset, chain)
            .notify_deposit(asset, chain, destination, funding_receipt)
            .await
    }

    async fn prepare_submission(&self, request: &BridgeRequest) -> Result<Option<u64>, String> {
        self.source(&request.asset, &request.source_chain)
            .prepare_submission(request)
            .await
    }

    async fn find_submissions(
        &self,
        request: &BridgeRequest,
        cursor: u64,
        claimed_bridge_ids: &[String],
    ) -> Result<Vec<BridgeSubmissionCandidate>, String> {
        self.source(&request.asset, &request.source_chain)
            .find_submissions(request, cursor, claimed_bridge_ids)
            .await
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        // Existing WAL handles must survive the addition of another provider.
        // ckSOL owns these prefixes; all pre-existing ckETH handles stay with EVM.
        if bridge_id.starts_with("cksol-") {
            self.solana.get_bridge_status(bridge_id).await
        } else {
            self.evm.get_bridge_status(bridge_id).await
        }
    }
}
