use std::sync::Arc;

use async_trait::async_trait;

use super::{
    BridgeBackend, BridgeDestination, BridgeError, BridgeFeeBudget, BridgeProvider, BridgeRequest, BridgeSend,
    BridgeStatus, BridgeSubmission, BridgeSubmissionCandidate, DepositNotice, ckbtc_bridge::CKBTC_HANDLE_PREFIX,
    resolve_route_by_source,
};

/// Keeps provider-specific submission handles with the backend that issued them.
pub struct RoutedBridgeBackend {
    pub evm: Arc<dyn BridgeBackend>,
    pub solana: Arc<dyn BridgeBackend>,
    pub btc: Arc<dyn BridgeBackend>,
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
        let mut btc = MockBridgeBackend::new();
        btc.expect_get_bridge_status().with(mockall::predicate::eq("ckbtc-retrieve:9"))
            .times(1).returning(|_| Ok(BridgeStatus::Pending));
        btc.expect_get_bridge_status().with(mockall::predicate::eq("ckbtc-mint:81:abc"))
            .times(1).returning(|_| Ok(BridgeStatus::Completed));
        btc.expect_get_minimum_bridge_amount().with(
            mockall::predicate::eq("ckBTC"), mockall::predicate::eq("ICP"), mockall::predicate::eq("BTC"),
        ).times(1).returning(|_, _, _| Ok(0.0005));
        let backend = RoutedBridgeBackend { evm: Arc::new(evm), solana: Arc::new(solana), btc: Arc::new(btc) };
        assert_eq!(backend.get_bridge_status("ckbtc-retrieve:9").await.unwrap(), BridgeStatus::Pending);
        assert_eq!(backend.get_bridge_status("ckbtc-mint:81:abc").await.unwrap(), BridgeStatus::Completed);
        assert_eq!(backend.get_minimum_bridge_amount("ckBTC", "ICP", "BTC").await.unwrap(), 0.0005);
        assert_eq!(backend.get_bridge_status("cksol-withdraw:8").await.unwrap(), BridgeStatus::Completed);
        assert_eq!(backend.get_bridge_status("legacy-evm-handle").await.unwrap(), BridgeStatus::Pending);
        assert_eq!(backend.get_source_balance("ckSOL", "ICP", "wallet").await.unwrap(), 1.5);
    }
}

impl RoutedBridgeBackend {
    /// The catalog names the minter behind a source. A pair outside the
    /// catalog, and every pre-catalog handle, stays with the EVM backend.
    fn source(&self, asset: &str, chain: &str) -> &dyn BridgeBackend {
        match resolve_route_by_source(asset, chain).map(|route| route.route_kind.provider()) {
            Some(BridgeProvider::CkSolMinter) => self.solana.as_ref(),
            Some(BridgeProvider::CkBtcMinter) => self.btc.as_ref(),
            Some(BridgeProvider::CkEthMinter) | None => self.evm.as_ref(),
        }
    }
}

#[async_trait]
impl BridgeBackend for RoutedBridgeBackend {
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        self.source(asset, chain).get_source_balance(asset, chain, address).await
    }

    async fn get_gas_balance(&self, chain: &str, address: &str) -> Result<f64, String> {
        // Gas is native to a chain, so the chain alone picks the backend.
        let native = match chain.to_ascii_uppercase().as_str() {
            "SOL" => "SOL",
            "BTC" => "BTC",
            _ => "ETH",
        };
        self.source(native, chain).get_gas_balance(chain, address).await
    }

    async fn get_source_fee_budget(&self, asset: &str, chain: &str, target: &str) -> Result<f64, String> {
        self.source(asset, chain).get_source_fee_budget(asset, chain, target).await
    }

    async fn source_fee_due(
        &self,
        asset: &str,
        chain: &str,
        target: &str,
        source_address: &str,
        amount: f64,
    ) -> Result<f64, String> {
        self.source(asset, chain)
            .source_fee_due(asset, chain, target, source_address, amount)
            .await
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

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, BridgeError> {
        self.source(&request.asset, &request.source_chain).submit_bridge(request).await
    }

    async fn deposit_address(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
    ) -> Result<String, String> {
        self.source(asset, chain).deposit_address(asset, chain, destination).await
    }

    async fn prepare_bridge_send(&self, request: &BridgeRequest) -> Result<BridgeSend, BridgeError> {
        self.source(&request.asset, &request.source_chain)
            .prepare_bridge_send(request)
            .await
    }

    async fn track_deposit(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
        handle: &str,
        receipt: &str,
    ) -> Result<DepositNotice, String> {
        self.source(asset, chain)
            .track_deposit(asset, chain, destination, handle, receipt)
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

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, BridgeError> {
        // Existing WAL handles must survive the addition of another provider.
        // ckSOL owns these prefixes; all pre-existing ckETH handles stay with EVM.
        if bridge_id.starts_with("cksol-") {
            self.solana.get_bridge_status(bridge_id).await
        } else if bridge_id.starts_with(CKBTC_HANDLE_PREFIX) {
            self.btc.get_bridge_status(bridge_id).await
        } else {
            self.evm.get_bridge_status(bridge_id).await
        }
    }
}
