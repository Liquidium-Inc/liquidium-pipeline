use async_trait::async_trait;
use candid::Principal;
use icrc_ledger_types::icrc1::account::Account;
use std::str::FromStr;

use super::{
    BridgeBackend, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus, BridgeSubmission,
    catalog::BRIDGE_ROUTE_CATALOG, resolve_route, validate_destination_for_route,
};

pub struct SolanaBridgeBackend {
    pub bridge_ic_owner_principal: Principal,
}

impl SolanaBridgeBackend {
    pub fn new(bridge_ic_owner_principal: Principal) -> Self {
        Self {
            bridge_ic_owner_principal,
        }
    }

    fn parse_source_icp_account(source_address: &str) -> Result<Account, String> {
        if let Ok(account) = Account::from_str(source_address.trim()) {
            return Ok(account);
        }
        if let Ok(owner) = Principal::from_str(source_address.trim()) {
            return Ok(Account {
                owner,
                subaccount: None,
            });
        }
        Err(format!(
            "invalid source ICP account '{}'; expected principal or Account text",
            source_address
        ))
    }

    fn validate_solana_pubkey(address: &str, field_name: &str) -> Result<(), String> {
        let trimmed = address.trim();
        if trimmed.is_empty() {
            return Err(format!("{field_name} must not be empty"));
        }

        let decoded = bs58::decode(trimmed)
            .into_vec()
            .map_err(|e| format!("invalid {field_name} '{trimmed}': {e}"))?;
        if decoded.len() != 32 {
            return Err(format!(
                "invalid {field_name} '{trimmed}': expected 32-byte pubkey, got {} bytes",
                decoded.len()
            ));
        }

        Ok(())
    }

    fn validate_request_for_route(&self, route: &BridgeRouteSpec, request: &BridgeRequest) -> Result<(), String> {
        validate_destination_for_route(route, &request.destination)?;

        match route.route_kind {
            BridgeRouteKind::SolanaToIcp => {
                Self::validate_solana_pubkey(&request.source_address, "source Solana address")
            }
            BridgeRouteKind::IcpToSolana => {
                let source = Self::parse_source_icp_account(&request.source_address)?;
                if source.owner != self.bridge_ic_owner_principal {
                    return Err(format!(
                        "source ICP account owner {} does not match configured bridge owner {}",
                        source.owner, self.bridge_ic_owner_principal
                    ));
                }
                if source.subaccount.is_some() {
                    return Err(
                        "solana reverse route requires principal-only source account (subaccount must be None)"
                            .to_string(),
                    );
                }
                Ok(())
            }
            _ => Err(format!(
                "route {}@{} -> {} is not supported by SolanaBridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    fn unsupported_provider_error(route: &BridgeRouteSpec) -> String {
        format!(
            "solana bridge provider not implemented for route {}@{} -> {}",
            route.source_asset, route.source_chain, route.target_asset
        )
    }

    fn solana_route_from_submit_request(request: &BridgeRequest) -> Result<&'static BridgeRouteSpec, String> {
        let route = resolve_route(&request.asset, &request.source_chain, &request.target_asset).ok_or_else(|| {
            format!(
                "unsupported bridge route {}@{} -> {}; no route metadata found",
                request.asset, request.source_chain, request.target_asset
            )
        })?;

        if route.route_kind != BridgeRouteKind::SolanaToIcp && route.route_kind != BridgeRouteKind::IcpToSolana {
            return Err(format!(
                "route {}@{} -> {} is not supported by SolanaBridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            ));
        }

        Ok(route)
    }

    fn solana_route_from_source(asset: &str, chain: &str) -> Result<&'static BridgeRouteSpec, String> {
        BRIDGE_ROUTE_CATALOG
            .iter()
            .find(|route| {
                (route.route_kind == BridgeRouteKind::SolanaToIcp || route.route_kind == BridgeRouteKind::IcpToSolana)
                    && asset.eq_ignore_ascii_case(route.source_asset)
                    && chain.eq_ignore_ascii_case(route.source_chain)
            })
            .ok_or_else(|| format!("unsupported source route {}@{} for solana bridge backend", asset, chain))
    }
}

#[async_trait]
impl BridgeBackend for SolanaBridgeBackend {
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        let route = Self::solana_route_from_source(asset, chain)?;

        match route.route_kind {
            BridgeRouteKind::SolanaToIcp => Self::validate_solana_pubkey(address, "source Solana address")?,
            BridgeRouteKind::IcpToSolana => {
                let source = Self::parse_source_icp_account(address)?;
                if source.owner != self.bridge_ic_owner_principal {
                    return Err(format!(
                        "source ICP account owner {} does not match configured bridge owner {}",
                        source.owner, self.bridge_ic_owner_principal
                    ));
                }
                if source.subaccount.is_some() {
                    return Err(
                        "solana reverse route requires principal-only source account (subaccount must be None)"
                            .to_string(),
                    );
                }
            }
            _ => {}
        }

        Err(Self::unsupported_provider_error(route))
    }

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        let route = Self::solana_route_from_submit_request(&request)?;
        self.validate_request_for_route(route, &request)?;
        Err(Self::unsupported_provider_error(route))
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        Err(format!(
            "solana bridge provider not implemented; cannot query bridge status for '{}'",
            bridge_id
        ))
    }
}

#[cfg(test)]
mod tests {
    use candid::Principal;
    use icrc_ledger_types::icrc1::account::Account;

    use super::super::{BridgeBackend, BridgeDestination, BridgeDestinationKind};
    use super::{BridgeRequest, BridgeRouteKind, BridgeRouteSpec, SolanaBridgeBackend};

    fn backend() -> SolanaBridgeBackend {
        SolanaBridgeBackend::new(Principal::from_text("aaaaa-aa").expect("principal"))
    }

    #[test]
    fn validate_request_rejects_non_solana_route_kind() {
        let backend = backend();
        let route = BridgeRouteSpec {
            source_asset: "USDC",
            source_chain: "ETH",
            target_asset: "ckUSDC",
            destination_kind: BridgeDestinationKind::IcpAccount,
            route_kind: BridgeRouteKind::CkEthErc20Forward,
            evm_token_address: None,
            ckerc20_ledger_id: None,
            min_sweep_amount: 0.0,
        };
        let request = BridgeRequest {
            asset: "USDC".to_string(),
            source_chain: "ETH".to_string(),
            source_address: "0x1111111111111111111111111111111111111111".to_string(),
            target_asset: "ckUSDC".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: Principal::management_canister(),
                subaccount: None,
            }),
            amount: 1.0,
        };

        let err = backend
            .validate_request_for_route(&route, &request)
            .expect_err("non-solana route kind must fail");
        assert!(err.contains("is not supported by SolanaBridgeBackend"));
    }

    #[test]
    fn validate_request_checks_solana_to_icp_source_address() {
        let backend = backend();
        let route = BridgeRouteSpec {
            source_asset: "SOL",
            source_chain: "SOL",
            target_asset: "ckSOL",
            destination_kind: BridgeDestinationKind::IcpAccount,
            route_kind: BridgeRouteKind::SolanaToIcp,
            evm_token_address: None,
            ckerc20_ledger_id: None,
            min_sweep_amount: 0.0,
        };

        let mut request = BridgeRequest {
            asset: "SOL".to_string(),
            source_chain: "SOL".to_string(),
            source_address: "not-a-base58-@@".to_string(),
            target_asset: "ckSOL".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: Principal::management_canister(),
                subaccount: None,
            }),
            amount: 1.0,
        };
        let err = backend
            .validate_request_for_route(&route, &request)
            .expect_err("malformed Solana source must fail");
        assert!(err.contains("invalid source Solana address"));

        request.source_address = "So11111111111111111111111111111111111111112".to_string();
        backend
            .validate_request_for_route(&route, &request)
            .expect("valid Solana source must pass");
    }

    #[test]
    fn validate_request_checks_icp_to_solana_owner_and_subaccount() {
        let backend = backend();
        let route = BridgeRouteSpec {
            source_asset: "ckSOL",
            source_chain: "ICP",
            target_asset: "SOL",
            destination_kind: BridgeDestinationKind::SolanaAddress,
            route_kind: BridgeRouteKind::IcpToSolana,
            evm_token_address: None,
            ckerc20_ledger_id: None,
            min_sweep_amount: 0.0,
        };

        let mut request = BridgeRequest {
            asset: "ckSOL".to_string(),
            source_chain: "ICP".to_string(),
            source_address: Principal::anonymous().to_text(),
            target_asset: "SOL".to_string(),
            destination: BridgeDestination::SolanaAddress("So11111111111111111111111111111111111111112".to_string()),
            amount: 1.0,
        };

        let err = backend
            .validate_request_for_route(&route, &request)
            .expect_err("owner mismatch must fail");
        assert!(err.contains("does not match configured bridge owner"));

        request.source_address = Account {
            owner: Principal::from_text("aaaaa-aa").expect("principal"),
            subaccount: Some([1u8; 32]),
        }
        .to_string();
        let err = backend
            .validate_request_for_route(&route, &request)
            .expect_err("subaccount source must fail");
        assert!(err.contains("subaccount must be None"));

        request.source_address = Principal::from_text("aaaaa-aa").expect("principal").to_text();
        backend
            .validate_request_for_route(&route, &request)
            .expect("principal-only source owned by bridge owner must pass");
    }

    #[test]
    fn validated_route_still_returns_provider_not_implemented() {
        let backend = backend();
        let route = BridgeRouteSpec {
            source_asset: "ckSOL",
            source_chain: "ICP",
            target_asset: "SOL",
            destination_kind: BridgeDestinationKind::SolanaAddress,
            route_kind: BridgeRouteKind::IcpToSolana,
            evm_token_address: None,
            ckerc20_ledger_id: None,
            min_sweep_amount: 0.0,
        };
        let request = BridgeRequest {
            asset: "ckSOL".to_string(),
            source_chain: "ICP".to_string(),
            source_address: Principal::from_text("aaaaa-aa").expect("principal").to_text(),
            target_asset: "SOL".to_string(),
            destination: BridgeDestination::SolanaAddress("So11111111111111111111111111111111111111112".to_string()),
            amount: 1.0,
        };

        backend
            .validate_request_for_route(&route, &request)
            .expect("request should pass skeleton validation");
        let err = SolanaBridgeBackend::unsupported_provider_error(&route);
        assert!(err.contains("provider not implemented"));
    }

    #[tokio::test]
    async fn submit_bridge_without_catalog_route_fails_fast() {
        let backend = backend();
        let request = BridgeRequest {
            asset: "SOL".to_string(),
            source_chain: "SOL".to_string(),
            source_address: "So11111111111111111111111111111111111111112".to_string(),
            target_asset: "ckSOL".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: Principal::management_canister(),
                subaccount: None,
            }),
            amount: 1.0,
        };

        let err = backend
            .submit_bridge(request)
            .await
            .expect_err("solana routes are not catalog-seeded yet");
        assert!(err.contains("unsupported bridge route"));
    }

    #[tokio::test]
    async fn get_bridge_status_is_not_implemented() {
        let backend = backend();
        let err = backend
            .get_bridge_status("bridge-id-1")
            .await
            .expect_err("status polling should be disabled in skeleton");
        assert!(err.contains("provider not implemented"));
    }
}
