use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::{env, sync::Arc};

use candid::Principal;

use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, token_registry::TokenRegistry};

use crate::backend::evm_backend::EvmBackend;
use crate::backend::icp_backend::IcpBackend;

#[derive(Debug)]
pub enum RegistryLoadError {
    MissingIcpDecimals { spec: String, source: String },
    MissingIcpFee { spec: String, source: String },
    Other(String),
}

impl std::fmt::Display for RegistryLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryLoadError::MissingIcpDecimals { spec, source } => {
                write!(f, "icp decimals for `{spec}` failed: {source}")
            }
            RegistryLoadError::MissingIcpFee { spec, source } => {
                write!(f, "icp fee for `{spec}` failed: {source}")
            }
            RegistryLoadError::Other(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for RegistryLoadError {}

// Helpers for env parsing

fn load_env_specs(key: &str) -> Result<Vec<String>, RegistryLoadError> {
    let raw = env::var(key).map_err(|e| RegistryLoadError::Other(format!("missing {key}: {e}")))?;
    let specs = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok(specs)
}

// spec format: "chain:address:symbol"
fn split_spec(spec: &str) -> Result<(String, String, String), RegistryLoadError> {
    let parts: Vec<&str> = spec.split(':').collect();
    if parts.len() != 3 {
        return Err(RegistryLoadError::Other(format!(
            "invalid asset spec `{spec}` (expected chain:address:symbol)"
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string(), parts[2].to_string()))
}

// Uses ICP/EVM backends to resolve decimals and build a ChainToken.
async fn resolve_chain_token<IB, EB>(
    spec: &str,
    icp_backend: &Arc<IB>,
    evm_backend: &Arc<EB>,
) -> Result<ChainToken, RegistryLoadError>
where
    IB: IcpBackend + Send + Sync,
    EB: EvmBackend + Send + Sync,
{
    let (chain, address, symbol) = split_spec(spec)?;

    if chain == "icp" {
        let ledger: Principal = address
            .parse()
            .map_err(|_| RegistryLoadError::Other(format!("invalid ICP principal `{address}` in `{spec}`")))?;
        let decimals = icp_backend
            .icrc1_decimals(ledger)
            .await
            .map_err(|e| RegistryLoadError::MissingIcpDecimals {
                spec: spec.to_string(),
                source: e.to_string(),
            })?;

        let fee = icp_backend.icrc1_fee(ledger).await.map_err(|e| RegistryLoadError::MissingIcpFee {
            spec: spec.to_string(),
            source: e.to_string(),
        })?;

        Ok(ChainToken::Icp {
            ledger,
            symbol,
            decimals,
            fee,
        })
    } else if let Some(chain_name) = chain.strip_prefix("evm-") {
        // EVM chain: evm-eth, evm-arb, etc.
        if address == "native" {
            // Native asset (ETH, ARB, etc.)
            // Native assets always have 18 decimals
            Ok(ChainToken::EvmNative {
                chain: chain_name.to_string(),
                symbol,
                decimals: 18,
                fee: 0u8.into(),
            })
        } else {
            // ERC-20 token
            let decimals = evm_backend
                .erc20_decimals(chain_name, &address)
                .await
                .map_err(|e| RegistryLoadError::Other(format!("evm decimals for `{spec}` failed: {e}")))?;

            Ok(ChainToken::EvmErc20 {
                chain: chain_name.to_string(),
                token_address: address,
                symbol,
                decimals,
                fee: 0u8.into(),
            })
        }
    } else {
        Err(RegistryLoadError::Other(format!(
            "unsupported chain `{chain}` in spec `{spec}`"
        )))
    }
}

// Load DEBT_ASSETS and COLLATERAL_ASSETS into a deduped TokenRegistry.
//
// Env format:
//   DEBT_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,evm-arb:0x...:USDC
//   COLLATERAL_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,...
pub async fn load_token_registry<IB, EB>(
    icp_backend: Arc<IB>,
    evm_backend: Arc<EB>,
) -> Result<TokenRegistry, RegistryLoadError>
where
    IB: IcpBackend + Send + Sync,
    EB: EvmBackend + Send + Sync,
{
    let debt_specs = load_env_specs("DEBT_ASSETS")?;
    let coll_specs = load_env_specs("COLLATERAL_ASSETS")?;

    let mut all_specs = HashSet::new();
    for s in &debt_specs {
        all_specs.insert(s.clone());
    }
    for s in &coll_specs {
        all_specs.insert(s.clone());
    }

    let mut tokens = HashMap::new();
    for spec in &all_specs {
        let token = resolve_chain_token(spec, &icp_backend, &evm_backend).await?;
        let id = token.asset_id();
        tokens.insert(id, token);
    }

    let collateral_ids = coll_specs
        .into_iter()
        .map(|spec| {
            AssetId::from_str(&spec)
                .map_err(|e| RegistryLoadError::Other(format!("invalid collateral asset spec '{}': {e}", spec)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let debt_ids = debt_specs
        .into_iter()
        .map(|spec| {
            AssetId::from_str(&spec)
                .map_err(|e| RegistryLoadError::Other(format!("invalid debt asset spec '{}': {e}", spec)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TokenRegistry::with_roles(tokens, collateral_ids, debt_ids))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use candid::Nat;
    use tokio::sync::Mutex;

    use super::{RegistryLoadError, load_token_registry};
    use crate::backend::evm_backend::MockEvmBackend;
    use crate::backend::icp_backend::MockIcpBackend;
    use liquidium_pipeline_core::tokens::token_registry::TokenRegistryTrait;

    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    struct EnvGuard {
        debt_prev: Option<String>,
        coll_prev: Option<String>,
    }

    impl EnvGuard {
        fn set(debt: &str, coll: &str) -> Self {
            let debt_prev = std::env::var("DEBT_ASSETS").ok();
            let coll_prev = std::env::var("COLLATERAL_ASSETS").ok();
            // SAFETY: tests are serialized with ENV_LOCK and values are valid UTF-8.
            unsafe {
                std::env::set_var("DEBT_ASSETS", debt);
                std::env::set_var("COLLATERAL_ASSETS", coll);
            }
            Self {
                debt_prev,
                coll_prev,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: tests are serialized with ENV_LOCK and values are valid UTF-8.
            unsafe {
                match &self.debt_prev {
                    Some(v) => std::env::set_var("DEBT_ASSETS", v),
                    None => std::env::remove_var("DEBT_ASSETS"),
                }
                match &self.coll_prev {
                    Some(v) => std::env::set_var("COLLATERAL_ASSETS", v),
                    None => std::env::remove_var("COLLATERAL_ASSETS"),
                }
            }
        }
    }

    #[tokio::test]
    async fn decimals_error_maps_to_missing_icp_decimals() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set("icp:aaaaa-aa:ICP", "icp:aaaaa-aa:ICP");

        let mut icp = MockIcpBackend::new();
        icp.expect_icrc1_decimals()
            .returning(|_| Err("decimals-failed".into()));

        let evm = MockEvmBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm))
            .await
            .expect_err("expected error");

        match err {
            RegistryLoadError::MissingIcpDecimals { spec, source } => {
                assert_eq!(spec, "icp:aaaaa-aa:ICP");
                assert!(source.contains("decimals-failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn fee_error_maps_to_missing_icp_fee() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set("icp:aaaaa-aa:ICP", "icp:aaaaa-aa:ICP");

        let mut icp = MockIcpBackend::new();
        icp.expect_icrc1_decimals().returning(|_| Ok(8));
        icp.expect_icrc1_fee()
            .returning(|_| Err("fee-failed".into()));

        let evm = MockEvmBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm))
            .await
            .expect_err("expected error");

        match err {
            RegistryLoadError::MissingIcpFee { spec, source } => {
                assert_eq!(spec, "icp:aaaaa-aa:ICP");
                assert!(source.contains("fee-failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn bad_spec_maps_to_other() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set("icp:bad-spec", "icp:bad-spec");

        let icp = MockIcpBackend::new();
        let evm = MockEvmBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm))
            .await
            .expect_err("expected error");

        match err {
            RegistryLoadError::Other(message) => {
                assert!(message.contains("invalid asset spec"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn happy_path_builds_registry() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set("icp:aaaaa-aa:ICP", "icp:aaaaa-aa:ICP");

        let mut icp = MockIcpBackend::new();
        icp.expect_icrc1_decimals().returning(|_| Ok(8));
        icp.expect_icrc1_fee().returning(|_| Ok(Nat::from(10u8)));

        let evm = MockEvmBackend::new();

        let registry = load_token_registry(Arc::new(icp), Arc::new(evm))
            .await
            .expect("registry");

        assert_eq!(registry.tokens.len(), 1);
        assert_eq!(registry.collateral_assets().len(), 1);
        assert_eq!(registry.debt_assets().len(), 1);
    }
}
