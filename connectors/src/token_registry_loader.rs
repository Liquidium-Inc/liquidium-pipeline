use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::{env, sync::Arc};

use candid::Principal;

use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, token_registry::TokenRegistry};

use crate::backend::evm_backend::EvmBackend;
use crate::backend::icp_backend::IcpBackend;
use crate::backend::solana_backend::SolanaBackend;

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

// Uses ICP/EVM/Solana backends to resolve decimals and build a ChainToken.
async fn resolve_chain_token<IB, EB, SB>(
    spec: &str,
    icp_backend: &Arc<IB>,
    evm_backend: &Arc<EB>,
    solana_backend: &Arc<SB>,
) -> Result<ChainToken, RegistryLoadError>
where
    IB: IcpBackend + Send + Sync,
    EB: EvmBackend + Send + Sync,
    SB: SolanaBackend + Send + Sync,
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

        let fee = icp_backend
            .icrc1_fee(ledger)
            .await
            .map_err(|e| RegistryLoadError::MissingIcpFee {
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
    } else if chain == "sol" {
        if address == "native" {
            // Native SOL (lamports, 9 decimals).
            Ok(ChainToken::SolanaNative {
                symbol,
                decimals: 9,
                fee: 0u8.into(),
            })
        } else {
            // Validate mint shape early so malformed specs fail fast.
            let mint_bytes = bs58::decode(&address)
                .into_vec()
                .map_err(|e| RegistryLoadError::Other(format!("invalid Solana mint `{address}` in `{spec}`: {e}")))?;
            if mint_bytes.len() != 32 {
                return Err(RegistryLoadError::Other(format!(
                    "invalid Solana mint `{address}` in `{spec}`: expected 32-byte pubkey"
                )));
            }
            let decimals = solana_backend
                .spl_decimals(&address)
                .await
                .map_err(|e| RegistryLoadError::Other(format!("sol decimals for `{spec}` failed: {e}")))?;

            Ok(ChainToken::SolanaSpl {
                mint: address,
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
pub async fn load_token_registry<IB, EB, SB>(
    icp_backend: Arc<IB>,
    evm_backend: Arc<EB>,
    solana_backend: Arc<SB>,
) -> Result<TokenRegistry, RegistryLoadError>
where
    IB: IcpBackend + Send + Sync,
    EB: EvmBackend + Send + Sync,
    SB: SolanaBackend + Send + Sync,
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
        let token = resolve_chain_token(spec, &icp_backend, &evm_backend, &solana_backend).await?;
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
    use crate::backend::solana_backend::MockSolanaBackend;
    use liquidium_pipeline_core::tokens::chain_token::ChainToken;
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
            Self { debt_prev, coll_prev }
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
            .returning(|_| Err("decimals-failed".to_string()));

        let evm = MockEvmBackend::new();
        let sol = MockSolanaBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
            .await
            .expect_err("expected error");

        match err {
            RegistryLoadError::MissingIcpDecimals { spec, source } => {
                assert_eq!(spec, "icp:aaaaa-aa:ICP");
                assert_eq!(source, "decimals-failed");
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
        icp.expect_icrc1_fee().returning(|_| Err("fee-failed".to_string()));

        let evm = MockEvmBackend::new();
        let sol = MockSolanaBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
            .await
            .expect_err("expected error");

        match err {
            RegistryLoadError::MissingIcpFee { spec, source } => {
                assert_eq!(spec, "icp:aaaaa-aa:ICP");
                assert_eq!(source, "fee-failed");
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
        let sol = MockSolanaBackend::new();

        let err = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
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
        let sol = MockSolanaBackend::new();

        let registry = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
            .await
            .expect("registry");

        assert_eq!(registry.tokens.len(), 1);
        assert_eq!(registry.collateral_assets().len(), 1);
        assert_eq!(registry.debt_assets().len(), 1);
    }

    #[tokio::test]
    async fn sol_native_spec_builds_registry_entry() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set("sol:native:SOL", "sol:native:SOL");

        let icp = MockIcpBackend::new();
        let evm = MockEvmBackend::new();
        let sol = MockSolanaBackend::new();

        let registry = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
            .await
            .expect("registry");

        assert_eq!(registry.tokens.len(), 1);
        let (asset_id, token) = registry.tokens.iter().next().expect("token");
        assert_eq!(asset_id.chain, "sol");
        assert_eq!(asset_id.address, "native");
        assert_eq!(asset_id.symbol, "SOL");
        assert_eq!(token.decimals(), 9);
    }

    #[tokio::test]
    async fn sol_spl_spec_resolves_decimals_from_solana_backend() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set(
            "sol:So11111111111111111111111111111111111111112:WSOL",
            "sol:So11111111111111111111111111111111111111112:WSOL",
        );

        let icp = MockIcpBackend::new();
        let evm = MockEvmBackend::new();
        let mut sol = MockSolanaBackend::new();
        sol.expect_spl_decimals()
            .withf(|mint| mint == "So11111111111111111111111111111111111111112")
            .return_once(|_| Ok(9));

        let registry = load_token_registry(Arc::new(icp), Arc::new(evm), Arc::new(sol))
            .await
            .expect("registry");

        assert_eq!(registry.tokens.len(), 1);
        let (asset_id, token) = registry.tokens.iter().next().expect("token");
        assert_eq!(asset_id.chain, "sol");
        assert_eq!(asset_id.address, "So11111111111111111111111111111111111111112");
        assert_eq!(asset_id.symbol, "WSOL");
        match token {
            ChainToken::SolanaSpl { decimals, .. } => assert_eq!(*decimals, 9),
            other => panic!("unexpected token variant: {other:?}"),
        }
    }
}
