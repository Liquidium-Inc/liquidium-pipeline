use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::{env, sync::Arc};

use candid::Principal;

use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, token_registry::TokenRegistry};

use crate::backend::evm_backend::EvmBackend;
use crate::backend::icp_backend::IcpBackend;

// Helpers for env parsing

fn load_env_specs(key: &str) -> Result<Vec<String>, String> {
    let raw = env::var(key).map_err(|e| format!("missing {key}: {e}"))?;
    let specs = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok(specs)
}

// spec format: "chain:address:symbol"
fn split_spec(spec: &str) -> Result<(String, String, String), String> {
    let parts: Vec<&str> = spec.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("invalid asset spec `{spec}` (expected chain:address:symbol)"));
    }
    Ok((parts[0].to_string(), parts[1].to_string(), parts[2].to_string()))
}

// Uses ICP/EVM backends to resolve decimals and build a ChainToken.
async fn resolve_chain_token<IB, EB>(
    spec: &str,
    icp_backend: &Arc<IB>,
    evm_backend: &Arc<EB>,
) -> Result<ChainToken, String>
where
    IB: IcpBackend + Send + Sync,
    EB: EvmBackend + Send + Sync,
{
    let (chain, address, symbol) = split_spec(spec)?;

    if chain == "icp" {
        let ledger: Principal = address
            .parse()
            .map_err(|_| format!("invalid ICP principal `{address}` in `{spec}`"))?;
        let decimals = icp_backend
            .icrc1_decimals(ledger)
            .await
            .map_err(|e| format!("icp decimals for `{spec}` failed: {e}"))?;

        let fee = icp_backend
            .icrc1_fee(ledger)
            .await
            .map_err(|e| format!("icp decimals for `{spec}` failed: {e}"))?;

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
                .map_err(|e| format!("evm decimals for `{spec}` failed: {e}"))?;

            Ok(ChainToken::EvmErc20 {
                chain: chain_name.to_string(),
                token_address: address,
                symbol,
                decimals,
                fee: 0u8.into(),
            })
        }
    } else {
        Err(format!("unsupported chain `{chain}` in spec `{spec}`"))
    }
}

// Load DEBT_ASSETS and COLLATERAL_ASSETS into a deduped TokenRegistry.
//
// Env format:
//   DEBT_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,evm-arb:0x...:USDC
//   COLLATERAL_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,...
pub async fn load_token_registry<IB, EB>(icp_backend: Arc<IB>, evm_backend: Arc<EB>) -> Result<TokenRegistry, String>
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
            AssetId::from_str(&spec).map_err(|e| format!("invalid collateral asset spec '{}': {e}", spec))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let debt_ids = debt_specs
        .into_iter()
        .map(|spec| AssetId::from_str(&spec).map_err(|e| format!("invalid debt asset spec '{}': {e}", spec)))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TokenRegistry::with_roles(tokens, collateral_ids, debt_ids))
}
