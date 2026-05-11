use std::{str::FromStr, sync::Arc};

use candid::{CandidType, Encode, Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::{
    amount_utils::amount_to_nat_units_strict,
    bridge_backend::{
        BridgeBackend, BridgeDestination, BridgeRequest, BridgeStatus, DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS,
        SolanaBridgeBackend,
    },
    icp_backend::IcpBackendImpl,
    solana_backend::{SolanaBackend, SolanaBackendImpl},
};
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use serde::Deserialize;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{signature::Keypair, signer::Signer};

use crate::config::Config;
const SOL_DECIMALS: u8 = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SolanaPrefundPlan {
    from_address: String,
    to_address: String,
    amount_lamports: Nat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BridgeSolDirection {
    SolToCkSol,
    CkSolToSol,
}

impl BridgeSolDirection {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "sol-to-cksol" | "sol2cksol" => Ok(Self::SolToCkSol),
            "cksol-to-sol" | "cksol2sol" => Ok(Self::CkSolToSol),
            other => Err(format!(
                "invalid direction '{}'; expected one of: sol-to-cksol, cksol-to-sol",
                other
            )),
        }
    }
}

fn parse_icp_account_like(address: &str) -> Result<Account, String> {
    if let Ok(account) = Account::from_str(address) {
        return Ok(account);
    }
    if let Ok(owner) = Principal::from_text(address) {
        return Ok(Account {
            owner,
            subaccount: None,
        });
    }
    Err(format!(
        "invalid ICP account-like address '{}'; expected Account or principal",
        address
    ))
}

fn bridge_solana_address(config: &Config) -> String {
    Keypair::new_from_array(config.bridge_solana_private_key_bytes)
        .pubkey()
        .to_string()
}

fn liquidator_solana_address(config: &Config) -> String {
    Keypair::new_from_array(config.solana_private_key_bytes)
        .pubkey()
        .to_string()
}

fn resolve_sol_to_cksol_source(source: Option<&str>, bridge_source_address: &str) -> Result<String, String> {
    if let Some(source_override) = source.map(str::trim).filter(|v| !v.is_empty())
        && source_override != bridge_source_address
    {
        return Err(format!(
            "for sol-to-cksol source must be configured bridge SOL source address {}; got {}",
            bridge_source_address, source_override
        ));
    }

    Ok(bridge_source_address.to_string())
}

fn build_sol_to_cksol_prefund_plan(
    liquidator_source_address: String,
    bridge_source_address: String,
    amount: f64,
    bridge_available_lamports: Nat,
    prefund_buffer_lamports: u64,
) -> Result<Option<SolanaPrefundPlan>, String> {
    let amount_lamports = amount_to_nat_units_strict(amount, SOL_DECIMALS)?;
    let required_budget = amount_lamports + Nat::from(prefund_buffer_lamports);
    if bridge_available_lamports >= required_budget {
        return Ok(None);
    }

    Ok(Some(SolanaPrefundPlan {
        from_address: liquidator_source_address,
        to_address: bridge_source_address,
        amount_lamports: required_budget - bridge_available_lamports,
    }))
}

fn format_bridge_status(status: &BridgeStatus) -> String {
    match status {
        BridgeStatus::Pending => "Pending".to_string(),
        BridgeStatus::Completed => "Completed".to_string(),
        BridgeStatus::Unknown => "Unknown".to_string(),
        BridgeStatus::Failed { reason } => {
            format!("Failed ({})", reason.as_deref().unwrap_or("no reason"))
        }
        BridgeStatus::Canceled { reason } => {
            format!("Canceled ({})", reason.as_deref().unwrap_or("no reason"))
        }
    }
}

fn build_backend(
    config: Arc<Config>,
    fee_buffer_lamports: u64,
    proxy_canister: Option<Principal>,
) -> Result<SolanaBridgeBackend<Agent, IcpBackendImpl<Agent>, SolanaBackendImpl>, String> {
    let agent = Arc::new(
        Agent::builder()
            .with_url(config.ic_url.clone())
            .with_identity(config.bridge_ic_identity.clone())
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|e| format!("ic agent(bridge-sol command) build failed: {e}"))?,
    );

    let icp_backend = Arc::new(IcpBackendImpl::new(agent.clone()));
    let solana_backend = Arc::new(SolanaBackendImpl::from_url_and_secret_key(
        config.solana_rpc_url.clone(),
        config.bridge_solana_private_key_bytes,
    ));
    let bridge_source_address = bridge_solana_address(&config);

    Ok(SolanaBridgeBackend::new(
        agent,
        icp_backend,
        solana_backend,
        config.bridge_cksol_minter_canister,
        config.bridge_cksol_ledger_canister,
        config.bridge_ic_owner_principal,
        proxy_canister,
        bridge_source_address,
        fee_buffer_lamports,
    ))
}

#[derive(CandidType)]
struct CkSolGetDepositAddressArgs {
    owner: Option<Principal>,
    subaccount: Option<[u8; 32]>,
}

#[derive(CandidType)]
struct CkSolProcessDepositArgs {
    owner: Option<Principal>,
    subaccount: Option<[u8; 32]>,
    signature: String,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolMinterInfo {
    manual_deposit_fee: u64,
    automated_deposit_fee: u64,
    deposit_consolidation_fee: Nat,
    minimum_withdrawal_amount: u64,
    minimum_deposit_amount: u64,
    withdrawal_fee: u64,
    process_deposit_required_cycles: Nat,
    balance: u64,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolInsufficientCyclesError {
    expected: Nat,
    received: Nat,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolProcessDepositError {
    InsufficientCycles(CkSolInsufficientCyclesError),
    TemporarilyUnavailable(String),
    AlreadyProcessing,
    TransactionNotFound,
    InvalidDepositTransaction(String),
    ValueTooSmall {
        minimum_deposit_amount: u64,
        deposit_amount: u64,
    },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolDepositId {
    signature: String,
    account: Account,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolDepositStatus {
    Processing {
        deposit_amount: u64,
        amount_to_mint: u64,
        deposit_id: CkSolDepositId,
    },
    Quarantined(CkSolDepositId),
    Minted {
        block_index: u64,
        minted_amount: u64,
        deposit_id: CkSolDepositId,
    },
}

fn map_process_deposit_error(err: CkSolProcessDepositError) -> String {
    match err {
        CkSolProcessDepositError::InsufficientCycles(detail) => format!(
            "process_deposit rejected: InsufficientCycles (expected={}, received={})",
            detail.expected, detail.received
        ),
        CkSolProcessDepositError::TemporarilyUnavailable(message) => {
            format!("process_deposit rejected: TemporarilyUnavailable ({message})")
        }
        CkSolProcessDepositError::AlreadyProcessing => "process_deposit rejected: AlreadyProcessing".to_string(),
        CkSolProcessDepositError::TransactionNotFound => "process_deposit rejected: TransactionNotFound".to_string(),
        CkSolProcessDepositError::InvalidDepositTransaction(message) => {
            format!("process_deposit rejected: InvalidDepositTransaction ({message})")
        }
        CkSolProcessDepositError::ValueTooSmall {
            minimum_deposit_amount,
            deposit_amount,
        } => format!(
            "process_deposit rejected: ValueTooSmall (minimum_deposit_amount={}, deposit_amount={})",
            minimum_deposit_amount, deposit_amount
        ),
    }
}

fn format_process_deposit_status(status: &CkSolDepositStatus) -> String {
    match status {
        CkSolDepositStatus::Processing {
            deposit_amount,
            amount_to_mint,
            deposit_id,
        } => format!(
            "Processing (signature={}, owner={}, subaccount={}, deposit_amount={}, amount_to_mint={})",
            deposit_id.signature,
            deposit_id.account.owner,
            deposit_id
                .account
                .subaccount
                .map(hex::encode)
                .unwrap_or_else(|| "none".to_string()),
            deposit_amount,
            amount_to_mint
        ),
        CkSolDepositStatus::Quarantined(deposit_id) => format!(
            "Quarantined (signature={}, owner={}, subaccount={})",
            deposit_id.signature,
            deposit_id.account.owner,
            deposit_id
                .account
                .subaccount
                .map(hex::encode)
                .unwrap_or_else(|| "none".to_string())
        ),
        CkSolDepositStatus::Minted {
            block_index,
            minted_amount,
            deposit_id,
        } => format!(
            "Minted (block_index={}, minted_amount={}, signature={}, owner={}, subaccount={})",
            block_index,
            minted_amount,
            deposit_id.signature,
            deposit_id.account.owner,
            deposit_id
                .account
                .subaccount
                .map(hex::encode)
                .unwrap_or_else(|| "none".to_string())
        ),
    }
}

const DEPOSIT_BRIDGE_ID_PREFIX: &str = "cksol-deposit:";

fn parse_optional_principal(raw: Option<&str>, label: &str) -> Result<Option<Principal>, String> {
    let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };

    Principal::from_text(value)
        .map(Some)
        .map_err(|e| format!("invalid {} principal '{}': {e}", label, value))
}

fn resolve_proxy_canister(
    override_proxy_canister: Option<Principal>,
    config: &Config,
) -> Result<Option<Principal>, String> {
    if let Some(proxy) = override_proxy_canister {
        return Ok(Some(proxy));
    }

    Ok(config.bridge_ic_proxy_canister)
}

async fn resolve_deposit_address_for_account(
    backend: &SolanaBridgeBackend<Agent, IcpBackendImpl<Agent>, SolanaBackendImpl>,
    account: &Account,
) -> Result<String, String> {
    let args = CkSolGetDepositAddressArgs {
        owner: Some(account.owner),
        subaccount: account.subaccount,
    };
    let arg_blob = Encode!(&args).map_err(|e| format!("encode get_deposit_address args failed: {e}"))?;
    backend
        .agent
        .call_query::<String>(&backend.cksol_minter_canister, "get_deposit_address", arg_blob)
        .await
        .map_err(|e| {
            format!(
                "get_deposit_address failed for minter {}: {}",
                backend.cksol_minter_canister, e
            )
        })
}

fn decode_subaccount(encoded: &str) -> Result<Option<[u8; 32]>, String> {
    if encoded == "none" {
        return Ok(None);
    }

    let bytes = hex::decode(encoded).map_err(|e| format!("invalid encoded subaccount '{}': {e}", encoded))?;
    if bytes.len() != 32 {
        return Err(format!(
            "invalid encoded subaccount '{}': expected 32 bytes, got {}",
            encoded,
            bytes.len()
        ));
    }

    let mut subaccount = [0u8; 32];
    subaccount.copy_from_slice(&bytes);
    Ok(Some(subaccount))
}

fn parse_deposit_bridge_account(bridge_id: &str) -> Result<Account, String> {
    let raw = bridge_id
        .strip_prefix(DEPOSIT_BRIDGE_ID_PREFIX)
        .ok_or_else(|| format!("bridge id '{}' is not a ckSOL deposit bridge id", bridge_id))?;

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 4 && parts.len() != 5 {
        return Err(format!(
            "invalid ckSOL deposit bridge id '{}': expected 4 or 5 fields",
            bridge_id
        ));
    }

    let owner = Principal::from_text(parts[0])
        .map_err(|e| format!("invalid owner '{}' in bridge id '{}': {e}", parts[0], bridge_id))?;
    let subaccount = decode_subaccount(parts[1])?;

    Ok(Account { owner, subaccount })
}

fn parse_subaccount_override(raw: Option<&str>) -> Result<Option<Option<[u8; 32]>>, String> {
    let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(decode_subaccount(value)?))
}

async fn fetch_minter_info(
    backend: &SolanaBridgeBackend<Agent, IcpBackendImpl<Agent>, SolanaBackendImpl>,
) -> Result<CkSolMinterInfo, String> {
    let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
    match backend
        .agent
        .call_query::<CkSolMinterInfo>(&backend.cksol_minter_canister, "get_minter_info", args.clone())
        .await
    {
        Ok(v) => Ok(v),
        Err(query_err) => backend
            .agent
            .call_update::<CkSolMinterInfo>(&backend.cksol_minter_canister, "get_minter_info", args)
            .await
            .map_err(|update_err| {
                format!(
                    "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                    backend.cksol_minter_canister
                )
            }),
    }
}

pub async fn submit(
    direction_raw: &str,
    amount: f64,
    source: Option<&str>,
    destination: Option<&str>,
    check_status: bool,
    skip_prefund: bool,
    prefund_buffer_lamports: u64,
    proxy_canister: Option<&str>,
) -> Result<(), String> {
    if !amount.is_finite() || amount <= 0.0 {
        return Err(format!("amount must be positive and finite, got {}", amount));
    }

    let config = Config::load().await?;
    let direction = BridgeSolDirection::parse(direction_raw)?;
    let proxy_canister_override = parse_optional_principal(proxy_canister, "proxy canister")?;
    let resolved_proxy_canister = resolve_proxy_canister(proxy_canister_override, &config)?;
    if direction == BridgeSolDirection::SolToCkSol && resolved_proxy_canister.is_none() {
        return Err(
            "bridge proxy canister is not configured. Set BRIDGE_IC_PROXY_CANISTER or pass --proxy-canister for sol-to-cksol"
                .to_string(),
        );
    }
    let backend = build_backend(config.clone(), prefund_buffer_lamports, resolved_proxy_canister)?;
    let bridge_source_address = bridge_solana_address(&config);

    let default_destination = match direction {
        BridgeSolDirection::SolToCkSol => config.bridge_ic_owner_principal.to_text(),
        BridgeSolDirection::CkSolToSol => bridge_source_address.clone(),
    };

    let destination_value = destination
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(&default_destination);

    let request = match direction {
        BridgeSolDirection::SolToCkSol => {
            let destination_account = parse_icp_account_like(destination_value)?;
            let source_value = resolve_sol_to_cksol_source(source, &bridge_source_address)?;
            let bridge_available_lamports = backend.solana_backend.native_balance().await.map_err(|e| {
                format!(
                    "failed to read bridge source SOL balance before prefund (source={}): {}",
                    bridge_source_address, e
                )
            })?;
            let prefund_plan = build_sol_to_cksol_prefund_plan(
                liquidator_solana_address(&config),
                bridge_source_address.clone(),
                amount,
                bridge_available_lamports,
                prefund_buffer_lamports,
            )?;
            if !skip_prefund {
                if let Some(prefund_plan) = prefund_plan {
                    let liquidator_solana_backend = SolanaBackendImpl::from_url_and_secret_key(
                        config.solana_rpc_url.clone(),
                        config.solana_private_key_bytes,
                    );
                    let prefund_sig = liquidator_solana_backend
                        .native_transfer(&prefund_plan.to_address, prefund_plan.amount_lamports.clone())
                        .await
                        .map_err(|e| {
                            format!(
                                "sol-to-cksol prefund transfer failed ({} -> {}, {} lamports): {}",
                                prefund_plan.from_address, prefund_plan.to_address, prefund_plan.amount_lamports, e
                            )
                        })?;

                    println!(
                        "Prefund transfer submitted: from={} to={} amount_lamports={} signature={}",
                        prefund_plan.from_address, prefund_plan.to_address, prefund_plan.amount_lamports, prefund_sig
                    );
                } else {
                    println!(
                        "Prefund skipped: bridge source already has enough SOL for amount+buffer (source={})",
                        bridge_source_address
                    );
                }
            }

            BridgeRequest {
                asset: "SOL".to_string(),
                source_chain: "SOL".to_string(),
                source_address: source_value,
                target_asset: "ckSOL".to_string(),
                destination: BridgeDestination::IcpAccount(destination_account),
                amount,
            }
        }
        BridgeSolDirection::CkSolToSol => {
            let source_value = source
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| config.bridge_ic_owner_principal.to_text());

            BridgeRequest {
                asset: "ckSOL".to_string(),
                source_chain: "ICP".to_string(),
                source_address: source_value,
                target_asset: "SOL".to_string(),
                destination: BridgeDestination::SolanaAddress(destination_value.to_string()),
                amount,
            }
        }
    };

    println!(
        "Submitting bridge request: direction={} amount={} source={} destination={}",
        direction_raw, amount, request.source_address, destination_value
    );

    let submission = backend.submit_bridge(request).await?;
    println!("Bridge submitted. bridge_id={}", submission.bridge_id);

    if check_status {
        let status = backend.get_bridge_status(&submission.bridge_id).await?;
        println!(
            "Immediate bridge status for {}: {}",
            submission.bridge_id,
            format_bridge_status(&status)
        );
    }

    Ok(())
}

pub async fn status(bridge_id: &str) -> Result<(), String> {
    let bridge_id = bridge_id.trim();
    if bridge_id.is_empty() {
        return Err("bridge_id must not be empty".to_string());
    }

    let config = Config::load().await?;
    let backend = build_backend(config, DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS, None)?;
    let status = backend.get_bridge_status(bridge_id).await?;

    println!("Bridge status for {}: {}", bridge_id, format_bridge_status(&status));
    Ok(())
}

pub async fn print_deposit_txs(bridge_id: &str, limit: usize) -> Result<(), String> {
    let bridge_id = bridge_id.trim();
    if bridge_id.is_empty() {
        return Err("bridge_id must not be empty".to_string());
    }

    let max_limit = limit.clamp(1, 100);
    let account = parse_deposit_bridge_account(bridge_id)?;
    let config = Config::load().await?;
    let backend = build_backend(config.clone(), DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS, None)?;

    let deposit_address = resolve_deposit_address_for_account(&backend, &account).await?;
    let deposit_pubkey = Pubkey::from_str(&deposit_address).map_err(|e| {
        format!(
            "minter returned invalid Solana deposit address '{}': {e}",
            deposit_address
        )
    })?;

    println!("ckSOL deposit account owner: {}", account.owner);
    println!(
        "ckSOL deposit account subaccount: {}",
        account
            .subaccount
            .map(hex::encode)
            .unwrap_or_else(|| "none".to_string())
    );
    println!("Resolved deposit SOL address: {}", deposit_address);

    let rpc = RpcClient::new(config.solana_rpc_url.clone());
    let signatures = rpc
        .get_signatures_for_address_with_config(
            &deposit_pubkey,
            GetConfirmedSignaturesForAddress2Config {
                before: None,
                until: None,
                limit: Some(max_limit),
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )
        .await
        .map_err(|e| {
            format!(
                "failed to fetch signatures for deposit address {}: {e}",
                deposit_address
            )
        })?;

    if signatures.is_empty() {
        println!("No confirmed transactions found for this deposit address.");
        return Ok(());
    }

    println!(
        "Recent deposit-address transactions (newest first, limit={}):",
        max_limit
    );
    for (idx, tx) in signatures.iter().enumerate() {
        let err = tx
            .err
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "ok".to_string());
        let status = tx
            .confirmation_status
            .as_ref()
            .map(|s| format!("{s:?}"))
            .unwrap_or_else(|| "unknown".to_string());
        let block_time = tx
            .block_time
            .map(|t| t.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        println!(
            "{}. sig={} slot={} status={} err={} block_time={}",
            idx + 1,
            tx.signature,
            tx.slot,
            status,
            err,
            block_time
        );
        println!(
            "   devnet explorer: https://solscan.io/tx/{}?cluster=devnet",
            tx.signature
        );
    }

    Ok(())
}

pub async fn process_deposit(
    signature: &str,
    bridge_id: Option<&str>,
    owner: Option<&str>,
    subaccount: Option<&str>,
    proxy_canister: Option<&str>,
) -> Result<(), String> {
    let signature = signature.trim();
    if signature.is_empty() {
        return Err("signature must not be empty".to_string());
    }

    let config = Config::load().await?;
    let proxy_canister_override = parse_optional_principal(proxy_canister, "proxy canister")?;
    let proxy_canister = resolve_proxy_canister(proxy_canister_override, &config)?.ok_or_else(|| {
        "bridge proxy canister is not configured. Set BRIDGE_IC_PROXY_CANISTER or pass --proxy-canister".to_string()
    })?;
    let backend = build_backend(
        config.clone(),
        DEFAULT_SOLANA_BRIDGE_FEE_BUFFER_LAMPORTS,
        Some(proxy_canister),
    )?;

    let account_from_bridge_id = match bridge_id.map(str::trim).filter(|v| !v.is_empty()) {
        Some(value) => Some(parse_deposit_bridge_account(value)?),
        None => None,
    };

    let owner_override = parse_optional_principal(owner, "owner")?;
    let subaccount_override = parse_subaccount_override(subaccount)?;

    let resolved_owner = owner_override
        .or(account_from_bridge_id.as_ref().map(|a| a.owner))
        .unwrap_or(config.bridge_ic_owner_principal);
    let resolved_subaccount = subaccount_override.unwrap_or_else(|| account_from_bridge_id.and_then(|a| a.subaccount));
    let resolved_account = Account {
        owner: resolved_owner,
        subaccount: resolved_subaccount,
    };

    let minter_info = fetch_minter_info(&backend).await?;
    let args = CkSolProcessDepositArgs {
        owner: Some(resolved_account.owner),
        subaccount: resolved_account.subaccount,
        signature: signature.to_string(),
    };
    let arg_blob = Encode!(&args).map_err(|e| format!("encode process_deposit args failed: {e}"))?;
    let result: Result<CkSolDepositStatus, CkSolProcessDepositError> = backend
        .agent
        .call_update_via_proxy(
            &proxy_canister,
            &backend.cksol_minter_canister,
            "process_deposit",
            arg_blob,
            minter_info.process_deposit_required_cycles,
        )
        .await
        .map_err(|e| {
            format!(
                "process_deposit proxy-forwarded call failed (proxy={} minter={}): {e}",
                proxy_canister, backend.cksol_minter_canister
            )
        })?;

    match result {
        Ok(status) => {
            println!("process_deposit result: {}", format_process_deposit_status(&status));
            Ok(())
        }
        Err(err) => Err(map_process_deposit_error(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BridgeSolDirection, build_sol_to_cksol_prefund_plan, parse_deposit_bridge_account, parse_optional_principal,
        parse_subaccount_override, resolve_proxy_canister, resolve_sol_to_cksol_source,
    };
    use crate::config::Config;
    use candid::{Nat, Principal};
    use ic_agent::identity::AnonymousIdentity;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn parse_direction_accepts_expected_values() {
        assert_eq!(
            BridgeSolDirection::parse("sol-to-cksol").expect("must parse"),
            BridgeSolDirection::SolToCkSol
        );
        assert_eq!(
            BridgeSolDirection::parse("cksol-to-sol").expect("must parse"),
            BridgeSolDirection::CkSolToSol
        );
        assert_eq!(
            BridgeSolDirection::parse("sol2cksol").expect("must parse"),
            BridgeSolDirection::SolToCkSol
        );
        assert_eq!(
            BridgeSolDirection::parse("cksol2sol").expect("must parse"),
            BridgeSolDirection::CkSolToSol
        );
    }

    #[test]
    fn parse_direction_rejects_invalid_values() {
        let err = BridgeSolDirection::parse("foo").expect_err("must fail");
        assert!(err.contains("invalid direction"));
    }

    #[test]
    fn parse_deposit_bridge_account_extracts_owner_and_none_subaccount() {
        let owner = Principal::from_text("aaaaa-aa").expect("principal");
        let bridge_id = format!("cksol-deposit:{}:none:0:1", owner);
        let parsed = parse_deposit_bridge_account(&bridge_id).expect("must parse");
        assert_eq!(parsed.owner, owner);
        assert!(parsed.subaccount.is_none());
    }

    #[test]
    fn parse_deposit_bridge_account_supports_extended_signature_format() {
        let owner = Principal::from_text("aaaaa-aa").expect("principal");
        let bridge_id = format!("cksol-deposit:{}:none:0:1:sig-abc", owner);
        let parsed = parse_deposit_bridge_account(&bridge_id).expect("must parse");
        assert_eq!(parsed.owner, owner);
        assert!(parsed.subaccount.is_none());
    }

    #[test]
    fn sol_to_cksol_source_resolution_is_canonical_bridge_source() {
        let bridge_source = "11111111111111111111111111111111";
        assert_eq!(
            resolve_sol_to_cksol_source(None, bridge_source).expect("none source uses bridge source"),
            bridge_source
        );
        assert_eq!(
            resolve_sol_to_cksol_source(Some(bridge_source), bridge_source).expect("matching source must pass"),
            bridge_source
        );

        let err = resolve_sol_to_cksol_source(Some("So11111111111111111111111111111111111111112"), bridge_source)
            .expect_err("mismatch source must fail");
        assert!(err.contains("must be configured bridge SOL source address"));
    }

    #[test]
    fn sol_to_cksol_prefund_plan_transfers_only_shortfall_to_bridge() {
        let plan = build_sol_to_cksol_prefund_plan(
            "liq-source".to_string(),
            "bridge-source".to_string(),
            0.5,
            Nat::from(100_000_000u64),
            2_000_000,
        )
        .expect("prefund plan")
        .expect("shortfall must require transfer");

        assert_eq!(plan.from_address, "liq-source");
        assert_eq!(plan.to_address, "bridge-source");
        assert_eq!(plan.amount_lamports, Nat::from(402_000_000u64));
    }

    #[test]
    fn sol_to_cksol_prefund_plan_returns_none_when_bridge_already_funded() {
        let plan = build_sol_to_cksol_prefund_plan(
            "liq-source".to_string(),
            "bridge-source".to_string(),
            0.5,
            Nat::from(700_000_000u64),
            2_000_000,
        )
        .expect("prefund plan");
        assert!(plan.is_none());
    }

    #[test]
    fn parse_optional_principal_handles_none_and_invalid() {
        assert!(parse_optional_principal(None, "proxy").expect("none").is_none());
        assert!(parse_optional_principal(Some(""), "proxy").expect("empty").is_none());
        let err = parse_optional_principal(Some("not-principal"), "proxy").expect_err("must fail");
        assert!(err.contains("invalid proxy principal"));
    }

    #[test]
    fn parse_subaccount_override_supports_none_and_hex() {
        let parsed = parse_subaccount_override(Some("none")).expect("none");
        assert_eq!(parsed, Some(None));
        let parsed = parse_subaccount_override(Some(&"ab".repeat(32))).expect("hex");
        assert!(parsed.expect("override").is_some());
    }

    #[test]
    fn proxy_canister_resolution_prefers_cli_override_over_env_value() {
        let env_proxy = Principal::from_text("rwlgt-iiaaa-aaaaa-aaaaa-cai").expect("principal");
        let cli_proxy = Principal::from_text("aaaaa-aa").expect("principal");
        let config = Arc::new(Config {
            liquidator_identity: Arc::new(AnonymousIdentity {}),
            trader_identity: Arc::new(AnonymousIdentity {}),
            bridge_ic_identity: Arc::new(AnonymousIdentity {}),
            liquidator_principal: Principal::from_text("aaaaa-aa").expect("principal"),
            trader_principal: Principal::from_text("2vxsx-fae").expect("principal"),
            ic_url: "http://localhost:4943".to_string(),
            evm_rpc_url: "http://localhost:8545".to_string(),
            solana_rpc_url: "http://localhost:8899".to_string(),
            evm_private_key: String::new(),
            solana_private_key_bytes: [0u8; 32],
            bridge_evm_private_key: String::new(),
            bridge_solana_private_key_bytes: [0u8; 32],
            bridge_evm_address: String::new(),
            bridge_ic_owner_principal: Principal::from_text("aaaaa-aa").expect("principal"),
            bridge_btc_address: String::new(),
            bridge_cketh_minter_canister: Principal::from_text("sv3dd-oaaaa-aaaar-qacoa-cai").expect("principal"),
            bridge_cksol_minter_canister: Principal::from_text("ljyxk-riaaa-aaaar-qb5mq-cai").expect("principal"),
            bridge_cksol_ledger_canister: Principal::from_text("la34w-haaaa-aaaar-qb5na-cai").expect("principal"),
            bridge_ic_proxy_canister: Some(env_proxy),
            lending_canister: Principal::from_text("aaaaa-aa").expect("principal"),
            export_path: String::new(),
            buy_bad_debt: false,
            db_path: String::new(),
            max_allowed_dex_slippage: 125,
            max_allowed_cex_slippage_bps: 200,
            bad_debt_collateral_slippage_bps: 500,
            cex_min_exec_usd: 1.0,
            cex_slice_target_ratio: 0.7,
            cex_buy_truncation_trigger_ratio: 0.25,
            cex_buy_inverse_overspend_bps: 10,
            cex_buy_inverse_max_retries: 1,
            cex_buy_inverse_enabled: true,
            cex_retry_base_secs: 5,
            cex_retry_max_secs: 120,
            cex_min_net_edge_bps: 150,
            cex_delay_buffer_bps: 75,
            cex_route_fee_bps: 25,
            cex_force_over_usd_threshold: 12.5,
            cex_mexc_available_pairs: vec![],
            cex_mexc_max_hops: 2,
            swapper: crate::config::SwapperMode::Cex,
            cex_credentials: HashMap::new(),
            opportunity_account_filter: vec![],
        });

        let resolved = resolve_proxy_canister(Some(cli_proxy), &config).expect("resolve");
        assert_eq!(resolved, Some(cli_proxy));
        let resolved = resolve_proxy_canister(None, &config).expect("resolve");
        assert_eq!(resolved, Some(env_proxy));
    }
}
