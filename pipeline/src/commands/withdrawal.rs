//! One withdrawal, shared by the CLI and the TUI: where it leaves from, where
//! it goes, and how much it sends. "all" sends the balance less what the source
//! must keep to pay for the send.
use std::str::FromStr;
use std::sync::Arc;

use alloy::network::AnyNetwork;
use alloy::primitives::Address as EvmAddress;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::balance_service::BalanceService;
use liquidium_pipeline_core::tokens::asset_id::AssetId;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::transfer::transfer_service::TransferService;
use num_traits::ToPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{message::Message, pubkey::Pubkey, signature::Keypair, signer::Signer};
use solana_system_interface::instruction as system_instruction;

use crate::config::{Config, ConfigTrait};
use crate::context::PipelineContext;

/// The least ETH kept for gas, whatever the gas price.
const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 200_000_000_000_000;
const EVM_NATIVE_TRANSFER_GAS_LIMIT: u128 = 21_000;
const EVM_ERC20_TRANSFER_GAS_LIMIT: u128 = 100_000;
const EVM_GAS_RESERVE_MULTIPLIER: u128 = 2;
// Keeps the source system account rent-exempt after the send, with fee headroom.
const SOLANA_NATIVE_FEE_RESERVE_LAMPORTS: u128 = 2_000_000;
const SOLANA_SPL_FEE_RESERVE_LAMPORTS: u128 = 3_000_000;

/// The account a withdrawal leaves from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WithdrawSource {
    Main,
    Trader,
    Recovery,
    Bridge,
}

impl WithdrawSource {
    /// Reads a CLI source name or its short form.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "main" | "m" | "liquidator" => Some(Self::Main),
            "trader" | "t" => Some(Self::Trader),
            "recovery" | "r" => Some(Self::Recovery),
            "bridge" | "b" => Some(Self::Bridge),
            _ => None,
        }
    }

    pub(crate) fn balances(self, ctx: &PipelineContext) -> Arc<BalanceService> {
        match self {
            Self::Main => ctx.main_service.clone(),
            Self::Trader => ctx.trader_service.clone(),
            Self::Recovery => ctx.recovery_service.clone(),
            Self::Bridge => ctx.bridge_service.clone(),
        }
    }

    pub(crate) fn transfers(self, ctx: &PipelineContext) -> Arc<TransferService> {
        match self {
            Self::Main => ctx.main_transfers.clone(),
            Self::Trader => ctx.trader_transfers.clone(),
            Self::Recovery => ctx.recovery_transfers.clone(),
            Self::Bridge => ctx.bridge_transfers.clone(),
        }
    }

    /// The address the funds leave from on the token's chain.
    pub(crate) fn address(self, ctx: &PipelineContext, token: &ChainToken) -> String {
        let config = &ctx.config;
        match token {
            ChainToken::Icp { .. } => match self {
                Self::Main => Account::from(config.liquidator_principal).to_string(),
                Self::Trader => Account::from(config.trader_principal).to_string(),
                Self::Recovery => config.get_recovery_account().to_string(),
                Self::Bridge => config.bridge_ic_account().to_string(),
            },
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => match self {
                Self::Bridge => config.bridge_evm_address.clone(),
                _ => ctx.evm_address.clone(),
            },
            ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => self.solana_address(config),
        }
    }

    fn evm_private_key(self, config: &Config) -> &str {
        match self {
            Self::Bridge => &config.bridge_evm_private_key,
            _ => &config.evm_private_key,
        }
    }

    fn solana_address(self, config: &Config) -> String {
        let secret_key = match self {
            Self::Bridge => config.bridge_solana_private_key_bytes,
            _ => config.solana_private_key_bytes,
        };
        Keypair::new_from_array(secret_key).pubkey().to_string()
    }
}

/// Where a withdrawal goes. Trader and recovery exist on ICP only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WithdrawDestination {
    Main,
    Trader,
    Recovery,
    Bridge,
    /// An address the operator types.
    Manual,
}

impl WithdrawDestination {
    /// Reads a CLI destination. Text that names no account is a typed address.
    pub(crate) fn parse(text: &str) -> Self {
        match text.trim().to_ascii_lowercase().as_str() {
            "main" => Self::Main,
            "trader" => Self::Trader,
            "recovery" => Self::Recovery,
            "bridge" => Self::Bridge,
            _ => Self::Manual,
        }
    }
}

/// The account a withdrawal pays into. `typed` is read only for `Manual`.
pub(crate) fn destination_account(
    ctx: &PipelineContext,
    token: &ChainToken,
    destination: WithdrawDestination,
    typed: &str,
) -> Result<ChainAccount, String> {
    use WithdrawDestination::{Bridge, Main, Manual, Recovery, Trader};
    let config = &ctx.config;
    let is_evm = matches!(token, ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. });
    match (token, destination) {
        (_, Manual) => parse_typed_destination(token, typed),
        (ChainToken::Icp { .. }, Main) => Ok(ChainAccount::Icp(config.liquidator_principal.into())),
        (ChainToken::Icp { .. }, Trader) => Ok(ChainAccount::Icp(config.trader_principal.into())),
        (ChainToken::Icp { .. }, Recovery) => Ok(ChainAccount::Icp(config.get_recovery_account())),
        (ChainToken::Icp { .. }, Bridge) => Ok(ChainAccount::Icp(config.bridge_ic_account())),
        (_, Trader | Recovery) => {
            Err("destination trader/recovery is ICP-only; use main, bridge or a typed address".to_string())
        }
        (_, Main) if is_evm => Ok(ChainAccount::Evm(ctx.evm_address.clone())),
        (_, Bridge) if is_evm => Ok(ChainAccount::Evm(config.bridge_evm_address.clone())),
        (_, Main) => Ok(ChainAccount::Solana(WithdrawSource::Main.solana_address(config))),
        (_, Bridge) => Ok(ChainAccount::Solana(WithdrawSource::Bridge.solana_address(config))),
    }
}

/// Reads an address typed for the token's chain. Native ICP also takes a
/// checksummed ledger account id, the form exchanges give for deposits.
pub(crate) fn parse_typed_destination(token: &ChainToken, typed: &str) -> Result<ChainAccount, String> {
    let typed = typed.trim();
    match token {
        ChainToken::Icp { ledger, .. } => match native_icp_account_id(*ledger, typed) {
            Some(account_id) => Ok(ChainAccount::IcpLedger(account_id)),
            None => Account::from_str(typed)
                .map(ChainAccount::Icp)
                .map_err(|_| format!("invalid destination ICP account: {typed}")),
        },
        ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => parse_evm_destination(typed)
            .map(|address| ChainAccount::Evm(address.to_string()))
            .ok_or_else(|| format!("invalid destination EVM address: {typed}")),
        ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => typed
            .parse::<Pubkey>()
            .map(|address| ChainAccount::Solana(address.to_string()))
            .map_err(|_| format!("invalid destination Solana address: {typed}")),
    }
}

/// A mixed-case address carries an EIP-55 checksum, so a mistyped digit fails
/// it. A single-case address has no checksum to check.
fn parse_evm_destination(typed: &str) -> Option<EvmAddress> {
    let digits = typed.strip_prefix("0x").unwrap_or(typed);
    let mixed_case = digits.chars().any(|c| c.is_ascii_lowercase()) && digits.chars().any(|c| c.is_ascii_uppercase());
    if mixed_case {
        EvmAddress::parse_checksummed(format!("0x{digits}"), None).ok()
    } else {
        typed.parse::<EvmAddress>().ok()
    }
}

/// A ledger account id counts only on the native ICP ledger, and only with a
/// matching checksum.
fn native_icp_account_id(ledger: Principal, typed: &str) -> Option<String> {
    let native_ledger = Principal::from_text(crate::utils::ICP_LEDGER_PRINCIPAL).ok()?;
    let normalized = typed.trim().to_ascii_lowercase();
    (ledger == native_ledger && ic_ledger_types::AccountIdentifier::from_hex(&normalized).is_ok()).then_some(normalized)
}

/// The amount a withdrawal sends: `text` exactly, or for "all" the balance
/// less what the source must keep to pay for this send.
pub(crate) async fn withdraw_amount(
    ctx: &PipelineContext,
    source: WithdrawSource,
    asset: &AssetId,
    token: &ChainToken,
    text: &str,
    destination: &ChainAccount,
) -> Result<ChainTokenAmount, String> {
    let all = text.trim().eq_ignore_ascii_case("all");
    let exact = || ChainTokenAmount::from_decimal_str(token.clone(), text);
    let units = match token {
        ChainToken::Icp { fee, .. } => {
            if !all {
                return exact();
            }
            let balance = balance_units(ctx, source, asset).await?;
            balance_after(balance, to_units(fee, "ledger fee")?, "the ledger fee")?
        }
        ChainToken::EvmNative { .. } => {
            if !all {
                return exact();
            }
            let gas = evm_gas(&ctx.config, source, EVM_NATIVE_TRANSFER_GAS_LIMIT).await?;
            balance_after(gas.native_balance_wei, gas.reserve_wei, "the EVM gas reserve")?
        }
        ChainToken::EvmErc20 { .. } => {
            if !all {
                return exact();
            }
            let gas = evm_gas(&ctx.config, source, EVM_ERC20_TRANSFER_GAS_LIMIT).await?;
            balance_after(gas.native_balance_wei, gas.reserve_wei, "the EVM gas reserve")?;
            balance_units(ctx, source, asset).await?
        }
        ChainToken::SolanaNative { .. } => {
            let balance = balance_units(ctx, source, asset).await?;
            if !all {
                let amount = exact()?;
                let send = to_units(&amount.value, "amount")?;
                if balance < send.saturating_add(SOLANA_NATIVE_FEE_RESERVE_LAMPORTS) {
                    return Err(format!(
                        "SOL balance {balance} cannot send {send} and keep the {SOLANA_NATIVE_FEE_RESERVE_LAMPORTS}-lamport fee reserve"
                    ));
                }
                return Ok(amount);
            }
            let ChainAccount::Solana(to) = destination else {
                return Err("a SOL withdrawal needs a Solana destination".to_string());
            };
            let from = source.solana_address(&ctx.config);
            let fee = solana_transfer_fee(&ctx.config.solana_rpc_url, &from, to).await?;
            balance_after(balance, fee, "the Solana transfer fee")?
        }
        ChainToken::SolanaSpl { .. } => {
            let sol = solana_native_balance(ctx, source).await?;
            balance_after(
                sol,
                SOLANA_SPL_FEE_RESERVE_LAMPORTS,
                "the SOL fee reserve for SPL transfers",
            )?;
            if !all {
                return exact();
            }
            balance_units(ctx, source, asset).await?
        }
    };
    if units == 0 {
        return Err(format!("no {} to send", asset.symbol));
    }
    Ok(ChainTokenAmount::from_raw(token.clone(), Nat::from(units)))
}

/// What is left to send after keeping `keep` back. Refused when nothing is left.
fn balance_after(balance: u128, keep: u128, kept_for: &str) -> Result<u128, String> {
    match balance.checked_sub(keep) {
        Some(left) if left > 0 => Ok(left),
        _ => Err(format!("balance {balance} does not cover {kept_for} of {keep}")),
    }
}

fn to_units(value: &Nat, what: &str) -> Result<u128, String> {
    value.0.to_u128().ok_or_else(|| format!("{what} {value} is too large"))
}

async fn balance_units(ctx: &PipelineContext, source: WithdrawSource, asset: &AssetId) -> Result<u128, String> {
    let balance = source.balances(ctx).get_balance(asset).await?;
    to_units(&balance.value, &format!("{} balance", asset.symbol))
}

async fn solana_native_balance(ctx: &PipelineContext, source: WithdrawSource) -> Result<u128, String> {
    let native = ctx
        .registry
        .tokens
        .iter()
        .find_map(|(id, token)| matches!(token, ChainToken::SolanaNative { .. }).then(|| id.clone()))
        .ok_or_else(|| {
            "missing Solana native asset in registry; expected an entry like `sol:native:SOL`".to_string()
        })?;
    balance_units(ctx, source, &native).await
}

struct EvmGas {
    reserve_wei: u128,
    native_balance_wei: u128,
}

/// The ETH the source must keep for one send at the current gas price, and
/// the ETH it holds.
async fn evm_gas(config: &Config, source: WithdrawSource, gas_limit: u128) -> Result<EvmGas, String> {
    let signer: PrivateKeySigner = source
        .evm_private_key(config)
        .parse()
        .map_err(|e| format!("failed to parse EVM private key for source {source:?}: {e}"))?;
    let signer_address = signer.address();
    let rpc_url = config
        .evm_rpc_url
        .parse()
        // RPC URLs often carry an API key, so the error leaves the URL out.
        .map_err(|e| format!("invalid EVM RPC URL: {e}"))?;
    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(rpc_url);

    let gas_price_wei = provider
        .get_gas_price()
        .await
        .map_err(|e| format!("failed to fetch EVM gas price: {e}"))?;
    let dynamic_reserve = gas_price_wei
        .saturating_mul(gas_limit)
        .saturating_mul(EVM_GAS_RESERVE_MULTIPLIER);
    let native_balance_wei = provider
        .get_balance(signer_address)
        .await
        .map_err(|e| format!("failed to fetch EVM native balance for {signer_address}: {e}"))?
        .to::<u128>();
    Ok(EvmGas {
        reserve_wei: dynamic_reserve.max(EVM_NATIVE_GAS_BUFFER_WEI),
        native_balance_wei,
    })
}

async fn solana_transfer_fee(rpc_url: &str, from: &str, to: &str) -> Result<u128, String> {
    let from_pubkey = from
        .parse::<Pubkey>()
        .map_err(|e| format!("invalid Solana source pubkey `{from}`: {e}"))?;
    let to_pubkey = to
        .parse::<Pubkey>()
        .map_err(|e| format!("invalid Solana destination pubkey `{to}`: {e}"))?;

    let rpc = RpcClient::new(rpc_url.to_string());
    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .map_err(|e| format!("solana get_latest_blockhash failed: {e}"))?;
    let ix = system_instruction::transfer(&from_pubkey, &to_pubkey, 1);
    let msg = Message::new_with_blockhash(&[ix], Some(&from_pubkey), &blockhash);
    let fee = rpc
        .get_fee_for_message(&msg)
        .await
        .map_err(|e| format!("solana get_fee_for_message failed: {e}"))?;
    if fee == 0 {
        return Err("solana estimated transfer fee is zero; refusing sweep".to_string());
    }
    Ok(u128::from(fee))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn icp_token(ledger: &str) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_text(ledger).unwrap(),
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        }
    }

    #[test]
    fn all_keeps_back_what_the_send_costs() {
        assert_eq!(balance_after(100, 3, "the ledger fee"), Ok(97));
        for (balance, keep) in [(3, 3), (2, 3), (0, 0)] {
            let refused = balance_after(balance, keep, "the ledger fee").expect_err("nothing left");
            assert!(refused.contains("does not cover the ledger fee"), "{refused}");
        }
        let huge = Nat::from(u128::MAX) + Nat::from(1u8);
        assert!(to_units(&huge, "balance").is_err());
    }

    #[test]
    fn cli_names_map_to_sources_and_destinations() {
        assert_eq!(WithdrawSource::parse(" Liquidator "), Some(WithdrawSource::Main));
        assert_eq!(WithdrawSource::parse("r"), Some(WithdrawSource::Recovery));
        assert_eq!(WithdrawSource::parse("other"), None);
        assert_eq!(WithdrawDestination::parse("BRIDGE"), WithdrawDestination::Bridge);
        assert_eq!(WithdrawDestination::parse("aaaaa-aa"), WithdrawDestination::Manual);
    }

    #[test]
    fn only_a_checksummed_account_id_on_the_icp_ledger_is_a_ledger_destination() {
        let icp = Principal::from_text(crate::utils::ICP_LEDGER_PRINCIPAL).unwrap();
        let other = Principal::from_text("xevnm-gaaaa-aaaar-qafnq-cai").unwrap();
        let id = "D26A53ADB4DE6404F3D4A7B7978D233A99AFF6DFD35B45C41AE1AAB4CA249B9F";
        assert_eq!(
            native_icp_account_id(icp, id).as_deref(),
            Some(id.to_ascii_lowercase().as_str())
        );
        assert_eq!(native_icp_account_id(other, id), None, "ICRC tokens need an Account");
        let typo = "e26a53adb4de6404f3d4a7b7978d233a99aff6dfd35b45c41ae1aab4ca249b9f";
        assert_eq!(native_icp_account_id(icp, typo), None, "a bad checksum must not pass");
        assert_eq!(native_icp_account_id(icp, "main"), None);
    }

    #[test]
    fn typed_destinations_follow_the_token_chain() {
        let icp = icp_token(crate::utils::ICP_LEDGER_PRINCIPAL);
        let id = "d26a53adb4de6404f3d4a7b7978d233a99aff6dfd35b45c41ae1aab4ca249b9f";
        assert_eq!(
            parse_typed_destination(&icp, id),
            Ok(ChainAccount::IcpLedger(id.to_string()))
        );
        assert_eq!(
            parse_typed_destination(&icp, " aaaaa-aa "),
            Ok(ChainAccount::Icp(Principal::management_canister().into()))
        );
        let ckusdc = icp_token(crate::utils::CKUSDC_LEDGER_PRINCIPAL);
        assert!(parse_typed_destination(&ckusdc, id).is_err());

        let evm = ChainToken::EvmNative {
            chain: "eth".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        };
        assert!(parse_typed_destination(&evm, "0x2222222222222222222222222222222222222222").is_ok());
        assert!(parse_typed_destination(&evm, "aaaaa-aa").is_err());
    }

    /// Review finding 32: a mixed-case address with one wrong digit passed, so
    /// a withdrawal could pay an address nobody owns.
    #[test]
    fn a_mixed_case_evm_address_must_match_its_checksum() {
        let evm = ChainToken::EvmNative {
            chain: "eth".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        };
        let checksummed = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
        assert!(parse_typed_destination(&evm, checksummed).is_ok());
        assert!(parse_typed_destination(&evm, &checksummed.to_ascii_lowercase()).is_ok());
        assert!(parse_typed_destination(&evm, &checksummed[2..]).is_ok());
        assert!(parse_typed_destination(&evm, "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96046").is_err());
    }
}
