//! Liquidations paid from the liquidator's Liquidium supply position (`LIQUIDATION_FUNDING=supply`).
//!
//! In supply mode the bot's capital sits supplied in the debt pools. The lending canister's
//! `liquidate_from_supply` burns that supply to repay the borrower, so the wallet only holds
//! swap proceeds until they are supplied again.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use candid::{CandidType, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::balance_service::BalanceService;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;
use liquidium_pipeline_core::types::protocol_types::{AssetType, ProtocolError};
use serde::{Deserialize, Serialize};

/// Lending endpoint used in wallet mode: the canister pulls the debt asset from the wallet.
pub const WALLET_LIQUIDATE_METHOD: &str = "liquidate_with_slippage";
/// Lending endpoint used in supply mode: the canister burns the caller's supply.
pub const SUPPLY_LIQUIDATE_METHOD: &str = "liquidate_from_supply";

/// Signed requests must expire within the canister's signature window (5 minutes).
const REQUEST_EXPIRY_SECS: u64 = 4 * 60;
/// RAY precision used by the lending canister's share indices.
const ONE_RAY: u128 = 1_000_000_000_000_000_000_000_000_000;
/// Native units held back per supply-funded offer. Each burn rounds its shares up, which can
/// cost up to one share (a couple of native units at realistic indices) beyond the offer.
pub const SUPPLY_BURN_ROUNDING_RESERVE: u64 = 2;
/// How often idle wallet balances are supplied back to the pools.
pub const RESUPPLY_INTERVAL: Duration = Duration::from_secs(60);
/// Wallet balances below this many ledger fees are left alone to avoid dust deposits.
const RESUPPLY_MIN_FEE_MULTIPLE: u64 = 100;

/// Where the debt asset for a liquidation comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LiquidationFunding {
    /// Debt asset is pulled from the liquidator wallet via ICRC-2 (default).
    #[default]
    Wallet,
    /// Debt is repaid by burning the liquidator's supply in the debt pool.
    Supply,
}

impl LiquidationFunding {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "wallet" => Ok(Self::Wallet),
            "supply" => Ok(Self::Supply),
            other => Err(format!(
                "LIQUIDATION_FUNDING must be `wallet` or `supply`, got `{other}`"
            )),
        }
    }

    pub fn liquidate_method(self) -> &'static str {
        match self {
            Self::Wallet => WALLET_LIQUIDATE_METHOD,
            Self::Supply => SUPPLY_LIQUIDATE_METHOD,
        }
    }
}

/// The subset of the lending `PositionView` this module needs.
#[derive(Debug, Clone, CandidType, Deserialize)]
struct PositionSupplyView {
    deposit_scaled: Nat,
    lending_index_now: Nat,
}

/// The subset of the lending `Pool` record this module needs.
#[derive(Debug, Clone, CandidType, Deserialize)]
struct LendingPoolView {
    principal: Principal,
    asset_type: AssetType,
}

#[derive(Debug, Clone, CandidType)]
enum InflowSubaccount {
    #[allow(dead_code)]
    Repayment(Principal),
    Deposit(Principal),
}

#[derive(Debug, Clone, CandidType, Deserialize)]
struct InitializeAccountRequest {
    expiry_timestamp: u64,
}

#[derive(Debug, Clone, CandidType, Deserialize)]
struct IcpCallerAuth {
    principal: Principal,
    subaccount: Option<Vec<u8>>,
}

#[derive(Debug, Clone, CandidType, Deserialize)]
enum SignatureScheme {
    IcpCaller(IcpCallerAuth),
}

#[derive(Debug, Clone, CandidType, Deserialize)]
struct SignedRequest<T> {
    data: T,
    signature_info: SignatureScheme,
}

/// Reads how much of a debt asset the liquidator has supplied, for sizing liquidations.
#[async_trait]
pub trait SupplyBalanceSource: Send + Sync {
    async fn supplied_native(&self, pool_id: Principal) -> Result<Nat, String>;
}

/// [`SupplyBalanceSource`] backed by the lending canister's `get_position`.
pub struct LendingSupplyBalances<A: PipelineAgent> {
    agent: Arc<A>,
    lending_canister: Principal,
    profile: Principal,
}

impl<A: PipelineAgent> LendingSupplyBalances<A> {
    pub fn new(agent: Arc<A>, lending_canister: Principal, profile: Principal) -> Self {
        Self {
            agent,
            lending_canister,
            profile,
        }
    }
}

#[async_trait]
impl<A: PipelineAgent> SupplyBalanceSource for LendingSupplyBalances<A> {
    async fn supplied_native(&self, pool_id: Principal) -> Result<Nat, String> {
        supplied_native(self.agent.as_ref(), &self.lending_canister, self.profile, pool_id).await
    }
}

/// Returns the profile that owns the liquidator's default ICP account, if one is registered.
pub async fn lookup_profile<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidator: Principal,
) -> Result<Option<Principal>, String> {
    let args = Encode!(&liquidator.to_text()).map_err(|e| format!("get_wallet_profile encode error: {e}"))?;
    agent
        .call_query::<Option<Principal>>(lending_canister, "get_wallet_profile", args)
        .await
        .map_err(|e| format!("get_wallet_profile failed: {e}"))
}

/// Registers a Liquidium profile for the liquidator's ICP principal (the agent's identity).
pub async fn register_profile<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidator: Principal,
) -> Result<Principal, String> {
    let request = SignedRequest {
        data: InitializeAccountRequest {
            expiry_timestamp: request_expiry()?,
        },
        signature_info: caller_auth(liquidator),
    };
    let args = Encode!(&request).map_err(|e| format!("register_profile encode error: {e}"))?;
    agent
        .call_update::<Result<Principal, ProtocolError>>(lending_canister, "register_profile", args)
        .await?
        .map_err(|e| format!("register_profile rejected: {e:?}"))
}

#[derive(Debug, Clone, CandidType, Deserialize)]
enum AccountType {
    Native(Principal),
}

#[derive(Debug, Clone, CandidType, Deserialize)]
struct WithdrawRequest {
    account: AccountType,
    pool_id: Principal,
    amount: Nat,
    expiry_timestamp: u64,
}

fn caller_auth(liquidator: Principal) -> SignatureScheme {
    SignatureScheme::IcpCaller(IcpCallerAuth {
        principal: liquidator,
        subaccount: None,
    })
}

fn request_expiry() -> Result<u64, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock error: {e}"))?
        .as_secs();
    Ok(now + REQUEST_EXPIRY_SECS)
}

/// Withdraws `amount` of supply from `pool_id` back to the liquidator's own principal.
pub async fn withdraw_supply<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidator: Principal,
    profile: Principal,
    pool_id: Principal,
    amount: Nat,
) -> Result<(), String> {
    let request = SignedRequest {
        data: WithdrawRequest {
            account: AccountType::Native(liquidator),
            pool_id,
            amount,
            expiry_timestamp: request_expiry()?,
        },
        signature_info: caller_auth(liquidator),
    };
    let args = Encode!(&profile, &request).map_err(|e| format!("withdraw encode error: {e}"))?;
    agent
        .call_update::<Result<candid::Reserved, ProtocolError>>(lending_canister, "withdraw", args)
        .await?
        .map(|_| ())
        .map_err(|e| format!("withdraw rejected: {e:?}"))
}

/// Largest offer the canister accepts for `shares`: it burns `ceil(offer / index)` shares, so
/// `floor(shares * index)` is always covered. The index only grows, so the bound still holds
/// when the call lands later.
pub(crate) fn spendable_native(shares: &Nat, lending_index: &Nat) -> Nat {
    let index = if *lending_index == 0u8 {
        Nat::from(ONE_RAY)
    } else {
        lending_index.clone()
    };
    (shares.clone() * index) / Nat::from(ONE_RAY)
}

/// Native amount of `pool_id` supply the profile can spend on liquidations (zero without a position).
pub async fn supplied_native<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    profile: Principal,
    pool_id: Principal,
) -> Result<Nat, String> {
    let args = Encode!(&profile, &pool_id).map_err(|e| format!("get_position encode error: {e}"))?;
    let position = agent
        .call_query::<Option<PositionSupplyView>>(lending_canister, "get_position", args)
        .await
        .map_err(|e| format!("get_position failed for pool {pool_id}: {e}"))?;
    Ok(position
        .map(|p| spendable_native(&p.deposit_scaled, &p.lending_index_now))
        .unwrap_or_else(|| Nat::from(0u8)))
}

/// Finds the lending pool whose asset is the given ICP ledger.
pub async fn pool_for_ledger<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    ledger: Principal,
) -> Result<Principal, String> {
    let args = Encode!().map_err(|e| format!("list_pools encode error: {e}"))?;
    let pools = agent
        .call_query::<Vec<LendingPoolView>>(lending_canister, "list_pools", args)
        .await
        .map_err(|e| format!("list_pools failed: {e}"))?;
    pools
        .into_iter()
        .find(|pool| pool.asset_type == AssetType::CkAsset(ledger))
        .map(|pool| pool.principal)
        .ok_or_else(|| format!("no lending pool for ledger {ledger}"))
}

/// The pool subaccount that credits deposits to `profile`.
pub async fn deposit_subaccount<A: PipelineAgent>(
    agent: &A,
    pool: Principal,
    profile: Principal,
) -> Result<[u8; 32], String> {
    let args =
        Encode!(&InflowSubaccount::Deposit(profile)).map_err(|e| format!("get_inflow_subaccount encode error: {e}"))?;
    let subaccount = agent
        .call_query::<Vec<u8>>(&pool, "get_inflow_subaccount", args)
        .await
        .map_err(|e| format!("get_inflow_subaccount failed: {e}"))?;
    subaccount
        .as_slice()
        .try_into()
        .map_err(|_| format!("pool returned a {}-byte subaccount", subaccount.len()))
}

/// Tokens sitting in the deposit subaccount that the pool has not swept yet.
pub async fn unswept_deposit<A: PipelineAgent>(
    agent: &A,
    ledger: Principal,
    pool: Principal,
    subaccount: [u8; 32],
) -> Result<Nat, String> {
    let account = Account {
        owner: pool,
        subaccount: Some(subaccount),
    };
    let args = Encode!(&account).map_err(|e| format!("icrc1_balance_of encode error: {e}"))?;
    agent
        .call_query::<Nat>(&ledger, "icrc1_balance_of", args)
        .await
        .map_err(|e| format!("icrc1_balance_of failed: {e}"))
}

/// Asks the pool to sweep the deposit subaccount and credit the profile's supply. The ICP pool
/// only credits subaccounts it has been told about, so this call is required.
pub async fn sweep_deposit<A: PipelineAgent>(agent: &A, pool: Principal, subaccount: [u8; 32]) -> Result<(), String> {
    let args = Encode!(&subaccount.to_vec()).map_err(|e| format!("check_subaccount encode error: {e}"))?;
    agent
        .call_update::<Result<(), candid::Reserved>>(&pool, "check_subaccount", args)
        .await
        .map_err(|e| format!("check_subaccount failed: {e}"))?
        .map_err(|_| "check_subaccount rejected the sweep".to_string())
}

/// Supplies `amount` of an ICP-ledger token to `pool` on behalf of `profile`.
///
/// Transfers to the pool's deposit subaccount for the profile, then asks the pool to sweep it.
/// If either step fails, or the transfer outcome is unknown, the tokens may sit in the
/// subaccount. Recover them with [`unswept_deposit`] and [`sweep_deposit`], never by sending again.
pub async fn deposit<A: PipelineAgent>(
    agent: &A,
    transfers: &dyn TransferActions,
    token: &ChainToken,
    pool: Principal,
    profile: Principal,
    amount: Nat,
) -> Result<String, String> {
    if !matches!(token, ChainToken::Icp { .. }) {
        return Err(format!("{} is not an ICP ledger token", token.symbol()));
    }

    let subaccount = deposit_subaccount(agent, pool, profile).await?;
    let to = ChainAccount::Icp(Account {
        owner: pool,
        subaccount: Some(subaccount),
    });
    let tx = transfers
        .transfer(token, &to, amount)
        .await
        .map_err(|e| format!("supply transfer failed: {e}"))?;

    sweep_deposit(agent, pool, subaccount)
        .await
        .map_err(|e| format!("supply transfer {tx} landed but is not credited yet: {e}"))?;
    Ok(tx)
}

/// Amount to supply from a wallet balance: everything except the transfer fee, once the
/// balance is worth at least [`RESUPPLY_MIN_FEE_MULTIPLE`] fees.
pub(crate) fn resupply_amount(balance: &Nat, fee: &Nat) -> Option<Nat> {
    let minimum = fee.clone() * Nat::from(RESUPPLY_MIN_FEE_MULTIPLE);
    if *balance < minimum || *balance <= *fee {
        return None;
    }
    Some(balance.clone() - fee.clone())
}

/// Periodically supplies the liquidator wallet's debt-asset balances (swap proceeds) back to
/// their pools, so capital does not sit idle between liquidations.
///
/// Each pass reads the wallet balance first and only then checks that no liquidation is in
/// flight. Anything that lands after the read is left for the next pass, so a sweep never
/// moves tokens that arrived from a liquidation not yet recorded in the WAL. Amounts come from
/// live balances, so an ambiguous transfer is never repeated blindly.
struct ResupplyTarget {
    token: ChainToken,
    ledger: Principal,
    pool: Principal,
    subaccount: [u8; 32],
}

pub struct Resupplier<A: PipelineAgent> {
    agent: Arc<A>,
    profile: Principal,
    balances: Arc<BalanceService>,
    transfers: Arc<dyn TransferActions + Send + Sync>,
    targets: Vec<ResupplyTarget>,
    in_flight: Arc<AtomicUsize>,
}

impl<A: PipelineAgent + 'static> Resupplier<A> {
    /// Resolves the debt pool for every ICP-ledger debt token.
    pub async fn new(
        agent: Arc<A>,
        lending_canister: Principal,
        profile: Principal,
        debt_tokens: Vec<ChainToken>,
        balances: Arc<BalanceService>,
        transfers: Arc<dyn TransferActions + Send + Sync>,
        in_flight: Arc<AtomicUsize>,
    ) -> Result<Self, String> {
        let mut targets = Vec::new();
        for token in debt_tokens {
            let ChainToken::Icp { ledger, .. } = &token else {
                continue;
            };
            let ledger = *ledger;
            let pool = pool_for_ledger(agent.as_ref(), &lending_canister, ledger).await?;
            let subaccount = deposit_subaccount(agent.as_ref(), pool, profile).await?;
            targets.push(ResupplyTarget {
                token,
                ledger,
                pool,
                subaccount,
            });
        }

        Ok(Self {
            agent,
            profile,
            balances,
            transfers,
            targets,
            in_flight,
        })
    }

    pub async fn run_once(&self) {
        for target in &self.targets {
            // Finish any earlier deposit whose sweep failed or whose transfer outcome was unknown.
            self.sweep_leftover(target).await;

            let token = &target.token;
            let balance = match self.balances.get_balance(&token.asset_id()).await {
                Ok(balance) => balance.value,
                Err(err) => {
                    tracing::warn!(asset = %token.symbol(), error = %err, "resupply balance read failed");
                    continue;
                }
            };
            let Some(amount) = resupply_amount(&balance, &token.fee()) else {
                continue;
            };
            if self.in_flight.load(Ordering::SeqCst) > 0 {
                return;
            }

            match deposit(
                self.agent.as_ref(),
                self.transfers.as_ref(),
                token,
                target.pool,
                self.profile,
                amount.clone(),
            )
            .await
            {
                Ok(tx) => {
                    tracing::info!(asset = %token.symbol(), amount = %amount, tx = %tx, "Supplied idle wallet balance")
                }
                Err(err) => {
                    tracing::warn!(asset = %token.symbol(), error = %err, "resupply failed; retrying next pass")
                }
            }
        }
    }

    async fn sweep_leftover(&self, target: &ResupplyTarget) {
        let unswept = match unswept_deposit(self.agent.as_ref(), target.ledger, target.pool, target.subaccount).await {
            Ok(unswept) => unswept,
            Err(err) => {
                tracing::warn!(asset = %target.token.symbol(), error = %err, "could not read deposit subaccount");
                return;
            }
        };
        if unswept <= target.token.fee() {
            return;
        }
        match sweep_deposit(self.agent.as_ref(), target.pool, target.subaccount).await {
            Ok(()) => {
                tracing::info!(asset = %target.token.symbol(), amount = %unswept, "Credited a pending supply deposit")
            }
            Err(err) => {
                tracing::warn!(asset = %target.token.symbol(), error = %err, "pending supply deposit still not credited")
            }
        }
    }

    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RESUPPLY_INTERVAL).await;
                self.run_once().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_wallet() {
        // given / when / then
        assert_eq!(LiquidationFunding::parse("").unwrap(), LiquidationFunding::Wallet);
        assert_eq!(LiquidationFunding::parse("Wallet").unwrap(), LiquidationFunding::Wallet);
        assert_eq!(
            LiquidationFunding::parse(" supply ").unwrap(),
            LiquidationFunding::Supply
        );
        assert!(LiquidationFunding::parse("pool").is_err());
    }

    #[test]
    fn spendable_native_floors_so_the_ceil_burn_fits() {
        // given 1_500_000 shares at an index of 1.000001
        let shares = Nat::from(1_500_000u64);
        let index = Nat::from(ONE_RAY + ONE_RAY / 1_000_000);

        // when
        let spendable = spendable_native(&shares, &index);

        // then the canister's ceil(offer / index) burn never exceeds the shares held
        let ray = Nat::from(ONE_RAY);
        let burn = (spendable.clone() * ray + index.clone() - Nat::from(1u8)) / index;
        assert_eq!(spendable, Nat::from(1_500_001u64));
        assert!(burn <= shares);
    }

    #[test]
    fn resupply_amount_skips_dust_and_keeps_the_fee() {
        // given a 10_000 unit ledger fee
        let fee = Nat::from(10_000u32);

        // when / then
        assert_eq!(resupply_amount(&Nat::from(999_999u32), &fee), None);
        assert_eq!(
            resupply_amount(&Nat::from(1_000_000u32), &fee),
            Some(Nat::from(990_000u32))
        );
        assert_eq!(resupply_amount(&Nat::from(0u8), &Nat::from(0u8)), None);
    }

    #[test]
    fn funding_selects_lending_method() {
        // given / when / then
        assert_eq!(LiquidationFunding::Wallet.liquidate_method(), "liquidate_with_slippage");
        assert_eq!(LiquidationFunding::Supply.liquidate_method(), "liquidate_from_supply");
    }
}
