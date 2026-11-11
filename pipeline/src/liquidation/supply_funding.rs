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
use liquidium_pipeline_core::outbound::{OutboundIntent, OutboundSender};
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::{TransferActions, TransferFailure};
use liquidium_pipeline_core::types::protocol_types::{AssetType, LiquidationRequest, ProtocolError};
use serde::{Deserialize, Serialize};

use crate::liquidation::supply_resupply_journal::ResupplyJournal;
use crate::outbound::driver::{OutboundError, OutboundPhase, step_outbound};
use crate::utils::now_ts;
use crate::watchdog::{Watchdog, WatchdogEvent};

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
/// What one supply-funded liquidation costs in debt-asset native units, beyond the repaid debt:
/// the share-rounding reserve, plus the two ledger fees paid to supply the proceeds again (the
/// wallet transfer and the pool's sweep of the deposit subaccount). A top-up can cover several
/// liquidations, so this is the upper bound per liquidation.
pub fn supply_cycle_cost(token: &ChainToken) -> Nat {
    Nat::from(SUPPLY_BURN_ROUNDING_RESERVE) + token.fee() * Nat::from(2u8)
}

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

    /// Whether a refused cash payout is worth one more call that takes the collateral as supply.
    /// Only the two refusals an illiquid collateral pool produces qualify: no cash at all, or a
    /// partial fill that the minimum-collateral guard turned down. Any other cause refuses the
    /// second call the same way, and a refusal moves nothing.
    pub fn retries_with_supply_payout(self, refusal: &ProtocolError) -> bool {
        self == Self::Supply && matches!(refusal, ProtocolError::NoLiquidity | ProtocolError::NotAllowed(_))
    }

    /// Candid arguments for [`Self::liquidate_method`]. Only supply mode can name how collateral
    /// is delivered; wallet mode always gets cash.
    pub fn encode_liquidate_args(
        self,
        request: &LiquidationRequest,
        min_collateral_amount: &Nat,
        payout: CollateralPayout,
    ) -> Result<Vec<u8>, String> {
        match self {
            Self::Wallet => Encode!(request, min_collateral_amount),
            Self::Supply => Encode!(request, min_collateral_amount, &payout),
        }
        .map_err(|e| format!("liquidation args encode error: {e}"))
    }
}

/// Mirrors the lending canister's `CollateralPayout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CandidType, Deserialize)]
pub enum CollateralPayout {
    /// Transfer the collateral out of the collateral pool.
    Cash,
    /// Credit the collateral to the caller's profile as supply.
    Supply,
}

/// The subset of the lending `PositionView` this module needs.
#[derive(Debug, Clone, CandidType, Deserialize)]
struct PositionSupplyView {
    deposit_scaled: Nat,
    lending_index_now: Nat,
}

/// The subset of the lending `Pool` record this module needs.
#[derive(Debug, Clone, CandidType, Deserialize)]
pub(crate) struct LendingPoolView {
    pub(crate) principal: Principal,
    pub(crate) asset_type: AssetType,
    pub(crate) total_supply_at_last_sync: Nat,
    pub(crate) total_debt_at_last_sync: Nat,
    pub(crate) lending_index: Nat,
    pub(crate) borrow_index: Nat,
}

/// The part of the lending `OutflowDetails` a withdrawal answer is read for.
#[derive(Debug, Clone, CandidType, Deserialize)]
pub(crate) struct WithdrawOutflow {
    pub(crate) amount: Nat,
}

/// Where collateral that was credited as supply stands on its way to the swap account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollateralRelease {
    /// Still supplied in the collateral pool under the liquidator's profile.
    Held,
    /// A withdrawal call was started. If this is read back after a restart, the call may or
    /// may not have run, so only an operator can decide it.
    Withdrawing,
    /// The withdrawal was accepted at this unix time; the payout is on its way.
    Requested { at: i64 },
}

/// Why collateral held as supply was not released on this attempt.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReleaseError {
    /// The collateral pool cannot pay the amount out yet. Nothing was called.
    #[error("collateral pool has {available} free, {needed} needed")]
    NotYetLiquid { available: Nat, needed: Nat },
    /// The canister refused the withdrawal. Nothing moved; a later attempt is safe.
    #[error("withdrawal rejected: {0}")]
    Rejected(String),
    /// The call got no decided answer, so the withdrawal may have run.
    #[error("withdrawal has no decided answer: {0}")]
    Ambiguous(String),
    /// The liquidity read failed before any call was made.
    #[error("{0}")]
    NotAttempted(String),
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
    withdraw_to(
        agent,
        lending_canister,
        liquidator,
        profile,
        pool_id,
        liquidator,
        amount,
    )
    .await?
    .map(|_| ())
    .map_err(|e| format!("withdraw rejected: {e:?}"))
}

/// Calls the lending `withdraw` and pays `receiver`. The outer error is a call with no decided
/// answer; the inner one is a refusal by the canister, which moved nothing.
async fn withdraw_to<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidator: Principal,
    profile: Principal,
    pool_id: Principal,
    receiver: Principal,
    amount: Nat,
) -> Result<Result<Nat, ProtocolError>, String> {
    let request = SignedRequest {
        data: WithdrawRequest {
            account: AccountType::Native(receiver),
            pool_id,
            amount,
            expiry_timestamp: request_expiry()?,
        },
        signature_info: caller_auth(liquidator),
    };
    let args = Encode!(&profile, &request).map_err(|e| format!("withdraw encode error: {e}"))?;
    let answer = agent
        .call_update::<Result<WithdrawOutflow, ProtocolError>>(lending_canister, "withdraw", args)
        .await?;
    Ok(answer.map(|outflow| outflow.amount))
}

/// Cash the pool can pay out right now, from its last synced totals.
pub async fn pool_free_liquidity<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    pool_id: Principal,
) -> Result<Nat, String> {
    let args = Encode!().map_err(|e| format!("list_pools encode error: {e}"))?;
    let pool = agent
        .call_query::<Vec<LendingPoolView>>(lending_canister, "list_pools", args)
        .await
        .map_err(|e| format!("list_pools failed: {e}"))?
        .into_iter()
        .find(|pool| pool.principal == pool_id)
        .ok_or_else(|| format!("lending pool {pool_id} not found"))?;

    let supply = spendable_native(&pool.total_supply_at_last_sync, &pool.lending_index);
    // Debt is rounded up so the free amount is never overstated.
    let debt = spendable_native(&pool.total_debt_at_last_sync, &pool.borrow_index) + Nat::from(1u8);
    Ok(if supply > debt { supply - debt } else { Nat::from(0u8) })
}

/// Moves collateral that a liquidation credited as supply out to `receiver`.
///
/// The withdrawal is only called once the pool shows enough free cash, because the canister
/// traps on an illiquid withdrawal and a trap cannot be told apart from a lost answer here.
/// Returns the amount the canister withdrew, before the ledger fee of the payout.
pub async fn release_collateral<A: PipelineAgent>(
    agent: &A,
    lending_canister: &Principal,
    liquidator: Principal,
    profile: Principal,
    pool_id: Principal,
    receiver: Principal,
    amount: Nat,
) -> Result<Nat, ReleaseError> {
    let available = pool_free_liquidity(agent, lending_canister, pool_id)
        .await
        .map_err(ReleaseError::NotAttempted)?;
    if available < amount {
        return Err(ReleaseError::NotYetLiquid {
            available,
            needed: amount,
        });
    }

    match withdraw_to(agent, lending_canister, liquidator, profile, pool_id, receiver, amount).await {
        Ok(Ok(withdrawn)) => Ok(withdrawn),
        Ok(Err(refusal)) => Err(ReleaseError::Rejected(format!("{refusal:?}"))),
        Err(reason) => Err(ReleaseError::Ambiguous(reason)),
    }
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

/// Why a one-off deposit did not finish. The variant says whether tokens may have moved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DepositError {
    /// Nothing was sent.
    #[error("{0}")]
    NotSent(String),
    /// The transfer may or may not have happened. Check the deposit subaccount before sending again.
    #[error("supply transfer has no decided answer: {0}")]
    Ambiguous(String),
    /// The transfer landed but the pool has not credited it yet. A later sweep credits it.
    #[error("supply transfer {tx} landed but is not credited yet: {reason}")]
    NotCredited { tx: String, reason: String },
}

/// Supplies `amount` of an ICP-ledger token to `pool` on behalf of `profile`.
///
/// Transfers to the pool's deposit subaccount for the profile, then asks the pool to sweep it.
/// On [`DepositError::Ambiguous`] or [`DepositError::NotCredited`] the tokens may sit in the
/// subaccount. Recover them with [`unswept_deposit`] and [`sweep_deposit`], never by sending again.
pub async fn deposit<A: PipelineAgent>(
    agent: &A,
    transfers: &dyn TransferActions,
    token: &ChainToken,
    pool: Principal,
    profile: Principal,
    amount: Nat,
) -> Result<String, DepositError> {
    if !matches!(token, ChainToken::Icp { .. }) {
        return Err(DepositError::NotSent(format!(
            "{} is not an ICP ledger token",
            token.symbol()
        )));
    }

    let subaccount = deposit_subaccount(agent, pool, profile)
        .await
        .map_err(DepositError::NotSent)?;
    let to = ChainAccount::Icp(Account {
        owner: pool,
        subaccount: Some(subaccount),
    });
    let tx = transfers
        .transfer(token, &to, amount)
        .await
        .map_err(|failure| match failure {
            TransferFailure::Rejected(reason) => DepositError::NotSent(reason),
            TransferFailure::Ambiguous(reason) => DepositError::Ambiguous(reason),
        })?;

    sweep_deposit(agent, pool, subaccount)
        .await
        .map_err(|reason| DepositError::NotCredited { tx: tx.clone(), reason })?;
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

struct ResupplyTarget {
    token: ChainToken,
    ledger: Principal,
    pool: Principal,
    subaccount: [u8; 32],
}

/// Periodically supplies the liquidator wallet's debt-asset balances (swap proceeds) back to
/// their pools, so capital does not sit idle between liquidations.
///
/// Each pass reads the wallet balance first and only then checks that no liquidation is in
/// flight. Anything that lands after the read is left for the next pass, so a top-up never
/// moves tokens that arrived from a liquidation not yet recorded in the WAL.
///
/// Every transfer is saved in the [`ResupplyJournal`] before it is sent. A lost answer or a
/// restart resumes that same transfer, which the ledger applies at most once. No new top-up
/// starts for an asset while one is undecided, and one that stays undecided alerts an operator.
pub struct Resupplier<A: PipelineAgent> {
    agent: Arc<A>,
    profile: Principal,
    balances: Arc<BalanceService>,
    sender: Arc<dyn OutboundSender>,
    journal: ResupplyJournal,
    watchdog: Arc<dyn Watchdog>,
    targets: Vec<ResupplyTarget>,
    in_flight: Arc<AtomicUsize>,
}

impl<A: PipelineAgent + 'static> Resupplier<A> {
    /// Resolves the debt pool for every ICP-ledger debt token.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        agent: Arc<A>,
        lending_canister: Principal,
        profile: Principal,
        debt_tokens: Vec<ChainToken>,
        balances: Arc<BalanceService>,
        sender: Arc<dyn OutboundSender>,
        journal: ResupplyJournal,
        watchdog: Arc<dyn Watchdog>,
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
            sender,
            journal,
            watchdog,
            targets,
            in_flight,
        })
    }

    pub async fn run_once(&self) {
        // Pick up operator decisions recorded by `liquidator supply resolve-top-up`.
        if let Err(err) = self.journal.reload() {
            tracing::error!(error = %err, "resupply journal cannot be read; no top-up runs this pass");
            return;
        }
        for target in &self.targets {
            // Credit any earlier deposit that landed but was not swept.
            self.sweep_leftover(target).await;

            let key = target.ledger.to_text();
            if self.journal.get(&key).is_none() && !self.start_top_up(target, &key).await {
                continue;
            }
            self.advance_top_up(target, &key).await;
        }
    }

    /// Saves a new top-up for the wallet's idle balance. Returns false when there is nothing to
    /// send or the send could not be saved; nothing has moved in either case.
    async fn start_top_up(&self, target: &ResupplyTarget, key: &str) -> bool {
        let token = &target.token;
        let balance = match self.balances.get_balance(&token.asset_id()).await {
            Ok(balance) => balance.value,
            Err(err) => {
                tracing::warn!(asset = %token.symbol(), error = %err, "resupply balance read failed");
                return false;
            }
        };
        let Some(amount) = resupply_amount(&balance, &token.fee()) else {
            return false;
        };
        if self.in_flight.load(Ordering::SeqCst) > 0 {
            return false;
        }

        let intent = OutboundIntent {
            token: token.clone(),
            to: ChainAccount::Icp(Account {
                owner: target.pool,
                subaccount: Some(target.subaccount),
            }),
            amount_native: amount,
        };
        let mut record = None;
        if let Err(err) = step_outbound(&mut record, self.sender.as_ref(), Some(&intent), now_ts()).await {
            tracing::warn!(asset = %token.symbol(), error = %err, "could not prepare a supply top-up");
            return false;
        }
        // The send only runs once its record is on disk.
        if let Err(err) = self.journal.set(key, record) {
            tracing::warn!(asset = %token.symbol(), error = %err, "could not save a supply top-up; not sending it");
            return false;
        }
        true
    }

    /// Sends or settles the saved top-up, and credits it once it has landed.
    async fn advance_top_up(&self, target: &ResupplyTarget, key: &str) {
        let token = &target.token;
        let mut record = self.journal.get(key);
        let was_parked = matches!(record.as_ref().map(|r| &r.phase), Some(OutboundPhase::Parked { .. }));

        let result = step_outbound(&mut record, self.sender.as_ref(), None, now_ts()).await;
        if let Err(err) = self.journal.set(key, record) {
            // The stale record resumes the same send, which the ledger applies at most once.
            tracing::warn!(asset = %token.symbol(), error = %err, "could not update the supply top-up record");
        }

        match result {
            Ok(Some(tx)) => match sweep_deposit(self.agent.as_ref(), target.pool, target.subaccount).await {
                Ok(()) => tracing::info!(asset = %token.symbol(), tx = %tx, "Supplied idle wallet balance"),
                Err(err) => {
                    tracing::warn!(asset = %token.symbol(), tx = %tx, error = %err, "supply top-up landed but is not credited yet")
                }
            },
            Ok(None) => tracing::info!(asset = %token.symbol(), "supply top-up has no decided answer yet"),
            Err(OutboundError::Park(reason)) => {
                tracing::error!(asset = %token.symbol(), reason = %reason, "supply top-up needs an operator");
                if !was_parked {
                    self.watchdog
                        .notify(WatchdogEvent::OperatorRequired {
                            execution_id: format!("supply-top-up-{key}"),
                            venue: "protocol".to_string(),
                            pending_step: "supply-top-up".to_string(),
                            owner: self.profile.to_text(),
                            details: format!(
                                "a {} supply top-up may or may not have been sent, and no new top-up runs for it until this is decided: {reason}",
                                token.symbol()
                            ),
                        })
                        .await;
                }
            }
            Err(err) => {
                tracing::warn!(asset = %token.symbol(), error = %err, "supply top-up did not happen; a new one starts next pass")
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

    mod resupplier {
        use super::*;
        use crate::outbound::driver::OutboundRecord;
        use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
        use liquidium_pipeline_core::account::actions::MockAccountInfo;
        use liquidium_pipeline_core::outbound::{MockOutboundSender, PreparedSend, SendOutcome};
        use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
        use liquidium_pipeline_core::tokens::token_registry::MockTokenRegistryTrait;
        use std::path::PathBuf;
        use std::sync::Mutex;

        const FEE: u64 = 10_000;

        fn token() -> ChainToken {
            ChainToken::Icp {
                ledger: Principal::from_slice(b"ckUSDC"),
                symbol: "ckUSDC".to_string(),
                decimals: 6,
                fee: Nat::from(FEE),
            }
        }

        fn journal_path(name: &str) -> PathBuf {
            let path = std::env::temp_dir().join(format!("resupplier-{name}-{}.json", std::process::id()));
            let _ = std::fs::remove_file(&path);
            path
        }

        fn prepared() -> PreparedSend {
            PreparedSend {
                sender: "icp".into(),
                identity: "id-1".into(),
                payload: "{}".into(),
                replay_safe: true,
            }
        }

        #[derive(Default)]
        struct CountingWatchdog(Mutex<usize>);

        #[async_trait]
        impl Watchdog for CountingWatchdog {
            async fn notify(&self, _ev: WatchdogEvent<'_>) {
                *self.0.lock().unwrap() += 1;
            }
        }

        /// A resupplier whose wallet holds `wallet_balance` and whose deposit subaccount is empty.
        fn resupplier(
            agent: MockPipelineAgent,
            sender: MockOutboundSender,
            journal: ResupplyJournal,
            watchdog: Arc<CountingWatchdog>,
            wallet_balance: u64,
        ) -> Resupplier<MockPipelineAgent> {
            let mut registry = MockTokenRegistryTrait::new();
            registry.expect_get().returning(|_| Some(token()));
            let mut account = MockAccountInfo::new();
            account
                .expect_sync_balance()
                .returning(move |token| Ok(ChainTokenAmount::from_raw(token.clone(), Nat::from(wallet_balance))));

            Resupplier {
                agent: Arc::new(agent),
                profile: Principal::anonymous(),
                balances: Arc::new(BalanceService::new(Arc::new(registry), Arc::new(account))),
                sender: Arc::new(sender),
                journal,
                watchdog,
                targets: vec![ResupplyTarget {
                    token: token(),
                    ledger: Principal::from_slice(b"ckUSDC"),
                    pool: Principal::from_slice(b"pool"),
                    subaccount: [7u8; 32],
                }],
                in_flight: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn agent_with_empty_subaccount() -> MockPipelineAgent {
            let mut agent = MockPipelineAgent::new();
            agent.expect_call_query::<Nat>().returning(|_, _, _| Ok(Nat::from(0u8)));
            agent
        }

        #[tokio::test]
        async fn a_top_up_is_saved_before_it_is_sent_and_credited_once_it_lands() {
            // given a wallet holding 5 ckUSDC
            let path = journal_path("happy");
            let mut agent = agent_with_empty_subaccount();
            agent
                .expect_call_update::<Result<(), candid::Reserved>>()
                .withf(|_, method, _| method == "check_subaccount")
                .times(1)
                .returning(|_, _, _| Ok(Ok(())));

            let mut sender = MockOutboundSender::new();
            sender
                .expect_prepare()
                .withf(|intent| intent.amount_native == Nat::from(5_000_000u64 - FEE))
                .times(1)
                .returning(|_| Ok(prepared()));
            let saved_path = path.clone();
            sender.expect_submit().times(1).returning(move |_| {
                let saved = std::fs::read_to_string(&saved_path).expect("the send is on disk before it runs");
                assert!(saved.contains("id-1"));
                SendOutcome::Landed {
                    receipt: "block-9".into(),
                }
            });

            let journal = ResupplyJournal::open(path.clone(), 0).unwrap();
            let resupplier = resupplier(agent, sender, journal, Arc::default(), 5_000_000);

            // when
            resupplier.run_once().await;

            // then the decided send is cleared from the journal
            assert_eq!(ResupplyJournal::open(path.clone(), 0).unwrap().get(&token_key()), None);
            let _ = std::fs::remove_file(&path);
        }

        #[tokio::test]
        async fn an_undecided_top_up_blocks_new_ones_and_alerts_once() {
            // given a top-up sent long ago that the ledger still cannot decide
            let path = journal_path("undecided");
            let journal = ResupplyJournal::open(path.clone(), 0).unwrap();
            let mut record = OutboundRecord::prepared(prepared());
            record.phase = OutboundPhase::Submitted { at: 0 };
            journal.set(&token_key(), Some(record)).unwrap();

            let mut sender = MockOutboundSender::new();
            sender.expect_prepare().times(0);
            sender.expect_submit().times(0);
            sender.expect_settle().returning(|_| SendOutcome::Open {
                reason: "no answer".into(),
            });
            sender.expect_open_window_secs().returning(|_| 60);

            let watchdog = Arc::new(CountingWatchdog::default());
            let resupplier = resupplier(
                agent_with_empty_subaccount(),
                sender,
                journal,
                watchdog.clone(),
                5_000_000,
            );

            // when two passes run
            resupplier.run_once().await;
            resupplier.run_once().await;

            // then the send is parked for an operator, and the operator was told once
            let saved = ResupplyJournal::open(path.clone(), 0)
                .unwrap()
                .get(&token_key())
                .unwrap();
            assert!(matches!(saved.phase, OutboundPhase::Parked { .. }));
            assert_eq!(*watchdog.0.lock().unwrap(), 1);
            let _ = std::fs::remove_file(&path);
        }

        #[tokio::test]
        async fn no_top_up_starts_while_a_liquidation_is_in_flight() {
            // given
            let path = journal_path("in-flight");
            let mut sender = MockOutboundSender::new();
            sender.expect_prepare().times(0);
            let journal = ResupplyJournal::open(path.clone(), 0).unwrap();
            let resupplier = resupplier(
                agent_with_empty_subaccount(),
                sender,
                journal,
                Arc::default(),
                5_000_000,
            );
            resupplier.in_flight.store(1, Ordering::SeqCst);

            // when
            resupplier.run_once().await;

            // then
            assert_eq!(resupplier.journal.get(&token_key()), None);
            let _ = std::fs::remove_file(&path);
        }

        fn token_key() -> String {
            Principal::from_slice(b"ckUSDC").to_text()
        }
    }

    #[test]
    fn supply_mode_asks_for_collateral_in_cash() {
        // given
        let request = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(5u8),
            receiver_address: Principal::anonymous(),
            buy_bad_debt: false,
        };

        // when
        let cash = CollateralPayout::Cash;
        let wallet = LiquidationFunding::Wallet
            .encode_liquidate_args(&request, &Nat::from(1u8), cash)
            .unwrap();
        let supply = LiquidationFunding::Supply
            .encode_liquidate_args(&request, &Nat::from(1u8), cash)
            .unwrap();

        // then
        let (_, min): (LiquidationRequest, Nat) = candid::decode_args(&wallet).unwrap();
        assert_eq!(min, Nat::from(1u8));
        let (_, _, payout): (LiquidationRequest, Nat, CollateralPayout) = candid::decode_args(&supply).unwrap();
        assert_eq!(payout, CollateralPayout::Cash);
    }
}
