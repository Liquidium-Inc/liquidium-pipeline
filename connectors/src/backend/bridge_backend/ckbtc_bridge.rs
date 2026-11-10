//! `ckBTC@ICP -> BTC` through the ckBTC minter: an ICRC-2 approve, the burn
//! with `retrieve_btc_with_approval`, then its status until the Bitcoin
//! transaction confirms. The minter takes its fee out of the amount.
use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use num_traits::ToPrimitive;

use super::{
    BridgeBackend, BridgeDestination, BridgeError, BridgeFeeBudget, BridgeRequest, BridgeRouteKind, BridgeRouteSpec,
    BridgeStatus, BridgeSubmission, BridgeSubmissionCandidate, resolve_route,
    utils::{ensure_source_matches_bridge_owner, ledger_transaction, parse_source_icp_account},
    validate_destination_for_route,
};
use crate::{
    backend::{
        amount_utils::{amount_to_nat_units_strict, nat_units_to_amount_via_core},
        ckbtc_minter::{
            BTC_DECIMALS, CkBtcMinter, RetrieveBtcStatusV2, RetrieveBtcWithApprovalArgs, RetrieveBtcWithApprovalError,
            RetrieveBtcWithApprovalRet, display_txid,
        },
        icp_backend::IcpBackend,
        icp_backend_helpers::{ensure_icrc2_allowance, icrc1_balance_with_context, icrc1_fee_with_context},
    },
    pipeline_agent::PipelineAgent,
};

const CONTEXT: &str = "ckbtc bridge";
/// The handle of a retrieval: the burn index the minter tracks it by.
pub const RETRIEVE_BRIDGE_ID_PREFIX: &str = "ckbtc-retrieve:";

/// The minter's answer decides whether the burn happened. A ledger call the
/// minter could not complete may have burned, so only the lookup decides it.
impl From<RetrieveBtcWithApprovalError> for BridgeError {
    fn from(error: RetrieveBtcWithApprovalError) -> Self {
        let message = format!("retrieve_btc_with_approval error: {error:?}");
        match error {
            RetrieveBtcWithApprovalError::AlreadyProcessing => BridgeError::Deferred(message),
            RetrieveBtcWithApprovalError::AmountTooLow(_) => BridgeError::BelowMinimum(message),
            RetrieveBtcWithApprovalError::MalformedAddress(_)
            | RetrieveBtcWithApprovalError::InsufficientAllowance { .. }
            | RetrieveBtcWithApprovalError::InsufficientFunds { .. } => BridgeError::Refused(message),
            RetrieveBtcWithApprovalError::TemporarilyUnavailable(_)
            | RetrieveBtcWithApprovalError::GenericError { .. } => BridgeError::Ambiguous(message),
        }
    }
}

pub struct CkBtcBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    minter: CkBtcMinter<A>,
    icp_backend: Arc<B>,
    ledger: Principal,
    bridge_ic_owner_principal: Principal,
    /// One retrieval at a time: the minter's allowance is account-wide.
    retrieval: tokio::sync::Mutex<()>,
}

impl<A, B> CkBtcBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    pub fn new(
        agent: Arc<A>,
        icp_backend: Arc<B>,
        minter_canister: Principal,
        ledger_canister: Principal,
        bridge_ic_owner_principal: Principal,
    ) -> Self {
        Self {
            minter: CkBtcMinter::new(agent, minter_canister),
            icp_backend,
            ledger: ledger_canister,
            bridge_ic_owner_principal,
            retrieval: tokio::sync::Mutex::new(()),
        }
    }

    /// The one route this backend serves.
    fn retrieve_route(asset: &str, chain: &str, target_asset: &str) -> Option<&'static BridgeRouteSpec> {
        resolve_route(asset, chain, target_asset).filter(|route| route.route_kind == BridgeRouteKind::CkBtcToBtc)
    }

    fn route_for_request(request: &BridgeRequest) -> Result<&'static BridgeRouteSpec, String> {
        let route =
            Self::retrieve_route(&request.asset, &request.source_chain, &request.target_asset).ok_or_else(|| {
                format!(
                    "route {}@{} -> {} is not served by the ckBTC minter bridge",
                    request.asset, request.source_chain, request.target_asset
                )
            })?;
        validate_destination_for_route(route, &request.destination)?;
        Ok(route)
    }

    /// The bridge account, the only source the minter burns from here.
    fn source_account(&self, request: &BridgeRequest) -> Result<Account, String> {
        let account = parse_source_icp_account(&request.source_address)?;
        ensure_source_matches_bridge_owner(&account, self.bridge_ic_owner_principal)?;
        Ok(account)
    }

    fn satoshi(amount: f64) -> Result<u64, String> {
        amount_to_nat_units_strict(amount, BTC_DECIMALS)?
            .0
            .to_u64()
            .ok_or_else(|| format!("amount {amount} BTC does not fit the minter's u64 satoshi"))
    }

    fn btc_amount(satoshi: &Nat) -> Result<f64, String> {
        nat_units_to_amount_via_core(satoshi, BTC_DECIMALS)
    }

    /// Checks the request, the minimum and the balance, and sets the
    /// allowance. Nothing here burns, so every failure is a refusal.
    async fn prepare_retrieval(&self, request: &BridgeRequest) -> Result<RetrieveBtcWithApprovalArgs, BridgeError> {
        Self::route_for_request(request).map_err(BridgeError::Refused)?;
        let source = self.source_account(request).map_err(BridgeError::Refused)?;
        let BridgeDestination::BtcAddress(address) = &request.destination else {
            return Err(BridgeError::Refused(
                "a ckBTC retrieval needs a Bitcoin destination".to_string(),
            ));
        };
        let amount = Self::satoshi(request.amount).map_err(BridgeError::Refused)?;

        let minimum = self
            .minter
            .minter_info()
            .await
            .map_err(BridgeError::Refused)?
            .retrieve_btc_min_amount;
        if amount < minimum {
            return Err(BridgeError::BelowMinimum(format!(
                "ckBTC retrieval of {amount} satoshi is below the minter minimum {minimum}"
            )));
        }
        let fee = icrc1_fee_with_context(self.icp_backend.as_ref(), self.ledger, CONTEXT)
            .await
            .map_err(BridgeError::Refused)?;
        let balance = icrc1_balance_with_context(self.icp_backend.as_ref(), self.ledger, &source, CONTEXT)
            .await
            .map_err(BridgeError::Refused)?;
        let required = Nat::from(amount) + fee;
        if balance < required {
            return Err(BridgeError::Refused(format!(
                "bridge amount preflight failed: ckBTC balance {balance} is below the burn and approve budget {required} (source={})",
                request.source_address
            )));
        }
        let spender = Account {
            owner: self.minter.canister(),
            subaccount: None,
        };
        ensure_icrc2_allowance(
            self.icp_backend.as_ref(),
            self.ledger,
            &source,
            spender,
            Nat::from(amount),
            CONTEXT,
        )
        .await
        .map_err(BridgeError::Refused)?;
        Ok(RetrieveBtcWithApprovalArgs {
            from_subaccount: None,
            address: address.trim().to_string(),
            amount,
        })
    }

    /// The burn the ckBTC ledger recorded at `block_index`, as `(from, amount)`.
    async fn ledger_burn(&self, block_index: u64) -> Result<Option<(Account, Nat)>, String> {
        let transaction = ledger_transaction(self.minter.agent(), self.ledger, block_index, "ckBTC").await?;
        Ok(transaction.burn.map(|burn| (burn.from, burn.amount)))
    }

    fn retrieval_index(bridge_id: &str) -> Result<u64, String> {
        bridge_id
            .strip_prefix(RETRIEVE_BRIDGE_ID_PREFIX)
            .and_then(|index| index.parse().ok())
            .ok_or_else(|| format!("invalid ckBTC bridge id '{bridge_id}'"))
    }
}

#[async_trait]
impl<A, B> BridgeBackend for CkBtcBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        if Self::retrieve_route(asset, chain, "BTC").is_none() {
            return Err(format!(
                "unsupported source route {asset}@{chain} for the ckBTC minter bridge"
            ));
        }
        let account = parse_source_icp_account(address)?;
        let balance = icrc1_balance_with_context(self.icp_backend.as_ref(), self.ledger, &account, CONTEXT).await?;
        Self::btc_amount(&balance)
    }

    /// The approve fee stays on the source. The minter and Bitcoin fees come
    /// out of the amount before it reaches the destination.
    async fn get_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<BridgeFeeBudget, String> {
        if Self::retrieve_route(asset, chain, target_asset).is_none() {
            return Ok(BridgeFeeBudget::default());
        }
        let (approve_fee, withdrawal_fee) = tokio::try_join!(
            icrc1_fee_with_context(self.icp_backend.as_ref(), self.ledger, CONTEXT),
            self.minter.estimate_withdrawal_fee(None)
        )?;
        Ok(BridgeFeeBudget {
            source_fee_budget: Self::btc_amount(&approve_fee)?,
            destination_fee_budget: Self::btc_amount(&Nat::from(withdrawal_fee.total()))?,
            provider_fee_budget_native_units: None,
            side_costs: Vec::new(),
        })
    }

    async fn get_minimum_bridge_amount(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        if Self::retrieve_route(asset, chain, target_asset).is_none() {
            return Ok(0.0);
        }
        let minimum = self.minter.minter_info().await?.retrieve_btc_min_amount;
        Self::btc_amount(&Nat::from(minimum))
    }

    /// Burns through the minter and returns `ckbtc-retrieve:<burn index>`.
    /// A call without an answer stays open for `find_submissions`.
    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, BridgeError> {
        let _retrieval = self.retrieval.lock().await;
        let args = self.prepare_retrieval(&request).await?;
        match self.minter.retrieve_btc_with_approval(&args).await? {
            RetrieveBtcWithApprovalRet::Ok(ok) => Ok(BridgeSubmission {
                bridge_id: format!("{RETRIEVE_BRIDGE_ID_PREFIX}{}", ok.block_index),
            }),
            RetrieveBtcWithApprovalRet::Err(error) => Err(error.into()),
        }
    }

    async fn prepare_submission(&self, request: &BridgeRequest) -> Result<Option<u64>, String> {
        if Self::route_for_request(request).is_err() {
            return Ok(None);
        }
        let source = self.source_account(request)?;
        // A stale cursor only widens the later search, so a plain query is enough.
        let retrievals = self.minter.retrieve_btc_status_v2_by_account(&source, false).await?;
        Ok(Some(
            retrievals.iter().map(|entry| entry.block_index).max().unwrap_or(0),
        ))
    }

    /// The minter lists retrievals by burn index only, so the ledger burn
    /// proves the account and the amount. The burn carries no address; the
    /// submit lock keeps one open retrieval per source, so that is enough.
    async fn find_submissions(
        &self,
        request: &BridgeRequest,
        cursor: u64,
        claimed_bridge_ids: &[String],
    ) -> Result<Vec<BridgeSubmissionCandidate>, String> {
        Self::route_for_request(request)?;
        let source = self.source_account(request)?;
        let amount = Nat::from(Self::satoshi(request.amount)?);
        let claimed: HashSet<u64> = claimed_bridge_ids
            .iter()
            .filter_map(|id| Self::retrieval_index(id).ok())
            .collect();
        let mut candidates = Vec::new();
        for entry in self.minter.retrieve_btc_status_v2_by_account(&source, true).await? {
            if entry.block_index <= cursor || claimed.contains(&entry.block_index) {
                continue;
            }
            if let Some((from, burned)) = self.ledger_burn(entry.block_index).await?
                && from == source
                && burned == amount
            {
                candidates.push(BridgeSubmissionCandidate {
                    bridge_id: format!("{RETRIEVE_BRIDGE_ID_PREFIX}{}", entry.block_index),
                    sequence: entry.block_index,
                });
            }
        }
        candidates.sort_by_key(|candidate| candidate.sequence);
        Ok(candidates)
    }

    /// A broadcast Bitcoin transaction completes the leg: the receiver
    /// watches the chain itself, and the minter's six confirmations are only
    /// its own bookkeeping. Only a done reimbursement cancels.
    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, BridgeError> {
        let block_index = Self::retrieval_index(bridge_id)?;
        let status = self
            .minter
            .retrieve_btc_status_v2(block_index)
            .await
            .map_err(BridgeError::Unreadable)?;
        Ok(match status {
            RetrieveBtcStatusV2::Pending | RetrieveBtcStatusV2::Signing => BridgeStatus::Pending,
            RetrieveBtcStatusV2::Sending { txid } => {
                log::debug!(
                    "[ckbtc bridge] retrieval {block_index} is being sent as Bitcoin transaction {}",
                    display_txid(&txid)
                );
                BridgeStatus::Pending
            }
            RetrieveBtcStatusV2::Submitted { txid } | RetrieveBtcStatusV2::Confirmed { txid } => {
                log::info!(
                    "[ckbtc bridge] retrieval {block_index} is on Bitcoin in transaction {}",
                    display_txid(&txid)
                );
                BridgeStatus::Completed
            }
            RetrieveBtcStatusV2::AmountTooLow => {
                log::warn!(
                    "[ckbtc bridge] retrieval {block_index} waits: the amount does not cover the current Bitcoin fee"
                );
                BridgeStatus::Pending
            }
            RetrieveBtcStatusV2::WillReimburse(request) => {
                log::warn!(
                    "[ckbtc bridge] retrieval {block_index} will be reimbursed ({} satoshi to {}): {:?}",
                    request.amount,
                    request.account,
                    request.reason
                );
                BridgeStatus::Pending
            }
            RetrieveBtcStatusV2::Unknown => BridgeStatus::Unknown,
            RetrieveBtcStatusV2::Reimbursed(deposit) => BridgeStatus::Canceled {
                reason: Some(format!(
                    "the minter reimbursed retrieval {block_index} in ckBTC block {} ({} satoshi to {}): {:?}",
                    deposit.mint_block_index, deposit.amount, deposit.account, deposit.reason
                )),
            },
        })
    }
}

#[cfg(test)]
#[path = "ckbtc_bridge_tests.rs"]
mod ckbtc_bridge_tests;
