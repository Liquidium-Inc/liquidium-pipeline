//! The ckBTC minter as a bridge. Out, a burn through `retrieve_btc_with_approval`
//! until its Bitcoin transaction is broadcast; in, a deposit a third party paid
//! to the minter's address for the final account, minted by `update_balance`.
use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use num_traits::ToPrimitive;

use super::{
    BridgeBackend, BridgeDestination, BridgeError, BridgeFeeBudget, BridgeMintReceipt, BridgeRequest, BridgeRouteKind,
    BridgeRouteSpec, BridgeStatus, BridgeSubmission, BridgeSubmissionCandidate, DepositNotice, resolve_route,
    utils::{
        account_transactions, ensure_source_matches_bridge_owner, ledger_index, ledger_transaction,
        parse_source_icp_account,
    },
    validate_destination_for_route,
};
use crate::{
    backend::{
        amount_utils::{amount_to_nat_units_strict, nat_units_to_amount_via_core},
        ckbtc_minter::{
            BTC_DECIMALS, CkBtcMinter, OutPoint, RetrieveBtcStatusV2, RetrieveBtcWithApprovalArgs,
            RetrieveBtcWithApprovalError, RetrieveBtcWithApprovalRet, SuspendedReason, UpdateBalanceError,
            UpdateBalanceRet, UtxoStatus, display_txid,
        },
        icp_backend::IcpBackend,
        icp_backend_helpers::{ensure_icrc2_allowance, icrc1_balance_with_context, icrc1_fee_with_context},
    },
    pipeline_agent::PipelineAgent,
};

const CONTEXT: &str = "ckbtc bridge";
/// Every handle this backend issues starts with it.
pub const CKBTC_HANDLE_PREFIX: &str = "ckbtc-";
/// The handle of a retrieval: the burn index the minter tracks it by.
pub const RETRIEVE_BRIDGE_ID_PREFIX: &str = "ckbtc-retrieve:";
/// The handle of a minted deposit: the ckBTC ledger block of the mint, then
/// the Bitcoin transaction that paid it.
pub const MINT_BRIDGE_ID_PREFIX: &str = "ckbtc-mint:";
/// How many of the account's newest transactions are searched for a mint
/// another caller triggered. An account that only trades fills it in months.
const MINT_SEARCH_DEPTH: u64 = 200;

/// True when a minter memo holds `bytes`. The memo is CBOR with an address
/// or a transaction id as plain bytes inside, so they are searched directly.
fn memo_holds(memo: &[u8], bytes: &[u8]) -> bool {
    !bytes.is_empty() && memo.windows(bytes.len()).any(|window| window == bytes)
}

/// A Bitcoin transaction id as the caller writes it, in the explorer's form.
/// A Bitcoin transaction id as explorers print it. Anything else cannot
/// match a deposit, so it is refused rather than waited on.
fn parse_txid(text: &str) -> Result<String, String> {
    let txid = text.trim().trim_start_matches("0x").to_ascii_lowercase();
    if txid.len() == 64 && txid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(txid)
    } else {
        Err(format!("{text:?} is not a Bitcoin transaction id"))
    }
}

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

    /// A catalog route of `kind` between these assets.
    fn route_of(
        kind: BridgeRouteKind,
        asset: &str,
        chain: &str,
        target_asset: &str,
    ) -> Option<&'static BridgeRouteSpec> {
        resolve_route(asset, chain, target_asset).filter(|route| route.route_kind == kind)
    }

    /// The route out: a retrieval through the minter.
    fn retrieve_route(asset: &str, chain: &str, target_asset: &str) -> Option<&'static BridgeRouteSpec> {
        Self::route_of(BridgeRouteKind::CkBtcToBtc, asset, chain, target_asset)
    }

    /// The route in: a deposit to the minter's address for an ICP account.
    fn deposit_route(asset: &str, chain: &str, target_asset: &str) -> Option<&'static BridgeRouteSpec> {
        Self::route_of(BridgeRouteKind::BtcToCkBtc, asset, chain, target_asset)
    }

    /// The account a deposit route mints to.
    fn deposit_account(asset: &str, chain: &str, destination: &BridgeDestination) -> Result<Account, String> {
        let route = Self::deposit_route(asset, chain, "ckBTC")
            .ok_or_else(|| format!("route {asset}@{chain} -> ckBTC is not served by the ckBTC minter bridge"))?;
        validate_destination_for_route(route, destination)?;
        match destination {
            BridgeDestination::IcpAccount(account) => Ok(*account),
            _ => Err("a ckBTC deposit needs an ICP account".to_string()),
        }
    }

    /// The mint at ledger block `block_index`, as the receipt of the deposit
    /// Bitcoin transaction `txid` paid. A block that is no mint ends the wait.
    async fn mint_receipt(&self, block_index: u64, txid: &str) -> Result<BridgeMintReceipt, BridgeError> {
        let mint = ledger_transaction(self.minter.agent(), self.ledger, block_index, "ckBTC")
            .await
            .map_err(BridgeError::Unreadable)?
            .mint
            .ok_or_else(|| BridgeError::Other(format!("ckBTC block {block_index} is not a mint")))?;
        Ok(BridgeMintReceipt {
            transaction_hash: txid.to_string(),
            log_index: Nat::from(0u8),
            mint_block_index: Nat::from(block_index),
            amount: mint.amount,
            recipient: mint.to,
            token_symbol: "ckBTC".to_string(),
        })
    }

    fn accepted(block_index: u64, txid: &str) -> DepositNotice {
        DepositNotice::Accepted(BridgeSubmission {
            bridge_id: format!("{MINT_BRIDGE_ID_PREFIX}{block_index}:{txid}"),
        })
    }

    fn quarantined(txid: &str, value: u64) -> DepositNotice {
        DepositNotice::Held {
            reason: format!("the minter quarantined deposit {txid} ({value} satoshi) after its compliance check"),
        }
    }

    fn too_small(txid: &str, value: u64) -> DepositNotice {
        DepositNotice::Held {
            reason: format!("the minter refuses deposit {txid}: {value} satoshi is under its deposit minimum"),
        }
    }

    /// A call that did not answer moves nothing of ours, and a mint it made
    /// shows in the known UTXOs on the next notice. So the leg waits in its bound.
    fn unanswered(call: &str, error: String) -> DepositNotice {
        DepositNotice::NotYet {
            reason: format!("{call} did not answer: {error}"),
        }
    }

    /// Anyone may run `update_balance` for any account, so a deposit this
    /// call did not mint may be minted already. The minter forgets a deposit
    /// when it spends it, so the mint in the account's history decides.
    async fn minted_by_another_call(&self, account: &Account, txid: &str) -> Result<DepositNotice, String> {
        // The minter's memo holds the transaction id in Bitcoin's byte order.
        let mut txid_bytes = hex::decode(txid).map_err(|error| format!("deposit {txid}: {error}"))?;
        txid_bytes.reverse();
        match self.mint_block_of(account, &txid_bytes).await {
            Ok(Some(block_index)) => {
                log::info!(
                    "[ckbtc bridge] deposit {txid} was minted to {account} by another call, in ckBTC block {block_index}"
                );
                Ok(Self::accepted(block_index, txid))
            }
            Ok(None) => Ok(DepositNotice::NotYet {
                reason: format!("no mint to {account} names deposit {txid} yet"),
            }),
            Err(error) => Ok(Self::unanswered("the ckBTC index", error)),
        }
    }

    /// The newest mint to `account` with the deposit's transaction in its memo.
    async fn mint_block_of(&self, account: &Account, txid: &[u8]) -> Result<Option<u64>, String> {
        let agent = self.minter.agent();
        let index = ledger_index(agent, self.ledger).await?;
        let transactions = account_transactions(agent, index, account, MINT_SEARCH_DEPTH).await?;
        Ok(transactions
            .into_iter()
            .find(|(_, transaction)| {
                transaction.mint.as_ref().is_some_and(|mint| {
                    mint.to == *account
                        && mint
                            .memo
                            .as_ref()
                            .is_some_and(|memo| memo_holds(&memo.0, txid))
                })
            })
            .map(|(block_index, _)| block_index))
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

    /// The burn the ckBTC ledger recorded at `block_index`, as `(from, amount, memo)`.
    async fn ledger_burn(&self, block_index: u64) -> Result<Option<(Account, Nat, Option<Vec<u8>>)>, String> {
        let transaction = ledger_transaction(self.minter.agent(), self.ledger, block_index, "ckBTC").await?;
        Ok(transaction
            .burn
            .map(|burn| (burn.from, burn.amount, burn.memo.map(|memo| memo.0.into_vec()))))
    }

    fn retrieval_index(bridge_id: &str) -> Result<u64, String> {
        bridge_id
            .strip_prefix(RETRIEVE_BRIDGE_ID_PREFIX)
            .and_then(|index| index.parse().ok())
            .ok_or_else(|| format!("invalid ckBTC bridge id '{bridge_id}'"))
    }

    /// The mint block and the Bitcoin transaction of a `ckbtc-mint:` handle.
    fn mint_handle(bridge_id: &str) -> Option<(u64, &str)> {
        let (block_index, txid) = bridge_id.strip_prefix(MINT_BRIDGE_ID_PREFIX)?.split_once(':')?;
        Some((block_index.parse().ok()?, txid))
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

    /// Out, the approve fee stays on the source and the minter and Bitcoin
    /// fees come out of the amount. In, the minter's check fee comes out of
    /// the deposit before the mint.
    async fn get_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<BridgeFeeBudget, String> {
        if Self::deposit_route(asset, chain, target_asset).is_some() {
            let check_fee = self.minter.minter_info().await?.kyt_fee;
            return Ok(BridgeFeeBudget {
                destination_fee_budget: Self::btc_amount(&Nat::from(check_fee))?,
                ..BridgeFeeBudget::default()
            });
        }
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

    /// Out, the minter's retrieval minimum; in, its deposit minimum, under
    /// which a deposit never mints.
    async fn get_minimum_bridge_amount(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let minimum = if Self::retrieve_route(asset, chain, target_asset).is_some() {
            self.minter.minter_info().await?.retrieve_btc_min_amount
        } else if Self::deposit_route(asset, chain, target_asset).is_some() {
            self.minter.minter_info().await?.deposit_btc_min_amount.unwrap_or(0)
        } else {
            return Ok(0.0);
        };
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

    async fn deposit_address(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
    ) -> Result<String, String> {
        let account = Self::deposit_account(asset, chain, destination)?;
        self.minter.get_btc_address(&account).await
    }

    /// Asks the minter to mint what Bitcoin transaction `receipt` paid to the
    /// account's address. Nothing of ours moves, so a repeat is always safe.
    async fn track_deposit(
        &self,
        asset: &str,
        chain: &str,
        destination: &BridgeDestination,
        _handle: &str,
        receipt: &str,
    ) -> Result<DepositNotice, String> {
        let account = Self::deposit_account(asset, chain, destination)?;
        let txid = parse_txid(receipt)?;
        let paid_by = |outpoint: &OutPoint| display_txid(&outpoint.txid) == txid;
        let answer = match self.minter.update_balance(&account).await {
            Ok(answer) => answer,
            Err(error) => return Ok(Self::unanswered("update_balance", error)),
        };
        let statuses = match answer {
            UpdateBalanceRet::Ok(statuses) => statuses,
            UpdateBalanceRet::Err(UpdateBalanceError::NoNewUtxos {
                suspended_utxos,
                pending_utxos,
                required_confirmations,
                ..
            }) => {
                if let Some(suspended) = suspended_utxos
                    .unwrap_or_default()
                    .into_iter()
                    .find(|entry| paid_by(&entry.utxo.outpoint))
                {
                    return Ok(match suspended.reason {
                        SuspendedReason::Quarantined => Self::quarantined(&txid, suspended.utxo.value),
                        SuspendedReason::ValueTooSmall => Self::too_small(&txid, suspended.utxo.value),
                    });
                }
                if let Some(pending) = pending_utxos
                    .unwrap_or_default()
                    .into_iter()
                    .find(|utxo| paid_by(&utxo.outpoint))
                {
                    return Ok(DepositNotice::NotYet {
                        reason: format!(
                            "the minter sees the deposit with {} of {required_confirmations} confirmations",
                            pending.confirmations
                        ),
                    });
                }
                return self.minted_by_another_call(&account, &txid).await;
            }
            // A busy or unavailable minter mints nothing; asking again is safe.
            UpdateBalanceRet::Err(error) => {
                return Ok(DepositNotice::NotYet {
                    reason: format!("update_balance did not run: {error:?}"),
                });
            }
        };
        for status in statuses {
            match status {
                UtxoStatus::Minted {
                    block_index,
                    minted_amount,
                    utxo,
                } if paid_by(&utxo.outpoint) => {
                    log::info!(
                        "[ckbtc bridge] deposit {txid} minted {minted_amount} satoshi to {account} in ckBTC block {block_index}"
                    );
                    return Ok(Self::accepted(block_index, &txid));
                }
                UtxoStatus::Tainted(utxo) if paid_by(&utxo.outpoint) => {
                    return Ok(Self::quarantined(&txid, utxo.value));
                }
                UtxoStatus::ValueTooSmall(utxo) if paid_by(&utxo.outpoint) => {
                    return Ok(Self::too_small(&txid, utxo.value));
                }
                // The check passed and the ledger refused the mint; the next
                // call mints it first.
                UtxoStatus::Checked(utxo) if paid_by(&utxo.outpoint) => {
                    return Ok(DepositNotice::NotYet {
                        reason: format!(
                            "the minter checked deposit {txid} ({} satoshi) and mints it next",
                            utxo.value
                        ),
                    });
                }
                _ => {}
            }
        }
        self.minted_by_another_call(&account, &txid).await
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
    /// proves the account, the amount and the address: the minter writes the
    /// Bitcoin address into the burn memo. A burn without a memo stays a candidate.
    async fn find_submissions(
        &self,
        request: &BridgeRequest,
        cursor: u64,
        claimed_bridge_ids: &[String],
    ) -> Result<Vec<BridgeSubmissionCandidate>, String> {
        Self::route_for_request(request)?;
        let source = self.source_account(request)?;
        let amount = Nat::from(Self::satoshi(request.amount)?);
        let BridgeDestination::BtcAddress(address) = &request.destination else {
            return Err("a ckBTC retrieval needs a Bitcoin destination".to_string());
        };
        let address = address.trim();
        let claimed: HashSet<u64> = claimed_bridge_ids
            .iter()
            .filter_map(|id| Self::retrieval_index(id).ok())
            .collect();
        let mut candidates = Vec::new();
        for entry in self.minter.retrieve_btc_status_v2_by_account(&source, true).await? {
            if entry.block_index <= cursor || claimed.contains(&entry.block_index) {
                continue;
            }
            if let Some((from, burned, memo)) = self.ledger_burn(entry.block_index).await?
                && from == source
                && burned == amount
                && memo.is_none_or(|memo| memo_holds(&memo, address.as_bytes()))
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
        if let Some((block_index, txid)) = Self::mint_handle(bridge_id) {
            // The ledger is the proof of the mint, whoever asked for it.
            return Ok(BridgeStatus::Minted(self.mint_receipt(block_index, txid).await?));
        }
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
            // Final at the minter: the ckBTC is burned and no later poll changes
            // that. An operator decides, so nothing retries and burns again.
            RetrieveBtcStatusV2::AmountTooLow => {
                return Err(BridgeError::Ambiguous(format!(
                    "the minter closed ckBTC retrieval {block_index} as too low for the Bitcoin fee: it sends no Bitcoin and does not reimburse the burned ckBTC"
                )));
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
