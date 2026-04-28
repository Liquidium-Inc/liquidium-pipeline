use async_trait::async_trait;
use candid::{CandidType, Encode, Nat, Principal};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::{allowance::AllowanceArgs, approve::ApproveArgs},
};
use num_traits::ToPrimitive;
use serde::Deserialize;
use std::{str::FromStr, sync::Arc};

use super::{
    BridgeBackend, BridgeDestination, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, BridgeStatus, BridgeSubmission,
    catalog::BRIDGE_ROUTE_CATALOG, resolve_route, validate_destination_for_route,
};
use crate::{
    backend::{
        amount_utils::{amount_to_nat_units_strict, nat_units_to_amount_via_core},
        icp_backend::IcpBackend,
        icp_backend_helpers::{
            icrc1_balance_with_context, icrc1_decimals_with_context, icrc1_fee_with_context,
            icrc2_allowance_with_context, icrc2_approve_with_context,
        },
    },
    pipeline_agent::PipelineAgent,
};

const WITHDRAW_BRIDGE_ID_PREFIX: &str = "cksol-withdraw:";
const DEPOSIT_BRIDGE_ID_PREFIX: &str = "cksol-deposit:";

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolGetDepositAddressArgs {
    owner: Option<Principal>,
    subaccount: Option<[u8; 32]>,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolUpdateBalanceArgs {
    subaccount: Option<[u8; 32]>,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolUpdateBalanceError {
    QueueFull,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolWithdrawalArgs {
    from_subaccount: Option<[u8; 32]>,
    amount: u64,
    address: String,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolWithdrawalOk {
    block_index: u64,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolWithdrawalError {
    AlreadyProcessing,
    ValueTooSmall {
        minimum_withdrawal_amount: u64,
        withdrawal_amount: u64,
    },
    MalformedAddress(String),
    InsufficientFunds {
        balance: u64,
    },
    InsufficientAllowance {
        allowance: u64,
    },
    TemporarilyUnavailable(String),
}

#[derive(CandidType, Deserialize, Clone, Debug)]
struct CkSolWithdrawalStatusArgs {
    block_index: u64,
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolTxFinalizedStatus {
    Success {
        transaction_id: String,
        effective_transaction_fee: Option<Nat>,
    },
    Failure {
        transaction_id: String,
    },
}

#[derive(CandidType, Deserialize, Clone, Debug)]
enum CkSolWithdrawalStatus {
    NotFound,
    Pending,
    TxSent { transaction_id: String },
    TxFinalized(CkSolTxFinalizedStatus),
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DepositBridgePollState {
    account: Account,
    baseline: Nat,
    threshold: Nat,
}

pub struct SolanaBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    pub agent: Arc<A>,
    pub icp_backend: Arc<B>,
    pub cksol_minter_canister: Principal,
    pub cksol_ledger_canister: Principal,
    pub bridge_ic_owner_principal: Principal,
}

impl<A, B> SolanaBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    pub fn new(
        agent: Arc<A>,
        icp_backend: Arc<B>,
        cksol_minter_canister: Principal,
        cksol_ledger_canister: Principal,
        bridge_ic_owner_principal: Principal,
    ) -> Self {
        Self {
            agent,
            icp_backend,
            cksol_minter_canister,
            cksol_ledger_canister,
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

    fn resolve_solana_route_for_request(request: &BridgeRequest) -> Result<&'static BridgeRouteSpec, String> {
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

    fn ensure_principal_only_bridge_source(&self, source_address: &str) -> Result<Account, String> {
        let source = Self::parse_source_icp_account(source_address)?;
        if source.owner != self.bridge_ic_owner_principal {
            return Err(format!(
                "source ICP account owner {} does not match configured bridge owner {}",
                source.owner, self.bridge_ic_owner_principal
            ));
        }
        if source.subaccount.is_some() {
            return Err(
                "solana reverse route requires principal-only source account (subaccount must be None)".to_string(),
            );
        }
        Ok(source)
    }

    fn expect_icp_destination<'a>(
        route: &BridgeRouteSpec,
        destination: &'a BridgeDestination,
    ) -> Result<&'a Account, String> {
        match destination {
            BridgeDestination::IcpAccount(account) => Ok(account),
            _ => Err(format!(
                "unsupported destination for {}@{} -> {}; expected IcpAccount",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    fn expect_solana_destination<'a>(
        route: &BridgeRouteSpec,
        destination: &'a BridgeDestination,
    ) -> Result<&'a str, String> {
        match destination {
            BridgeDestination::SolanaAddress(address) => Ok(address.as_str()),
            _ => Err(format!(
                "unsupported destination for {}@{} -> {}; expected SolanaAddress",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    fn encode_subaccount(subaccount: Option<[u8; 32]>) -> String {
        match subaccount {
            Some(value) => hex::encode(value),
            None => "none".to_string(),
        }
    }

    fn decode_subaccount(encoded: &str) -> Result<Option<[u8; 32]>, String> {
        if encoded == "none" {
            return Ok(None);
        }

        let bytes =
            hex::decode(encoded).map_err(|e| format!("invalid encoded subaccount '{}' in bridge id: {e}", encoded))?;
        if bytes.len() != 32 {
            return Err(format!(
                "invalid encoded subaccount '{}' in bridge id: expected 32 bytes, got {}",
                encoded,
                bytes.len()
            ));
        }

        let mut subaccount = [0u8; 32];
        subaccount.copy_from_slice(&bytes);
        Ok(Some(subaccount))
    }

    fn encode_deposit_bridge_id(state: &DepositBridgePollState) -> String {
        format!(
            "{}{owner}:{subaccount}:{baseline}:{threshold}",
            DEPOSIT_BRIDGE_ID_PREFIX,
            owner = state.account.owner,
            subaccount = Self::encode_subaccount(state.account.subaccount),
            baseline = state.baseline,
            threshold = state.threshold
        )
    }

    fn parse_deposit_bridge_id(bridge_id: &str) -> Result<Option<DepositBridgePollState>, String> {
        let Some(raw_state) = bridge_id.strip_prefix(DEPOSIT_BRIDGE_ID_PREFIX) else {
            return Ok(None);
        };

        let parts: Vec<&str> = raw_state.split(':').collect();
        if parts.len() != 4 {
            return Err(format!(
                "invalid ckSOL deposit bridge id '{}': expected 4 fields",
                bridge_id
            ));
        }

        let owner = Principal::from_text(parts[0])
            .map_err(|e| format!("invalid owner '{}' in bridge id '{}': {e}", parts[0], bridge_id))?;
        let subaccount = Self::decode_subaccount(parts[1])?;
        let baseline = Nat::from_str(parts[2]).map_err(|e| {
            format!(
                "invalid baseline '{}' in ckSOL deposit bridge id '{}': {e}",
                parts[2], bridge_id
            )
        })?;
        let threshold = Nat::from_str(parts[3]).map_err(|e| {
            format!(
                "invalid threshold '{}' in ckSOL deposit bridge id '{}': {e}",
                parts[3], bridge_id
            )
        })?;

        Ok(Some(DepositBridgePollState {
            account: Account { owner, subaccount },
            baseline,
            threshold,
        }))
    }

    fn parse_withdraw_bridge_id(bridge_id: &str) -> Result<Option<u64>, String> {
        let Some(raw_index) = bridge_id.strip_prefix(WITHDRAW_BRIDGE_ID_PREFIX) else {
            return Ok(None);
        };
        let block_index = raw_index.parse::<u64>().map_err(|e| {
            format!(
                "invalid ckSOL withdrawal bridge id '{}': expected u64 burn index: {e}",
                bridge_id
            )
        })?;
        Ok(Some(block_index))
    }

    async fn minter_info(&self) -> Result<CkSolMinterInfo, String> {
        let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
        match self
            .agent
            .call_query::<CkSolMinterInfo>(&self.cksol_minter_canister, "get_minter_info", args.clone())
            .await
        {
            Ok(v) => Ok(v),
            Err(query_err) => self
                .agent
                .call_update::<CkSolMinterInfo>(&self.cksol_minter_canister, "get_minter_info", args)
                .await
                .map_err(|update_err| {
                    format!(
                        "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                        self.cksol_minter_canister
                    )
                }),
        }
    }

    async fn get_deposit_address(&self, args: CkSolGetDepositAddressArgs) -> Result<String, String> {
        let arg_blob = Encode!(&args).map_err(|e| format!("encode get_deposit_address args failed: {e}"))?;
        self.agent
            .call_query::<String>(&self.cksol_minter_canister, "get_deposit_address", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "get_deposit_address failed for minter {}: {}",
                    self.cksol_minter_canister, e
                )
            })
    }

    async fn update_balance(&self, args: CkSolUpdateBalanceArgs) -> Result<(), String> {
        let arg_blob = Encode!(&args).map_err(|e| format!("encode update_balance args failed: {e}"))?;
        let result: Result<(), CkSolUpdateBalanceError> = self
            .agent
            .call_update::<Result<(), CkSolUpdateBalanceError>>(&self.cksol_minter_canister, "update_balance", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "update_balance call failed for minter {}: {e}",
                    self.cksol_minter_canister
                )
            })?;

        match result {
            Ok(()) => Ok(()),
            Err(CkSolUpdateBalanceError::QueueFull) => {
                Err("update_balance rejected by ckSOL minter: monitored account queue is full".to_string())
            }
        }
    }

    fn map_withdraw_error(err: CkSolWithdrawalError) -> String {
        match err {
            CkSolWithdrawalError::AlreadyProcessing => "ckSOL withdraw rejected: AlreadyProcessing".to_string(),
            CkSolWithdrawalError::ValueTooSmall {
                minimum_withdrawal_amount,
                withdrawal_amount,
            } => format!(
                "ckSOL withdraw rejected: ValueTooSmall (minimum_withdrawal_amount={}, withdrawal_amount={})",
                minimum_withdrawal_amount, withdrawal_amount
            ),
            CkSolWithdrawalError::MalformedAddress(address) => {
                format!("ckSOL withdraw rejected: MalformedAddress ({address})")
            }
            CkSolWithdrawalError::InsufficientFunds { balance } => {
                format!("ckSOL withdraw rejected: InsufficientFunds (balance={balance})")
            }
            CkSolWithdrawalError::InsufficientAllowance { allowance } => {
                format!("ckSOL withdraw rejected: InsufficientAllowance (allowance={allowance})")
            }
            CkSolWithdrawalError::TemporarilyUnavailable(message) => {
                format!("ckSOL withdraw rejected: TemporarilyUnavailable ({message})")
            }
        }
    }

    async fn withdraw(&self, args: CkSolWithdrawalArgs) -> Result<CkSolWithdrawalOk, String> {
        let arg_blob = Encode!(&args).map_err(|e| format!("encode withdraw args failed: {e}"))?;
        let result: Result<CkSolWithdrawalOk, CkSolWithdrawalError> = self
            .agent
            .call_update::<Result<CkSolWithdrawalOk, CkSolWithdrawalError>>(
                &self.cksol_minter_canister,
                "withdraw",
                arg_blob,
            )
            .await
            .map_err(|e| format!("withdraw call failed for minter {}: {e}", self.cksol_minter_canister))?;

        result.map_err(Self::map_withdraw_error)
    }

    async fn withdrawal_status(&self, block_index: u64) -> Result<CkSolWithdrawalStatus, String> {
        let args = CkSolWithdrawalStatusArgs { block_index };
        let arg_blob = Encode!(&args).map_err(|e| format!("encode withdrawal_status args failed: {e}"))?;
        self.agent
            .call_update::<CkSolWithdrawalStatus>(&self.cksol_minter_canister, "withdrawal_status", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "withdrawal_status call failed for minter {} and burn index {}: {}",
                    self.cksol_minter_canister, block_index, e
                )
            })
    }

    async fn ensure_minter_allowance(&self, source_account: &Account, required_allowance: Nat) -> Result<(), String> {
        let allowance_args = AllowanceArgs {
            account: *source_account,
            spender: Account {
                owner: self.cksol_minter_canister,
                subaccount: None,
            },
        };
        let current_allowance = icrc2_allowance_with_context(
            self.icp_backend.as_ref(),
            self.cksol_ledger_canister,
            allowance_args,
            "cksol bridge",
        )
        .await?;
        if current_allowance >= required_allowance {
            return Ok(());
        }

        let approve_args = ApproveArgs {
            from_subaccount: None,
            spender: Account {
                owner: self.cksol_minter_canister,
                subaccount: None,
            },
            amount: required_allowance,
            expected_allowance: None,
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };
        icrc2_approve_with_context(
            self.icp_backend.as_ref(),
            self.cksol_ledger_canister,
            approve_args,
            "cksol bridge",
        )
        .await
        .map(|_| ())
    }

    async fn submit_solana_to_icp(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let source_address = request.source_address.trim();
        Self::validate_solana_pubkey(source_address, "source Solana address")?;

        let destination = Self::expect_icp_destination(route, &request.destination)?;
        if destination.owner != self.bridge_ic_owner_principal {
            return Err(format!(
                "destination ICP account owner {} does not match configured bridge owner {}",
                destination.owner, self.bridge_ic_owner_principal
            ));
        }

        let expected_source = self
            .get_deposit_address(CkSolGetDepositAddressArgs {
                owner: Some(self.bridge_ic_owner_principal),
                subaccount: destination.subaccount,
            })
            .await?;

        if source_address != expected_source {
            return Err(format!(
                "source Solana address {} does not match minter-derived deposit address {} for destination owner={} subaccount={}",
                source_address,
                expected_source,
                destination.owner,
                Self::encode_subaccount(destination.subaccount)
            ));
        }

        let decimals =
            icrc1_decimals_with_context(self.icp_backend.as_ref(), self.cksol_ledger_canister, "cksol bridge").await?;
        let amount_native = amount_to_nat_units_strict(request.amount, decimals)?;
        let amount_lamports = amount_native.0.to_u64().ok_or_else(|| {
            format!(
                "bridge amount too large for ckSOL lamports (u64): amount_native={}",
                amount_native
            )
        })?;

        let minter_info = self.minter_info().await?;
        if amount_lamports < minter_info.minimum_deposit_amount {
            return Err(format!(
                "bridge amount preflight failed: requested SOL amount is below minimum ckSOL deposit amount (requested={} lamports, minimum={} lamports)",
                amount_lamports, minter_info.minimum_deposit_amount
            ));
        }
        if amount_lamports <= minter_info.automated_deposit_fee {
            return Err(format!(
                "bridge amount preflight failed: requested SOL amount must exceed automated deposit fee (requested={} lamports, automated_deposit_fee={} lamports)",
                amount_lamports, minter_info.automated_deposit_fee
            ));
        }

        let baseline = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            self.cksol_ledger_canister,
            destination,
            "cksol bridge",
        )
        .await?;

        self.update_balance(CkSolUpdateBalanceArgs {
            subaccount: destination.subaccount,
        })
        .await?;

        let expected_mint_lamports = amount_lamports - minter_info.automated_deposit_fee;
        let threshold = baseline.clone() + Nat::from(expected_mint_lamports);
        let bridge_id = Self::encode_deposit_bridge_id(&DepositBridgePollState {
            account: *destination,
            baseline,
            threshold,
        });

        Ok(BridgeSubmission { bridge_id })
    }

    async fn submit_icp_to_solana(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let source_account = self.ensure_principal_only_bridge_source(&request.source_address)?;
        let destination = Self::expect_solana_destination(route, &request.destination)?;

        let decimals =
            icrc1_decimals_with_context(self.icp_backend.as_ref(), self.cksol_ledger_canister, "cksol bridge").await?;
        let amount_native = amount_to_nat_units_strict(request.amount, decimals)?;
        let approve_fee =
            icrc1_fee_with_context(self.icp_backend.as_ref(), self.cksol_ledger_canister, "cksol bridge").await?;

        let required_budget = amount_native.clone() + approve_fee;
        let available_balance = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            self.cksol_ledger_canister,
            &source_account,
            "cksol bridge",
        )
        .await?;
        if available_balance < required_budget {
            let available_formatted = nat_units_to_amount_via_core(&available_balance, decimals)?;
            let required_formatted = nat_units_to_amount_via_core(&required_budget, decimals)?;
            return Err(format!(
                "bridge amount preflight failed: ckSOL balance is below required burn+approve budget (available={} required={} source={})",
                available_formatted, required_formatted, request.source_address
            ));
        }

        self.ensure_minter_allowance(&source_account, amount_native.clone())
            .await?;

        let amount_lamports = amount_native.0.to_u64().ok_or_else(|| {
            format!(
                "bridge amount too large for ckSOL lamports (u64): amount_native={}",
                amount_native
            )
        })?;

        let withdraw = self
            .withdraw(CkSolWithdrawalArgs {
                from_subaccount: None,
                amount: amount_lamports,
                address: destination.to_string(),
            })
            .await?;

        Ok(BridgeSubmission {
            bridge_id: format!("{}{}", WITHDRAW_BRIDGE_ID_PREFIX, withdraw.block_index),
        })
    }
}

#[async_trait]
impl<A, B> BridgeBackend for SolanaBridgeBackend<A, B>
where
    A: PipelineAgent,
    B: IcpBackend,
{
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        let route = Self::solana_route_from_source(asset, chain)?;

        match route.route_kind {
            BridgeRouteKind::SolanaToIcp => {
                Self::validate_solana_pubkey(address, "source Solana address")?;
                Err(format!(
                    "source balance is not supported for route {}@{} -> {}; use automated update_balance flow",
                    route.source_asset, route.source_chain, route.target_asset
                ))
            }
            BridgeRouteKind::IcpToSolana => {
                let source = self.ensure_principal_only_bridge_source(address)?;
                let decimals =
                    icrc1_decimals_with_context(self.icp_backend.as_ref(), self.cksol_ledger_canister, "cksol bridge")
                        .await?;
                let balance = icrc1_balance_with_context(
                    self.icp_backend.as_ref(),
                    self.cksol_ledger_canister,
                    &source,
                    "cksol bridge",
                )
                .await?;
                nat_units_to_amount_via_core(&balance, decimals)
            }
            _ => Err(format!(
                "route {}@{} -> {} is not supported by SolanaBridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        let route = Self::resolve_solana_route_for_request(&request)?;
        validate_destination_for_route(route, &request.destination)?;

        match route.route_kind {
            BridgeRouteKind::SolanaToIcp => self.submit_solana_to_icp(route, &request).await,
            BridgeRouteKind::IcpToSolana => self.submit_icp_to_solana(route, &request).await,
            _ => Err(format!(
                "route {}@{} -> {} is not supported by SolanaBridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        if let Some(block_index) = Self::parse_withdraw_bridge_id(bridge_id)? {
            let status = self.withdrawal_status(block_index).await?;
            return Ok(match status {
                CkSolWithdrawalStatus::Pending | CkSolWithdrawalStatus::TxSent { .. } => BridgeStatus::Pending,
                CkSolWithdrawalStatus::TxFinalized(CkSolTxFinalizedStatus::Success { .. }) => BridgeStatus::Completed,
                CkSolWithdrawalStatus::TxFinalized(CkSolTxFinalizedStatus::Failure { transaction_id }) => {
                    BridgeStatus::Failed {
                        reason: Some(format!("ckSOL withdrawal transaction {} failed", transaction_id)),
                    }
                }
                CkSolWithdrawalStatus::NotFound => BridgeStatus::Unknown,
            });
        }

        if let Some(state) = Self::parse_deposit_bridge_id(bridge_id)? {
            let current_balance = icrc1_balance_with_context(
                self.icp_backend.as_ref(),
                self.cksol_ledger_canister,
                &state.account,
                "cksol bridge",
            )
            .await?;
            if current_balance >= state.threshold {
                return Ok(BridgeStatus::Completed);
            }
            return Ok(BridgeStatus::Pending);
        }

        Err(format!(
            "unsupported ckSOL bridge id '{}'; expected '{}' or '{}' prefixes",
            bridge_id, WITHDRAW_BRIDGE_ID_PREFIX, DEPOSIT_BRIDGE_ID_PREFIX
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BridgeBackend, BridgeDestination, BridgeRequest};
    use super::{
        CkSolMinterInfo, CkSolTxFinalizedStatus, CkSolUpdateBalanceError, CkSolWithdrawalError, CkSolWithdrawalOk,
        CkSolWithdrawalStatus, DEPOSIT_BRIDGE_ID_PREFIX, DepositBridgePollState, SolanaBridgeBackend,
        WITHDRAW_BRIDGE_ID_PREFIX,
    };
    use crate::{backend::icp_backend::MockIcpBackend, pipeline_agent::MockPipelineAgent};
    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use std::sync::{Arc, Mutex};

    fn bridge_owner() -> Principal {
        Principal::from_text("aaaaa-aa").expect("principal")
    }

    fn minter_canister() -> Principal {
        Principal::from_text("ljyxk-riaaa-aaaar-qb5mq-cai").expect("principal")
    }

    fn ledger_canister() -> Principal {
        Principal::from_text("la34w-haaaa-aaaar-qb5na-cai").expect("principal")
    }

    fn valid_solana_address() -> String {
        "So11111111111111111111111111111111111111112".to_string()
    }

    fn backend(
        mock_agent: MockPipelineAgent,
        mock_icp: MockIcpBackend,
    ) -> SolanaBridgeBackend<MockPipelineAgent, MockIcpBackend> {
        SolanaBridgeBackend::new(
            Arc::new(mock_agent),
            Arc::new(mock_icp),
            minter_canister(),
            ledger_canister(),
            bridge_owner(),
        )
    }

    fn sol_to_icp_request(destination_owner: Principal, subaccount: Option<[u8; 32]>) -> BridgeRequest {
        BridgeRequest {
            asset: "SOL".to_string(),
            source_chain: "SOL".to_string(),
            source_address: valid_solana_address(),
            target_asset: "ckSOL".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: destination_owner,
                subaccount,
            }),
            amount: 1.25,
        }
    }

    fn icp_to_sol_request(source_address: String) -> BridgeRequest {
        BridgeRequest {
            asset: "ckSOL".to_string(),
            source_chain: "ICP".to_string(),
            source_address,
            target_asset: "SOL".to_string(),
            destination: BridgeDestination::SolanaAddress(valid_solana_address()),
            amount: 1.0,
        }
    }

    fn default_minter_info() -> CkSolMinterInfo {
        CkSolMinterInfo {
            manual_deposit_fee: 10_000,
            automated_deposit_fee: 10_000_000,
            deposit_consolidation_fee: Nat::from(10_000_000_000u64),
            minimum_withdrawal_amount: 2_000_000,
            minimum_deposit_amount: 20_000_000,
            withdrawal_fee: 1_000_000,
            process_deposit_required_cycles: Nat::from(1_000_000_000_000u64),
            balance: 0,
        }
    }

    #[tokio::test]
    async fn submit_bridge_rejects_non_solana_route_kind() {
        let backend = backend(MockPipelineAgent::new(), MockIcpBackend::new());
        let request = BridgeRequest {
            asset: "USDC".to_string(),
            source_chain: "ETH".to_string(),
            source_address: "0x1111111111111111111111111111111111111111".to_string(),
            target_asset: "ckUSDC".to_string(),
            destination: BridgeDestination::IcpAccount(Account {
                owner: bridge_owner(),
                subaccount: None,
            }),
            amount: 1.0,
        };

        let err = backend
            .submit_bridge(request)
            .await
            .expect_err("non-solana route kind must fail");
        assert!(err.contains("is not supported by SolanaBridgeBackend"));
    }

    #[tokio::test]
    async fn solana_to_icp_destination_owner_mismatch_fails() {
        let backend = backend(MockPipelineAgent::new(), MockIcpBackend::new());
        let request = sol_to_icp_request(Principal::anonymous(), None);

        let err = backend
            .submit_bridge(request)
            .await
            .expect_err("destination owner mismatch must fail");
        assert!(err.contains("destination ICP account owner"));
        assert!(err.contains("does not match configured bridge owner"));
    }

    #[tokio::test]
    async fn solana_to_icp_source_must_match_minter_deposit_address() {
        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_query::<String>()
            .times(1)
            .returning(|_, _, _| Ok("AnotherSource1111111111111111111111111111111".to_string()));

        let backend = backend(mock_agent, MockIcpBackend::new());
        let request = sol_to_icp_request(bridge_owner(), Some([7u8; 32]));

        let err = backend
            .submit_bridge(request)
            .await
            .expect_err("source/deposit address mismatch must fail");
        assert!(err.contains("does not match minter-derived deposit address"));
    }

    #[tokio::test]
    async fn solana_to_icp_happy_path_calls_update_balance_and_returns_synthetic_bridge_id() {
        let source = valid_solana_address();
        let subaccount = Some([9u8; 32]);
        let mut request = sol_to_icp_request(bridge_owner(), subaccount);
        request.source_address = source.clone();
        request.amount = 2.0;

        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_query::<String>()
            .times(1)
            .returning(move |_, _, _| Ok(source.clone()));
        mock_agent
            .expect_call_query::<CkSolMinterInfo>()
            .times(1)
            .returning(|_, _, _| Ok(default_minter_info()));
        mock_agent
            .expect_call_update::<Result<(), CkSolUpdateBalanceError>>()
            .times(1)
            .returning(|_, _, _| Ok(Ok(())));

        let mut mock_icp = MockIcpBackend::new();
        mock_icp.expect_icrc1_decimals().times(1).returning(|_| Ok(9));
        mock_icp
            .expect_icrc1_balance()
            .times(1)
            .returning(|_, _| Ok(Nat::from(1_000_000_000u64)));

        let backend = backend(mock_agent, mock_icp);
        let submission = backend.submit_bridge(request).await.expect("bridge must submit");

        assert!(submission.bridge_id.starts_with(DEPOSIT_BRIDGE_ID_PREFIX));
        let decoded =
            SolanaBridgeBackend::<MockPipelineAgent, MockIcpBackend>::parse_deposit_bridge_id(&submission.bridge_id)
                .expect("bridge id must decode")
                .expect("bridge id must be deposit metadata");

        assert_eq!(decoded.account.owner, bridge_owner());
        assert_eq!(decoded.account.subaccount, subaccount);
        assert_eq!(decoded.baseline, Nat::from(1_000_000_000u64));
        // expected threshold = baseline + (2_000_000_000 - 10_000_000)
        assert_eq!(decoded.threshold, Nat::from(2_990_000_000u64));
    }

    #[tokio::test]
    async fn icp_to_solana_enforces_owner_and_principal_only_source() {
        let backend = backend(MockPipelineAgent::new(), MockIcpBackend::new());

        let wrong_owner = icp_to_sol_request(Principal::anonymous().to_text());
        let err = backend
            .submit_bridge(wrong_owner)
            .await
            .expect_err("owner mismatch must fail");
        assert!(err.contains("does not match configured bridge owner"));

        let with_subaccount = Account {
            owner: bridge_owner(),
            subaccount: Some([1u8; 32]),
        }
        .to_string();
        let err = backend
            .submit_bridge(icp_to_sol_request(with_subaccount))
            .await
            .expect_err("subaccount source must fail");
        assert!(err.contains("subaccount must be None"));
    }

    #[tokio::test]
    async fn icp_to_solana_allowance_and_withdraw_happy_path() {
        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_update::<Result<CkSolWithdrawalOk, CkSolWithdrawalError>>()
            .times(1)
            .returning(|_, _, _| Ok(Ok(CkSolWithdrawalOk { block_index: 42 })));

        let mut mock_icp = MockIcpBackend::new();
        mock_icp.expect_icrc1_decimals().times(1).returning(|_| Ok(9));
        mock_icp
            .expect_icrc1_fee()
            .times(1)
            .returning(|_| Ok(Nat::from(10_000u64)));
        mock_icp
            .expect_icrc1_balance()
            .times(1)
            .returning(|_, _| Ok(Nat::from(1_100_000_000u64)));
        mock_icp
            .expect_icrc2_allowance()
            .times(1)
            .returning(|_, _, _| Ok(Nat::from(0u8)));
        mock_icp
            .expect_icrc2_approve()
            .times(1)
            .returning(|_, _| Ok(Nat::from(7u8)));

        let backend = backend(mock_agent, mock_icp);
        let submission = backend
            .submit_bridge(icp_to_sol_request(bridge_owner().to_text()))
            .await
            .expect("withdraw submit must pass");
        assert_eq!(submission.bridge_id, format!("{}42", WITHDRAW_BRIDGE_ID_PREFIX));
    }

    #[tokio::test]
    async fn icp_to_solana_withdraw_error_mapping_is_explicit() {
        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_update::<Result<CkSolWithdrawalOk, CkSolWithdrawalError>>()
            .times(1)
            .returning(|_, _, _| Ok(Err(CkSolWithdrawalError::InsufficientAllowance { allowance: 123 })));

        let mut mock_icp = MockIcpBackend::new();
        mock_icp.expect_icrc1_decimals().times(1).returning(|_| Ok(9));
        mock_icp
            .expect_icrc1_fee()
            .times(1)
            .returning(|_| Ok(Nat::from(10_000u64)));
        mock_icp
            .expect_icrc1_balance()
            .times(1)
            .returning(|_, _| Ok(Nat::from(1_100_000_000u64)));
        mock_icp
            .expect_icrc2_allowance()
            .times(1)
            .returning(|_, _, _| Ok(Nat::from(1_000_000_000u64)));
        mock_icp.expect_icrc2_approve().times(0);

        let backend = backend(mock_agent, mock_icp);
        let err = backend
            .submit_bridge(icp_to_sol_request(bridge_owner().to_text()))
            .await
            .expect_err("withdraw error must propagate with explicit variant");
        assert!(err.contains("InsufficientAllowance"));
    }

    #[tokio::test]
    async fn get_bridge_status_maps_all_withdrawal_status_variants() {
        let status_call_count = Arc::new(Mutex::new(0u8));
        let status_call_count_clone = status_call_count.clone();

        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_call_update::<CkSolWithdrawalStatus>()
            .times(5)
            .returning(move |_, _, _| {
                let mut guard = status_call_count_clone.lock().expect("mutex");
                let out = match *guard {
                    0 => CkSolWithdrawalStatus::Pending,
                    1 => CkSolWithdrawalStatus::TxSent {
                        transaction_id: "sig-1".to_string(),
                    },
                    2 => CkSolWithdrawalStatus::TxFinalized(CkSolTxFinalizedStatus::Success {
                        transaction_id: "sig-2".to_string(),
                        effective_transaction_fee: None,
                    }),
                    3 => CkSolWithdrawalStatus::TxFinalized(CkSolTxFinalizedStatus::Failure {
                        transaction_id: "sig-3".to_string(),
                    }),
                    _ => CkSolWithdrawalStatus::NotFound,
                };
                *guard += 1;
                Ok(out)
            });

        let backend = backend(mock_agent, MockIcpBackend::new());

        assert_eq!(
            backend
                .get_bridge_status(&format!("{}1", WITHDRAW_BRIDGE_ID_PREFIX))
                .await
                .expect("status"),
            super::BridgeStatus::Pending
        );
        assert_eq!(
            backend
                .get_bridge_status(&format!("{}1", WITHDRAW_BRIDGE_ID_PREFIX))
                .await
                .expect("status"),
            super::BridgeStatus::Pending
        );
        assert_eq!(
            backend
                .get_bridge_status(&format!("{}1", WITHDRAW_BRIDGE_ID_PREFIX))
                .await
                .expect("status"),
            super::BridgeStatus::Completed
        );
        assert!(matches!(
            backend
                .get_bridge_status(&format!("{}1", WITHDRAW_BRIDGE_ID_PREFIX))
                .await
                .expect("status"),
            super::BridgeStatus::Failed { .. }
        ));
        assert_eq!(
            backend
                .get_bridge_status(&format!("{}1", WITHDRAW_BRIDGE_ID_PREFIX))
                .await
                .expect("status"),
            super::BridgeStatus::Unknown
        );
    }

    #[tokio::test]
    async fn get_bridge_status_polls_automated_deposit_threshold() {
        let poll_state = DepositBridgePollState {
            account: Account {
                owner: bridge_owner(),
                subaccount: Some([5u8; 32]),
            },
            baseline: Nat::from(100u64),
            threshold: Nat::from(150u64),
        };
        let bridge_id = SolanaBridgeBackend::<MockPipelineAgent, MockIcpBackend>::encode_deposit_bridge_id(&poll_state);

        let balance_call_count = Arc::new(Mutex::new(0u8));
        let balance_call_count_clone = balance_call_count.clone();
        let mut mock_icp = MockIcpBackend::new();
        mock_icp.expect_icrc1_balance().times(2).returning(move |_, _| {
            let mut guard = balance_call_count_clone.lock().expect("mutex");
            let out = if *guard == 0 {
                Nat::from(149u64)
            } else {
                Nat::from(150u64)
            };
            *guard += 1;
            Ok(out)
        });

        let backend = backend(MockPipelineAgent::new(), mock_icp);

        assert_eq!(
            backend.get_bridge_status(&bridge_id).await.expect("status"),
            super::BridgeStatus::Pending
        );
        assert_eq!(
            backend.get_bridge_status(&bridge_id).await.expect("status"),
            super::BridgeStatus::Completed
        );
    }

    #[tokio::test]
    async fn get_source_balance_for_icp_to_solana_reads_cksol_ledger_balance() {
        let mut mock_icp = MockIcpBackend::new();
        mock_icp.expect_icrc1_decimals().times(1).returning(|_| Ok(9));
        mock_icp
            .expect_icrc1_balance()
            .times(1)
            .returning(|_, _| Ok(Nat::from(123_000_000u64)));

        let backend = backend(MockPipelineAgent::new(), mock_icp);
        let balance = backend
            .get_source_balance("ckSOL", "ICP", &bridge_owner().to_text())
            .await
            .expect("balance");
        assert!((balance - 0.123).abs() < 1e-12);
    }

    #[tokio::test]
    async fn get_source_balance_for_solana_to_icp_is_explicitly_unsupported() {
        let backend = backend(MockPipelineAgent::new(), MockIcpBackend::new());

        let err = backend
            .get_source_balance("SOL", "SOL", &valid_solana_address())
            .await
            .expect_err("source balance on Solana side should be unsupported in this phase");
        assert!(err.contains("source balance is not supported"));
    }

    #[test]
    fn deposit_bridge_id_roundtrip_preserves_account_and_threshold() {
        let state = DepositBridgePollState {
            account: Account {
                owner: bridge_owner(),
                subaccount: Some([3u8; 32]),
            },
            baseline: Nat::from(42u64),
            threshold: Nat::from(99u64),
        };

        let encoded = SolanaBridgeBackend::<MockPipelineAgent, MockIcpBackend>::encode_deposit_bridge_id(&state);
        let decoded = SolanaBridgeBackend::<MockPipelineAgent, MockIcpBackend>::parse_deposit_bridge_id(&encoded)
            .expect("decode")
            .expect("some");
        assert_eq!(decoded, state);
    }
}
