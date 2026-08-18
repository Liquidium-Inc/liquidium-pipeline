use alloy::{
    consensus::Transaction as ConsensusTransaction,
    network::{AnyNetwork, ReceiptResponse, TransactionResponse},
    primitives::{Address, FixedBytes, TxHash, U256},
    providers::{Provider, WalletProvider},
    sol,
};
use async_trait::async_trait;
use candid::{Encode, Nat, Principal};
use icrc_ledger_types::{
    icrc1::account::Account,
    icrc2::{allowance::AllowanceArgs, approve::ApproveArgs},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex as TokioMutex;

use super::{
    ckerc20_bridge_utils::{
        destination_to_bytes32, ensure_source_matches_bridge_owner, ensure_source_matches_signer,
        expect_evm_destination, expect_icp_destination, parse_ckerc20_ledger_id, parse_evm_token_address,
        parse_source_icp_account, resolve_cketh_route_for_request,
    },
    mint_lookup::find_convert_mint,
    superseded::{classify_liveness, superseded_reason},
    types::{
        CkEthMinterInfo, Eip1559TransactionPrice, Eip1559TransactionPriceArg, EvmReceiptStatus, HelperContract,
        LedgerError, TxLiveness, WithdrawErc20Arg, WithdrawErc20Error, WithdrawErc20Ret, WithdrawalArg, WithdrawalRet,
    },
};
use crate::{
    backend::{
        amount_utils::{
            amount_to_base_units_strict, amount_to_nat_units_strict, base_units_to_amount_via_core,
            nat_units_to_amount_via_core,
        },
        bridge_backend::{
            BridgeBackend, BridgeDestination, BridgeFeeBudget, BridgeRequest, BridgeRouteKind, BridgeRouteSpec,
            BridgeStatus, BridgeSubmission, resolve_cketh_forward_route_by_source,
            resolve_cketh_forward_route_by_target, resolve_cketh_reverse_route_by_source,
        },
        evm_backend::EvmBackendImpl,
        icp_backend::IcpBackend,
        icp_backend_helpers::{
            icrc1_balance_with_context, icrc1_decimals_with_context, icrc1_fee_with_context,
            icrc2_allowance_with_context, icrc2_approve_with_context,
        },
    },
    pipeline_agent::PipelineAgent,
};

sol! {
    #[sol(rpc)]
    interface ICkErc20HelperNative {
        function deposit(bytes32 principal) external payable;
        function deposit(address token, uint256 amount, bytes32 principal) external;
    }
}

sol! {
    #[sol(rpc)]
    interface ICkErc20HelperWithSubaccount {
        function depositErc20(address erc20Address, uint256 amount, bytes32 principal, bytes32 subaccount) external;
    }
}

sol! {
    #[sol(rpc)]
    interface ICkEthHelperWithSubaccount {
        function depositEth(bytes32 principal, bytes32 subaccount) external payable;
    }
}

const ETH_DECIMALS: u8 = 18;
const MIN_ETH_FORWARD_GAS_RESERVE_WEI: u128 = 5_000_000_000_000_000;
const ETH_FORWARD_HELPER_GAS_LIMIT: u128 = 150_000;
const ETH_FORWARD_GAS_RESERVE_MULTIPLIER: u128 = 2;
const DEFAULT_CKETH_MIN_WITHDRAWAL_WEI: u128 = 5_000_000_000_000_000;
pub const FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX: &str = "finalizer_permanent_amount_floor";
pub const BRIDGE_AMOUNT_BELOW_MINIMUM_PREFIX: &str = FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX;

fn eth_forward_gas_reserve_from_price(gas_price_wei: u128, gas_limit: u128) -> U256 {
    let dynamic_reserve = U256::from(gas_price_wei)
        .saturating_mul(U256::from(gas_limit))
        .saturating_mul(U256::from(ETH_FORWARD_GAS_RESERVE_MULTIPLIER));
    dynamic_reserve.max(U256::from(MIN_ETH_FORWARD_GAS_RESERVE_WEI))
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BridgeEvmBackend: Send + Sync {
    fn signer_address(&self) -> Address;

    async fn native_balance_of(&self, owner: Address) -> Result<U256, String>;
    async fn native_tx_gas_reserve(&self, gas_limit: u128) -> Result<U256, String>;
    async fn erc20_balance_of(&self, token: Address, owner: Address) -> Result<U256, String>;
    async fn erc20_allowance_of(&self, token: Address, owner: Address, spender: Address) -> Result<U256, String>;
    async fn erc20_decimals_of(&self, token: Address) -> Result<u8, String>;
    async fn erc20_approve_and_wait(&self, token: Address, spender: Address, amount: U256) -> Result<TxHash, String>;

    async fn helper_deposit_native(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn helper_deposit_with_subaccount(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn helper_deposit_eth_native(
        &self,
        helper: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn helper_deposit_eth_with_subaccount(
        &self,
        helper: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String>;

    async fn receipt_status(&self, tx_hash: TxHash) -> Result<Option<EvmReceiptStatus>, String>;

    /// Reports what the node still knows about a transaction with no receipt.
    ///
    /// Says whether it mined, was replaced, or is still waiting its turn, so no
    /// caller has to read minedness out of a missing receipt.
    async fn tx_liveness(&self, tx_hash: TxHash) -> Result<TxLiveness, String>;
}

#[async_trait]
impl<P> BridgeEvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    fn signer_address(&self) -> Address {
        self.provider.default_signer_address()
    }

    async fn native_balance_of(&self, owner: Address) -> Result<U256, String> {
        self.provider
            .get_balance(owner)
            .await
            .map_err(|e| format!("native get_balance(owner={owner}) failed: {e}"))
    }

    async fn native_tx_gas_reserve(&self, gas_limit: u128) -> Result<U256, String> {
        let gas_price_wei = self
            .provider
            .get_gas_price()
            .await
            .map_err(|e| format!("native gas price fetch failed: {e}"))?;
        Ok(eth_forward_gas_reserve_from_price(gas_price_wei, gas_limit))
    }

    async fn erc20_balance_of(&self, token: Address, owner: Address) -> Result<U256, String> {
        self.erc20_balance_of_raw(token, owner).await
    }

    async fn erc20_allowance_of(&self, token: Address, owner: Address, spender: Address) -> Result<U256, String> {
        self.erc20_allowance_raw(token, owner, spender).await
    }

    async fn erc20_decimals_of(&self, token: Address) -> Result<u8, String> {
        self.erc20_decimals_raw(token).await
    }

    async fn erc20_approve_and_wait(&self, token: Address, spender: Address, amount: U256) -> Result<TxHash, String> {
        self.erc20_approve_and_wait_raw(token, spender, amount).await
    }

    async fn helper_deposit_native(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkErc20HelperNative::new(helper, self.provider.clone());
        let pending = helper_contract
            .deposit_1(token, amount, recipient_principal)
            .send()
            .await
            .map_err(|e| format!("native helper deposit failed (helper={helper}, token={token}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn helper_deposit_with_subaccount(
        &self,
        helper: Address,
        token: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkErc20HelperWithSubaccount::new(helper, self.provider.clone());
        let pending = helper_contract
            .depositErc20(token, amount, recipient_principal, recipient_subaccount)
            .send()
            .await
            .map_err(|e| format!("helper depositErc20 failed (helper={helper}, token={token}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn helper_deposit_eth_native(
        &self,
        helper: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkErc20HelperNative::new(helper, self.provider.clone());
        let pending = helper_contract
            .deposit_0(recipient_principal)
            .value(amount)
            .send()
            .await
            .map_err(|e| format!("native ETH helper deposit failed (helper={helper}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn helper_deposit_eth_with_subaccount(
        &self,
        helper: Address,
        amount: U256,
        recipient_principal: FixedBytes<32>,
        recipient_subaccount: FixedBytes<32>,
    ) -> Result<TxHash, String> {
        let helper_contract = ICkEthHelperWithSubaccount::new(helper, self.provider.clone());
        let pending = helper_contract
            .depositEth(recipient_principal, recipient_subaccount)
            .value(amount)
            .send()
            .await
            .map_err(|e| format!("ETH helper depositEth failed (helper={helper}): {e}"))?;
        Ok(*pending.tx_hash())
    }

    async fn receipt_status(&self, tx_hash: TxHash) -> Result<Option<EvmReceiptStatus>, String> {
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| format!("transaction receipt fetch failed for {tx_hash:#x}: {e}"))?;

        Ok(receipt.map(|receipt| EvmReceiptStatus {
            success: receipt.status(),
            block_number: receipt.block_number,
        }))
    }

    async fn tx_liveness(&self, tx_hash: TxHash) -> Result<TxLiveness, String> {
        let Some(tx) = self
            .provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| format!("transaction fetch failed for {tx_hash:#x}: {e}"))?
        else {
            return Ok(TxLiveness::Unknown);
        };

        let block_number = tx.block_number();
        // A mined transaction needs no nonce comparison, so skip the extra call.
        if block_number.is_some() {
            return Ok(TxLiveness::Mined);
        }

        let sender = tx.from();
        let tx_nonce = ConsensusTransaction::nonce(&tx);
        // Latest, not pending: a pending count includes this very transaction and
        // so could never show its nonce as consumed by another.
        let sender_count = self
            .provider
            .get_transaction_count(sender)
            .await
            .map_err(|e| format!("transaction count fetch failed for {sender}: {e}"))?;

        let liveness = classify_liveness(block_number, tx_nonce, sender_count);
        if !matches!(liveness, TxLiveness::Replaced { .. }) {
            return Ok(liveness);
        }

        // The two reads above can land on different nodes, so a stale one can
        // report the transaction unmined while a current one has already counted
        // its nonce. Read it once more before calling its funds unspent.
        let mined_since = self
            .provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| format!("transaction fetch failed for {tx_hash:#x}: {e}"))?
            .is_some_and(|tx| tx.block_number().is_some());

        Ok(if mined_since { TxLiveness::Mined } else { liveness })
    }
}

/// Bridge backend for native ETH/ckETH and `ERC20@ETH -> ckERC20` routes via ckETH minter helper contracts.
///
/// Token and route selection are resolved from bridge route metadata.
pub struct CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    pub agent: Arc<A>,
    pub icp_backend: Arc<B>,
    pub evm_backend: Arc<E>,
    pub cketh_minter_canister: Principal,
    pub bridge_ic_owner_principal: Principal,
    /// Serialises forward-bridge submissions that share one signer and token.
    ///
    /// An ERC20 allowance is wallet-wide state, not per-caller. Two legs
    /// bridging the same token each read it, decide whether to approve, and then
    /// submit a deposit against it. Interleaved, the first deposit to mine
    /// consumes the allowance the second was relying on, and that second deposit
    /// reverts on-chain: gas spent, and the withdrawn funds left sitting at the
    /// signer address with nothing to move them. The balance preflight races the
    /// same way, and `approve` sets rather than adds, so a second approval can
    /// also shrink an allowance the first leg still needs.
    ///
    /// Holding this across read-allowance -> approve -> deposit makes the whole
    /// sequence atomic, which is enough because one daemon owns the wallet.
    /// Keyed by token: different tokens share no allowance or balance.
    forward_bridge_locks: TokioMutex<HashMap<Address, Arc<TokioMutex<()>>>>,
    /// Serialises reverse-bridge submissions, for the same reason as
    /// `forward_bridge_locks`: an ICRC-2 allowance is account-wide, so two legs
    /// reading it, deciding whether to approve, and then having the minter spend
    /// it will interleave into one leg burning the other's approval.
    ///
    /// One lock rather than one per ledger, because a single reverse withdraw
    /// spends two allowances at once -- the ckERC20 being burned and the ckETH
    /// paying the EVM fee. Taking two locks would invite a deadlock for no gain:
    /// these submissions are a handful of canister calls each, and they all
    /// contend on the same minter and the same source account anyway.
    reverse_bridge_lock: TokioMutex<()>,
}

impl<A, B, E> CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    pub fn new(
        agent: Arc<A>,
        icp_backend: Arc<B>,
        evm_backend: Arc<E>,
        cketh_minter_canister: Principal,
        bridge_ic_owner_principal: Principal,
    ) -> Self {
        Self {
            agent,
            icp_backend,
            evm_backend,
            cketh_minter_canister,
            bridge_ic_owner_principal,
            forward_bridge_locks: TokioMutex::new(HashMap::new()),
            reverse_bridge_lock: TokioMutex::new(()),
        }
    }

    /// Claims the forward-bridge lock for one ERC20 token.
    async fn forward_bridge_lock(&self, token: Address) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut guard = self.forward_bridge_locks.lock().await;
            guard
                .entry(token)
                .or_insert_with(|| Arc::new(TokioMutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    async fn minter_info(&self) -> Result<CkEthMinterInfo, String> {
        let args = Encode!(&()).map_err(|e| format!("encode get_minter_info args failed: {e}"))?;
        match self
            .agent
            .call_query::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args.clone())
            .await
        {
            Ok(v) => Ok(v),
            Err(query_err) => self
                .agent
                .call_update::<CkEthMinterInfo>(&self.cketh_minter_canister, "get_minter_info", args)
                .await
                .map_err(|update_err| {
                    format!(
                        "get_minter_info failed (query: {query_err}; update: {update_err}) for canister {}",
                        self.cketh_minter_canister
                    )
                }),
        }
    }

    async fn helper_contract(&self) -> Result<HelperContract, String> {
        let info = self.minter_info().await?;

        if let Some(addr) = info.deposit_with_subaccount_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid deposit_with_subaccount helper address '{addr}': {e}"))?;
            return Ok(HelperContract::WithSubaccount(parsed));
        }

        if let Some(addr) = info.erc20_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid erc20 helper address '{addr}': {e}"))?;
            return Ok(HelperContract::Native(parsed));
        }

        Err(format!(
            "minter {} returned no helper contract address in get_minter_info",
            self.cketh_minter_canister
        ))
    }

    async fn eth_helper_contract(&self) -> Result<HelperContract, String> {
        let info = self.minter_info().await?;

        if let Some(addr) = info.deposit_with_subaccount_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid deposit_with_subaccount helper address '{addr}': {e}"))?;
            return Ok(HelperContract::WithSubaccount(parsed));
        }

        if let Some(addr) = info.eth_helper_contract_address {
            let parsed = addr
                .parse::<Address>()
                .map_err(|e| format!("invalid eth helper address '{addr}': {e}"))?;
            return Ok(HelperContract::Native(parsed));
        }

        Err(format!(
            "minter {} returned no ETH helper contract address in get_minter_info",
            self.cketh_minter_canister
        ))
    }

    async fn eip_1559_transaction_price(
        &self,
        ckerc20_ledger_id: Option<Principal>,
    ) -> Result<Eip1559TransactionPrice, String> {
        let args =
            Encode!(&ckerc20_ledger_id.map(|ckerc20_ledger_id| Eip1559TransactionPriceArg { ckerc20_ledger_id }))
                .map_err(|e| format!("encode eip_1559_transaction_price args failed: {e}"))?;
        self.agent
            .call_query::<Eip1559TransactionPrice>(&self.cketh_minter_canister, "eip_1559_transaction_price", args)
            .await
            .map_err(|e| {
                format!(
                    "eip_1559_transaction_price failed for minter {}: {}",
                    self.cketh_minter_canister, e
                )
            })
    }

    async fn cketh_ledger_id(&self) -> Result<Principal, String> {
        let minter_info = self.minter_info().await?;
        minter_info.cketh_ledger_id.ok_or_else(|| {
            format!(
                "minter {} returned no cketh_ledger_id in get_minter_info",
                self.cketh_minter_canister
            )
        })
    }

    async fn token_decimals(&self, token: Address) -> Result<u8, String> {
        self.evm_backend.erc20_decimals_of(token).await
    }

    /// Sets the minter's allowance, refusing if it is not what we just observed.
    ///
    /// `expected_allowance` is ICRC-2's compare-and-swap: if anything changed the
    /// allowance between the read and this call, the ledger rejects the approve
    /// rather than silently overwriting an approval another submission is still
    /// relying on. `reverse_bridge_lock` should already make that impossible
    /// in-process; this catches anything holding the same account from outside.
    async fn approve_minter_spend(&self, ledger: Principal, amount: Nat, expected: Nat) -> Result<(), String> {
        let approve_args = ApproveArgs {
            from_subaccount: None,
            spender: Account {
                owner: self.cketh_minter_canister,
                subaccount: None,
            },
            amount,
            expected_allowance: Some(expected),
            expires_at: None,
            fee: None,
            memo: None,
            created_at_time: None,
        };
        icrc2_approve_with_context(self.icp_backend.as_ref(), ledger, approve_args, "ckerc20 bridge")
            .await
            .map(|_| ())?;
        Ok(())
    }

    async fn minter_allowance(&self, source_account: &Account, ledger: Principal) -> Result<Nat, String> {
        let allowance_args = AllowanceArgs {
            account: *source_account,
            spender: Account {
                owner: self.cketh_minter_canister,
                subaccount: None,
            },
        };
        icrc2_allowance_with_context(self.icp_backend.as_ref(), ledger, allowance_args, "ckerc20 bridge").await
    }

    async fn ensure_minter_allowance(
        &self,
        source_account: &Account,
        ledger: Principal,
        required_allowance: Nat,
    ) -> Result<(), String> {
        let current_allowance = self.minter_allowance(source_account, ledger).await?;
        if current_allowance >= required_allowance {
            return Ok(());
        }

        self.approve_minter_spend(ledger, required_allowance, current_allowance)
            .await
    }

    fn with_fee_headroom(amount: &Nat) -> Nat {
        // Keep a modest buffer for quote drift between preflight and withdraw burn.
        amount.clone() + (amount.clone() / Nat::from(5u8))
    }

    async fn quoted_cketh_fee_budget(&self, quote_for_ledger: Option<Principal>) -> Result<Nat, String> {
        let fee_quote = self.eip_1559_transaction_price(quote_for_ledger).await?;
        Ok(Self::with_fee_headroom(&fee_quote.max_transaction_fee))
    }

    fn native_cketh_minimum_withdrawal_amount() -> Nat {
        Nat::from(DEFAULT_CKETH_MIN_WITHDRAWAL_WEI)
    }

    fn below_minimum_bridge_error(asset: &str, chain: &str, target_asset: &str, amount: f64, minimum: f64) -> String {
        format!(
            "{FINALIZER_PERMANENT_AMOUNT_FLOOR_PREFIX}: {}@{} -> {} amount below minimum withdrawal (amount={} minimum={})",
            asset, chain, target_asset, amount, minimum
        )
    }

    async fn ensure_native_cketh_withdrawal_minimum(
        &self,
        route: &BridgeRouteSpec,
        amount_native: &Nat,
        amount: f64,
    ) -> Result<(), String> {
        let minimum_native = Self::native_cketh_minimum_withdrawal_amount();
        if amount_native < &minimum_native {
            let minimum = nat_units_to_amount_via_core(&minimum_native, ETH_DECIMALS)?;
            return Err(Self::below_minimum_bridge_error(
                route.source_asset,
                route.source_chain,
                route.target_asset,
                amount,
                minimum,
            ));
        }
        Ok(())
    }

    async fn withdraw_erc20_call(&self, args: &WithdrawErc20Arg) -> Result<WithdrawErc20Ret, String> {
        let arg_blob = Encode!(args).map_err(|e| format!("encode withdraw_erc20 args failed: {e}"))?;
        self.agent
            .call_update::<WithdrawErc20Ret>(&self.cketh_minter_canister, "withdraw_erc20", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "withdraw_erc20 call failed for minter {}: {}",
                    self.cketh_minter_canister, e
                )
            })
    }

    async fn withdraw_eth_call(&self, args: &WithdrawalArg) -> Result<WithdrawalRet, String> {
        let arg_blob = Encode!(args).map_err(|e| format!("encode withdraw_eth args failed: {e}"))?;
        self.agent
            .call_update::<WithdrawalRet>(&self.cketh_minter_canister, "withdraw_eth", arg_blob)
            .await
            .map_err(|e| {
                format!(
                    "withdraw_eth call failed for minter {}: {}",
                    self.cketh_minter_canister, e
                )
            })
    }

    async fn submit_forward_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let token_address = parse_evm_token_address(route)?;

        let signer = self.evm_backend.signer_address();
        // Forward bridging spends from the connected EVM wallet only.
        ensure_source_matches_signer(&request.source_address, signer)?;

        let destination_account = expect_icp_destination(route, &request.destination)?;
        // ckETH helper contracts accept principal/subaccount as bytes32 values.
        let (recipient_principal_bytes32, recipient_subaccount_bytes32) = destination_to_bytes32(destination_account);

        // Prefer helper with subaccount support when minter exposes it; fall back to native helper otherwise.
        let helper = self.helper_contract().await?;
        if destination_account.subaccount.is_some() && matches!(helper, HelperContract::Native(_)) {
            return Err(format!(
                "destination {:?} includes an ICP subaccount, but native helper contract does not support subaccount destinations",
                request.destination
            ));
        }
        let helper_address = match helper {
            HelperContract::WithSubaccount(address) | HelperContract::Native(address) => address,
        };

        let decimals = self.token_decimals(token_address).await?;
        let amount_base_units = amount_to_base_units_strict(request.amount, decimals)?;

        // Everything from here to the deposit reads or spends wallet-wide state,
        // so it runs alone. Claimed after the preparation above, which touches
        // nothing shared, to keep the held section as short as the RPC round
        // trips allow.
        let _bridge_guard = self.forward_bridge_lock(token_address).await;

        let signer_balance = self
            .evm_backend
            .erc20_balance_of(token_address, signer)
            .await
            .map_err(|e| format!("ERC20 balanceOf(signer={signer}) failed for token {token_address}: {e}"))?;
        if signer_balance < amount_base_units {
            return Err(format!(
                "bridge amount preflight failed: ERC20 balance is below requested deposit amount (available={} required={} signer={} token={})",
                signer_balance, amount_base_units, signer, token_address
            ));
        }

        // Helper must be approved before deposit call can transfer ERC20 from signer.
        let helper_allowance = self
            .evm_backend
            .erc20_allowance_of(token_address, signer, helper_address)
            .await
            .map_err(|e| format!("ERC20 allowance(helper={helper_address}) failed: {e}"))?;
        if helper_allowance < amount_base_units {
            self.evm_backend
                .erc20_approve_and_wait(token_address, helper_address, amount_base_units)
                .await
                .map_err(|e| format!("ERC20 approve(helper={helper_address}) failed: {e}"))?;
        }

        let tx_hash = match helper {
            HelperContract::WithSubaccount(address) => self
                .evm_backend
                .helper_deposit_with_subaccount(
                    address,
                    token_address,
                    amount_base_units,
                    recipient_principal_bytes32,
                    recipient_subaccount_bytes32,
                )
                .await
                .map_err(|e| format!("helper depositErc20 failed: {e}"))?,
            HelperContract::Native(address) => self
                .evm_backend
                .helper_deposit_native(address, token_address, amount_base_units, recipient_principal_bytes32)
                .await
                .map_err(|e| format!("native helper deposit failed: {e}"))?,
        };

        // Return the submitted tx hash; completion is tracked asynchronously via get_bridge_status.
        Ok(BridgeSubmission {
            bridge_id: format!("{:#x}", tx_hash),
        })
    }

    /// Submits native ETH forward bridge flow (`ETH@ETH -> ckETH@ICP`).
    ///
    /// Behavior:
    /// - Source must match the configured EVM signer.
    /// - Destination must be an ICP account.
    /// - If the minter exposes `deposit_with_subaccount_helper_contract_address`,
    ///   we call `depositEth(bytes32 principal, bytes32 subaccount)`.
    /// - Otherwise we fall back to `eth_helper_contract_address` and call
    ///   `deposit(bytes32 principal)`, which only supports owner-only destinations.
    ///
    /// Safety invariant:
    /// - Preflight requires `signer_balance >= amount + dynamic gas reserve`
    ///   so we never attempt to bridge the entire ETH balance and strand gas.
    async fn submit_native_forward_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let signer = self.evm_backend.signer_address();
        ensure_source_matches_signer(&request.source_address, signer)?;

        let destination_account = expect_icp_destination(route, &request.destination)?;
        let (recipient_principal_bytes32, recipient_subaccount_bytes32) = destination_to_bytes32(destination_account);

        let helper = self.eth_helper_contract().await?;
        if destination_account.subaccount.is_some() && matches!(helper, HelperContract::Native(_)) {
            return Err(format!(
                "destination {:?} includes an ICP subaccount, but native ETH helper contract does not support subaccount destinations",
                request.destination
            ));
        }

        let amount_wei = amount_to_base_units_strict(request.amount, ETH_DECIMALS)?;
        let gas_reserve = self
            .evm_backend
            .native_tx_gas_reserve(ETH_FORWARD_HELPER_GAS_LIMIT)
            .await?;
        let required_balance = amount_wei
            .checked_add(gas_reserve)
            .ok_or_else(|| "ETH bridge amount plus gas reserve overflowed U256".to_string())?;
        let signer_balance = self
            .evm_backend
            .native_balance_of(signer)
            .await
            .map_err(|e| format!("native ETH balance preflight failed for signer={signer}: {e}"))?;
        if signer_balance < required_balance {
            return Err(format!(
                "bridge amount preflight failed: ETH balance is below requested deposit amount plus gas reserve (available={} required={} amount={} gas_reserve={} signer={})",
                signer_balance, required_balance, amount_wei, gas_reserve, signer
            ));
        }

        let tx_hash = match helper {
            HelperContract::WithSubaccount(address) => self
                .evm_backend
                .helper_deposit_eth_with_subaccount(
                    address,
                    amount_wei,
                    recipient_principal_bytes32,
                    recipient_subaccount_bytes32,
                )
                .await
                .map_err(|e| format!("ETH helper depositEth failed: {e}"))?,
            HelperContract::Native(address) => self
                .evm_backend
                .helper_deposit_eth_native(address, amount_wei, recipient_principal_bytes32)
                .await
                .map_err(|e| format!("native ETH helper deposit failed: {e}"))?,
        };

        Ok(BridgeSubmission {
            bridge_id: format!("{:#x}", tx_hash),
        })
    }

    async fn submit_reverse_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let source_account = parse_source_icp_account(&request.source_address)?;
        // Reverse flow is restricted to the configured bridge owner principal.
        ensure_source_matches_bridge_owner(&source_account, self.bridge_ic_owner_principal)?;

        // Held for the whole submission: everything below reads or spends the
        // minter's allowance on this account, which no other leg may disturb
        // between the read and the withdraw that consumes it.
        let _reverse_guard = self.reverse_bridge_lock.lock().await;

        let destination = expect_evm_destination(route, &request.destination)?;

        let ckerc20_ledger_id = parse_ckerc20_ledger_id(route)?;
        let ckusdc_decimals =
            icrc1_decimals_with_context(self.icp_backend.as_ref(), ckerc20_ledger_id, "ckerc20 bridge").await?;
        let amount_native = amount_to_nat_units_strict(request.amount, ckusdc_decimals)?;
        let ckusdc_approve_fee =
            icrc1_fee_with_context(self.icp_backend.as_ref(), ckerc20_ledger_id, "ckerc20 bridge").await?;

        let ckusdc_required_budget = amount_native.clone() + ckusdc_approve_fee.clone();

        let available_ckusdc = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            ckerc20_ledger_id,
            &source_account,
            "ckerc20 bridge",
        )
        .await?;
        if available_ckusdc < ckusdc_required_budget {
            let available_formatted = nat_units_to_amount_via_core(&available_ckusdc, ckusdc_decimals)?;
            let required_formatted = nat_units_to_amount_via_core(&ckusdc_required_budget, ckusdc_decimals)?;
            return Err(format!(
                "bridge amount preflight failed: ckUSDC balance is below required burn+approve budget (available={} required={} source={})",
                available_formatted, required_formatted, request.source_address
            ));
        }

        let cketh_ledger_id = self.cketh_ledger_id().await?;
        let required_fee_budget = match &request.provider_fee_budget_native_units {
            Some(quoted) => quoted.clone(),
            None => self.quoted_cketh_fee_budget(Some(ckerc20_ledger_id)).await?,
        };

        // Minter withdraw burns ckERC20 and consumes ckETH for the EVM execution fee.
        let available_cketh = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            cketh_ledger_id,
            &source_account,
            "ckerc20 bridge",
        )
        .await?;
        if available_cketh < required_fee_budget {
            let cketh_decimals =
                icrc1_decimals_with_context(self.icp_backend.as_ref(), cketh_ledger_id, "ckerc20 bridge")
                    .await
                    .unwrap_or(18);
            let available_formatted = nat_units_to_amount_via_core(&available_cketh, cketh_decimals)?;
            let required_formatted = nat_units_to_amount_via_core(&required_fee_budget, cketh_decimals)?;
            return Err(format!(
                "bridge fee preflight failed: ckETH balance is below required transaction fee budget (available={} required={} source={})",
                available_formatted, required_formatted, request.source_address
            ));
        }

        // Approve ckERC20 amount to burn for withdrawal.
        self.ensure_minter_allowance(&source_account, ckerc20_ledger_id, amount_native.clone())
            .await?;
        // Approve ckETH fee budget quoted by minter.
        self.ensure_minter_allowance(&source_account, cketh_ledger_id, required_fee_budget)
            .await?;

        let withdraw_args = WithdrawErc20Arg {
            ckerc20_ledger_id,
            recipient: destination.to_string(),
            from_cketh_subaccount: None,
            from_ckerc20_subaccount: None,
            amount: amount_native,
        };

        let first_try = self.withdraw_erc20_call(&withdraw_args).await?;
        let result = match first_try {
            ok @ WithdrawErc20Ret::Ok(_) => ok,
            WithdrawErc20Ret::Err(WithdrawErc20Error::CkEthLedgerError {
                error: LedgerError::InsufficientAllowance { failed_burn_amount, .. },
            }) => {
                // If fee drift exceeded pre-approved allowance, re-approve based on the
                // failed burn amount and retry once.
                let retry_fee_budget = Self::with_fee_headroom(&failed_burn_amount);
                self.ensure_minter_allowance(&source_account, cketh_ledger_id, retry_fee_budget)
                    .await?;
                self.withdraw_erc20_call(&withdraw_args).await?
            }
            err @ WithdrawErc20Ret::Err(_) => err,
        };

        match result {
            // Both block indexes identify the ICP-side withdrawal request.
            WithdrawErc20Ret::Ok(request_id) => Ok(BridgeSubmission {
                bridge_id: format!(
                    "ic-withdraw:{}:{}",
                    request_id.ckerc20_block_index, request_id.cketh_block_index
                ),
            }),
            WithdrawErc20Ret::Err(err) => Err(format!("withdraw_erc20 error: {:?}", err)),
        }
    }

    /// Submits native reverse bridge flow (`ckETH@ICP -> ETH@ETH`).
    ///
    /// Behavior:
    /// - Source must be the configured bridge ICP owner account with `subaccount = None`.
    /// - Destination must be an EVM address.
    /// - Uses the ckETH ledger from route metadata and calls minter `withdraw_eth`.
    ///
    /// Preflight and approval:
    /// - Requires source ckETH balance to cover
    ///   `withdraw_amount + ICRC-1 approve fee + quoted minter tx fee budget`.
    /// - Ensures minter allowance for `withdraw_amount + quoted fee budget` before submission.
    ///
    /// Submission handle:
    /// - Returns bridge id in `ic-withdraw-eth:<block_index>` format for status tracking.
    async fn submit_native_reverse_bridge(
        &self,
        route: &BridgeRouteSpec,
        request: &BridgeRequest,
    ) -> Result<BridgeSubmission, String> {
        let source_account = parse_source_icp_account(&request.source_address)?;
        ensure_source_matches_bridge_owner(&source_account, self.bridge_ic_owner_principal)?;

        // Held for the whole submission: everything below reads or spends the
        // minter's allowance on this account, which no other leg may disturb
        // between the read and the withdraw that consumes it.
        let _reverse_guard = self.reverse_bridge_lock.lock().await;

        let destination = expect_evm_destination(route, &request.destination)?;
        let cketh_ledger_id = parse_ckerc20_ledger_id(route)?;
        let amount_native = amount_to_nat_units_strict(request.amount, ETH_DECIMALS)?;
        self.ensure_native_cketh_withdrawal_minimum(route, &amount_native, request.amount)
            .await?;
        let approve_fee = icrc1_fee_with_context(self.icp_backend.as_ref(), cketh_ledger_id, "cketh bridge").await?;
        let required_fee_budget = match &request.provider_fee_budget_native_units {
            Some(quoted) => quoted.clone(),
            None => self.quoted_cketh_fee_budget(None).await?,
        };
        let required_budget = amount_native.clone() + approve_fee.clone() + required_fee_budget.clone();

        let available_cketh = icrc1_balance_with_context(
            self.icp_backend.as_ref(),
            cketh_ledger_id,
            &source_account,
            "cketh bridge",
        )
        .await?;
        if available_cketh < required_budget {
            let available_formatted = nat_units_to_amount_via_core(&available_cketh, ETH_DECIMALS)?;
            let required_formatted = nat_units_to_amount_via_core(&required_budget, ETH_DECIMALS)?;
            return Err(format!(
                "bridge amount preflight failed: ckETH balance is below required withdraw+approve+fee budget (available={} required={} source={})",
                available_formatted, required_formatted, request.source_address
            ));
        }

        let required_allowance = amount_native.clone() + required_fee_budget.clone();
        self.ensure_minter_allowance(&source_account, cketh_ledger_id, required_allowance)
            .await?;

        let withdraw_args = WithdrawalArg {
            amount: amount_native,
            recipient: destination.to_string(),
            from_subaccount: None,
        };

        match self.withdraw_eth_call(&withdraw_args).await? {
            WithdrawalRet::Ok(request_id) => Ok(BridgeSubmission {
                bridge_id: format!("ic-withdraw-eth:{}", request_id.block_index),
            }),
            WithdrawalRet::Err(err) => Err(format!("withdraw_eth error: {:?}", err)),
        }
    }
}

#[async_trait]
impl<A, B, E> BridgeBackend for CkErc20BridgeBackend<A, B, E>
where
    A: PipelineAgent,
    B: IcpBackend,
    E: BridgeEvmBackend,
{
    async fn get_source_balance(&self, asset: &str, chain: &str, address: &str) -> Result<f64, String> {
        if let Some(route) = resolve_cketh_forward_route_by_source(asset, chain) {
            if route.route_kind == BridgeRouteKind::EthToCkEth {
                let owner = address
                    .parse::<Address>()
                    .map_err(|e| format!("invalid source address '{address}': {e}"))?;
                let balance = self
                    .evm_backend
                    .native_balance_of(owner)
                    .await
                    .map_err(|e| format!("native ETH balance failed for owner {owner}: {e}"))?;
                let gas_reserve = self
                    .evm_backend
                    .native_tx_gas_reserve(ETH_FORWARD_HELPER_GAS_LIMIT)
                    .await?;
                let bridgeable = balance.saturating_sub(gas_reserve);
                return base_units_to_amount_via_core(bridgeable, ETH_DECIMALS);
            }

            let token_address = parse_evm_token_address(route)?;
            let owner = address
                .parse::<Address>()
                .map_err(|e| format!("invalid source address '{address}': {e}"))?;

            let base_units = self
                .evm_backend
                .erc20_balance_of(token_address, owner)
                .await
                .map_err(|e| format!("ERC20 balanceOf failed for token {token_address}: {e}"))?;
            let decimals = self.token_decimals(token_address).await?;
            return base_units_to_amount_via_core(base_units, decimals);
        }

        if let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) {
            let ledger_id = parse_ckerc20_ledger_id(route)?;
            let source_account = parse_source_icp_account(address)?;
            let decimals = icrc1_decimals_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge").await?;
            let balance =
                icrc1_balance_with_context(self.icp_backend.as_ref(), ledger_id, &source_account, "ckerc20 bridge")
                    .await?;
            return nat_units_to_amount_via_core(&balance, decimals);
        }

        Err(format!(
            "unsupported source route {}@{} for ckETH minter bridge backend",
            asset, chain
        ))
    }

    async fn get_source_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) else {
            return Ok(0.0);
        };

        if !route.target_asset.eq_ignore_ascii_case(target_asset) {
            return Ok(0.0);
        }

        match route.route_kind {
            BridgeRouteKind::CkEthErc20Reverse => {
                let ledger_id = parse_ckerc20_ledger_id(route)?;
                let (decimals, approve_fee) = tokio::try_join!(
                    icrc1_decimals_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge"),
                    icrc1_fee_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge")
                )?;
                nat_units_to_amount_via_core(&approve_fee, decimals)
            }
            BridgeRouteKind::CkEthToEth => {
                let ledger_id = parse_ckerc20_ledger_id(route)?;
                let (approve_fee, withdrawal_fee_budget) = tokio::try_join!(
                    icrc1_fee_with_context(self.icp_backend.as_ref(), ledger_id, "cketh bridge"),
                    self.quoted_cketh_fee_budget(None)
                )?;
                let total_budget = approve_fee + withdrawal_fee_budget;
                nat_units_to_amount_via_core(&total_budget, ETH_DECIMALS)
            }
            _ => Ok(0.0),
        }
    }

    async fn get_destination_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) else {
            return Ok(0.0);
        };

        if !route.target_asset.eq_ignore_ascii_case(target_asset) {
            return Ok(0.0);
        }

        if route.route_kind != BridgeRouteKind::CkEthToEth {
            return Ok(0.0);
        }

        let withdrawal_fee_budget = self.quoted_cketh_fee_budget(None).await?;
        nat_units_to_amount_via_core(&withdrawal_fee_budget, ETH_DECIMALS)
    }

    async fn get_fee_budget(&self, asset: &str, chain: &str, target_asset: &str) -> Result<BridgeFeeBudget, String> {
        let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) else {
            return Ok(BridgeFeeBudget::default());
        };

        if !route.target_asset.eq_ignore_ascii_case(target_asset) {
            return Ok(BridgeFeeBudget::default());
        }

        match route.route_kind {
            BridgeRouteKind::CkEthErc20Reverse => {
                let ledger_id = parse_ckerc20_ledger_id(route)?;
                let (decimals, approve_fee, withdrawal_fee_budget) = tokio::try_join!(
                    icrc1_decimals_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge"),
                    icrc1_fee_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge"),
                    self.quoted_cketh_fee_budget(Some(ledger_id))
                )?;
                Ok(BridgeFeeBudget {
                    source_fee_budget: nat_units_to_amount_via_core(&approve_fee, decimals)?,
                    destination_fee_budget: 0.0,
                    provider_fee_budget_native_units: Some(withdrawal_fee_budget),
                })
            }
            BridgeRouteKind::CkEthToEth => {
                let ledger_id = parse_ckerc20_ledger_id(route)?;
                let (approve_fee, withdrawal_fee_budget) = tokio::try_join!(
                    icrc1_fee_with_context(self.icp_backend.as_ref(), ledger_id, "cketh bridge"),
                    self.quoted_cketh_fee_budget(None)
                )?;
                let total_budget = approve_fee + withdrawal_fee_budget.clone();
                Ok(BridgeFeeBudget {
                    source_fee_budget: nat_units_to_amount_via_core(&total_budget, ETH_DECIMALS)?,
                    destination_fee_budget: nat_units_to_amount_via_core(&withdrawal_fee_budget, ETH_DECIMALS)?,
                    provider_fee_budget_native_units: Some(withdrawal_fee_budget),
                })
            }
            _ => Ok(BridgeFeeBudget::default()),
        }
    }

    async fn get_minimum_bridge_amount(&self, asset: &str, chain: &str, target_asset: &str) -> Result<f64, String> {
        let Some(route) = resolve_cketh_reverse_route_by_source(asset, chain) else {
            return Ok(0.0);
        };

        if !route.target_asset.eq_ignore_ascii_case(target_asset) {
            return Ok(0.0);
        }

        if route.route_kind != BridgeRouteKind::CkEthToEth {
            return Ok(0.0);
        }

        let minimum_native = Self::native_cketh_minimum_withdrawal_amount();
        nat_units_to_amount_via_core(&minimum_native, ETH_DECIMALS)
    }

    /// Executes a ckETH minter bridge submission for native ETH/ckETH and ckERC20 routes.
    ///
    /// Returns a [`BridgeSubmission`] with:
    /// - EVM tx hash for forward helper deposits.
    /// - `ic-withdraw:<ckerc20_block>:<cketh_block>` handle for reverse minter withdrawals.
    ///
    /// The method performs route validation and preflight checks before sending any
    /// on-chain/canister state-changing calls.
    async fn submit_bridge(&self, request: BridgeRequest) -> Result<BridgeSubmission, String> {
        // Resolve route metadata once; it validates asset/chain pair and destination kind.
        let route = resolve_cketh_route_for_request(&request)?;
        match route.route_kind {
            BridgeRouteKind::CkEthErc20Forward => self.submit_forward_bridge(route, &request).await,
            BridgeRouteKind::CkEthErc20Reverse => self.submit_reverse_bridge(route, &request).await,
            BridgeRouteKind::EthToCkEth => self.submit_native_forward_bridge(route, &request).await,
            BridgeRouteKind::CkEthToEth => self.submit_native_reverse_bridge(route, &request).await,
            _ => Err(format!(
                "route {}@{} -> {} is not supported by CkErc20BridgeBackend",
                route.source_asset, route.source_chain, route.target_asset
            )),
        }
    }

    async fn find_bridge_credit(
        &self,
        target_asset: &str,
        destination: &BridgeDestination,
        bridge_id: &str,
    ) -> Result<Option<f64>, String> {
        // Only a mint into an ICP account carries a memo naming its deposit. An
        // EVM destination has no equivalent, so it reports nothing rather than
        // guessing.
        let BridgeDestination::IcpAccount(account) = destination else {
            return Ok(None);
        };
        let Some(route) = resolve_cketh_forward_route_by_target(target_asset) else {
            return Ok(None);
        };
        let Some(index_id) = route.ckerc20_index_id else {
            return Ok(None);
        };
        let index = Principal::from_text(index_id)
            .map_err(|e| format!("invalid index canister id '{index_id}' for {target_asset}: {e}"))?;
        let ledger_id = parse_ckerc20_ledger_id(route)?;

        let Some(minted) = find_convert_mint(
            self.agent.as_ref(),
            self.cketh_minter_canister,
            index,
            *account,
            bridge_id,
        )
        .await?
        else {
            return Ok(None);
        };

        let decimals = icrc1_decimals_with_context(self.icp_backend.as_ref(), ledger_id, "ckerc20 bridge").await?;
        nat_units_to_amount_via_core(&minted, decimals).map(Some)
    }

    async fn get_bridge_status(&self, bridge_id: &str) -> Result<BridgeStatus, String> {
        if bridge_id.starts_with("ic-withdraw:") || bridge_id.starts_with("ic-withdraw-eth:") {
            return Ok(BridgeStatus::Completed);
        }

        let tx_hash = bridge_id
            .parse::<TxHash>()
            .map_err(|e| format!("invalid bridge id '{}': expected EVM tx hash: {e}", bridge_id))?;

        let receipt = self
            .evm_backend
            .receipt_status(tx_hash)
            .await
            .map_err(|e| format!("failed to fetch transaction receipt for bridge id '{}': {e}", bridge_id))?;

        let Some(receipt) = receipt else {
            // No receipt is usually just "not mined yet". Only a transaction that
            // is unmined and has lost its nonce to another hash can never mine;
            // everything else keeps waiting rather than inviting a resubmit.
            return Ok(match self.evm_backend.tx_liveness(tx_hash).await? {
                TxLiveness::Replaced {
                    tx_nonce,
                    sender_next_nonce,
                } => BridgeStatus::Failed {
                    reason: Some(superseded_reason(bridge_id, tx_nonce, sender_next_nonce)),
                },
                TxLiveness::Mined | TxLiveness::Pending | TxLiveness::Unknown => BridgeStatus::Pending,
            });
        };

        if receipt.success {
            return Ok(BridgeStatus::Completed);
        }

        Ok(BridgeStatus::Failed {
            reason: Some(format!(
                "bridge transaction {} reverted in block {}",
                bridge_id,
                receipt.block_number.unwrap_or_default()
            )),
        })
    }
}

#[cfg(test)]
#[path = "ckerc20_bridge_tests.rs"]
mod ckerc20_bridge_tests;
