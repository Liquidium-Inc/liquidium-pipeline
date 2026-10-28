use alloy::{
    eips::{BlockId, eip2718::Encodable2718},
    network::{AnyNetwork, Network, NetworkWallet, ReceiptResponse, TransactionBuilder},
    primitives::{Address, Bytes, TxHash, U256, keccak256},
    providers::{Provider, WalletProvider},
    rpc::types::TransactionRequest,
    transports::RpcError,
    sol,
};
use async_trait::async_trait;
use candid::Nat;
use liquidium_pipeline_core::outbound::SendOutcome;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::OnceLock, time::Instant};
use tokio::time::{Duration, sleep};

pub(crate) const ERC20_APPROVE_GAS_LIMIT: u64 = 120_000;
const GAS_BUFFER_BPS: u64 = 2_000; // +20%
const NONCE_RETRY_MAX_ATTEMPTS: u8 = 3;
const NONCE_RETRY_BASE_DELAY_MS: u64 = 200;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmBackend: Send + Sync {
    // ERC20 balance for the pipeline wallet on a given chain.
    async fn erc20_balance(&self, chain: &str, token_address: &str) -> Result<Nat, String>;

    // Native coin balance (ETH, ARB, etc) for the pipeline wallet.
    async fn native_balance(&self, chain: &str) -> Result<Nat, String>;

    // ERC20 transfer from the pipeline wallet.
    async fn erc20_transfer(
        &self,
        chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String>; // tx hash

    // Native transfer from the pipeline wallet.
    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: Nat) -> Result<String, String>;

    // Decimals
    async fn erc20_decimals(&self, chain: &str, token_address: &str) -> Result<u8, String>;
}

fn nat_to_u256(n: &Nat) -> Result<U256, String> {
    let bytes: Vec<u8> = n.0.to_bytes_be();
    if bytes.len() > 32 {
        return Err("Nat too large for U256".into());
    }
    Ok(U256::from_be_slice(&bytes))
}

fn u256_to_nat(v: U256) -> Nat {
    Nat::from(v.to::<u128>())
}

/// Some RPC stacks under-estimate gas, so every estimate gets headroom.
fn with_gas_buffer(estimate: u64) -> u64 {
    estimate.saturating_add(estimate.saturating_mul(GAS_BUFFER_BPS) / 10_000)
}

fn is_nonce_too_low_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("nonce too low")
        || normalized.contains("nonce has already been used")
        || normalized.contains("invalid transaction nonce")
}

/// A signed EVM transaction, saved before it is broadcast. Its hash and
/// nonce exist before it reaches the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedEvmTx {
    pub raw: Bytes,
    pub hash: TxHash,
    pub from: Address,
    pub nonce: u64,
}

/// What one broadcast says about a signed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Broadcast {
    /// A node took it into its mempool.
    Sent,
    /// The node refused this exact transaction, so that node does not hold it.
    Rejected(String),
    /// No decided answer: a transport error, or the network may know it already.
    Unknown(String),
}

/// Node refusals that prove this exact transaction failed validation, so no
/// node took it. Any other error object, gateway errors included, leaves the
/// answer to the chain.
fn is_validation_refusal(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "insufficient funds",
        "intrinsic gas too low",
        "exceeds block gas limit",
        "invalid sender",
        "max fee per gas less than block base fee",
        "fee cap less than block base fee",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

/// What the chain says about a signed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvmReading {
    Landed,
    /// Mined and reverted, and the nonce is final: no funds moved.
    Reverted,
    /// Mined and reverted, but not final yet: a reorg could still change it.
    RevertNotFinal,
    /// No receipt, and the finalized nonce moved past it: it cannot land.
    NonceUsed,
    Pending,
}

/// Decides from the receipt and from the wallet's finalized nonce, which the
/// caller reads before the receipt.
fn read_evm_outcome(receipt_success: Option<bool>, finalized_nonce: u64, tx_nonce: u64) -> EvmReading {
    match receipt_success {
        Some(true) => EvmReading::Landed,
        Some(false) if finalized_nonce > tx_nonce => EvmReading::Reverted,
        Some(false) => EvmReading::RevertNotFinal,
        None if finalized_nonce > tx_nonce => EvmReading::NonceUsed,
        None => EvmReading::Pending,
    }
}

/// How long a nonce of an accepted broadcast outranks the node's pending count.
const LOCAL_NONCE_TRUST_WINDOW: Duration = Duration::from_secs(120);

/// The nonce after the last broadcast a node accepted, for each chain and
/// signer. One key signs on several chains, each with its own count.
type NonceKey = (u64, Address);
type NonceMarks = HashMap<NonceKey, (u64, Instant)>;

fn nonce_marks() -> &'static std::sync::Mutex<NonceMarks> {
    static MARKS: OnceLock<std::sync::Mutex<NonceMarks>> = OnceLock::new();
    MARKS.get_or_init(Default::default)
}

/// The node's pending count, or the mark of a recent accepted broadcast when
/// that is ahead. A load-balanced RPC can miss a send it accepted moments ago.
fn next_nonce(marks: &NonceMarks, signer: NonceKey, node_count: u64, now: Instant) -> u64 {
    marks
        .get(&signer)
        .filter(|(_, marked_at)| now.duration_since(*marked_at) < LOCAL_NONCE_TRUST_WINDOW)
        .map_or(node_count, |(mark, _)| (*mark).max(node_count))
}

/// Records a broadcast a node may hold. A signature that is never sent sets
/// no mark, so it leaves no gap in the sequence.
fn note_broadcast(marks: &mut NonceMarks, signer: NonceKey, nonce: u64, now: Instant) {
    let mark = marks.get(&signer).map_or(nonce + 1, |(mark, _)| (*mark).max(nonce + 1));
    marks.insert(signer, (mark, now));
}

pub struct EvmBackendImpl<P> {
    pub provider: P,
    /// Fixed for a provider, so it is read once.
    chain_id: tokio::sync::OnceCell<u64>,
}

impl<P: Provider<AnyNetwork>> EvmBackendImpl<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            chain_id: tokio::sync::OnceCell::new(),
        }
    }

    async fn chain_id(&self) -> Result<u64, String> {
        self.chain_id
            .get_or_try_init(|| async {
                let chain_id = self.provider.get_chain_id().await;
                chain_id.map_err(|e| format!("chain id read failed: {e}"))
            })
            .await
            .copied()
    }
}

impl<P> EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    /// A pending nonce above the included nonce means a transaction from
    /// this signer is still in the mempool. Submitting another one behind it
    /// is the condition the nonce retry below is built to survive.
    pub(crate) async fn has_pending_nonce_gap(&self, signer: Address) -> Result<bool, String> {
        let latest = self
            .provider
            .get_transaction_count(signer)
            .await
            .map_err(|e| e.to_string())?;
        let pending = self
            .provider
            .get_transaction_count(signer)
            .pending()
            .await
            .map_err(|e| e.to_string())?;
        Ok(pending > latest)
    }

    /// Fills, signs and encodes a request for the wallet's next nonce. It
    /// broadcasts nothing.
    pub(crate) async fn sign_request(
        &self,
        request: <AnyNetwork as Network>::TransactionRequest,
    ) -> Result<SignedEvmTx, String> {
        let from = self.provider.default_signer_address();
        let (chain_id, fees) = tokio::try_join!(self.chain_id(), async {
            let fees = self.provider.estimate_eip1559_fees().await;
            fees.map_err(|e| format!("fee estimate failed: {e}"))
        })?;
        let mut request = request.with_from(from).with_chain_id(chain_id);
        let (estimate, node_count) = tokio::try_join!(
            async {
                let estimate = self.provider.estimate_gas(request.clone()).await;
                estimate.map_err(|e| format!("gas estimate failed: {e}"))
            },
            async {
                let node_count = self.provider.get_transaction_count(from).pending().await;
                node_count.map_err(|e| format!("nonce read failed for {from}: {e}"))
            },
        )?;
        let nonce = next_nonce(
            &nonce_marks().lock().expect("nonce marks lock"),
            (chain_id, from),
            node_count,
            Instant::now(),
        );
        request.set_nonce(nonce);
        request.set_gas_limit(with_gas_buffer(estimate));
        request.set_max_fee_per_gas(fees.max_fee_per_gas);
        request.set_max_priority_fee_per_gas(fees.max_priority_fee_per_gas);
        let envelope = request
            .build(self.provider.wallet())
            .await
            .map_err(|e| format!("sign transaction failed: {e}"))?;
        let raw = Bytes::from(envelope.encoded_2718());
        Ok(SignedEvmTx {
            hash: keccak256(&raw),
            raw,
            from,
            nonce,
        })
    }

    pub(crate) async fn broadcast_signed(&self, tx: &SignedEvmTx) -> Broadcast {
        let broadcast = match self.provider.send_raw_transaction(&tx.raw).await {
            Ok(_) => Broadcast::Sent,
            Err(RpcError::ErrorResp(error)) if is_validation_refusal(&error.message) => {
                Broadcast::Rejected(error.message.to_string())
            }
            Err(error) => Broadcast::Unknown(error.to_string()),
        };
        // A node may hold the transaction, so the next send takes a later nonce.
        if !matches!(broadcast, Broadcast::Rejected(_)) {
            match self.chain_id().await {
                Ok(chain_id) => {
                    let mut marks = nonce_marks().lock().expect("nonce marks lock");
                    note_broadcast(&mut marks, (chain_id, tx.from), tx.nonce, Instant::now());
                }
                Err(error) => log::warn!("nonce {} of {} is not marked: {error}", tx.nonce, tx.from),
            }
        }
        broadcast
    }

    /// Decides a signed transaction from the chain: its receipt first, then
    /// the finalized nonce. A pending transaction is broadcast again.
    pub(crate) async fn signed_outcome(&self, tx: &SignedEvmTx) -> SendOutcome {
        // Read the nonce first, so the receipt read covers every block the
        // nonce covers.
        let finalized_nonce = match self
            .provider
            .get_transaction_count(tx.from)
            .block_id(BlockId::finalized())
            .await
        {
            Ok(nonce) => nonce,
            Err(error) => {
                return SendOutcome::Open {
                    reason: format!("finalized nonce read failed: {error}"),
                };
            }
        };
        let receipt_success = match self.provider.get_transaction_receipt(tx.hash).await {
            Ok(receipt) => receipt.map(|receipt| receipt.status()),
            Err(error) => {
                return SendOutcome::Open {
                    reason: format!("receipt read failed for {:#x}: {error}", tx.hash),
                };
            }
        };
        match read_evm_outcome(receipt_success, finalized_nonce, tx.nonce) {
            EvmReading::Landed => SendOutcome::Landed {
                receipt: format!("{:#x}", tx.hash),
            },
            // A final revert moved no funds; the caller decides whether to send again.
            EvmReading::Reverted => SendOutcome::Reverted {
                reason: format!("transaction {:#x} reverted", tx.hash),
            },
            EvmReading::RevertNotFinal => SendOutcome::Open {
                reason: format!("transaction {:#x} reverted; waiting for finality", tx.hash),
            },
            // The two reads may come from different nodes behind one URL, so
            // a missing receipt is no proof. An operator decides after the window.
            EvmReading::NonceUsed => SendOutcome::Open {
                reason: format!(
                    "nonce {} of {} is final but no receipt for {:#x} was found",
                    tx.nonce, tx.from, tx.hash
                ),
            },
            EvmReading::Pending => {
                // A node may have dropped it, and the same bytes cannot land
                // twice. A node that still holds it needs no new copy.
                if matches!(self.provider.get_transaction_by_hash(tx.hash).await, Ok(None)) {
                    let _ = self.provider.send_raw_transaction(&tx.raw).await;
                }
                SendOutcome::Open {
                    reason: format!("transaction {:#x} is not mined yet", tx.hash),
                }
            }
        }
    }

    pub(crate) async fn erc20_balance_of_raw(&self, token: Address, owner: Address) -> Result<U256, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .balanceOf(owner)
            .call()
            .await
            .map_err(|e| format!("ERC20 balanceOf failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_allowance_raw(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .allowance(owner, spender)
            .call()
            .await
            .map_err(|e| format!("ERC20 allowance(owner={owner}, spender={spender}) failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_decimals_raw(&self, token: Address) -> Result<u8, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .decimals()
            .call()
            .await
            .map_err(|e| format!("ERC20 decimals() failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_approve_and_wait_raw(
        &self,
        token: Address,
        spender: Address,
        amount: U256,
    ) -> Result<TxHash, String> {
        let contract = IERC20::new(token, self.provider.clone());
        let estimated_gas = contract.approve(spender, amount).estimate_gas().await.ok();
        let gas_limit = estimated_gas
            .map(|estimate| with_gas_buffer(estimate).max(ERC20_APPROVE_GAS_LIMIT))
            .unwrap_or(ERC20_APPROVE_GAS_LIMIT);

        let mut attempt: u8 = 0;
        let pending = loop {
            attempt = attempt.saturating_add(1);
            let send_result = contract
                .approve(spender, amount)
                // Some RPC/provider stacks under-estimate ERC20 approve gas, causing
                // intermittent out-of-gas failures. Use estimated gas with headroom.
                .gas(gas_limit)
                .send()
                .await;

            match send_result {
                Ok(pending) => break pending,
                Err(err) => {
                    let err_message = err.to_string();
                    if attempt < NONCE_RETRY_MAX_ATTEMPTS && is_nonce_too_low_error(&err_message) {
                        let backoff_ms = NONCE_RETRY_BASE_DELAY_MS.saturating_mul(attempt as u64);
                        sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(format!(
                        "ERC20 approve(spender={spender}) failed for token {token}: {err}"
                    ));
                }
            }
        };
        let tx_hash = *pending.tx_hash();

        let receipt = pending
            .get_receipt()
            .await
            .map_err(|e| format!("ERC20 approve confirmation failed for token {token}: {e}"))?;

        // Inclusion alone is not approval: a reverted/out-of-gas transaction
        // consumes a nonce but changes no allowance. Never submit a helper call
        // against an approval whose receipt failed.
        if !receipt.status() {
            return Err(format!("ERC20 approval {tx_hash:#x} reverted for token {token}"));
        }

        Ok(tx_hash)
    }
}

#[async_trait]
impl<P> EvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    async fn erc20_balance(&self, _chain: &str, token_address: &str) -> Result<Nat, String> {
        let owner = self.provider.wallet().default_signer_address();

        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;

        let res = self.erc20_balance_of_raw(token_addr, owner).await?;

        Ok(u256_to_nat(res))
    }

    async fn native_balance(&self, _chain: &str) -> Result<Nat, String> {
        let bal: U256 = self
            .provider
            .get_balance(self.wallet_address())
            .await
            .map_err(|e| format!("native get_balance failed: {e}"))?;

        Ok(u256_to_nat(bal))
    }

    async fn erc20_transfer(
        &self,
        _chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String> {
        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;
        let to_addr: Address = to.parse().map_err(|e| format!("invalid recipient address: {e}"))?;

        let amount = nat_to_u256(&amount_wei)?;

        let contract = IERC20::new(token_addr, self.provider.clone());

        let tx = contract
            .transfer(to_addr, amount)
            .send()
            .await
            .map_err(|e| format!("erc20 transfer send failed: {e}"))?;

        let tx_hash = format!("{:#x}", tx.tx_hash());
        Ok(tx_hash)
    }

    async fn native_transfer(&self, _chain: &str, to: &str, amount_wei: Nat) -> Result<String, String> {
        let to_addr: Address = to.parse().map_err(|e| format!("invalid recipient address: {e}"))?;
        let amount = nat_to_u256(&amount_wei)?;

        let tx = TransactionRequest::default().with_to(to_addr).with_value(amount);

        let pending = self
            .provider
            .send_transaction(tx.into())
            .await
            .map_err(|e| format!("native transfer send failed: {e}"))?;

        let tx_hash = pending
            .watch()
            .await
            .map_err(|e| format!("native transfer watch failed: {e}"))?;

        Ok(format!("{:#x}", tx_hash))
    }

    async fn erc20_decimals(&self, _chain: &str, token_address: &str) -> Result<u8, String> {
        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;

        self.erc20_decimals_raw(token_addr)
            .await
            .map_err(|e| format!("erc20 decimals() failed: {e}"))
    }
}

impl<P> EvmBackendImpl<P>
where
    P: WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    fn wallet_address(&self) -> Address {
        self.provider.default_signer_address()
    }
}

#[cfg(test)]
mod tests {
    use super::is_nonce_too_low_error;
    use super::{Address, Duration, Instant, LOCAL_NONCE_TRUST_WINDOW, NonceMarks, next_nonce, note_broadcast};

    #[test]
    fn a_stale_node_count_never_reissues_a_broadcast_nonce() {
        let mut marks = NonceMarks::default();
        let (signer, now) = ((1, Address::repeat_byte(1)), Instant::now());
        note_broadcast(&mut marks, signer, 70, now);
        // The node has not seen the send that took 70 yet.
        assert_eq!(next_nonce(&marks, signer, 70, now), 71);
    }

    #[test]
    fn a_signature_that_is_never_sent_leaves_no_gap() {
        let marks = NonceMarks::default();
        let (signer, now) = ((1, Address::repeat_byte(2)), Instant::now());
        assert_eq!(next_nonce(&marks, signer, 70, now), 70);
        assert_eq!(next_nonce(&marks, signer, 70, now), 70);
    }

    #[test]
    fn a_node_that_leads_the_mark_wins() {
        let mut marks = NonceMarks::default();
        let (signer, now) = ((1, Address::repeat_byte(3)), Instant::now());
        note_broadcast(&mut marks, signer, 10, now);
        assert_eq!(next_nonce(&marks, signer, 40, now), 40);
    }

    #[test]
    fn each_chain_and_signer_has_its_own_mark() {
        let mut marks = NonceMarks::default();
        let now = Instant::now();
        note_broadcast(&mut marks, (1, Address::repeat_byte(4)), 5, now);
        assert_eq!(next_nonce(&marks, (1, Address::repeat_byte(4)), 5, now), 6);
        assert_eq!(next_nonce(&marks, (1, Address::repeat_byte(5)), 5, now), 5);
        assert_eq!(next_nonce(&marks, (2, Address::repeat_byte(4)), 5, now), 5);
    }

    #[test]
    fn past_the_window_the_node_decides_again() {
        let mut marks = NonceMarks::default();
        let (signer, start) = ((1, Address::repeat_byte(6)), Instant::now());
        note_broadcast(&mut marks, signer, 70, start);
        let later = start + LOCAL_NONCE_TRUST_WINDOW + Duration::from_secs(1);
        assert_eq!(next_nonce(&marks, signer, 70, later), 70);
    }

    #[test]
    fn nonce_too_low_detector_matches_common_error_shapes() {
        assert!(is_nonce_too_low_error("error code -32000: Nonce too low"));
        assert!(is_nonce_too_low_error(
            "transaction rejected: nonce has already been used"
        ));
        assert!(is_nonce_too_low_error("invalid transaction nonce"));
        assert!(!is_nonce_too_low_error("insufficient funds for gas * price + value"));
    }

    #[test]
    fn the_chain_decides_a_signed_transaction() {
        use super::{EvmReading, read_evm_outcome};
        assert_eq!(read_evm_outcome(Some(true), 0, 5), EvmReading::Landed);
        assert_eq!(read_evm_outcome(Some(false), 9, 5), EvmReading::Reverted);
        // A revert in a block that can still reorg is not acted on yet.
        assert_eq!(read_evm_outcome(Some(false), 5, 5), EvmReading::RevertNotFinal);
        // Another transaction took the nonce, and it is final.
        assert_eq!(read_evm_outcome(None, 6, 5), EvmReading::NonceUsed);
        // Still pending, or mined but not final yet.
        assert_eq!(read_evm_outcome(None, 5, 5), EvmReading::Pending);
        assert_eq!(read_evm_outcome(None, 0, 5), EvmReading::Pending);
    }

    #[test]
    fn only_validation_refusals_count_as_never_sent() {
        use super::is_validation_refusal;
        for message in ["insufficient funds for gas * price + value", "intrinsic gas too low"] {
            assert!(is_validation_refusal(message), "{message}");
        }
        for message in [
            "already known",
            "nonce too low: next nonce 8, tx nonce 7",
            "replacement transaction underpriced",
            "transaction underpriced",
            "Internal error",
            "limit exceeded",
        ] {
            assert!(!is_validation_refusal(message), "{message}");
        }
    }
}
