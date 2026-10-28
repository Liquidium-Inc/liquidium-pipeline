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
    /// Mined, but the call reverted: the nonce is used and no funds moved.
    Reverted,
    /// No receipt, and the finalized nonce moved past it: it cannot land.
    NonceUsed,
    Pending,
}

/// Decides from the receipt and from the wallet's finalized nonce, which the
/// caller reads before the receipt.
fn read_evm_outcome(receipt_success: Option<bool>, finalized_nonce: u64, tx_nonce: u64) -> EvmReading {
    match receipt_success {
        Some(true) => EvmReading::Landed,
        Some(false) => EvmReading::Reverted,
        None if finalized_nonce > tx_nonce => EvmReading::NonceUsed,
        None => EvmReading::Pending,
    }
}

pub struct EvmBackendImpl<P> {
    pub provider: P,
}

impl<P: Provider<AnyNetwork>> EvmBackendImpl<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
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
        let (nonce, chain_id, fees) = tokio::try_join!(
            async {
                let nonce = self.provider.get_transaction_count(from).pending().await;
                nonce.map_err(|e| format!("nonce read failed for {from}: {e}"))
            },
            async {
                let chain_id = self.provider.get_chain_id().await;
                chain_id.map_err(|e| format!("chain id read failed: {e}"))
            },
            async {
                let fees = self.provider.estimate_eip1559_fees().await;
                fees.map_err(|e| format!("fee estimate failed: {e}"))
            },
        )?;
        let mut request = request.with_from(from).with_nonce(nonce).with_chain_id(chain_id);
        let estimate = self
            .provider
            .estimate_gas(request.clone())
            .await
            .map_err(|e| format!("gas estimate failed: {e}"))?;
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
        match self.provider.send_raw_transaction(&tx.raw).await {
            Ok(_) => Broadcast::Sent,
            Err(RpcError::ErrorResp(error)) if is_validation_refusal(&error.message) => {
                Broadcast::Rejected(error.message.to_string())
            }
            Err(error) => Broadcast::Unknown(error.to_string()),
        }
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
            // A revert moved no funds, but a new send would most likely revert
            // too, so an operator decides after the window.
            EvmReading::Reverted => SendOutcome::Open {
                reason: format!("transaction {:#x} reverted", tx.hash),
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
