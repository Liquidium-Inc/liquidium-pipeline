use std::sync::Arc;
use std::str::FromStr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use candid::Nat;
use num_traits::ToPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};

use liquidium_pipeline_core::outbound::SendOutcome;
use liquidium_pipeline_core::transfer::actions::TransferFailure;
use serde::{Deserialize, Serialize};

/// How long a plain transfer waits for a decided answer before it reports
/// an open one. A blockhash expires after about 60 seconds and finalizes
/// about 15 later; the finalizer stage shares 300 seconds with other rows.
const PLAIN_TRANSFER_DEADLINE: Duration = Duration::from_secs(75);
const PLAIN_TRANSFER_POLL: Duration = Duration::from_secs(2);

/// A signed transfer, kept so the same transaction can be sent and read again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedSolanaTx {
    pub transaction: Transaction,
    pub signature: String,
    /// The transaction cannot land after the finalized block height passes this.
    pub last_valid_block_height: u64,
}

/// What one read of the cluster says about a signed transfer.
#[derive(Debug, PartialEq, Eq)]
enum Reading {
    Landed,
    FailedOnChain(String),
    Expired,
    Open,
}

/// Decides one read; `status` is `None` for a failed read, `Some(None)` for an unknown signature.
/// Absence proves expiry only when the answering node had seen the finalized slot.
fn read_outcome(
    status: Option<Option<(bool, Option<String>)>>,
    status_slot: Option<u64>,
    finalized: Option<(u64, u64)>,
    last_valid_block_height: u64,
) -> Reading {
    match status {
        Some(Some((true, None))) => Reading::Landed,
        Some(Some((true, Some(error)))) => Reading::FailedOnChain(error),
        Some(None)
            if finalized.is_some_and(|(height, slot)| {
                height > last_valid_block_height && status_slot.is_some_and(|seen| seen >= slot)
            }) =>
        {
            Reading::Expired
        }
        _ => Reading::Open,
    }
}

use solana_system_interface::instruction as system_instruction;
use spl_associated_token_account::{get_associated_token_address, instruction::create_associated_token_account};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SolanaBackend: Send + Sync {
    // Native SOL balance (lamports) for the pipeline wallet.
    async fn native_balance(&self) -> Result<Nat, String>;

    // SPL token balance (base units) for the pipeline wallet.
    async fn spl_balance(&self, mint: &str) -> Result<Nat, String>;

    // SPL token decimals from mint account metadata.
    async fn spl_decimals(&self, mint: &str) -> Result<u8, String>;

    // Native SOL transfer from the pipeline wallet.
    async fn native_transfer(&self, to: &str, amount_lamports: Nat) -> Result<String, TransferFailure>; // signature

    // SPL token transfer from the pipeline wallet.
    // Creates destination ATA if missing.
    async fn spl_transfer(&self, mint: &str, to: &str, amount: Nat) -> Result<String, TransferFailure>; // signature

    /// Builds and signs a transfer without sending it. `mint` is `None` for SOL.
    async fn prepare_transfer(&self, mint: Option<String>, to: &str, amount: Nat) -> Result<SignedSolanaTx, String>;

    /// Sends a signed transfer once, without waiting for confirmation.
    async fn send_signed(&self, tx: &SignedSolanaTx) -> SendOutcome;

    /// Reads the cluster once for a signed transfer.
    async fn signed_outcome(&self, tx: &SignedSolanaTx) -> SendOutcome;
}

pub struct SolanaBackendImpl {
    rpc: Arc<RpcClient>,
    owner: Pubkey,
    keypair_bytes: [u8; 64],
}

impl SolanaBackendImpl {
    pub fn new(rpc: RpcClient, keypair: Keypair) -> Self {
        Self {
            rpc: Arc::new(rpc),
            owner: keypair.pubkey(),
            keypair_bytes: keypair.to_bytes(),
        }
    }

    pub fn from_url(rpc_url: impl Into<String>, keypair: Keypair) -> Self {
        Self::new(RpcClient::new(rpc_url.into()), keypair)
    }

    pub fn from_url_and_secret_key(rpc_url: impl Into<String>, secret_key: [u8; 32]) -> Self {
        let keypair = Keypair::new_from_array(secret_key);
        Self::new(RpcClient::new(rpc_url.into()), keypair)
    }

    pub fn wallet_address(&self) -> String {
        self.owner.to_string()
    }

    fn parse_pubkey(value: &str, label: &str) -> Result<Pubkey, String> {
        value
            .parse::<Pubkey>()
            .map_err(|e| format!("invalid {} pubkey `{}`: {e}", label, value))
    }

    fn nat_to_u64(value: &Nat, label: &str) -> Result<u64, String> {
        value
            .0
            .to_u64()
            .ok_or_else(|| format!("{label} too large for Solana u64 amount"))
    }

    fn u64_to_nat(value: u64) -> Nat {
        Nat::from(value)
    }

    fn is_missing_account_error(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("accountnotfound")
            || lower.contains("could not find account")
            || lower.contains("invalid param: could not find account")
    }

    fn signer(&self) -> Result<Keypair, String> {
        Keypair::try_from(self.keypair_bytes.as_slice())
            .map_err(|e| format!("failed to reconstruct Solana signer from key bytes: {e}"))
    }

    /// Signs at the client's commitment: the send runs its preflight there,
    /// so a blockhash from another bank would be refused.
    async fn sign(&self, instructions: Vec<Instruction>) -> Result<SignedSolanaTx, String> {
        if instructions.is_empty() {
            return Err("cannot send empty Solana transaction".to_string());
        }
        let (recent_blockhash, last_valid_block_height) = self
            .rpc
            .get_latest_blockhash_with_commitment(self.rpc.commitment())
            .await
            .map_err(|e| format!("solana get_latest_blockhash failed: {e}"))?;
        let signer = self.signer()?;
        let transaction = Transaction::new_signed_with_payer(&instructions, Some(&self.owner), &[&signer], recent_blockhash);
        Ok(SignedSolanaTx {
            signature: transaction.signatures[0].to_string(),
            transaction,
            last_valid_block_height,
        })
    }

    async fn native_instructions(&self, to: &str, amount_lamports: &Nat) -> Result<Vec<Instruction>, String> {
        let to_pubkey = Self::parse_pubkey(to, "recipient")?;
        let lamports = Self::nat_to_u64(amount_lamports, "native transfer amount")?;
        Ok(vec![system_instruction::transfer(&self.owner, &to_pubkey, lamports)])
    }

    async fn spl_instructions(&self, mint: &str, to: &str, amount: &Nat) -> Result<Vec<Instruction>, String> {
        let mint_pubkey = Self::parse_pubkey(mint, "mint")?;
        let recipient = Self::parse_pubkey(to, "recipient")?;
        let amount_raw = Self::nat_to_u64(amount, "SPL transfer amount")?;

        let source_ata = get_associated_token_address(&self.owner, &mint_pubkey);
        let destination_ata = get_associated_token_address(&recipient, &mint_pubkey);
        let decimals = self.spl_decimals(mint).await?;

        let mut instructions = Vec::new();
        if !self.account_exists(&destination_ata).await? {
            instructions.push(create_associated_token_account(
                &self.owner,
                &recipient,
                &mint_pubkey,
                &spl_token::id(),
            ));
        }
        instructions.push(
            spl_token::instruction::transfer_checked(
                &spl_token::id(),
                &source_ata,
                &mint_pubkey,
                &destination_ata,
                &self.owner,
                &[],
                amount_raw,
                decimals,
            )
            .map_err(|e| format!("failed to build SPL transfer_checked instruction: {e}"))?,
        );
        Ok(instructions)
    }

    /// A plain transfer for callers without a saved record: send, then read
    /// the cluster until it decides or the deadline passes.
    async fn transfer_and_wait(&self, tx: SignedSolanaTx) -> Result<String, TransferFailure> {
        let deadline = Instant::now() + PLAIN_TRANSFER_DEADLINE;
        let mut outcome = self.send_signed(&tx).await;
        loop {
            match outcome {
                SendOutcome::Landed { receipt } => return Ok(receipt),
                SendOutcome::NeverLanded { reason }
                | SendOutcome::BelowMinimum { reason }
                | SendOutcome::Deferred { reason } => return Err(TransferFailure::Rejected(reason)),
                SendOutcome::Open { reason } if Instant::now() >= deadline => {
                    return Err(TransferFailure::Ambiguous(format!(
                        "solana transaction {} has no decided answer: {reason}",
                        tx.signature
                    )));
                }
                SendOutcome::Open { .. } => {
                    tokio::time::sleep(PLAIN_TRANSFER_POLL).await;
                    outcome = self.signed_outcome(&tx).await;
                }
            }
        }
    }

    async fn account_exists(&self, account: &Pubkey) -> Result<bool, String> {
        match self.rpc.get_account(account).await {
            Ok(_) => Ok(true),
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_account_error(&message) {
                    Ok(false)
                } else {
                    Err(format!(
                        "solana get_account({}) failed while checking ATA existence: {}",
                        account, err
                    ))
                }
            }
        }
    }
}

#[async_trait]
impl SolanaBackend for SolanaBackendImpl {
    async fn native_balance(&self) -> Result<Nat, String> {
        let lamports = self
            .rpc
            .get_balance(&self.owner)
            .await
            .map_err(|e| format!("solana get_balance failed for {}: {e}", self.owner))?;
        Ok(Self::u64_to_nat(lamports))
    }

    async fn spl_balance(&self, mint: &str) -> Result<Nat, String> {
        let mint_pubkey = Self::parse_pubkey(mint, "mint")?;
        let ata = get_associated_token_address(&self.owner, &mint_pubkey);

        match self.rpc.get_token_account_balance(&ata).await {
            Ok(balance) => {
                let raw = balance
                    .amount
                    .parse::<u64>()
                    .map_err(|e| format!("invalid SPL balance amount format for mint {mint}: {e}"))?;
                Ok(Self::u64_to_nat(raw))
            }
            Err(err) => {
                let message = err.to_string();
                if Self::is_missing_account_error(&message) {
                    // Missing ATA => no balance.
                    Ok(Nat::from(0u8))
                } else {
                    Err(format!("solana get_token_account_balance failed for ATA {}: {}", ata, err))
                }
            }
        }
    }

    async fn spl_decimals(&self, mint: &str) -> Result<u8, String> {
        let mint_pubkey = Self::parse_pubkey(mint, "mint")?;
        let supply = self
            .rpc
            .get_token_supply(&mint_pubkey)
            .await
            .map_err(|e| format!("solana get_token_supply failed for mint {}: {}", mint_pubkey, e))?;
        Ok(supply.decimals)
    }

    async fn native_transfer(&self, to: &str, amount_lamports: Nat) -> Result<String, TransferFailure> {
        let tx = self
            .prepare_transfer(None, to, amount_lamports)
            .await
            .map_err(TransferFailure::Rejected)?;
        self.transfer_and_wait(tx).await
    }

    async fn spl_transfer(&self, mint: &str, to: &str, amount: Nat) -> Result<String, TransferFailure> {
        let tx = self
            .prepare_transfer(Some(mint.to_string()), to, amount)
            .await
            .map_err(TransferFailure::Rejected)?;
        self.transfer_and_wait(tx).await
    }

    async fn prepare_transfer(&self, mint: Option<String>, to: &str, amount: Nat) -> Result<SignedSolanaTx, String> {
        let instructions = match mint {
            None => self.native_instructions(to, &amount).await?,
            Some(mint) => self.spl_instructions(&mint, to, &amount).await?,
        };
        self.sign(instructions).await
    }

    async fn send_signed(&self, tx: &SignedSolanaTx) -> SendOutcome {
        match self.rpc.send_transaction(&tx.transaction).await {
            Ok(_) => SendOutcome::Open {
                reason: "sent; waiting for confirmation".to_string(),
            },
            // A refusal, such as "already processed" for a replayed copy, does
            // not prove that an earlier copy never landed. The cluster decides:
            // the status cache, or the blockhash expiry.
            Err(error) => SendOutcome::Open {
                reason: format!("solana send of {} has no answer: {error}", tx.signature),
            },
        }
    }

    async fn signed_outcome(&self, tx: &SignedSolanaTx) -> SendOutcome {
        let Ok(signature) = Signature::from_str(&tx.signature) else {
            return SendOutcome::Open {
                reason: format!("invalid saved Solana signature {}", tx.signature),
            };
        };
        // Finalized first: the status must come from a node at least that fresh.
        let finalized = self
            .rpc
            .get_epoch_info_with_commitment(CommitmentConfig::finalized())
            .await
            .ok()
            .map(|epoch| (epoch.block_height, epoch.absolute_slot));
        let response = self.rpc.get_signature_statuses_with_history(&[signature]).await.ok();
        let status_slot = response.as_ref().map(|response| response.context.slot);
        let status = response.map(|response| {
            response.value.into_iter().next().flatten().map(|status| {
                (
                    status.satisfies_commitment(CommitmentConfig::confirmed()),
                    status.err.map(|error| error.to_string()),
                )
            })
        });
        match read_outcome(status, status_slot, finalized, tx.last_valid_block_height) {
            Reading::Landed => SendOutcome::Landed {
                receipt: tx.signature.clone(),
            },
            Reading::FailedOnChain(error) => SendOutcome::NeverLanded {
                reason: format!("solana transaction {} failed on chain and moved no funds: {error}", tx.signature),
            },
            Reading::Expired => SendOutcome::NeverLanded {
                reason: format!("solana transaction {} expired unconfirmed and moved no funds", tx.signature),
            },
            Reading::Open => SendOutcome::Open {
                reason: format!("solana transaction {} is not confirmed yet", tx.signature),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use candid::Nat;

    use super::SolanaBackendImpl;

    #[test]
    fn nat_to_u64_rejects_oversized_values() {
        let value = Nat::from(u128::MAX);
        let err = SolanaBackendImpl::nat_to_u64(&value, "amount").expect_err("expected overflow");
        assert!(err.contains("too large"));
    }

    #[test]
    fn parse_pubkey_returns_contextual_error() {
        let err = SolanaBackendImpl::parse_pubkey("not-a-pubkey", "recipient").expect_err("expected parse error");
        assert!(err.contains("invalid recipient pubkey"));
    }

    #[test]
    fn missing_account_detector_matches_common_rpc_shapes() {
        assert!(SolanaBackendImpl::is_missing_account_error("AccountNotFound: pubkey"));
        assert!(SolanaBackendImpl::is_missing_account_error(
            "Invalid param: could not find account"
        ));
        assert!(!SolanaBackendImpl::is_missing_account_error("blockhash not found"));
    }
}

#[cfg(test)]
mod reading_tests {
    use super::*;

    #[test]
    fn a_transfer_is_decided_only_by_a_confirmed_status_or_a_fresh_proof_of_expiry() {
        let last_valid = 100;
        let finalized = Some((101, 5_000));
        assert_eq!(read_outcome(Some(Some((true, None))), Some(1), Some((50, 1)), last_valid), Reading::Landed);
        assert_eq!(
            read_outcome(Some(Some((true, Some("InsufficientFunds".into())))), Some(1), None, last_valid),
            Reading::FailedOnChain("InsufficientFunds".into())
        );
        // Seen but not confirmed: wait.
        assert_eq!(read_outcome(Some(Some((false, None))), Some(9_000), finalized, last_valid), Reading::Open);
        // Absent on a node that saw the finalized slot, after expiry: it never landed.
        assert_eq!(read_outcome(Some(None), Some(5_000), finalized, last_valid), Reading::Expired);
        // Absent on a node behind that slot proves nothing.
        assert_eq!(read_outcome(Some(None), Some(4_999), finalized, last_valid), Reading::Open);
        // Absent before expiry, or at the last valid height: it can still land.
        assert_eq!(read_outcome(Some(None), Some(9_000), Some((100, 5_000)), last_valid), Reading::Open);
        // A failed read proves nothing.
        assert_eq!(read_outcome(None, None, finalized, last_valid), Reading::Open);
        assert_eq!(read_outcome(Some(None), Some(9_000), None, last_valid), Reading::Open);
    }
}
