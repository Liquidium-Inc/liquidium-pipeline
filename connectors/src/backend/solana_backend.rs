use std::sync::Arc;
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

use crate::backend::SIDE_EFFECT_AMBIGUOUS_PREFIX;

/// How long a send without confirmation is checked before it stays open.
/// A blockhash expires after about 60 seconds and finalizes about 15 later;
/// the finalizer stage shares its 300-second budget with other rows.
const UNCONFIRMED_SEND_DEADLINE: Duration = Duration::from_secs(75);
const UNCONFIRMED_SEND_POLL: Duration = Duration::from_secs(2);

/// What a send without confirmation turned out to be.
#[derive(Debug, PartialEq, Eq)]
enum SendOutcome {
    Landed,
    FailedOnChain(String),
    Expired,
    Open,
}

/// Decides one status read. `status` is `None` when the read failed, and
/// `Some(None)` when the cluster does not know the signature. A signature that
/// is absent after its blockhash expired can no longer land.
fn decide_send_outcome(
    status: Option<Option<(bool, Option<String>)>>,
    finalized_block_height: Option<u64>,
    last_valid_block_height: u64,
) -> SendOutcome {
    match status {
        Some(Some((true, None))) => SendOutcome::Landed,
        Some(Some((true, Some(error)))) => SendOutcome::FailedOnChain(error),
        Some(None) if finalized_block_height.is_some_and(|height| height > last_valid_block_height) => {
            SendOutcome::Expired
        }
        _ => SendOutcome::Open,
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
    async fn native_transfer(&self, to: &str, amount_lamports: Nat) -> Result<String, String>; // signature

    // SPL token transfer from the pipeline wallet.
    // Creates destination ATA if missing.
    async fn spl_transfer(&self, mint: &str, to: &str, amount: Nat) -> Result<String, String>; // signature
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

    async fn send_instructions(&self, instructions: Vec<Instruction>) -> Result<String, String> {
        if instructions.is_empty() {
            return Err("cannot send empty Solana transaction".to_string());
        }

        // The send runs its preflight at the client's commitment, so the
        // blockhash must come from the same bank or the preflight rejects it.
        let (recent_blockhash, last_valid_block_height) = self
            .rpc
            .get_latest_blockhash_with_commitment(self.rpc.commitment())
            .await
            .map_err(|e| format!("solana get_latest_blockhash failed: {e}"))?;

        let signer = self.signer()?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.owner),
            &[&signer],
            recent_blockhash,
        );
        let signature = tx.signatures[0];

        match self.rpc.send_and_confirm_transaction(&tx).await {
            Ok(signature) => Ok(signature.to_string()),
            Err(error) => {
                self.settle_unconfirmed_send(signature, last_valid_block_height, error.to_string())
                    .await
            }
        }
    }

    /// A failed confirmation does not prove the transaction stayed local. The
    /// signature is known, so the cluster can say whether it landed; after the
    /// blockhash expires, an absent signature proves that it never will.
    async fn settle_unconfirmed_send(
        &self,
        signature: Signature,
        last_valid_block_height: u64,
        send_error: String,
    ) -> Result<String, String> {
        let deadline = Instant::now() + UNCONFIRMED_SEND_DEADLINE;
        loop {
            let status = self
                .rpc
                .get_signature_statuses_with_history(&[signature])
                .await
                .ok()
                .map(|response| {
                    response.value.into_iter().next().flatten().map(|status| {
                        (
                            status.satisfies_commitment(CommitmentConfig::confirmed()),
                            status.err.map(|error| error.to_string()),
                        )
                    })
                });
            let finalized_block_height = self
                .rpc
                .get_block_height_with_commitment(CommitmentConfig::finalized())
                .await
                .ok();
            match decide_send_outcome(status, finalized_block_height, last_valid_block_height) {
                SendOutcome::Landed => return Ok(signature.to_string()),
                SendOutcome::FailedOnChain(error) => {
                    return Err(format!("solana transaction {signature} failed on chain and moved no funds: {error}"));
                }
                SendOutcome::Expired => {
                    return Err(format!(
                        "solana transaction {signature} expired unconfirmed and moved no funds: {send_error}"
                    ));
                }
                SendOutcome::Open if Instant::now() >= deadline => {
                    return Err(format!(
                        "{SIDE_EFFECT_AMBIGUOUS_PREFIX}: solana transaction {signature} has no decided answer: {send_error}"
                    ));
                }
                SendOutcome::Open => tokio::time::sleep(UNCONFIRMED_SEND_POLL).await,
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

    async fn native_transfer(&self, to: &str, amount_lamports: Nat) -> Result<String, String> {
        let to_pubkey = Self::parse_pubkey(to, "recipient")?;
        let lamports = Self::nat_to_u64(&amount_lamports, "native transfer amount")?;

        let ix = system_instruction::transfer(&self.owner, &to_pubkey, lamports);
        self.send_instructions(vec![ix]).await
    }

    async fn spl_transfer(&self, mint: &str, to: &str, amount: Nat) -> Result<String, String> {
        let mint_pubkey = Self::parse_pubkey(mint, "mint")?;
        let recipient = Self::parse_pubkey(to, "recipient")?;
        let amount_raw = Self::nat_to_u64(&amount, "SPL transfer amount")?;

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

        let transfer_ix = spl_token::instruction::transfer_checked(
            &spl_token::id(),
            &source_ata,
            &mint_pubkey,
            &destination_ata,
            &self.owner,
            &[],
            amount_raw,
            decimals,
        )
        .map_err(|e| format!("failed to build SPL transfer_checked instruction: {e}"))?;
        instructions.push(transfer_ix);

        self.send_instructions(instructions).await
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
mod send_outcome_tests {
    use super::*;

    #[test]
    fn a_send_is_settled_only_by_a_confirmed_status_or_a_proven_expiry() {
        let last_valid = 100;
        // The cluster confirmed it: it landed, or it failed and moved nothing.
        assert_eq!(decide_send_outcome(Some(Some((true, None))), Some(50), last_valid), SendOutcome::Landed);
        assert_eq!(
            decide_send_outcome(Some(Some((true, Some("InsufficientFunds".into())))), Some(50), last_valid),
            SendOutcome::FailedOnChain("InsufficientFunds".into())
        );
        // Seen but not confirmed yet: wait.
        assert_eq!(decide_send_outcome(Some(Some((false, None))), Some(150), last_valid), SendOutcome::Open);
        // Absent after the finalized height passed the blockhash: it never landed.
        assert_eq!(decide_send_outcome(Some(None), Some(101), last_valid), SendOutcome::Expired);
        // Absent before expiry, or at the last valid height: it still can land.
        assert_eq!(decide_send_outcome(Some(None), Some(100), last_valid), SendOutcome::Open);
        assert_eq!(decide_send_outcome(Some(None), Some(80), last_valid), SendOutcome::Open);
        // A failed read proves nothing, even after expiry.
        assert_eq!(decide_send_outcome(None, Some(500), last_valid), SendOutcome::Open);
        assert_eq!(decide_send_outcome(Some(None), None, last_valid), SendOutcome::Open);
    }
}
