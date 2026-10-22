use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use num_traits::ToPrimitive;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
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

        let recent_blockhash = self
            .rpc
            .get_latest_blockhash()
            .await
            .map_err(|e| format!("solana get_latest_blockhash failed: {e}"))?;

        let signer = self.signer()?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.owner),
            &[&signer],
            recent_blockhash,
        );

        let signature = self
            .rpc
            .send_and_confirm_transaction(&tx)
            .await
            .map_err(|e| format!("solana send_and_confirm_transaction failed: {e}"))?;

        Ok(signature.to_string())
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
