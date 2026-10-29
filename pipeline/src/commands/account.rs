use icrc_ledger_types::icrc1::account::Account;

use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{env, io::Write, path::PathBuf};

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use bip39::{Language, Mnemonic};
use candid::Principal;
use ic_ledger_types::{AccountIdentifier, Subaccount};
use solana_sdk::{signature::Keypair, signer::Signer};

use prettytable::{Cell, Row, Table, format};
use tracing::info;

use crate::config::{Config, ConfigTrait};
use crate::output::plain_logs_enabled;

/// One address the pipeline signs for or receives on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressEntry {
    pub role: &'static str,
    pub chain: &'static str,
    pub address: String,
}

/// Every account of the three identities: liquidator, trader and bridge.
/// Built from the config once, so the table and the log lines agree.
pub struct AddressBook {
    pub liquidator_principal: Principal,
    pub trader_principal: Principal,
    /// The account the daemon's recovery sweep pays into.
    pub recovery_account: Account,
    pub trader_evm: Result<String, String>,
    pub trader_solana: String,
    pub bridge_principal: Principal,
    pub bridge_evm: String,
    pub bridge_btc: String,
    pub bridge_solana: String,
}

/// The ICP ledger account id of a principal's default subaccount, which is
/// the form exchanges and the native ICP ledger use.
fn icp_account_id(owner: Principal) -> String {
    AccountIdentifier::new(&owner, &Subaccount([0; 32])).to_hex()
}

impl AddressBook {
    pub fn from_config(config: &Config) -> Self {
        let trader_evm = config
            .evm_private_key
            .parse::<PrivateKeySigner>()
            .map(|signer| signer.address().to_string())
            .map_err(|_| "invalid EVM private key in config".to_string());
        Self {
            liquidator_principal: config.liquidator_principal,
            trader_principal: config.trader_principal,
            recovery_account: config.get_recovery_account(),
            trader_evm,
            trader_solana: Keypair::new_from_array(config.solana_private_key_bytes)
                .pubkey()
                .to_string(),
            bridge_principal: config.bridge_ic_owner_principal,
            bridge_evm: config.bridge_evm_address.clone(),
            bridge_btc: config.bridge_btc_address.clone(),
            bridge_solana: Keypair::new_from_array(config.bridge_solana_private_key_bytes)
                .pubkey()
                .to_string(),
        }
    }

    pub fn entries(&self) -> Vec<AddressEntry> {
        let entry = |role, chain, address: String| AddressEntry { role, chain, address };
        let trader_evm = match &self.trader_evm {
            Ok(address) => address.clone(),
            Err(reason) => format!("({reason})"),
        };
        vec![
            entry("Liquidator", "ICP principal", self.liquidator_principal.to_string()),
            entry("Liquidator", "ICP account id", icp_account_id(self.liquidator_principal)),
            entry("Trader", "ICP principal", self.trader_principal.to_string()),
            entry("Trader", "ICP account id", icp_account_id(self.trader_principal)),
            entry("Trader recovery", "ICP account", self.recovery_account.to_string()),
            entry("Trader", "EVM", trader_evm),
            entry("Trader", "Solana", self.trader_solana.clone()),
            entry("Bridge", "ICP principal", self.bridge_principal.to_string()),
            entry("Bridge", "ICP account id", icp_account_id(self.bridge_principal)),
            entry("Bridge", "EVM", self.bridge_evm.clone()),
            entry("Bridge", "Bitcoin", self.bridge_btc.clone()),
            entry("Bridge", "Solana", self.bridge_solana.clone()),
        ]
    }

    /// The public addresses the simulator workbench funds, by the keys its scripts read.
    pub fn simulator_identity(&self) -> serde_json::Value {
        serde_json::json!({
            "liquidator": self.liquidator_principal.to_text(),
            "trader": self.trader_principal.to_text(),
            "bridge": self.bridge_principal.to_text(),
            "bridgeEvm": self.bridge_evm,
            "solana": self.trader_solana,
            "bridgeSolana": self.bridge_solana,
        })
    }
}

pub async fn show() -> Result<()> {
    let config = Config::load()
        .await
        .map_err(|error| anyhow!("could not load account: {error}"))?;
    let book = AddressBook::from_config(&config);
    if crate::simulator::ENABLED {
        let path = env::var("SIM_IDENTITY_FILE").context("SIM_IDENTITY_FILE")?;
        fs::write(path, serde_json::to_vec_pretty(&book.simulator_identity())?)?;
        return Ok(());
    }
    let entries = book.entries();

    if plain_logs_enabled() {
        for entry in &entries {
            info!(role = entry.role, chain = entry.chain, address = %entry.address, "Account identity");
        }
        return Ok(());
    }

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![Cell::new("Role"), Cell::new("Chain"), Cell::new("Address")]));
    for entry in &entries {
        table.add_row(Row::new(vec![
            Cell::new(entry.role),
            Cell::new(entry.chain),
            Cell::new(&entry.address),
        ]));
    }
    table.printstd();
    Ok(())
}

fn default_mnemonic_path() -> Option<PathBuf> {
    let home = env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".liquidium-pipeline/wallets/mnemonic.txt"))
}

fn expand_tilde(p: &str) -> PathBuf {
    if let Some(stripped) = p.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    PathBuf::from(p)
}

pub async fn new() -> Result<()> {
    let mnemonic_path: PathBuf = match env::var("MNEMONIC_FILE") {
        Ok(p) => expand_tilde(&p),
        Err(_) => default_mnemonic_path()
            .ok_or_else(|| anyhow!("HOME environment variable not set and MNEMONIC_FILE not specified"))?,
    };

    // Declining to overwrite an existing wallet is the guard working, not a
    // failure, so it stays a success exit.
    if mnemonic_path.exists() {
        println!(
            "Mnemonic file already exists at {}. Not overwriting.",
            mnemonic_path.display()
        );
        println!("If you want to regenerate, delete the file manually and run this command again.");
        return Ok(());
    }

    let mnemonic = Mnemonic::generate_in(Language::English, 24).context("failed to generate mnemonic")?;
    let phrase = mnemonic.to_string();

    if let Some(parent) = mnemonic_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    #[cfg(unix)]
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&mnemonic_path)
        .with_context(|| format!("failed to create {}", mnemonic_path.display()))?;

    #[cfg(not(unix))]
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&mnemonic_path)
        .with_context(|| format!("failed to create {}", mnemonic_path.display()))?;

    if let Err(e) = writeln!(file, "{phrase}") {
        let _ = fs::remove_file(&mnemonic_path);
        return Err(anyhow::Error::new(e).context(format!("failed to write {}", mnemonic_path.display())));
    }

    println!(
        "✅ New 24-word mnemonic generated and saved to {}",
        mnemonic_path.display()
    );
    println!("   IMPORTANT: Back this up securely. Anyone with this phrase controls all derived wallets.");
    println!(
        "   Tip: set MNEMONIC_FILE={} in your environment, or configure your pipeline to read it.",
        mnemonic_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use liquidium_pipeline_connectors::account::icp_account::RECOVERY_ACCOUNT;

    use super::*;

    #[test]
    fn the_address_book_lists_every_identity_on_every_chain() {
        let book = AddressBook {
            liquidator_principal: Principal::management_canister(),
            trader_principal: Principal::anonymous(),
            recovery_account: Account {
                owner: Principal::anonymous(),
                subaccount: Some(*RECOVERY_ACCOUNT),
            },
            trader_evm: Err("invalid EVM private key in config".into()),
            trader_solana: "So11111111111111111111111111111111111111112".into(),
            bridge_principal: Principal::from_text("xeefw-xlb5n-2hfz2-35dam-x6lxm-v2fsu-faxj6-4tbgi-mpwir-pqa4s-mae")
                .unwrap(),
            bridge_evm: "0x2222222222222222222222222222222222222222".into(),
            bridge_btc: "bc1qbridge".into(),
            bridge_solana: "BridgeSol".into(),
        };

        let entries = book.entries();

        let roles: Vec<&str> = entries.iter().map(|e| e.role).collect();
        assert_eq!(roles.iter().filter(|r| **r == "Trader").count(), 4);
        assert_eq!(roles.iter().filter(|r| **r == "Bridge").count(), 5);
        // The sweep pays into the trader's subaccount, not the liquidator's.
        let recovery = entries.iter().find(|e| e.role == "Trader recovery").unwrap();
        assert_eq!(recovery.address, book.recovery_account.to_string());
        assert!(recovery.address.starts_with(&book.trader_principal.to_text()));
        assert!(!entries.iter().any(|e| e.role == "Liquidator recovery"));
        let trader_evm = entries.iter().find(|e| e.role == "Trader" && e.chain == "EVM").unwrap();
        assert_eq!(trader_evm.address, "(invalid EVM private key in config)");
        let bridge_icp_id = entries
            .iter()
            .find(|e| e.role == "Bridge" && e.chain == "ICP account id")
            .unwrap();
        assert_eq!(bridge_icp_id.address.len(), 64);
        assert_eq!(bridge_icp_id.address, icp_account_id(book.bridge_principal));
    }
}
