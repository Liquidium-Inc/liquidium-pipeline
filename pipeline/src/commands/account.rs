use icrc_ledger_types::icrc1::account::Account;

use liquidium_pipeline_connectors::account::icp_account::RECOVERY_ACCOUNT;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{env, io::Write, path::PathBuf};

use alloy::signers::local::PrivateKeySigner;
use bip39::{Language, Mnemonic};

use prettytable::{Cell, Row, Table, format};

use liquidium_pipeline_commons::error::format_with_code;

use crate::config::Config;

pub async fn show() {
    // Load Config
    let config = Config::load();

    match config.await {
        Ok(config) => {
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
            table.set_titles(Row::new(vec![Cell::new("Role"), Cell::new("Principal")]));

            table.add_row(Row::new(vec![
                Cell::new("Liquidator Account"),
                Cell::new(&config.liquidator_principal.to_string()),
            ]));

            let recovery = Account {
                owner: config.liquidator_principal,
                subaccount: Some(*RECOVERY_ACCOUNT),
            };
            table.add_row(Row::new(vec![
                Cell::new("Recovery Account"),
                Cell::new(&recovery.to_string()),
            ]));

            match config.evm_private_key.parse::<PrivateKeySigner>() {
                Ok(evm_signer) => {
                    let evm_address = evm_signer.address();
                    table.add_row(Row::new(vec![
                        Cell::new("EVM Account"),
                        Cell::new(&format!("{evm_address}")),
                    ]));
                }
                Err(_) => {
                    table.add_row(Row::new(vec![
                        Cell::new("EVM Account"),
                        Cell::new("(invalid EVM private key in config)"),
                    ]));
                }
            }

            table.printstd();
        }
        Err(err) => {
            eprintln!("Could not load account: {}", format_with_code(&err));
        }
    }
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

pub async fn new() {
    let mnemonic_path: PathBuf = match env::var("MNEMONIC_FILE") {
        Ok(p) => expand_tilde(&p),
        Err(_) => match default_mnemonic_path() {
            Some(p) => p,
            None => {
                eprintln!("HOME environment variable not set and MNEMONIC_FILE not specified");
                return;
            }
        },
    };

    if mnemonic_path.exists() {
        println!(
            "Mnemonic file already exists at {}. Not overwriting.",
            mnemonic_path.display()
        );
        println!("If you want to regenerate, delete the file manually and run this command again.");
        return;
    }

    let mnemonic = match Mnemonic::generate_in(Language::English, 24) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Failed to generate mnemonic: {e}");
            return;
        }
    };
    let phrase = mnemonic.to_string();

    if let Some(parent) = mnemonic_path.parent()
        && let Err(e) = fs::create_dir_all(parent)
    {
        eprintln!("Failed to create directory {}: {e}", parent.display());
        return;
    }

    #[cfg(unix)]
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&mnemonic_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to create {}: {e}", mnemonic_path.display());
            return;
        }
    };

    #[cfg(not(unix))]
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&mnemonic_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to create {}: {e}", mnemonic_path.display());
            return;
        }
    };

    if let Err(e) = writeln!(file, "{phrase}") {
        let _ = fs::remove_file(&mnemonic_path);
        eprintln!("Failed to write {}: {e}", mnemonic_path.display());
        return;
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
}
