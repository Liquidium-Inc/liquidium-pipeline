use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline::config::Config;
use liquidium_pipeline_connectors::account::icp_account::RECOVERY_ACCOUNT;
use openssl::pkey::PKey;
use std::{fs, path::Path};

use prettytable::{Cell, Row, Table, format};

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
                owner: config.trader_principal,
                subaccount: Some(*RECOVERY_ACCOUNT),
            };
            table.add_row(Row::new(vec![
                Cell::new("Recovery Account"),
                Cell::new(&recovery.to_string()),
            ]));

            table.printstd();
        }
        Err(_) => {
            println!("Could not load account.");
        }
    }
}

use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{env, io::Write, path::PathBuf};

fn default_identity_path() -> PathBuf {
    // $HOME/.config/liquidator/id.pem
    let home = env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".liquidium-pipeline/wallets/id.pem")
}

fn default_trader_identity_path() -> PathBuf {
    // $HOME/.config/liquidator/trader.pem
    let home = env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".liquidium-pipeline/wallets/trader.pem")
}

fn write_new_ed25519_key(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory {}: {e}", parent.display()))?;
    }

    if Path::new(path).exists() {
        println!("ℹ️ Identity already exists at {}. Skipping creation.", path.display());
        return Ok(());
    }

    let key = PKey::generate_ed25519().map_err(|e| format!("Failed to generate key: {e}"))?;
    let private_pem = key
        .private_key_to_pem_pkcs8()
        .map_err(|e| format!("Failed to encode key: {e}"))?;

    #[cfg(unix)]
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("Failed to create {}: {e}", path.display()))?;

    #[cfg(not(unix))]
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("Failed to create {}: {e}", path.display()))?;

    if let Err(e) = file.write_all(&private_pem) {
        let _ = fs::remove_file(path);
        return Err(format!("Failed to write {}: {e}", path.display()));
    }

    Ok(())
}

pub async fn new() {
    // Load config locations if you haven't already done that globally
    let _ = dotenv::from_filename(format!(
        "{}/.liquidium-pipeline/config.env",
        env::var("HOME").unwrap_or_default()
    ));
    let _ = dotenv::dotenv();

    let path: PathBuf = env::var("IDENTITY_PEM")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_identity_path());

    let trader_path: PathBuf = env::var("TRADER_IDENTITY_PEM")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_trader_identity_path());

    // Create liquidator identity
    match write_new_ed25519_key(&path) {
        Ok(()) => {
            println!("✅ Liquidator private key saved to {}", path.display());
        }
        Err(e) => {
            eprintln!("❌ Liquidator key error: {e}");
        }
    }

    // Create trader identity
    match write_new_ed25519_key(&trader_path) {
        Ok(()) => {
            println!("✅ Trader private key saved to {}", trader_path.display());
        }
        Err(e) => {
            eprintln!("❌ Trader key error: {e}");
        }
    }

    println!(
        "   Tip: set IDENTITY_PEM={} and \n TRADER_IDENTITY_PEM={} in your environment",
        path.display(),
        trader_path.display()
    );
}
