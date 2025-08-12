use openssl::pkey::PKey;
use std::{fs, path::Path};

use crate::config::Config;

pub async fn show() {
    // Load Config
    let config = Config::load();

    match config.await {
        Ok(config) => {
            println!("Account {}", config.liquidator_principal);
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

pub async fn new() {
    // Load config locations if you haven't already done that globally
    let _ = dotenv::from_filename(format!(
        "{}/.liquidium-pipeline/wallets/config.env",
        env::var("HOME").unwrap_or_default()
    ));
    let _ = dotenv::dotenv();

    let path: PathBuf = env::var("IDENTITY_PEM")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_identity_path());

    if let Some(parent) = path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            eprintln!("❌ Failed to create directory {}: {e}", parent.display());
            return;
        }
    }

    if Path::new(&path).exists() {
        eprintln!(
            "⚠️  Identity already exists at {}. Aborting to prevent overwrite.",
            path.display()
        );
        eprintln!("    (Set a new path via IDENTITY_PEM if you want another file.)");
        return;
    }

    // Generate Ed25519 keypair
    let key = match PKey::generate_ed25519() {
        Ok(k) => k,
        Err(e) => {
            eprintln!("❌ Failed to generate key: {e}");
            return;
        }
    };

    let private_pem = match key.private_key_to_pem_pkcs8() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("❌ Failed to encode key: {e}");
            return;
        }
    };

    // Write with 0600 perms
    #[cfg(unix)]
    let mut file = match OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("❌ Failed to create {}: {e}", path.display());
            return;
        }
    };

    #[cfg(not(unix))]
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("❌ Failed to create {}: {e}", path.display());
            return;
        }
    };

    if let Err(e) = file.write_all(&private_pem) {
        eprintln!("❌ Failed to write {}: {e}", path.display());
        let _ = fs::remove_file(&path);
        return;
    }

    println!("✅ Private key saved to {}", path.display());
    println!(
        "   Tip: ensure your config points to it (IDENTITY_PEM={})",
        path.display()
    );
}
