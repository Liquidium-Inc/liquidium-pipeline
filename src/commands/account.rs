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

pub async fn new() {
    // Check if private.pem exists
    if Path::new("id.pem").exists() {
        println!("id.pem already exists. Aborting to prevent overwrite.");
        return;
    }

    // Generate Ed25519 keypair
    let key = PKey::generate_ed25519().unwrap();

    // Export private key PEM
    let private_pem = key.private_key_to_pem_pkcs8().unwrap();
    let _ = fs::write("id.pem", &private_pem);
    println!("Private key saved to id.pem");
}
