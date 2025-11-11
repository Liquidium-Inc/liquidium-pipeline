//! # EVM Bridge Client
//!
//! A standalone Rust library for interacting with EVM chains and the ICP CkDeposit bridge.
//!
//! ## Features
//! - Generic EVM client with EIP-1559 support, retries, and confirmation tracking
//! - ERC-20 token operations with automatic allowance management
//! - ICP bridge integration for depositing ETH and ERC-20 tokens
//!
//! ## Usage
//!
//! ```no_run
//! use evm_bridge_client::*;
//! use alloy::primitives::{Address, U256};
//! use alloy::signers::local::PrivateKeySigner;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), EvmError> {
//!     // Setup
//!     let rpc = RpcConfig {
//!         rpc_url: "https://eth-mainnet.g.alchemy.com/v2/KEY".into(),
//!         chain_id: 1,
//!     };
//!     let policy = TxPolicyConfig::default();
//!     let signer = PrivateKeySigner::random();
//!     let from = signer.address();
//!
//!     let client = EvmClient::new(rpc, signer, from, policy).await?;
//!
//!     // Send ETH
//!     let receipt = client.send_eth(
//!         "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb".parse()?,
//!         U256::from(1000000000000000000u128) // 1 ETH
//!     ).await?;
//!
//!     // ERC-20 operations
//!     let token_addr: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".parse()?;
//!     let erc20 = Erc20Client::new(token_addr, client.clone());
//!     let balance = erc20.balance_of(from).await?;
//!
//!     // ICP Bridge deposit
//!     let bridge_addr: Address = "0x...".parse()?;
//!     let bridge = IcpBridge::new(bridge_addr, client);
//!     let principal = [0u8; 32];
//!     let subaccount = [0u8; 32];
//!
//!     let (receipt, event) = bridge.deposit_eth(
//!         principal,
//!         subaccount,
//!         U256::from(100000000000000000u128) // 0.1 ETH
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod erc20;
pub mod errors;
pub mod icp_bridge;
pub mod types;

pub use client::EvmClient;
pub use config::{RpcConfig, TxPolicyConfig};
pub use erc20::Erc20Client;
pub use errors::EvmError;
pub use icp_bridge::IcpHypeBridge;
pub use types::*;
