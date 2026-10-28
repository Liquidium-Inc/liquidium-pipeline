//! The production EVM send path against a local anvil chain: a signed ckETH
//! helper deposit, sent through `EvmSender` and settled from the chain.
//! Run with `cargo test -p liquidium-pipeline-connectors -- --ignored anvil`.
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use alloy::network::AnyNetwork;
use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::{Provider, ProviderBuilder, WalletProvider};
use alloy::signers::local::PrivateKeySigner;
use liquidium_pipeline_core::outbound::{OutboundSender, SendOutcome};

use super::{EvmSender, evm_prepared_send};
use crate::backend::bridge_backend::ckerc20_bridge::BridgeEvmBackend;
use crate::backend::evm_backend::EvmBackendImpl;

/// Anvil's first development key. It holds test ETH on every anvil chain.
const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

struct Anvil(Child);

impl Drop for Anvil {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("a free local port")
}

async fn mine(provider: &impl Provider<AnyNetwork>) {
    provider
        .raw_request::<_, serde_json::Value>("evm_mine".into(), [(); 0])
        .await
        .expect("evm_mine");
}

/// Starts anvil without automine, so a test can see the pending state.
async fn start_anvil() -> (
    Anvil,
    impl Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Clone + 'static,
    Address,
) {
    let port = free_port();
    let anvil = Anvil(
        Command::new("anvil")
            // Each chain gets its own id, as real chains do, so their nonce
            // counts stay apart in the process-wide nonce mark.
            .args([
                "--port",
                &port.to_string(),
                "--chain-id",
                &port.to_string(),
                "--no-mining",
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("anvil on PATH"),
    );
    let signer: PrivateKeySigner = ANVIL_KEY.parse().unwrap();
    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .wallet(signer)
        .connect_http(format!("http://127.0.0.1:{port}").parse().unwrap());
    for _ in 0..100 {
        if provider.get_chain_id().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A STOP opcode accepts any call and its value, like the helper's deposit.
    let helper = Address::repeat_byte(0x42);
    provider
        .raw_request::<_, serde_json::Value>("anvil_setCode".into(), (helper, Bytes::from_static(&[0x00])))
        .await
        .expect("anvil_setCode");
    (anvil, provider, helper)
}

#[tokio::test]
#[ignore = "needs anvil on PATH"]
async fn anvil_a_signed_helper_deposit_lands_once() {
    let (_anvil, provider, helper) = start_anvil().await;
    let backend = Arc::new(EvmBackendImpl::new(provider.clone()));
    let amount = U256::from(1_000_000u64);
    let tx = backend
        .sign_helper_deposit_eth_native(helper, amount, FixedBytes::repeat_byte(7))
        .await
        .expect("the helper call is signed");
    let prepared = evm_prepared_send(&tx).unwrap();
    let sender = EvmSender::new(backend.clone());

    assert!(
        matches!(sender.submit(&prepared).await, SendOutcome::Open { .. }),
        "sent, not mined"
    );
    assert!(
        matches!(sender.settle(&prepared).await, SendOutcome::Open { .. }),
        "pending"
    );
    // The node already holds the bytes, so a second submit cannot clear the record.
    let again = sender.submit(&prepared).await;
    assert!(!matches!(again, SendOutcome::NeverLanded { .. }), "{again:?}");

    mine(&provider).await;
    assert_eq!(
        sender.settle(&prepared).await,
        SendOutcome::Landed {
            receipt: format!("{:#x}", tx.hash)
        }
    );
    assert_eq!(provider.get_balance(helper).await.unwrap(), amount);

    // After it landed, a resend is refused and moves nothing.
    let late = sender.submit(&prepared).await;
    assert!(!matches!(late, SendOutcome::NeverLanded { .. }), "{late:?}");
    mine(&provider).await;
    assert_eq!(
        provider.get_balance(helper).await.unwrap(),
        amount,
        "the value moved once"
    );
}

#[tokio::test]
#[ignore = "needs anvil on PATH"]
async fn anvil_a_refusal_for_missing_funds_never_landed() {
    let (_anvil, provider, helper) = start_anvil().await;
    let backend = Arc::new(EvmBackendImpl::new(provider.clone()));
    let tx = backend
        .sign_helper_deposit_eth_native(helper, U256::from(1_000u64), FixedBytes::repeat_byte(7))
        .await
        .expect("the helper call is signed");
    provider
        .raw_request::<_, serde_json::Value>("anvil_setBalance".into(), (tx.from, U256::ZERO))
        .await
        .expect("anvil_setBalance");

    let outcome = EvmSender::new(backend).submit(&evm_prepared_send(&tx).unwrap()).await;
    assert!(matches!(outcome, SendOutcome::NeverLanded { .. }), "{outcome:?}");
}
