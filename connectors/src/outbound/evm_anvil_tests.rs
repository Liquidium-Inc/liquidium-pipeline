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
        matches!(sender.settle(&prepared).await, SendOutcome::InFlight { .. }),
        "pending, and it can still land"
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

/// Review finding 10: alloy's nonce cache never saw the signed deposits, so
/// a later approve tried spent nonces until its retries ran out.
#[tokio::test]
#[ignore = "needs anvil on PATH"]
async fn anvil_an_approve_after_signed_deposits_takes_the_next_nonce() {
    let (_anvil, provider, helper) = start_anvil().await;
    let backend = Arc::new(EvmBackendImpl::new(provider.clone()));
    let miner = {
        let provider = provider.clone();
        tokio::spawn(async move {
            loop {
                mine(&provider).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    // The helper's STOP code accepts an approve call as well.
    let spender = Address::repeat_byte(0x11);
    backend
        .erc20_approve_and_wait_raw(helper, spender, U256::from(1u64))
        .await
        .expect("the first approve lands");
    let sender = EvmSender::new(backend.clone());
    for salt in 1..=3u8 {
        let tx = backend
            .sign_helper_deposit_eth_native(helper, U256::from(1_000u64), FixedBytes::repeat_byte(salt))
            .await
            .expect("the deposit is signed");
        sender.submit(&evm_prepared_send(&tx).unwrap()).await;
    }

    backend
        .erc20_approve_and_wait_raw(helper, spender, U256::from(2u64))
        .await
        .expect("the second approve takes the nonce after the deposits");
    miner.abort();
}

/// Runtime code that emits one ERC-20 `Transfer(from, to, amount)` log on any call.
fn transfer_emitter(from: Address, to: Address, amount: U256) -> Bytes {
    let mut code = Vec::new();
    // MSTORE(0, amount): the log data.
    code.push(0x7f);
    code.extend_from_slice(&amount.to_be_bytes::<32>());
    code.extend_from_slice(&[0x60, 0x00, 0x52]);
    // LOG3 reads offset, size, topic0, topic1, topic2 from the top of the stack.
    for topic in [
        to.into_word(),
        from.into_word(),
        alloy::primitives::keccak256("Transfer(address,address,uint256)"),
    ] {
        code.push(0x7f);
        code.extend_from_slice(topic.as_slice());
    }
    code.extend_from_slice(&[0x60, 0x20, 0x60, 0x00, 0xa3, 0x00]);
    Bytes::from(code)
}

/// Sends a call from the anvil key and returns its hash. Nothing is mined.
async fn send_call(provider: &impl Provider<AnyNetwork>, to: Address, value: U256) -> alloy::primitives::TxHash {
    use alloy::network::TransactionBuilder;
    let request = alloy::rpc::types::TransactionRequest::default()
        .with_to(to)
        .with_value(value)
        .with_gas_limit(200_000);
    *provider
        .send_transaction(alloy::serde::WithOtherFields::new(request))
        .await
        .expect("the call is sent")
        .tx_hash()
}

#[tokio::test]
#[ignore = "needs anvil on PATH"]
async fn anvil_a_transaction_shows_what_it_paid_to_an_address() {
    use crate::backend::evm_backend::EvmPayment;
    let (_anvil, provider, _helper) = start_anvil().await;
    let backend = EvmBackendImpl::new(provider.clone());
    let set_code = |address: Address, code: Bytes| {
        let provider = provider.clone();
        async move {
            provider
                .raw_request::<_, serde_json::Value>("anvil_setCode".into(), (address, code))
                .await
                .expect("anvil_setCode");
        }
    };
    let wallet = Address::repeat_byte(0x22);
    let other = Address::repeat_byte(0x33);
    let token = Address::repeat_byte(0x55);
    let other_token = Address::repeat_byte(0x66);
    let reverting = Address::repeat_byte(0x77);
    let sender = Address::repeat_byte(0x99);
    set_code(token, transfer_emitter(sender, wallet, U256::from(5_000u64))).await;
    // PUSH1 0, PUSH1 0, REVERT.
    set_code(reverting, Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xfd])).await;

    let native = send_call(&provider, wallet, U256::from(777u64)).await;
    // Sent and not mined: the chain has no receipt.
    assert_eq!(
        backend.transaction_payment(native, None, wallet).await.unwrap(),
        EvmPayment::NotOnChain
    );
    mine(&provider).await;
    assert_eq!(
        backend.transaction_payment(native, None, wallet).await.unwrap(),
        EvmPayment::Paid(U256::from(777u64))
    );
    // The same transaction did not pay another address in a way that a receipt shows.
    assert_eq!(
        backend.transaction_payment(native, None, other).await.unwrap(),
        EvmPayment::NotVisible
    );

    let transfer = send_call(&provider, token, U256::ZERO).await;
    let failed = send_call(&provider, reverting, U256::ZERO).await;
    mine(&provider).await;
    assert_eq!(
        backend.transaction_payment(transfer, Some(token), wallet).await.unwrap(),
        EvmPayment::Paid(U256::from(5_000u64))
    );
    // Another receiver and another token both read as nothing paid.
    assert_eq!(
        backend.transaction_payment(transfer, Some(token), other).await.unwrap(),
        EvmPayment::Paid(U256::ZERO)
    );
    assert_eq!(
        backend
            .transaction_payment(transfer, Some(other_token), wallet)
            .await
            .unwrap(),
        EvmPayment::Paid(U256::ZERO)
    );
    assert_eq!(
        backend.transaction_payment(failed, Some(token), wallet).await.unwrap(),
        EvmPayment::Reverted
    );
}
