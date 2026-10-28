//! Checks that MEXC accepts our `withdrawOrderId` and returns it in the
//! withdraw history. Without `--confirm` it only reads the history.
use std::time::Duration;

use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use crate::finalizers::mexc::mexc_finalizer::mexc_withdraw_order_id;
use crate::swappers::mexc::mexc_adapter::MexcClient;
use crate::utils::now_ts;

const HISTORY_POLLS: u32 = 24;
const HISTORY_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub async fn check(asset: &str, network: &str, address: &str, amount: f64, confirm: bool) -> Result<(), String> {
    let client = MexcClient::from_env()?;
    let order_id = mexc_withdraw_order_id(&format!("withdraw-check-{}", now_ts()), 0);

    println!("asset={asset} network={network} amount={amount} address={address}");
    println!("withdrawOrderId={order_id}");
    // A read before the send proves that the keys and the history endpoint work.
    let before = client.find_withdraw_record(asset, &order_id).await?;
    println!(
        "history read: ok ({})",
        if before.is_some() { "a record already has this id" } else { "no record has this id yet" }
    );
    // Live rows trade from the same account, so the test amount must be spare.
    let free = client.get_balance(asset).await?;
    println!("free {asset} on MEXC: {free}");
    let deposit = client.get_deposit_address(asset, network).await?;
    println!(
        "to fund this test, deposit {asset} to: {} (memo/tag: {})",
        deposit.address,
        deposit.tag.as_deref().filter(|tag| !tag.trim().is_empty()).unwrap_or("none")
    );
    if !confirm {
        println!("dry run: nothing sent. Add --confirm to send this withdrawal.");
        return Ok(());
    }

    let receipt = client
        .withdraw(asset, network, address, amount, &order_id)
        .await
        .map_err(|error| format!("MEXC refused or did not answer the withdrawal: {error}"))?;
    let mexc_id = receipt.internal_id.clone().unwrap_or_default();
    println!("accepted: MEXC id={mexc_id} coin={}", receipt.asset);

    for poll in 1..=HISTORY_POLLS {
        tokio::time::sleep(HISTORY_POLL_INTERVAL).await;
        if let Some(record) = client.find_withdraw_record(asset, &order_id).await? {
            println!(
                "RESULT: ok. The history returns the order id: id={} withdrawOrderId={:?} status={}",
                record.id, record.withdraw_order_id, record.status
            );
            return Ok(());
        }
        if !mexc_id.is_empty()
            && let Some(record) = client.find_withdraw_record(asset, &mexc_id).await?
        {
            return Err(format!(
                "MEXC accepted the withdrawal but its history record has no order id: id={} withdrawOrderId={:?} status={}",
                record.id, record.withdraw_order_id, record.status
            ));
        }
        println!("poll {poll}/{HISTORY_POLLS}: the withdrawal is not in the history yet");
    }
    Err(format!(
        "withdrawal {mexc_id} is not in the history after {}s",
        HISTORY_POLLS as u64 * HISTORY_POLL_INTERVAL.as_secs()
    ))
}
