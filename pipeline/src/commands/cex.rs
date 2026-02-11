use liquidium_pipeline_commons::error::format_with_code;
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use crate::config::Config;
use crate::swappers::mexc::mexc_adapter::MexcClient;

pub async fn mexc_deposit_address(asset: &str, network: Option<&str>) -> Result<(), String> {
    let _ = Config::load()
        .await
        .map_err(|e| format!("config load failed: {}", format_with_code(&e)))?;
    let client = MexcClient::from_env()?;
    let network = network.unwrap_or("ICP");
    let addr = client.get_deposit_address(asset, network).await?;

    println!("MEXC deposit address:");
    println!("  asset   : {}", addr.asset);
    println!("  network : {}", addr.network);
    println!("  address : {}", addr.address);
    if let Some(tag) = addr.tag.as_ref()
        && !tag.is_empty() {
            println!("  tag     : {}", tag);
        }

    Ok(())
}
