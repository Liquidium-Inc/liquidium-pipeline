use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;

use crate::config::Config;
use crate::error::{AppError, error_codes};
use crate::swappers::mexc::mexc_adapter::MexcClient;

pub async fn mexc_deposit_address(asset: &str, network: Option<&str>) -> Result<(), AppError> {
    let _ = Config::load()
        .await
        .map_err(|e| AppError::from_def(error_codes::CONFIG_ERROR).with_context(format!("config load failed: {e}")))?;
    let client = MexcClient::from_env()?;
    let network = network.unwrap_or("ICP");
    let addr = client.get_deposit_address(asset, network).await?;

    println!("MEXC deposit address:");
    println!("  asset   : {}", addr.asset);
    println!("  network : {}", addr.network);
    println!("  address : {}", addr.address);
    if let Some(tag) = addr.tag.as_ref()
        && !tag.is_empty()
    {
        println!("  tag     : {}", tag);
    }

    Ok(())
}
