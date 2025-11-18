use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{CexBackend, DepositAddress, WithdrawalReceipt};
use serde_json::json;

use ccxt::binance::{Binance as Mexc, BinanceImpl as MexcImpl};
use ccxt::exchange::{Value, normalize};

pub struct MexcCcxtAdapter {
    inner: tokio::sync::Mutex<MexcImpl>,
}

impl MexcCcxtAdapter {
    pub fn new(api_key: &str, secret: &str) -> Self {
        let cfg = Value::Json(json!({
            "apiKey": api_key,
            "secret": secret,
        }));
        Self {
            inner: tokio::sync::Mutex::new(MexcImpl::new(cfg)),
        }
    }
}

#[async_trait]
impl CexBackend for MexcCcxtAdapter {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let mut ex = self.inner.lock().await;

        let orderbook_raw = ex
            .fetch_order_book(market.into(), Value::Undefined, Value::Undefined)
            .await;

        let ob = normalize(&orderbook_raw).expect("could not get quote");
        let best = ob["asks"][0][0].as_f64().ok_or("missing best ask")?;

        Ok(amount_in * best)
    }

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        let mut ex = self.inner.lock().await;

        let res = ex
            .create_order(
                market.into(),
                "market".into(),
                side.into(),
                amount_in.into(),
                Value::Undefined,
                Value::Undefined,
            )
            .await;

        let norm = normalize(&res).expect("coould not normalize res");
        let filled_quote = norm["cost"].as_f64().ok_or("missing filled quote amount (cost)")?;

        Ok(filled_quote)
    }

    async fn get_deposit_address(&self, asset: &str, network: Option<&str>) -> Result<DepositAddress, String> {
        let mut ex = self.inner.lock().await;

        // ccxt has fetch_deposit_address or fetch_deposit_addresses
        // depending on exchange; adjust to what Mexc exposes in ccxt-rs.
        let res = ex
            .fetch_deposit_address(
                "OGY".into(),
                match network {
                    Some(n) => Value::from("OGY"),
                    None => Value::Undefined,
                },
            )
            .await;

        let norm = normalize(&res).expect("could not get deposit address");

        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.map(|s| s.to_string()),
            address: norm["address"].as_str().ok_or("missing deposit address")?.to_string(),
            tag: norm["tag"].as_str().map(|s| s.to_string()),
        })
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        let mut ex = self.inner.lock().await;

        let res = ex
            .fetch_balance(Value::Undefined)
            .await;

        let norm = normalize(&res).ok_or("could not normalize balance response".to_string())?;

        let free = norm["free"][asset]
            .as_f64()
            .ok_or_else(|| format!("missing free balance for asset {}", asset))?;

        Ok(free)
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        tag: Option<&str>,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String> {
        let mut ex = self.inner.lock().await;

        // ccxt withdraw method signature is usually:
        // withdraw(code, amount, address, tag, params)
        let res = ex
            .withdraw(
                asset.into(),
                amount.into(),
                address.into(),
                match tag {
                    Some(t) => Value::from(t),
                    None => Value::Undefined,
                },
                Value::Json(json!({ "network": network.to_string() })),
            )
            .await;

        let norm = normalize(&res).expect("withdraw failed");

        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: norm["txid"].as_str().map(|s| s.to_string()),
            internal_id: norm["id"].as_str().map(|s| s.to_string()),
        })
    }
}
