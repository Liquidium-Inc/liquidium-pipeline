use async_trait::async_trait;

use serde_json::json;

use crate::connectors::ccxt_client::{CcxtClient, DepositAddress, WithdrawalReceipt};
use ccxt::binance::{Binance as Mexc, BinanceImpl as MexcImpl};
use ccxt::exchange::{normalize, Value};

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
impl CcxtClient for MexcCcxtAdapter {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let mut ex = self.inner.lock().await;

        let orderbook_raw = Mexc::fetch_order_book(&mut ex, market.into(), Value::Undefined, Value::Undefined).await;

        let ob = normalize(&orderbook_raw).map_err(|e| e.to_string())?;
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
        let res = Mexc::fetch_deposit_address(
            &mut ex,
            asset.into(),
            match network {
                Some(n) => Value::from(n),
                None => Value::Undefined,
            },
        )
        .await;

        let norm = normalize(&res).map_err(|e| e.to_string())?;

        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.map(|s| s.to_string()),
            address: norm["address"].as_str().ok_or("missing deposit address")?.to_string(),
            tag: norm["tag"].as_str().map(|s| s.to_string()),
        })
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
        let res = Mexc::withdraw(
            &mut ex,
            asset.into(),
            amount.into(),
            address.into(),
            match tag {
                Some(t) => Value::from(t),
                None => Value::Undefined,
            },
            match network {
                Some(n) => Value::Json(json!({ "network": n })),
                None => Value::Undefined,
            },
        )
        .await;

        let norm = normalize(&res).map_err(|e| e.to_string())?;

        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.map(|s| s.to_string()),
            amount,
            txid: norm["txid"].as_str().map(|s| s.to_string()),
            internal_id: norm["id"].as_str().map(|s| s.to_string()),
        })
    }
}
