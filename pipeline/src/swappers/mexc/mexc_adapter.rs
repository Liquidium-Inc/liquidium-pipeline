use std::env;

use async_trait::async_trait;

use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, DepositAddress, WithdrawStatus, WithdrawalReceipt,
};
use log::info;
use rust_decimal::Decimal;

fn from_mexc_raw(s: &str) -> WithdrawStatus {
    match s {
        // adjust to whatever MEXC actually returns
        "WAIT" | "PENDING" | "PROCESSING" => WithdrawStatus::Pending,
        "SUCCESS" | "FINISHED" | "DONE" => WithdrawStatus::Completed,
        "FAILED" | "FAIL" => WithdrawStatus::Failed,
        "CANCEL" | "CANCELED" => WithdrawStatus::Canceled,
        _ => WithdrawStatus::Unknown,
    }
}

use mexc_rs::spot::{
    MexcSpotApiClientWithAuthentication,
    v3::{
        self,
        account_information::{AccountBalance, AccountInformationEndpoint},
        deposit_address::DepositAddressEndpoint,
        depth::{DepthEndpoint, DepthParams},
        enums::{OrderSide, OrderStatus},
        get_order::{GetOrderEndpoint, GetOrderParams},
        order::{OrderEndpoint, OrderParams},
        withdraw::{WithdrawEndpoint, WithdrawHistoryRequest, WithdrawRequest},
    },
};

use num_traits::ToPrimitive;

pub struct MexcClient {
    inner: tokio::sync::Mutex<MexcSpotApiClientWithAuthentication>,
}

impl MexcClient {
    pub fn new(api_key: &str, secret: &str) -> Self {
        let api = MexcSpotApiClientWithAuthentication::new(
            mexc_rs::spot::MexcSpotApiEndpoint::Base,
            api_key.to_string(),
            secret.to_string(),
        );
        Self {
            inner: tokio::sync::Mutex::new(api),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let api_key = env::var("MEXC_API_KEY").map_err(|_| "MEXC_API_KEY not set".to_string())?;
        let api_secret = env::var("MEXC_API_SECRET").map_err(|_| "MEXC_API_SECRET not set".to_string())?;

        Ok(Self::new(&api_key, &api_secret))
    }


}

#[async_trait]
impl CexBackend for MexcClient {
    async fn get_quote(&self, market: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let ob = ex
            .depth(DepthParams {
                limit: None,
                symbol: market,
            })
            .await
            .map_err(|e| e.to_string())?;

        // asks: Vec<Vec<f64>> = [ [price, qty], ... ]
        let asks = &ob.asks;

        if asks.is_empty() {
            return Err("no asks".into());
        }

        let mut remaining = Decimal::from_f64_retain(amount_in).expect("could not convert f64");
        let mut cost = Decimal::ZERO;

        for level in asks {
            let price = level.price;
            let qty = level.quantity;

            if remaining <= Decimal::ZERO {
                break;
            }

            let take = qty.min(remaining);
            cost += take * price;
            remaining -= take;
        }

        if remaining > Decimal::ZERO {
            return Err("not enough liquidity".into());
        }

        cost.to_f64().ok_or("f64 conversion failed".to_string())
    }

    async fn execute_swap(&self, market: &str, side: &str, amount_in: f64) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        info!("Swapping {} {} {}", market, side, amount_in);
        // place market order using quote_order_quantity
        let res = ex
            .order(OrderParams {
                symbol: &market.replace("/", ""),
                side: if side == "sell" {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                order_type: v3::enums::OrderType::Market,
                quantity: Some(Decimal::from_f64_retain(amount_in).ok_or("could not convert amount_in to Decimal")?),
                new_client_order_id: None,
                price: None,
                quote_order_quantity: None,
            })
            .await
            .map_err(|e| format!("Swap err: {}", e))?;

        // fetch final state of the order
        let order_res = ex
            .get_order(GetOrderParams {
                symbol: market,
                order_id: Some(&res.order_id),
                new_client_order_id: None,
                original_client_order_id: None,
            })
            .await
            .map_err(|e| format!("Get_order err: {}", e))?;

        // check it actually executed
        match order_res.status {
            OrderStatus::Filled => {}
            other => {
                return Err(format!("order not executed, status: {:?}", other));
            }
        }

        if order_res.executed_quantity <= Decimal::ZERO {
            return Err("order has zero executed quantity".into());
        }

        let filled_quote = order_res
            .cummulative_quote_quantity
            .to_f64()
            .ok_or("cannot convert cummulative_quote_quantity to f64")?;

        Ok(filled_quote)
    }

    async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let ex = self.inner.lock().await;

        println!("{:?} {:?}", asset, network);

        let res = ex
            .get_deposit_address(asset.to_string(), None)
            .await
            .map_err(|e| e.to_string())?;

        println!("{:?}", res);

        let addr = &res
            .iter()
            .find(|item| item.network.to_lowercase().contains(&network.to_lowercase()))
            .expect("Address not found");

        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.to_string(),
            address: addr.address.clone(),
            tag: addr.memo.clone(),
        })
    }

    async fn get_balance(&self, asset: &str) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let res = ex.account_information().await.map_err(|e| e.to_string())?;

        let balance = res
            .balances
            .iter()
            .find(|item| item.asset == asset.to_string())
            .cloned()
            .unwrap_or(AccountBalance {
                asset: asset.to_string(),
                free: Decimal::ZERO,
                locked: Decimal::ZERO,
            });

        let var_name = "could not convert balance to f64";
        Ok(balance.free.to_f64().expect(var_name))
    }

    async fn withdraw(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, String> {
        let ex = self.inner.lock().await;

        let res = ex
            .withdraw(WithdrawRequest {
                address: address.to_string(),
                amount: amount.to_string(),
                coin: asset.to_string(),
                memo: None,
                network: Some(network.to_string()),
                remark: None,
                withdraw_order_id: None,
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some(res.id),
        })
    }

    async fn get_withdraw_status_by_id(&self, coin: &str, withdraw_id: &str) -> Result<WithdrawStatus, String> {
        let ex = self.inner.lock().await;
        let records = ex
            .withdraw_history(WithdrawHistoryRequest {
                coin: Some(coin.to_string()),
                status: None,
                limit: Some(50),
                start_time: None,
                end_time: None,
            })
            .await
            .map_err(|e| e.to_string())?;

        let rec = records
            .into_iter()
            .find(|r| r.id == withdraw_id || r.withdraw_order_id.as_deref() == Some(withdraw_id));

        let rec = match rec {
            Some(r) => r,
            None => return Ok(WithdrawStatus::Unknown),
        };

        let status = from_mexc_raw(rec.status.as_str());
        Ok(status)
    }
}
