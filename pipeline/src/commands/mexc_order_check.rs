//! Checks how MEXC handles an IOC limit order: one priced above the market,
//! which must not fill, and one at the best bid, which must. Without
//! `--confirm` it only prints the plan.
use liquidium_pipeline_connectors::backend::cex_backend::CexBackend;
use mexc_rs::spot::v3::enums::{OrderSide, OrderStatus};
use rust_decimal::{Decimal, prelude::FromPrimitive};

use crate::swappers::mexc::mexc_adapter::{LimitOrderReport, MexcClient};
use crate::utils::now_ts;

/// The no-fill probe sits this far above the best ask.
const ABOVE_MARKET: f64 = 1.15;

pub async fn check(market: &str, quantity: f64, price_decimals: u32, confirm: bool) -> Result<(), String> {
    let client = MexcClient::from_env()?;
    let (base, _) = market.split_once('_').ok_or("market must look like ICP_USDT")?;
    let symbol = market.replace('_', "");
    let quantity = Decimal::from_f64(quantity).ok_or("invalid quantity")?;

    let free = client.get_balance(base).await?;
    let book = client.get_orderbook(market, Some(5)).await?;
    let best_bid = book.bids.first().ok_or("no bids")?.price;
    let best_ask = book.asks.first().ok_or("no asks")?.price;
    let price = |value: f64| {
        Decimal::from_f64(value)
            .map(|price| price.round_dp(price_decimals))
            .ok_or("invalid price")
    };
    let no_fill_price = price(best_ask * ABOVE_MARKET)?;
    let fill_price = price(best_bid)?;

    println!("market={market} symbol={symbol} free {base}={free} best_bid={best_bid} best_ask={best_ask}");
    println!("probe 1: IOC sell {quantity} {base} at {no_fill_price} (above the market; must not fill)");
    println!("probe 2: IOC sell {quantity} {base} at {fill_price} (the best bid; must fill)");
    if !confirm {
        println!("dry run: nothing sent. Add --confirm to place both orders.");
        return Ok(());
    }

    let stamp = now_ts();
    let first = client
        .place_ioc_limit_order(
            &symbol,
            OrderSide::Sell,
            quantity,
            no_fill_price,
            &format!("lqcheck{stamp}a"),
        )
        .await?;
    report("probe 1", &first);
    // A resting order would be a live sell above the market. Cancel it at once.
    if matches!(first.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
        let cancelled = client.cancel_order(&symbol, &first.order_id).await?;
        report("probe 1 cancelled", &cancelled);
        return Err("MEXC left the IOC order open; IOC is not honoured".to_string());
    }

    let second = client
        .place_ioc_limit_order(
            &symbol,
            OrderSide::Sell,
            quantity,
            fill_price,
            &format!("lqcheck{stamp}b"),
        )
        .await?;
    report("probe 2", &second);
    if matches!(second.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
        let cancelled = client.cancel_order(&symbol, &second.order_id).await?;
        report("probe 2 cancelled", &cancelled);
        return Err("MEXC left the IOC order open; IOC is not honoured".to_string());
    }

    let average = if second.executed_quantity > Decimal::ZERO {
        second.cumulative_quote_quantity / second.executed_quantity
    } else {
        Decimal::ZERO
    };
    println!(
        "RESULT: probe 1 {:?} with {} filled; probe 2 {:?} with {} filled at {average} average",
        first.status, first.executed_quantity, second.status, second.executed_quantity
    );
    Ok(())
}

fn report(label: &str, order: &LimitOrderReport) {
    println!(
        "{label}: order_id={} status={:?} executed={} quote={}",
        order.order_id, order.status, order.executed_quantity, order.cumulative_quote_quantity
    );
}
