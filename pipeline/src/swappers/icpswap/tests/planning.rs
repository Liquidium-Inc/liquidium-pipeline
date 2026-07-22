use std::{collections::HashMap, sync::Arc};

use candid::{Int, Nat, Principal};
use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

use crate::swappers::model::SwapRequest;

use super::{
    client::MockIcpswapReadClient,
    types::{IcpswapClientError, IcpswapError, IcpswapPoolData, IcpswapQuoteError, IcpswapToken, IcpswapTokenMetadata},
    venue::IcpswapVenue,
};

fn principal(id: u8) -> Principal {
    Principal::from_slice(&[id])
}

fn chain_token(ledger: Principal, symbol: &str, fee: u64) -> ChainToken {
    ChainToken::Icp {
        ledger,
        symbol: symbol.to_string(),
        decimals: 8,
        fee: Nat::from(fee),
    }
}

fn metadata(token: ChainToken) -> IcpswapTokenMetadata {
    IcpswapTokenMetadata {
        token,
        standard: "ICRC2".to_string(),
    }
}

fn request(input: &ChainToken, output: &ChainToken) -> SwapRequest {
    SwapRequest {
        pay_asset: input.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
        receive_asset: output.asset_id(),
        receive_address: None,
        max_slippage_bps: Some(100),
        venue_hint: Some("icpswap".to_string()),
    }
}

fn pool_data(pool: Principal, token0: Principal, token1: Principal, fee: Nat) -> IcpswapPoolData {
    IcpswapPoolData {
        key: format!("{}_{}_{}", token0, token1, fee),
        token0: IcpswapToken {
            address: token0.to_text(),
            standard: "ICRC2".to_string(),
        },
        token1: IcpswapToken {
            address: token1.to_text(),
            standard: "ICRC2".to_string(),
        },
        fee,
        tick_spacing: Int::from(60),
        canister_id: pool,
    }
}

#[tokio::test]
async fn selects_highest_net_output_across_all_fee_tiers() {
    let input = chain_token(principal(1), "INPUT", 10);
    let output = chain_token(principal(2), "OUTPUT", 10);
    let pools = HashMap::from([
        (Nat::from(500u64), (principal(5), Nat::from(990u64))),
        (Nat::from(3_000u64), (principal(6), Nat::from(1_100u64))),
        (Nat::from(10_000u64), (principal(7), Nat::from(1_000u64))),
    ]);
    let pools_for_discovery = pools.clone();
    let outputs_by_pool: HashMap<Principal, Nat> = pools.values().cloned().collect();
    let input_ledger = principal(1);
    let output_ledger = principal(2);

    let mut client = MockIcpswapReadClient::new();
    client.expect_ledger_fee().times(2).returning(move |ledger| {
        assert!(ledger == input_ledger || ledger == output_ledger);
        Ok(Nat::from(10u64))
    });
    client.expect_get_pool().times(3).returning(move |_, _, fee| {
        let (pool, _) = pools_for_discovery.get(fee).expect("configured fee");
        let (token0, token1) = if fee == &Nat::from(500u64) {
            (output_ledger, input_ledger)
        } else {
            (input_ledger, output_ledger)
        };
        Ok(pool_data(*pool, token0, token1, fee.clone()))
    });
    client.expect_quote().times(3).returning(move |pool, args| {
        if pool == principal(5) {
            assert!(!args.zero_for_one);
        } else {
            assert!(args.zero_for_one);
        }
        assert_eq!(args.amount_in, "99980");
        assert_eq!(args.amount_out_minimum, "0");
        Ok(outputs_by_pool.get(&pool).expect("known pool").clone())
    });

    let venue = IcpswapVenue::new(
        Arc::new(client),
        vec![metadata(input.clone()), metadata(output.clone())],
        vec![Nat::from(10_000u64), Nat::from(500u64), Nat::from(3_000u64)],
        200,
    )
    .expect("venue");
    let result = venue.preview_route(&request(&input, &output)).await.expect("quote");

    assert_eq!(result.route.pool, principal(6));
    assert_eq!(result.route.fee_tier, Nat::from(3_000u64));
    assert_eq!(result.route.amount_in.value, Nat::from(99_980u64));
    assert_eq!(result.route.gross_quoted_out.value, Nat::from(1_100u64));
    assert_eq!(result.route.input_ledger_fee.value, Nat::from(10u64));
    assert_eq!(result.route.output_ledger_fee.value, Nat::from(10u64));
    assert_eq!(result.route.net_expected_output().value, Nat::from(1_090u64));
    assert_eq!(result.route.amount_out_minimum.value, Nat::from(1_089u64));
    assert_eq!(result.quote.receive_amount, Nat::from(1_090u64));
    assert_eq!(result.quote.legs[0].route_id, principal(6).to_text());
    assert_eq!(result.quote.legs[0].gas_fee, Nat::from(10u64));
}

#[tokio::test]
async fn keeps_usable_quote_when_another_fee_tier_fails() {
    let input = chain_token(principal(1), "INPUT", 10);
    let output = chain_token(principal(2), "OUTPUT", 10);
    let input_ledger = principal(1);
    let output_ledger = principal(2);

    let mut client = MockIcpswapReadClient::new();
    client.expect_ledger_fee().times(2).returning(|_| Ok(Nat::from(10u64)));
    client.expect_get_pool().times(2).returning(move |_, _, fee| {
        if fee == &Nat::from(500u64) {
            Err(IcpswapClientError::Protocol {
                method: "getPool",
                error: IcpswapError::CommonError,
            })
        } else {
            Ok(pool_data(principal(6), input_ledger, output_ledger, fee.clone()))
        }
    });
    client
        .expect_quote()
        .times(1)
        .return_once(|_, _| Ok(Nat::from(1_000u64)));

    let venue = IcpswapVenue::new(
        Arc::new(client),
        vec![metadata(input.clone()), metadata(output.clone())],
        vec![Nat::from(500u64), Nat::from(3_000u64)],
        100,
    )
    .expect("venue");
    let result = venue.preview_route(&request(&input, &output)).await.expect("quote");

    assert_eq!(result.route.pool, principal(6));
    assert_eq!(result.route.net_expected_output().value, Nat::from(990u64));
}

#[tokio::test]
async fn reports_every_failure_when_no_pool_is_usable() {
    let input = chain_token(principal(1), "INPUT", 10);
    let output = chain_token(principal(2), "OUTPUT", 10);

    let mut client = MockIcpswapReadClient::new();
    client.expect_ledger_fee().times(2).returning(|_| Ok(Nat::from(10u64)));
    client.expect_get_pool().times(2).returning(|_, _, _| {
        Err(IcpswapClientError::Protocol {
            method: "getPool",
            error: IcpswapError::CommonError,
        })
    });
    client.expect_quote().times(0);

    let venue = IcpswapVenue::new(
        Arc::new(client),
        vec![metadata(input.clone()), metadata(output.clone())],
        vec![Nat::from(500u64), Nat::from(3_000u64)],
        100,
    )
    .expect("venue");
    let error = venue.preview_route(&request(&input, &output)).await.unwrap_err();

    assert!(matches!(error, IcpswapQuoteError::NoUsablePools { failures } if failures.len() == 2));
}

#[tokio::test]
async fn rejects_spend_budget_that_cannot_cover_transfer_and_deposit_fees() {
    let input = chain_token(principal(1), "INPUT", 10);
    let output = chain_token(principal(2), "OUTPUT", 10);
    let mut request = request(&input, &output);
    request.pay_amount.value = Nat::from(20u64);

    let mut client = MockIcpswapReadClient::new();
    client.expect_ledger_fee().times(2).returning(|_| Ok(Nat::from(10u64)));
    client.expect_get_pool().times(0);
    client.expect_quote().times(0);

    let venue = IcpswapVenue::new(
        Arc::new(client),
        vec![metadata(input), metadata(output)],
        vec![Nat::from(3_000u64)],
        100,
    )
    .expect("venue");

    let error = venue.preview_route(&request).await.unwrap_err();
    assert_eq!(
        error,
        IcpswapQuoteError::InputFeesExceedBudget {
            budget: Nat::from(20u64),
            fees: Nat::from(20u64),
        }
    );
}

#[tokio::test]
async fn equal_outputs_choose_lower_fee_tier_deterministically() {
    let input = chain_token(principal(1), "INPUT", 10);
    let output = chain_token(principal(2), "OUTPUT", 10);
    let input_ledger = principal(1);
    let output_ledger = principal(2);

    let mut client = MockIcpswapReadClient::new();
    client.expect_ledger_fee().times(2).returning(|_| Ok(Nat::from(10u64)));
    client.expect_get_pool().times(2).returning(move |_, _, fee| {
        let pool = if fee == &Nat::from(500u64) {
            principal(5)
        } else {
            principal(6)
        };
        Ok(pool_data(pool, input_ledger, output_ledger, fee.clone()))
    });
    client.expect_quote().times(2).returning(|_, _| Ok(Nat::from(1_000u64)));

    let venue = IcpswapVenue::new(
        Arc::new(client),
        vec![metadata(input.clone()), metadata(output.clone())],
        vec![Nat::from(3_000u64), Nat::from(500u64)],
        100,
    )
    .expect("venue");
    let result = venue.preview_route(&request(&input, &output)).await.expect("quote");

    assert_eq!(result.route.fee_tier, Nat::from(500u64));
    assert_eq!(result.route.pool, principal(5));
}
