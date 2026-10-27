use super::*;

#[test]
fn every_status_the_mexc_client_renders_is_a_known_withdraw_status() {
    for code in 1..=10 {
        let status = mexc_rs::spot::v3::withdraw::WithdrawStatus::from_code(code).expect("documented code");
        assert_ne!(
            from_mexc_raw(status.as_str()),
            WithdrawStatus::Unknown,
            "{} must not read as an absent withdrawal",
            status.as_str()
        );
    }
}

#[test]
fn only_refusals_before_execution_allow_another_withdraw_alias() {
    let response = |code: v3::ErrorCode, raw_code: i32| {
        v3::ApiError::ErrorResponse(v3::ErrorResponse {
            code,
            raw_code,
            msg: "test".into(),
            _extend: None,
        })
    };
    for refused in [
        response(v3::ErrorCode::InsufficientBalance, 10101),
        response(v3::ErrorCode::CurrencyDoesNotExist, 10232),
        v3::ApiError::MalformedRequest,
        v3::ApiError::RateLimitExceeded,
        v3::ApiError::WebApplicationFirewallViolated,
    ] {
        assert!(is_definite_rejection(&refused), "{refused:?}");
    }
    for open in [
        response(v3::ErrorCode::InternalError, 500),
        response(v3::ErrorCode::GatewayTimeout, 504),
        response(v3::ErrorCode::TransferOperationProcessing, 10200),
        response(v3::ErrorCode::InternalSystemErrorPleaseContactSupport, 20002),
        v3::ApiError::InternalServerError,
        v3::ApiError::UnableToParseResponse,
    ] {
        assert!(!is_definite_rejection(&open), "{open:?}");
    }
}

#[test]
fn only_mexc_code_10254_is_a_below_minimum_withdrawal() {
    let response = |code: v3::ErrorCode, raw_code: i32| {
        v3::ApiError::ErrorResponse(v3::ErrorResponse {
            code,
            raw_code,
            msg: "text that changes with the language".into(),
            _extend: None,
        })
    };
    let below_minimum = response(v3::ErrorCode::InvalidResponse, 10254);
    let no_balance = response(v3::ErrorCode::InsufficientBalance, 10101);
    assert!(is_below_withdraw_minimum(&below_minimum));
    assert!(!is_below_withdraw_minimum(&no_balance));
    assert!(!is_below_withdraw_minimum(&v3::ApiError::MalformedRequest));
}

#[test]
fn a_history_read_skips_only_aliases_that_mexc_does_not_list() {
    let response = |code: v3::ErrorCode, raw_code: i32| {
        v3::ApiError::ErrorResponse(v3::ErrorResponse {
            code,
            raw_code,
            msg: "test".into(),
            _extend: None,
        })
    };
    assert!(is_unlisted_alias(&response(
        v3::ErrorCode::ParamOrigClientOrderIdOrOrderIdMustBeSentButBothWereEmptyNull,
        700004
    )));
    assert!(is_unlisted_alias(&response(v3::ErrorCode::CurrencyDoesNotExist, 10232)));
    assert!(!is_unlisted_alias(&response(v3::ErrorCode::InternalError, 500)));
    assert!(!is_unlisted_alias(&v3::ApiError::RateLimitExceeded));
    assert_eq!(mexc_withdraw_coin_candidates("SOL"), vec!["SOL", "CKSOL"]);
}

// Labels recorded from the live deposit address endpoint on 2026-10-21.
#[test]
fn deposit_network_matches_live_mexc_labels() {
    let cands = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    for (label, candidates) in [
        ("Solana(SOL)", cands(&["SOL"])),
        ("Bitcoin(BTC)", cands(&["BTC"])),
        ("Ethereum(ERC20)", cands(&["ETH", "ERC20"])),
        ("Internet Computer(ICP)", cands(&["ICP"])),
        ("CKBTC", cands(&["CKBTC", "ICP"])),
        ("CKUSDT", cands(&["CKUSDT", "ICP"])),
    ] {
        assert!(super::deposit_network_matches(label, &candidates), "{label}");
    }
    for (label, candidates) in [
        ("CKETH", cands(&["ETH", "ERC20"])),
        ("MEGAETH", cands(&["ETH", "ERC20"])),
        ("Wormhole-SOL", cands(&["SOL"])),
        ("Solana(SOL)", cands(&["ETH", "ERC20"])),
        ("BNB Smart Chain(BEP20)", cands(&["BTC"])),
    ] {
        assert!(!super::deposit_network_matches(label, &candidates), "{label}");
    }
}

#[test]
fn market_order_admission_rejects_limit_only_books() {
    for order_types in [serde_json::json!(["LIMIT"]), serde_json::json!([])] {
        let info = serde_json::json!({"orderTypes": order_types});
        let error = MexcClient::validate_market_order_support(&info, "ETHUSDC").unwrap_err();
        assert!(error.contains("ETHUSDC"));
        assert!(error.contains("does not support MARKET"));
    }
    for info in [
        serde_json::json!({"orderTypes": ["LIMIT", "MARKET"]}),
        serde_json::json!({}),
    ] {
        assert!(MexcClient::validate_market_order_support(&info, "ETHUSDC").is_ok());
    }
}

#[tokio::test]
async fn native_deposit_prefers_eth_and_ignores_cketh_address() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
        }
        let request = String::from_utf8(request).unwrap();
        assert!(request.contains("coin=ETH"), "{request}");
        assert!(!request.contains("coin=CKETH"), "{request}");
        let body = r#"[{"coin":"ETH","network":"CKETH","address":"wrong-ic-account","memo":""},{"coin":"ETH","network":"ETH","address":"0x1111111111111111111111111111111111111111","memo":""}]"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    let client = MexcClient {
        inner: tokio::sync::Mutex::new(MexcSpotApiClientWithAuthentication::new(
            mexc_rs::spot::MexcSpotApiEndpoint::Custom(endpoint),
            "test-key".into(),
            "test-secret".into(),
        )),
        symbol_filters: tokio::sync::Mutex::new(HashMap::new()),
        coin_config: CoinConfigCache::default(),
        http: reqwest::Client::new(),
    };
    let address = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.get_deposit_address("ETH", "ETH"),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    assert_eq!(address.network, "ETH");
    assert_eq!(address.address, "0x1111111111111111111111111111111111111111");
}

#[test]
fn withdrawal_submission_and_history_cover_native_and_ck_aliases() {
    assert_eq!(mexc_withdraw_coin_candidates(" usdc "), ["USDC", "CKUSDC"]);
    assert_eq!(mexc_withdraw_coin_candidates("ckUSDC"), ["CKUSDC", "USDC"]);
    assert_eq!(mexc_withdraw_coin_candidates("ICP"), ["ICP", "CKICP"]);
}

// Buy fill mapping should keep quote as input-consumed and apply fee haircut to base output.
#[test]
fn map_fill_report_buy_uses_quote_as_input_and_base_as_output() {
    let report = MexcClient::map_fill_report("buy", Decimal::new(185, 6), Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
        .expect("map should work");
    let expected_output = MexcClient::apply_output_fee(Decimal::new(185, 6), MEXC_SPOT_FEE_BPS)
        .to_f64()
        .expect("convert expected buy output");
    assert!((report.input_consumed - 12.8).abs() < 1e-12);
    assert!((report.output_received - expected_output).abs() < 1e-12);
}

// Sell fill mapping should keep base as input-consumed and apply fee haircut to quote output.
#[test]
fn map_fill_report_sell_uses_base_as_input_and_quote_as_output() {
    let report =
        MexcClient::map_fill_report("sell", Decimal::new(185, 6), Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
            .expect("map should work");
    let expected_output = MexcClient::apply_output_fee(Decimal::new(1280, 2), MEXC_SPOT_FEE_BPS)
        .to_f64()
        .expect("convert expected sell output");
    assert!((report.input_consumed - 0.000185).abs() < 1e-12);
    assert!((report.output_received - expected_output).abs() < 1e-12);
}

// exchangeInfo returns takerCommission as ratio; we convert ratio -> bps.
#[test]
fn parse_taker_fee_bps_converts_ratio_to_bps() {
    let info = serde_json::json!({
        "takerCommission": "0.003"
    });
    let bps = MexcClient::parse_taker_fee_bps(&info).expect("bps");
    assert!((bps - 30.0).abs() < 1e-12);
}

// Guard against mis-scaled fee payloads that would otherwise imply impossible fee rates.
#[test]
fn parse_taker_fee_bps_rejects_values_above_10000() {
    // `takerCommission` is interpreted as a ratio; "2.0" means 200%.
    // Converted to bps this is 20_000 bps, above MAX_TAKER_FEE_BPS (10_000),
    // so parsing must fail and the caller should use fallback fee logic instead.
    let info = serde_json::json!({
        "takerCommission": "2.0"
    });
    let bps = MexcClient::parse_taker_fee_bps(&info);
    assert!(bps.is_none());
}

// Missing per-symbol fee must fall back to default fee plus round handler.
#[test]
fn resolve_taker_fee_bps_falls_back_to_default_when_missing() {
    let filters = SymbolFilters::default();
    let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
    assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
    assert!(fallback);
}

// Valid symbol fee should be used and padded by round handler.
#[test]
fn resolve_taker_fee_bps_adds_round_handler_to_symbol_fee() {
    let filters = SymbolFilters {
        taker_fee_bps: Some(5.0),
        ..SymbolFilters::default()
    };
    let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
    assert!((bps - 5.01).abs() < 1e-12);
    assert!(!fallback);
}

// Out-of-range symbol fees must be discarded and replaced with default fallback.
#[test]
fn resolve_taker_fee_bps_falls_back_for_symbol_fee_above_10000() {
    let filters = SymbolFilters {
        taker_fee_bps: Some(10_000.01),
        ..SymbolFilters::default()
    };
    let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
    assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
    assert!(fallback);
}

// Even boundary values should fallback when adding round handler would exceed hard cap.
#[test]
fn resolve_taker_fee_bps_falls_back_when_round_handler_pushes_over_10000() {
    let filters = SymbolFilters {
        taker_fee_bps: Some(10_000.0),
        ..SymbolFilters::default()
    };
    let (bps, fallback) = MexcClient::resolve_taker_fee_bps(Some(&filters));
    assert!((bps - (MEXC_SPOT_FEE_BPS + ROUND_HANDLER)).abs() < 1e-12);
    assert!(fallback);
}

#[test]
fn detects_order_missing_lookup_error_shapes() {
    assert!(is_order_missing_lookup_error(
        "400 Bad Request {\"msg\":\"Order does not exist.\",\"code\":-2013}"
    ));
    assert!(is_order_missing_lookup_error(
        "Get_order err: code=-2013 msg=Unknown order sent."
    ));
    assert!(!is_order_missing_lookup_error("order not executed, status: New"));
}

#[test]
fn candidate_symbols_are_normalized_and_deduped() {
    let candidates = MexcClient::candidate_symbols("CKBTCBTC", "CKBTC_BTC", "ckbtc-btc");
    assert_eq!(candidates, vec!["CKBTCBTC".to_string()]);
}

#[test]
fn evm_network_candidates_include_erc20_alias() {
    assert_eq!(
        mexc_network_candidates("ETH", "ETH"),
        vec!["ETH".to_string(), "ERC20".to_string()]
    );
    assert_eq!(
        mexc_network_candidates("USDC", "evm-eth"),
        vec!["EVM-ETH".to_string(), "ERC20".to_string()]
    );
}

#[test]
fn evm_withdraw_network_uses_erc20_address_book_network() {
    assert_eq!(mexc_withdraw_network("ETH", "ETH"), "ERC20");
    assert_eq!(mexc_withdraw_network("USDC", "evm-eth"), "ERC20");
    assert_eq!(mexc_withdraw_network("ICP", "ICP"), "ICP");
    assert_eq!(mexc_withdraw_network("ckBTC", "ICP"), "CKBTC");
}

#[test]
fn from_mexc_raw_is_case_insensitive_and_trim_tolerant() {
    assert_eq!(from_mexc_raw("Success"), WithdrawStatus::Completed);
    assert_eq!(from_mexc_raw(" finished "), WithdrawStatus::Completed);
    assert_eq!(from_mexc_raw("cAnCeLeD"), WithdrawStatus::Canceled);
    assert_eq!(from_mexc_raw(" processing "), WithdrawStatus::Pending);
}

#[test]
fn withdraw_status_requires_txid_for_completed_statuses() {
    assert_eq!(
        withdraw_status_from_mexc_record("Success", None, None),
        WithdrawStatus::Pending
    );
    assert_eq!(
        withdraw_status_from_mexc_record("DONE", Some(" "), Some("   ")),
        WithdrawStatus::Pending
    );
}

#[test]
fn withdraw_status_accepts_tx_id_or_trans_hash_for_completion() {
    assert_eq!(
        withdraw_status_from_mexc_record("SUCCESS", Some("0xabc"), None),
        WithdrawStatus::Completed
    );
    assert_eq!(
        withdraw_status_from_mexc_record("FINISHED", None, Some("0xdef")),
        WithdrawStatus::Completed
    );
}

#[test]
fn withdraw_status_keeps_non_completed_states_unchanged() {
    assert_eq!(
        withdraw_status_from_mexc_record("FAILED", None, None),
        WithdrawStatus::Failed
    );
    assert_eq!(
        withdraw_status_from_mexc_record("PENDING", Some("0xabc"), None),
        WithdrawStatus::Pending
    );
    assert_eq!(
        withdraw_status_from_mexc_record("UNKNOWN_STATUS", None, None),
        WithdrawStatus::Unknown
    );
    assert_eq!(
        withdraw_status_from_mexc_record("UNKNOWN_STATUS", Some("0xabc"), None),
        WithdrawStatus::Completed
    );
}

#[test]
fn parse_mexc_transaction_fee_accepts_valid_values() {
    assert_eq!(parse_mexc_transaction_fee("0"), Some(0.0));
    assert_eq!(parse_mexc_transaction_fee(" 0.123 "), Some(0.123));
}

#[test]
fn parse_mexc_transaction_fee_rejects_invalid_values() {
    assert_eq!(parse_mexc_transaction_fee(""), None);
    assert_eq!(parse_mexc_transaction_fee("-0.1"), None);
    assert_eq!(parse_mexc_transaction_fee("abc"), None);
}

/// Audit S3: the limit sits at the cap, inside MEXC's band, rounded strict.
#[test]
fn the_ioc_price_is_the_cap_inside_the_price_band() {
    let limit = PriceLimit {
        reference_price: 2.611,
        max_slippage_bps: 200.0,
    };
    let filters = |band: Option<Decimal>| SymbolFilters {
        quote_precision: Some(3),
        buy_price_band: band,
        sell_price_band: band,
        ..SymbolFilters::default()
    };
    let price = |side: &str, band| MexcClient::ioc_limit_price(side, &limit, Some(&filters(band))).unwrap();

    // ICP bands are 20%, so the 2% cap applies: 2.55878 rounds up, 2.66322 down.
    assert_eq!(price("sell", Some(Decimal::new(2, 1))), Decimal::new(2559, 3));
    assert_eq!(price("buy", Some(Decimal::new(2, 1))), Decimal::new(2663, 3));
    // A 0.5% band (BTC, ETH) clamps the cap to 0.45%.
    assert_eq!(price("sell", Some(Decimal::new(5, 3))), Decimal::new(2600, 3));
    assert_eq!(price("buy", Some(Decimal::new(5, 3))), Decimal::new(2622, 3));
    assert!(
        MexcClient::ioc_limit_price(
            "sell",
            &PriceLimit {
                reference_price: 0.0,
                ..limit
            },
            None
        )
        .is_err()
    );
}

#[test]
fn an_ioc_buy_never_spends_more_than_its_quote_input() {
    let filters = SymbolFilters {
        base_precision: Some(2),
        ..SymbolFilters::default()
    };
    let price = Decimal::new(2663, 3);
    let quantity = MexcClient::ioc_buy_quantity(Decimal::new(10, 0), price, Some(&filters), "ICPUSDT").unwrap();
    assert_eq!(quantity, Decimal::new(375, 2));
    assert!(quantity * price <= Decimal::new(10, 0));
}

#[test]
fn a_cancelled_order_reports_what_filled_before_the_cancel() {
    assert!(MexcClient::is_final_order_status(OrderStatus::Canceled));
    assert!(MexcClient::is_final_order_status(OrderStatus::PartiallyCanceled));
    assert!(!MexcClient::is_final_order_status(OrderStatus::New));
    assert!(!MexcClient::is_final_order_status(OrderStatus::PartiallyFilled));

    let nothing = MexcClient::final_fill_report("sell", Decimal::ZERO, Decimal::ZERO, 5.0).unwrap();
    assert_eq!((nothing.input_consumed, nothing.output_received), (0.0, 0.0));
    let partial = MexcClient::final_fill_report("sell", Decimal::new(45, 2), Decimal::new(117495, 5), 0.0).unwrap();
    assert_eq!((partial.input_consumed, partial.output_received), (0.45, 1.17495));
}

/// Serves one canned response per connection and returns each request's head.
async fn serve(responses: Vec<(u16, &'static str)>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            requests.push(
                String::from_utf8_lossy(&request)
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_string(),
            );
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    (endpoint, server)
}

fn client_at(endpoint: String) -> MexcClient {
    MexcClient {
        inner: tokio::sync::Mutex::new(MexcSpotApiClientWithAuthentication::new(
            mexc_rs::spot::MexcSpotApiEndpoint::Custom(endpoint),
            "test-key".into(),
            "test-secret".into(),
        )),
        symbol_filters: tokio::sync::Mutex::new(HashMap::new()),
        coin_config: CoinConfigCache::default(),
        http: reqwest::Client::new(),
    }
}

const ACCEPTED: &str = r#"{"symbol":"ICPUSDT","orderId":"market-1","orderListId":-1,"price":"0","origQty":"0.45","type":"MARKET","side":"SELL","transactTime":1793030000000}"#;

async fn submit(endpoint: String) -> Result<(String, String), CexBackendError> {
    let client = client_at(endpoint);
    let ex = client.inner.lock().await;
    client
        .submit_order(
            &ex,
            &["ICPUSDT".to_string()],
            OrderSide::Sell,
            Some(MarketOrder {
                quantity: Some(Decimal::new(45, 2)),
                quote_order_quantity: None,
            }),
            Some(IocOrder {
                quantity: Decimal::new(45, 2),
                price: Decimal::new(2559, 3),
            }),
            Some("lqclient1"),
            "ICP_USDT",
            "sell",
        )
        .await
}

/// Audit S3: IOC first; a refused IOC order placed nothing, so a market
/// order follows under the same client id.
#[tokio::test]
async fn a_refused_ioc_order_falls_back_to_a_market_order() {
    let refused = r#"{"code":30041,"msg":"current order type can not place order"}"#;
    let (endpoint, server) = serve(vec![(400, refused), (200, ACCEPTED)]).await;

    let placed = tokio::time::timeout(std::time::Duration::from_secs(5), submit(endpoint))
        .await
        .unwrap()
        .expect("the market order is placed");

    assert_eq!(placed, ("ICPUSDT".to_string(), "market-1".to_string()));
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0].contains("type=IMMEDIATE_OR_CANCEL") && requests[0].contains("price=2.559"),
        "{}",
        requests[0]
    );
    assert!(
        requests[1].contains("type=MARKET") && !requests[1].contains("price="),
        "{}",
        requests[1]
    );
    assert!(
        requests
            .iter()
            .all(|request| request.contains("newClientOrderId=lqclient1"))
    );
}

/// An answer that may hide a placed order never gets a second order.
#[tokio::test]
async fn an_ambiguous_ioc_error_sends_no_market_order() {
    let failed = r#"{"code":-1,"msg":"internal error"}"#;
    let (endpoint, server) = serve(vec![(500, failed)]).await;

    let error = tokio::time::timeout(std::time::Duration::from_secs(5), submit(endpoint))
        .await
        .unwrap()
        .expect_err("no fallback on an ambiguous answer");

    assert!(error.to_string().contains("Swap err"), "{error}");
    assert_eq!(server.await.unwrap().len(), 1);
}

/// Only an unsupported order type sends the market order. Any other refusal
/// of the IOC order, such as its price, must not become an order without a limit.
#[tokio::test]
async fn a_refused_ioc_price_sends_no_market_order() {
    let refused = r#"{"code":30010,"msg":"no valid trade price"}"#;
    // The server would accept a market order, so only the refusal rule stops one.
    let (endpoint, server) = serve(vec![(400, refused), (200, ACCEPTED)]).await;

    let placed = tokio::time::timeout(std::time::Duration::from_secs(5), submit(endpoint))
        .await
        .unwrap();
    server.abort();

    assert!(placed.is_err(), "no market order after a price refusal: {placed:?}");
}

fn coin(coin: &str, networks: &[(&str, &str, bool, &str)]) -> mexc_rs::spot::v3::coin_config::CoinConfig {
    serde_json::from_value(serde_json::json!({
        "coin": coin,
        "networkList": networks
            .iter()
            .map(|(network, net_work, enabled, fee)| serde_json::json!({
                "coin": coin,
                "network": network,
                "netWork": net_work,
                "withdrawEnable": enabled,
                "withdrawFee": fee,
                "withdrawMin": "0.01",
            }))
            .collect::<Vec<_>>(),
    }))
    .unwrap()
}

#[test]
fn withdraw_terms_follow_the_network_a_withdrawal_would_use() {
    let config = vec![
        coin("SOL", &[("Solana(SOL)", "SOL", true, "0.008")]),
        coin(
            "ETH",
            &[
                ("Ethereum(ERC20)", "ETH", true, "0.0015"),
                ("Arbitrum One", "ARB", true, "0.0001"),
            ],
        ),
    ];

    let sol = find_withdraw_terms(&config, "SOL", "SOL").expect("SOL terms");
    assert_eq!(
        sol,
        WithdrawTerms {
            fee: 0.008,
            minimum: 0.01
        }
    );
    let eth = find_withdraw_terms(&config, "ETH", "ETH").expect("ETH terms");
    assert_eq!(eth.fee, 0.0015, "the Ethereum network, not Arbitrum");
}

#[test]
fn withdraw_terms_refuse_a_disabled_or_unknown_network() {
    let config = vec![coin("USDC", &[("Ethereum(ERC20)", "ETH", false, "1")])];

    let disabled = find_withdraw_terms(&config, "USDC", "ETH").unwrap_err();
    assert!(disabled.contains("disabled"), "{disabled}");
    let unknown = find_withdraw_terms(&config, "BTC", "BTC").unwrap_err();
    assert!(unknown.contains("no withdrawal terms"), "{unknown}");
}

/// A live BTCUSDT `exchangeInfo` shape: MEXC sends only PERCENT_PRICE_BY_SIDE,
/// so the order-size limits come from the top-level fields or not at all.
#[test]
fn symbol_limits_are_read_from_the_fields_mexc_actually_publishes() {
    let info = serde_json::json!({
        "symbol": "BTCUSDT",
        "baseAssetPrecision": 8,
        "quotePrecision": 2,
        "quoteAmountPrecision": "1",
        "baseSizePrecision": "0.000001",
        "filters": [{
            "filterType": "PERCENT_PRICE_BY_SIDE",
            "bidMultiplierUp": "0.005",
            "askMultiplierDown": "0.005"
        }]
    });

    let filters = MexcClient::symbol_filters_from_info(&info);

    assert_eq!(filters.min_notional, Decimal::from_str_exact("1").ok());
    assert_eq!(filters.min_qty, Decimal::from_str_exact("0.000001").ok());
    assert_eq!(filters.step_size, None, "a minimum is not a step");
    assert_eq!(filters.base_precision, Some(8));
}

#[test]
fn a_zero_limit_is_absent_rather_than_an_enforced_floor() {
    let info = serde_json::json!({
        "symbol": "CKBTCBTC",
        "quoteAmountPrecision": "0.000005",
        "baseSizePrecision": "0",
        "filters": []
    });

    let filters = MexcClient::symbol_filters_from_info(&info);

    assert_eq!(filters.min_notional, Decimal::from_str_exact("0.000005").ok());
    assert_eq!(filters.min_qty, None);
}

#[test]
fn an_explicit_filter_outranks_the_top_level_fallback() {
    let info = serde_json::json!({
        "symbol": "BTCUSDT",
        "quoteAmountPrecision": "1",
        "baseSizePrecision": "0.000001",
        "filters": [
            {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.05"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "10"}
        ]
    });

    let filters = MexcClient::symbol_filters_from_info(&info);

    assert_eq!(filters.min_notional, Decimal::from_str_exact("10").ok());
    assert_eq!(filters.step_size, Decimal::from_str_exact("0.01").ok());
    assert_eq!(filters.min_qty, Decimal::from_str_exact("0.05").ok());
}

/// ICP_USDT: `baseSizePrecision` 0.0001 against `baseAssetPrecision` 2. Used as
/// a step it rounded to four decimals, which MEXC refused (liquidation 1620).
#[test]
fn a_minimum_order_size_finer_than_the_venue_precision_does_not_set_the_scale() {
    let info = serde_json::json!({
        "symbol": "ICPUSDT",
        "baseAssetPrecision": 2,
        "quotePrecision": 3,
        "quoteAmountPrecision": "1",
        "baseSizePrecision": "0.0001",
        "filters": []
    });
    let filters = MexcClient::symbol_filters_from_info(&info);
    let sell = |filters: &SymbolFilters| {
        let amount = Decimal::from_str_exact("7.8073757").expect("test amount");
        let (_, qty, _) = MexcClient::prepare_sell_order(amount, Some(filters), "ICP_USDT").expect("a valid sell");
        qty
    };

    assert_eq!(sell(&filters), Decimal::from_str_exact("7.8").ok());

    // A real LOT_SIZE filter still decides where one is sent.
    let with_lot_size = SymbolFilters {
        step_size: Decimal::from_str_exact("0.001").ok(),
        ..filters
    };
    assert_eq!(sell(&with_lot_size), Decimal::from_str_exact("7.807").ok());
}
