//! Method lookup, the route check, withdrawals and their status. Every method
//! name and network here is what Kraken returns today.
use liquidium_pipeline_connectors::backend::cex_backend::{
    CexBackend, FundingRoute, FundingRouteError, WithdrawError, WithdrawStatus,
};
use rust_decimal::Decimal;

use super::support::btc_usd_client;
use crate::swappers::kraken::{
    api::{
        KrakenApiError, KrakenDepositAddress, KrakenFundingMethod, KrakenTransferStatus, KrakenWithdrawal,
        KrakenWithdrawalAddress, MockKrakenApi,
    },
    funding::{WITHDRAWAL_HISTORY_MAX, WITHDRAWAL_RECENT_PAGE, addresses_match, unique_method},
};

fn method(name: &str, network: Option<&str>) -> KrakenFundingMethod {
    KrakenFundingMethod {
        method: name.to_string(),
        network: network.map(str::to_string),
        minimum: Decimal::ZERO,
    }
}

fn bitcoin_method(minimum: Decimal) -> KrakenFundingMethod {
    KrakenFundingMethod {
        minimum,
        ..method("Bitcoin", Some("Bitcoin"))
    }
}

fn verified(address: &str) -> KrakenWithdrawalAddress {
    KrakenWithdrawalAddress {
        address: address.into(),
        method: "Bitcoin".into(),
        key: "verified-key".into(),
        verified: true,
    }
}

fn route(deposit_amount: f64, withdraw_address: &str) -> FundingRoute {
    FundingRoute {
        deposit_asset: "BTC".into(),
        deposit_network: "bitcoin".into(),
        deposit_amount,
        withdraw_asset: "BTC".into(),
        withdraw_network: "bitcoin".into(),
        withdraw_address: withdraw_address.into(),
    }
}

/// An API whose BTC deposit has `minimum` and the given memo, and whose
/// verified BTC withdrawal address is `bc1qdestination`.
fn funding_api(minimum: Decimal, tag: Option<&'static str>) -> MockKrakenApi {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .withf(|asset| asset == "XBT")
        .returning(move |_| Ok(vec![bitcoin_method(minimum)]));
    api.expect_deposit_addresses()
        .withf(|asset, method| asset == "XBT" && method == "Bitcoin")
        .returning(move |_, _| {
            Ok(vec![KrakenDepositAddress {
                address: "bc1qdeposit".into(),
                tag: tag.map(str::to_string),
            }])
        });
    api.expect_withdrawal_methods()
        .withf(|asset| asset == "XBT")
        .returning(|_| Ok(vec![bitcoin_method(Decimal::new(1, 3))]));
    api.expect_withdrawal_addresses()
        .withf(|asset| asset == "XBT")
        .returning(|_| Ok(vec![verified("bc1qdestination")]));
    api
}

/// A method name cannot identify a chain: `Tether USD (SPL)` contains "eth",
/// and `Ethereum (Polygon)` settles on Polygon. The reported network picks one.
#[test]
fn withdrawal_methods_resolve_to_the_one_ethereum_settlement() {
    let usdt = vec![
        method("USDT - Avalanche C-Chain", Some("Avalanche C-Chain")),
        method("Tether USD (ERC20)", Some("Ethereum")),
        method("Tether USD (TRC20)", Some("Tron")),
        method("Tether USD (SPL)", Some("Solana")),
        method("Tether USD (Polygon)", Some("Polygon")),
        method("USDT0 - Sei (EVM)", Some("Sei - EVM")),
    ];
    assert_eq!(
        unique_method(usdt, "USDT", "ETH", "withdrawal").unwrap().method,
        "Tether USD (ERC20)"
    );

    let eth = vec![
        method("Ether", Some("Ethereum")),
        method("Ethereum (Polygon)", Some("Polygon")),
        method("Arbitrum One", Some("Arbitrum One")),
        method("ETH - Base", Some("Base")),
    ];
    assert_eq!(unique_method(eth, "ETH", "ETH", "withdrawal").unwrap().method, "Ether");

    let usdc = vec![
        method("USDC", Some("Ethereum")),
        method("USDC (Polygon)", Some("Polygon (USDC.e)")),
        method("Optimism", Some("Optimism (USDC.e)")),
    ];
    assert_eq!(unique_method(usdc, "USDC", "ETH", "withdrawal").unwrap().method, "USDC");
}

/// Deposit methods report no network, so each accepted spelling is named.
#[test]
fn deposit_methods_resolve_without_a_reported_network() {
    let eth = vec![
        method("Ether (Hex)", None),
        method("ETH - Polygon (Unified)", None),
        method("ETH - Arbitrum One (Unified)", None),
        method("zkSync Era", None),
        method("Linea", None),
    ];
    assert_eq!(
        unique_method(eth, "ETH", "ETH", "deposit").unwrap().method,
        "Ether (Hex)"
    );

    let usdc = vec![
        method("USDC - Ethereum (Unified)", None),
        method("USDC.e - Optimism (Unified)", None),
        method("USDC - Stellar XLM", None),
    ];
    assert_eq!(
        unique_method(usdc, "USDC", "ETH", "deposit").unwrap().method,
        "USDC - Ethereum (Unified)"
    );

    // A chain this pipeline does not translate matches by name.
    let icp = vec![method("Internet Computer Protocol (ICP)", None)];
    assert_eq!(
        unique_method(icp, "ICP", "ICP", "deposit").unwrap().method,
        "Internet Computer Protocol (ICP)"
    );
}

/// Lightning and wrapped BTC on Ethereum live under BTC too, and neither is
/// the on-chain Bitcoin settlement.
#[test]
fn bitcoin_excludes_lightning_and_wrapped_variants() {
    let btc = vec![
        method("Bitcoin", Some("Bitcoin")),
        method("Bitcoin Lightning", Some("Lightning")),
        method("kBTC - Ethereum", Some("Ethereum (kBTC)")),
    ];
    assert_eq!(
        unique_method(btc.clone(), "BTC", "BTC", "withdrawal").unwrap().method,
        "Bitcoin"
    );

    let deposits = vec![
        method("Bitcoin", None),
        method("Bitcoin Lightning", None),
        method("kBTC - Ethereum (Unified)", None),
    ];
    assert_eq!(
        unique_method(deposits, "BTC", "BTC", "deposit").unwrap().method,
        "Bitcoin"
    );

    // Asking for ETH never selects the kBTC wrapper.
    assert!(unique_method(btc, "BTC", "ETH", "withdrawal").is_err());
}

/// Liquidation 1615: Kraken stored the verified address in lower case, and
/// the pipeline held the EIP-55 form.
#[test]
fn a_checksummed_destination_matches_its_lowercased_stored_address() {
    assert!(addresses_match(
        "0xa21522b91e8ed11a9afcc09f718319277b75c381",
        "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
    ));
    assert!(!addresses_match(
        "0xd9a5b3a87e971e09c30829206506b4cc0548984c",
        "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
    ));
}

/// Base58 encodes different values in `A` and `a`.
#[test]
fn non_hex_addresses_are_compared_exactly() {
    assert!(addresses_match(
        "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
        "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
    ));
    assert!(!addresses_match(
        "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
        "1bvbmseystwetqtfn5au4m4gfg7xjanvn2"
    ));
}

#[tokio::test]
async fn a_route_to_a_verified_address_above_the_minimum_passes() {
    let client = btc_usd_client(funding_api(Decimal::new(1, 4), Some("memo-7")));

    let deposit = client.get_deposit_address("BTC", "bitcoin").await.unwrap();
    assert_eq!(deposit.address, "bc1qdeposit");
    assert_eq!(deposit.tag.as_deref(), Some("memo-7"));
    client
        .check_funding_route(&route(0.1, "bc1qdestination"))
        .await
        .expect("a verified route");
}

#[tokio::test]
async fn a_deposit_below_the_minimum_is_refused_before_the_withdrawal_is_read() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .returning(|_| Ok(vec![bitcoin_method(Decimal::new(1, 2))]));
    api.expect_deposit_addresses().returning(|_, _| {
        Ok(vec![KrakenDepositAddress {
            address: "bc1qdeposit".into(),
            tag: None,
        }])
    });
    api.expect_withdrawal_methods().never();
    api.expect_withdrawal_addresses().never();

    let error = btc_usd_client(api)
        .check_funding_route(&route(0.001, "bc1qdestination"))
        .await
        .expect_err("below the deposit minimum");

    assert!(matches!(error, FundingRouteError::BelowMinimum(_)), "{error}");
}

#[tokio::test]
async fn an_unverified_destination_is_unsupported() {
    let error = btc_usd_client(funding_api(Decimal::ZERO, None))
        .check_funding_route(&route(0.1, "bc1qsomewhere-else"))
        .await
        .expect_err("not a verified address");

    assert!(matches!(error, FundingRouteError::Unsupported(_)), "{error}");
}

/// An ICP transfer cannot carry Kraken's memo, so the deposit would not be credited.
#[tokio::test]
async fn an_icp_deposit_that_needs_a_memo_is_unsupported() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .returning(|_| Ok(vec![method("Internet Computer Protocol (ICP)", None)]));
    api.expect_deposit_addresses().returning(|_, _| {
        Ok(vec![KrakenDepositAddress {
            address: "icp-account".into(),
            tag: Some("123".into()),
        }])
    });
    let mut icp_route = route(10.0, "bc1qdestination");
    icp_route.deposit_asset = "ICP".into();
    icp_route.deposit_network = "ICP".into();

    let error = btc_usd_client(api)
        .check_funding_route(&icp_route)
        .await
        .expect_err("a memo is needed");

    assert!(matches!(error, FundingRouteError::Unsupported(_)), "{error}");
}

#[tokio::test]
async fn a_failed_read_leaves_the_route_unknown() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .returning(|_| Err(KrakenApiError::Transport("timed out".into())));

    let error = btc_usd_client(api)
        .check_funding_route(&route(0.1, "bc1qdestination"))
        .await
        .expect_err("the read failed");

    assert!(matches!(error, FundingRouteError::Unavailable(_)), "{error}");
}

#[tokio::test]
async fn the_withdraw_terms_are_the_method_minimum_and_its_quoted_fee() {
    let mut api = funding_api(Decimal::ZERO, None);
    api.expect_withdrawal_fee()
        .withf(|asset, key, amount| asset == "XBT" && key == "verified-key" && *amount == Decimal::new(1, 3))
        .once()
        .returning(|_, _, _| Ok(Decimal::new(15, 5)));

    let terms = btc_usd_client(api).get_withdraw_terms("BTC", "bitcoin").await.unwrap();

    assert_eq!(terms.minimum, 0.001);
    assert_eq!(terms.fee, 0.000_15);
}

/// Kraken refuses a small amount with no code, so the client names it.
#[tokio::test]
async fn a_withdrawal_below_the_method_minimum_is_refused_before_submission() {
    let mut api = funding_api(Decimal::ZERO, None);
    api.expect_withdraw().never();

    let error = btc_usd_client(api)
        .withdraw("BTC", "bitcoin", "bc1qdestination", 0.000_1, "lq1w0")
        .await
        .expect_err("below the minimum");

    assert!(matches!(error, WithdrawError::BelowMinimum(_)), "{error}");
}

#[tokio::test]
async fn each_withdrawal_failure_keeps_its_kind() {
    let cases: [(KrakenApiError, fn(&WithdrawError) -> bool); 3] = [
        (
            KrakenApiError::Rejected("EFunding:Insufficient funds".into()),
            |error| matches!(error, WithdrawError::Refused(_)),
        ),
        (KrakenApiError::Unreachable("connection refused".into()), |error| {
            matches!(error, WithdrawError::Unreachable(_))
        }),
        (KrakenApiError::Transport("operation timed out".into()), |error| {
            matches!(error, WithdrawError::Ambiguous(_))
        }),
    ];
    for (failure, expected) in cases {
        let mut api = funding_api(Decimal::ZERO, None);
        api.expect_withdraw().once().return_once(move |_, _, _, _| Err(failure));
        let error = btc_usd_client(api)
            .withdraw("BTC", "bitcoin", "bc1qdestination", 0.01, "lq1w0")
            .await
            .expect_err("the withdrawal failed");
        assert!(expected(&error), "{error:?}");
    }
}

/// Review finding 9: nothing is sent before the reads succeed, so a failed
/// read must take the outage wait, not spend a retry on a refusal.
#[tokio::test]
async fn a_failed_read_before_a_withdrawal_waits_like_an_outage() {
    for failure in [
        KrakenApiError::Unreachable("connection refused".into()),
        KrakenApiError::Transport("operation timed out".into()),
    ] {
        let mut api = MockKrakenApi::new();
        api.expect_withdrawal_methods().once().return_once(move |_| Err(failure));
        api.expect_withdraw().never();
        let error = btc_usd_client(api)
            .withdraw("BTC", "bitcoin", "bc1qdestination", 0.01, "lq1w0")
            .await
            .expect_err("the read failed");
        assert!(matches!(error, WithdrawError::Unreachable(_)), "{error:?}");
    }

    let mut api = funding_api(Decimal::ZERO, None);
    api.expect_withdraw().never();
    let error = btc_usd_client(api)
        .withdraw("BTC", "bitcoin", "bc1qunverified", 0.01, "lq1w0")
        .await
        .expect_err("not a verified address");
    assert!(matches!(error, WithdrawError::Refused(_)), "{error:?}");
}

/// A pending withdrawal is read every cycle and is nearly always among the
/// newest, so the recent page answers first.
#[tokio::test]
async fn a_status_read_tries_the_recent_page_before_the_whole_history() {
    let recent = || KrakenWithdrawal {
        ref_id: "RECENT-1".into(),
        tx_id: Some("0xrecent".into()),
        fee: Decimal::new(1, 4),
        status: KrakenTransferStatus::Completed,
    };
    let old = || KrakenWithdrawal {
        ref_id: "OLD-1".into(),
        tx_id: None,
        fee: Decimal::new(2, 4),
        status: KrakenTransferStatus::Pending,
    };
    let mut api = MockKrakenApi::new();
    api.expect_withdrawals()
        .withf(|asset, limit| asset == "XBT" && *limit == WITHDRAWAL_RECENT_PAGE)
        .times(3)
        .returning(move |_, _| Ok(vec![recent()]));
    api.expect_withdrawals()
        .withf(|asset, limit| asset == "XBT" && *limit == WITHDRAWAL_HISTORY_MAX)
        .times(2)
        .returning(move |_, _| Ok(vec![recent(), old()]));
    let client = btc_usd_client(api);

    let snapshot = client
        .get_withdraw_status_snapshot_by_id("BTC", "RECENT-1")
        .await
        .unwrap();
    assert_eq!(snapshot.status, WithdrawStatus::Completed);
    assert_eq!(snapshot.txid.as_deref(), Some("0xrecent"));

    let snapshot = client.get_withdraw_status_snapshot_by_id("BTC", "OLD-1").await.unwrap();
    assert_eq!(snapshot.status, WithdrawStatus::Pending);

    // Our own order id is not a Kraken ref id, so it is never found.
    let snapshot = client.get_withdraw_status_snapshot_by_id("BTC", "lq1w0").await.unwrap();
    assert_eq!(snapshot.status, WithdrawStatus::Unknown);
}

/// Methods and addresses rarely change, so a second route check within the
/// cache window reads nothing from Kraken again.
#[tokio::test]
async fn a_second_route_check_reads_the_cached_methods_and_addresses() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .once()
        .returning(|_| Ok(vec![bitcoin_method(Decimal::ZERO)]));
    api.expect_deposit_addresses().once().returning(|_, _| {
        Ok(vec![KrakenDepositAddress {
            address: "bc1qdeposit".into(),
            tag: None,
        }])
    });
    api.expect_withdrawal_methods()
        .once()
        .returning(|_| Ok(vec![bitcoin_method(Decimal::ZERO)]));
    api.expect_withdrawal_addresses()
        .once()
        .returning(|_| Ok(vec![verified("bc1qdestination")]));
    let client = btc_usd_client(api);

    for _ in 0..2 {
        client
            .check_funding_route(&route(0.1, "bc1qdestination"))
            .await
            .expect("a verified route");
    }
}

/// A failed read is not kept, so the next check reads Kraken again.
#[tokio::test]
async fn a_failed_read_is_not_cached() {
    let mut api = MockKrakenApi::new();
    let mut reads = 0;
    api.expect_deposit_methods().times(2).returning(move |_| {
        reads += 1;
        if reads == 1 {
            return Err(KrakenApiError::Transport("timed out".into()));
        }
        Ok(vec![bitcoin_method(Decimal::ZERO)])
    });
    api.expect_deposit_addresses().returning(|_, _| {
        Ok(vec![KrakenDepositAddress {
            address: "bc1qdeposit".into(),
            tag: None,
        }])
    });
    let client = btc_usd_client(api);

    assert!(client.get_deposit_address("BTC", "bitcoin").await.is_err());
    assert!(client.get_deposit_address("BTC", "bitcoin").await.is_ok());
}
