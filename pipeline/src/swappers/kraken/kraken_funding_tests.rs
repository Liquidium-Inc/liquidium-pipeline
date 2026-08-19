//! Tests for `kraken_funding`. Every method name and network in them is
//! what Kraken returns today.

use super::super::test_support::btc_usd_client;
use super::*;
use crate::swappers::kraken::kraken_api::{
    KrakenDepositAddress, KrakenWithdrawal, KrakenWithdrawalAddress, MockKrakenApi,
};

fn method(name: &str, network: Option<&str>) -> KrakenFundingMethod {
    KrakenFundingMethod {
        method: name.to_string(),
        network: network.map(str::to_string),
        minimum: Decimal::ZERO,
    }
}

/// Every method name and network below is what Kraken returns today.
///
/// A method name cannot identify a chain, and both counterexamples are live:
/// `Tether USD (SPL)` matches a substring search for "eth" because "Tether"
/// does, and `Ethereum (Polygon)` contains "ethereum" while settling on
/// Polygon. Selecting on the reported network instead is what makes each of
/// these resolve to exactly one method.
#[test]
fn withdrawal_methods_resolve_to_the_one_ethereum_settlement() {
    let usdt = vec![
        method("USDT - Avalanche C-Chain", Some("Avalanche C-Chain")),
        method("Tether USD (ERC20)", Some("Ethereum")),
        method("Tether USD (TRC20)", Some("Tron")),
        method("Tether USD (SPL)", Some("Solana")),
        method("Tether USD (Aptos)", Some("Aptos")),
        method("Tether USD (Polygon)", Some("Polygon")),
        method("USDT0 - Sei (EVM)", Some("Sei - EVM")),
    ];
    let chosen = unique_method(usdt, "USDT", "ETH", "withdrawal").expect("exactly one Ethereum method");
    assert_eq!(chosen.method, "Tether USD (ERC20)");

    let eth = vec![
        method("Ether", Some("Ethereum")),
        method("Ethereum (Polygon)", Some("Polygon")),
        method("Arbitrum One", Some("Arbitrum One")),
        method("ETH - Base", Some("Base")),
    ];
    let chosen = unique_method(eth, "ETH", "ETH", "withdrawal").expect("exactly one Ethereum method");
    assert_eq!(chosen.method, "Ether");

    let usdc = vec![
        method("USDC", Some("Ethereum")),
        method("USDC (Polygon)", Some("Polygon (USDC.e)")),
        method("Optimism", Some("Optimism (USDC.e)")),
    ];
    let chosen = unique_method(usdc, "USDC", "ETH", "withdrawal").expect("exactly one Ethereum method");
    assert_eq!(chosen.method, "USDC");
}

/// Deposit methods report no network at all, so the name is the only signal
/// and each accepted spelling has to be recognised deliberately.
#[test]
fn deposit_methods_resolve_without_a_reported_network() {
    let eth = vec![
        method("Ether (Hex)", None),
        method("ETH - Polygon (Unified)", None),
        method("ETH - Arbitrum One (Unified)", None),
        method("zkSync Era", None),
        method("Linea", None),
    ];
    let chosen = unique_method(eth, "ETH", "ETH", "deposit").expect("native ETH deposit");
    assert_eq!(chosen.method, "Ether (Hex)");

    let usdc = vec![
        method("USDC - Ethereum (Unified)", None),
        method("USDC.e - Optimism (Unified)", None),
        method("USDC - Stellar XLM", None),
    ];
    let chosen = unique_method(usdc, "USDC", "ETH", "deposit").expect("Ethereum USDC deposit");
    assert_eq!(chosen.method, "USDC - Ethereum (Unified)");

    // An asset whose chain this pipeline does not translate falls back to a
    // plain name match.
    let icp = vec![method("Internet Computer Protocol (ICP)", None)];
    let chosen = unique_method(icp, "ICP", "ICP", "deposit").expect("ICP deposit");
    assert_eq!(chosen.method, "Internet Computer Protocol (ICP)");
}

/// Wrapped BTC on Ethereum and Lightning both live under the BTC asset, and
/// neither is the on-chain Bitcoin settlement the bridge expects.
#[test]
fn bitcoin_selection_excludes_lightning_and_wrapped_variants() {
    let btc = vec![
        method("Bitcoin", Some("Bitcoin")),
        method("Bitcoin Lightning", Some("Lightning")),
        method("kBTC - Ethereum", Some("Ethereum (kBTC)")),
    ];
    let chosen = unique_method(btc.clone(), "BTC", "BTC", "withdrawal").expect("on-chain Bitcoin");
    assert_eq!(chosen.method, "Bitcoin");

    // Deposits carry no network, so the name-based path must exclude them too.
    let deposits = vec![
        method("Bitcoin", None),
        method("Bitcoin Lightning", None),
        method("kBTC - Ethereum (Unified)", None),
    ];
    let chosen = unique_method(deposits, "BTC", "BTC", "deposit").expect("on-chain Bitcoin");
    assert_eq!(chosen.method, "Bitcoin");

    // Asking for ETH must never select the kBTC-on-Ethereum wrapper.
    assert!(unique_method(btc, "BTC", "ETH", "withdrawal").is_err());
}

#[test]
fn bitcoin_network_does_not_select_lightning() {
    let methods = vec![
        KrakenFundingMethod {
            method: "Bitcoin".into(),
            network: Some("Bitcoin".into()),
            minimum: Decimal::ZERO,
        },
        KrakenFundingMethod {
            method: "Lightning".into(),
            network: Some("Lightning".into()),
            minimum: Decimal::ZERO,
        },
    ];
    assert_eq!(
        unique_method(methods, "BTC", "bitcoin", "deposit").unwrap().method,
        "Bitcoin"
    );
}

/// The real pairing that failed liquidation 1615: Kraken had stored the
/// verified address lowercased, the pipeline held the EIP-55 checksummed
/// form, and the exact comparison reported a verified destination as
/// unverified after the collateral was already seized.
#[test]
fn a_checksummed_destination_matches_its_lowercased_stored_address() {
    assert!(addresses_match(
        "0xa21522b91e8ed11a9afcc09f718319277b75c381",
        "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
    ));
}

#[test]
fn a_different_hex_address_still_does_not_match() {
    assert!(!addresses_match(
        "0xd9a5b3a87e971e09c30829206506b4cc0548984c",
        "0xa21522B91E8Ed11A9AFcC09f718319277b75c381"
    ));
}

/// Base58 encodes different values in `A` and `a`, so folding case there
/// could match an address the operator never verified.
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
async fn funding_uses_existing_deposit_address_and_exact_verified_withdrawal() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods()
        .withf(|asset| asset == "XBT")
        .times(2)
        .returning(|_| {
            Ok(vec![KrakenFundingMethod {
                method: "Bitcoin".into(),
                network: Some("Bitcoin".into()),
                minimum: Decimal::ZERO,
            }])
        });
    api.expect_deposit_addresses()
        .withf(|asset, method| asset == "XBT" && method == "Bitcoin")
        .times(2)
        .returning(|_, _| {
            Ok(vec![KrakenDepositAddress {
                address: "bc1qdeposit".into(),
                tag: Some("memo-7".into()),
            }])
        });
    api.expect_withdrawal_methods()
        .withf(|asset| asset == "XBT")
        .once()
        .returning(|_| {
            Ok(vec![KrakenFundingMethod {
                method: "Bitcoin".into(),
                network: Some("Bitcoin".into()),
                minimum: Decimal::ZERO,
            }])
        });
    api.expect_withdrawal_addresses()
        .withf(|asset| asset == "XBT")
        .once()
        .returning(|_| {
            Ok(vec![KrakenWithdrawalAddress {
                address: "bc1qdestination".into(),
                method: "Bitcoin".into(),
                key: "verified-key".into(),
                verified: true,
            }])
        });
    let client = btc_usd_client(api);

    let deposit = client
        .get_deposit_address("BTC", "bitcoin")
        .await
        .expect("existing address");
    assert_eq!(deposit.address, "bc1qdeposit");
    assert_eq!(deposit.tag.as_deref(), Some("memo-7"));
    client
        .validate_funding_route(&FundingRoutePreflight {
            deposit_asset: "BTC".into(),
            deposit_network: "bitcoin".into(),
            withdraw_asset: "BTC".into(),
            withdraw_network: "bitcoin".into(),
            withdraw_address: "bc1qdestination".into(),
            deposit_amount: 0.1,
            withdraw_amount: 0.1,
        })
        .await
        .expect("verified route");
}

#[tokio::test]
async fn funding_preflight_rejects_below_minimum_deposit_before_planning() {
    let mut api = MockKrakenApi::new();
    api.expect_deposit_methods().once().returning(|_| {
        Ok(vec![KrakenFundingMethod {
            method: "Bitcoin".into(),
            network: Some("Bitcoin".into()),
            minimum: Decimal::new(1, 2),
        }])
    });
    api.expect_deposit_addresses().once().returning(|_, _| {
        Ok(vec![KrakenDepositAddress {
            address: "bc1qdeposit".into(),
            tag: None,
        }])
    });
    api.expect_withdrawal_methods().never();
    api.expect_withdrawal_addresses().never();
    let client = btc_usd_client(api);

    let error = client
        .validate_funding_route(&FundingRoutePreflight {
            deposit_asset: "BTC".into(),
            deposit_network: "bitcoin".into(),
            withdraw_asset: "BTC".into(),
            withdraw_network: "bitcoin".into(),
            withdraw_address: "bc1qdestination".into(),
            deposit_amount: 0.001,
            withdraw_amount: 0.1,
        })
        .await
        .expect_err("deposit minimum must reject preview");

    assert!(error.contains("deposit amount"));
    assert!(error.contains("below Bitcoin minimum"));
}

#[tokio::test]
async fn withdrawal_below_method_minimum_is_rejected_before_submission() {
    let mut api = MockKrakenApi::new();
    api.expect_withdrawal_methods().once().returning(|_| {
        Ok(vec![KrakenFundingMethod {
            method: "Bitcoin".into(),
            network: Some("Bitcoin".into()),
            minimum: Decimal::new(1, 2),
        }])
    });
    api.expect_withdrawal_addresses().once().returning(|_| {
        Ok(vec![KrakenWithdrawalAddress {
            address: "bc1qdestination".into(),
            method: "Bitcoin".into(),
            key: "verified-key".into(),
            verified: true,
        }])
    });
    api.expect_withdraw().never();
    let client = btc_usd_client(api);

    let error = client
        .withdraw("BTC", "bitcoin", "bc1qdestination", 0.001)
        .await
        .expect_err("below minimum");

    // The finalizer decides what becomes of the amount on the variant, not
    // on the wording, so the refusal raised here has to be the typed one.
    assert!(matches!(&error, CexWithdrawError::BelowMinimum(message) if message.contains("below Bitcoin minimum")));
}

/// One withdrawal is looked up every worker cycle for as long as it is
/// pending, and it is nearly always among the newest, so the recent page
/// answers first and the whole history is only read when it does not.
#[tokio::test]
async fn a_withdrawal_status_poll_reads_the_recent_page_before_the_whole_history() {
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
        .withf(|asset, limit| asset == "XBT" && *limit == KRAKEN_WITHDRAWAL_RECENT_PAGE)
        .times(2)
        .returning(move |_, _| Ok(vec![recent()]));
    api.expect_withdrawals()
        .withf(|asset, limit| asset == "XBT" && *limit == KRAKEN_WITHDRAWAL_HISTORY_MAX)
        .once()
        .returning(move |_, _| Ok(vec![recent(), old()]));
    let client = btc_usd_client(api);

    // Found on the first page: the history is never asked for.
    let snapshot = client
        .get_withdraw_status_snapshot_by_id("BTC", "RECENT-1")
        .await
        .expect("recent withdrawal");
    assert_eq!(snapshot.status, WithdrawStatus::Completed);
    assert_eq!(snapshot.txid.as_deref(), Some("0xrecent"));

    // Off the first page: the whole history is read once, and it is there.
    let snapshot = client
        .get_withdraw_status_snapshot_by_id("BTC", "OLD-1")
        .await
        .expect("older withdrawal");
    assert_eq!(snapshot.status, WithdrawStatus::Pending);
}
