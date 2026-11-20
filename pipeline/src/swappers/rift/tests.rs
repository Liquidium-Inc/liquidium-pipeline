//! The wire format as Rift sends it, and the typed failures per status.
use liquidium_pipeline_connectors::backend::rift_backend::{
    RiftAsset, RiftError, RiftOrderRequest, RiftOrderStatus, RiftQuoteRequest, RiftVenueLists,
};

use super::client::quote_body;
use super::protocol::{
    classify_failure, matched_order, matched_order_state, matched_quote, parse_order, parse_order_state, parse_quote,
};

/// A live quote from 2026-11-09, trimmed to the fields the client reads plus the ones it ignores.
const QUOTE: &str = r#"{
  "id": "01a1bf37-536f-7d03-9e45-0e9e9c2e351d",
  "from": "bitcoin.btc",
  "to": "ethereum.usdt",
  "amount_in": "0.05",
  "estimated_amount_out": "3699.353418",
  "estimated_execution_cost": "0.047291",
  "route": [
    {"type": "venue", "venue": "chainflip", "execution": {"mode": "bitcoin"},
     "from": "bitcoin.btc", "to": "ethereum.usdt", "amount_in": "0.05", "amount_out": "3699.400709"},
    {"type": "delivery", "amount_in": "3699.400709", "execution_cost": "0.047291",
     "integrator_fee": "0", "amount_out": "3699.353418"}
  ],
  "expires_at": "2026-11-09T07:40:36.527271Z",
  "venue_allowlist": [],
  "integrator_fee_bps": null,
  "format": "formatted"
}"#;

/// A venue list pins or blocks Rift's venues on the quote; the order inherits
/// the route. Empty lists send nothing, so Rift stays free to choose.
#[test]
fn venue_lists_go_on_the_quote_only_when_set() {
    let mut request = RiftQuoteRequest {
        from: RiftAsset::new("bitcoin", "btc"),
        to: RiftAsset::new("ethereum", "usdt"),
        amount_in: "0.0006".to_string(),
        venues: RiftVenueLists::default(),
    };
    let body = quote_body(&request);
    assert_eq!(body["from"], "bitcoin.btc");
    assert!(body.get("venue_allowlist").is_none());
    assert!(body.get("venue_blocklist").is_none());

    request.venues.block = vec!["rift_otc".to_string()];
    request.venues.allow = vec!["near_intents".to_string(), "relay".to_string()];
    let body = quote_body(&request);
    assert_eq!(body["venue_blocklist"], serde_json::json!(["rift_otc"]));
    assert_eq!(body["venue_allowlist"], serde_json::json!(["near_intents", "relay"]));
}

#[test]
fn a_quote_reads_its_amounts_as_text_and_its_venues_in_order() {
    let quote = parse_quote(QUOTE).expect("quote");
    assert_eq!(quote.id, "01a1bf37-536f-7d03-9e45-0e9e9c2e351d");
    assert_eq!(quote.from, RiftAsset::new("bitcoin", "btc"));
    assert_eq!(
        quote.to,
        RiftAsset::new("Ethereum", "USDT"),
        "assets compare in lower case"
    );
    assert_eq!(quote.amount_in, "0.05");
    assert_eq!(quote.estimated_amount_out, "3699.353418");
    assert_eq!(quote.estimated_execution_cost, "0.047291");
    assert_eq!(quote.expires_at, "2026-11-09T07:40:36.527271Z");
    assert_eq!(quote.venues, vec!["chainflip"]);
}

#[test]
fn numeric_amounts_read_the_same_as_text() {
    let quote = parse_quote(
        r#"{"id":"q","from":"ethereum.usdc","to":"ethereum.usdt","amount_in":10000,
            "estimated_amount_out":10012.08063,"expires_at":"2026-11-09T07:40:47Z"}"#,
    )
    .expect("quote");
    assert_eq!(
        (quote.amount_in.as_str(), quote.estimated_amount_out.as_str()),
        ("10000", "10012.08063")
    );
    assert_eq!(quote.estimated_execution_cost, "0", "no cost field reads as none");
    assert!(quote.venues.is_empty());
}

#[test]
fn an_asset_without_a_chain_is_malformed() {
    let error = parse_quote(
        r#"{"id":"q","from":"btc","to":"ethereum.usdt","amount_in":"1","estimated_amount_out":"1","expires_at":"x"}"#,
    )
    .expect_err("no chain");
    assert!(matches!(error, RiftError::Malformed(_)), "{error}");
}

/// An order answer in the shape of Rift's API reference, with the fields that the client ignores.
const ORDER: &str = r#"{
  "id": "o1", "quote_id": "q1", "vault_id": "v",
  "from": "ethereum.usdc", "to": "ethereum.usdt",
  "amount_in": "100", "amount_out": null, "payout_tx": null, "format": "formatted",
  "deposit_address": "0xdeposit", "deposit_tx": null,
  "to_address": "0x8ba1f109551bD432803012645Ac136ddd64DBA72",
  "refund_address": "0x1111111111111111111111111111111111111111",
  "status": "awaiting_deposit",
  "deposit_deadline": "2026-11-16T07:40:36Z", "created_at": "2026-11-09T07:40:36Z"
}"#;

/// The request that `ORDER` answers.
fn order_request() -> RiftOrderRequest {
    RiftOrderRequest {
        quote_id: "q1".to_string(),
        from: RiftAsset::new("ethereum", "usdc"),
        to: RiftAsset::new("ethereum", "usdt"),
        amount_in: "100".to_string(),
        to_address: "0x8ba1f109551bd432803012645ac136ddd64dba72".to_string(),
        refund_address: "0x1111111111111111111111111111111111111111".to_string(),
    }
}

#[test]
fn an_order_carries_its_deposit_address_deadline_and_status() {
    let order = matched_order(&order_request(), parse_order(ORDER).expect("order")).expect("matched");
    assert_eq!((order.id.as_str(), order.quote_id.as_str()), ("o1", "q1"));
    assert_eq!(order.deposit_address, "0xdeposit");
    assert_eq!(order.status, RiftOrderStatus::AwaitingDeposit);
    assert!(!order.status.funds_moved());
}

#[test]
fn every_order_state_maps_and_only_two_mean_no_funds_moved() {
    for (text, status, moved) in [
        ("awaiting_deposit", RiftOrderStatus::AwaitingDeposit, false),
        ("funded", RiftOrderStatus::Funded, true),
        ("underfunded", RiftOrderStatus::Underfunded, true),
        ("executing", RiftOrderStatus::Executing, true),
        ("delivered", RiftOrderStatus::Delivered, true),
        ("refunded", RiftOrderStatus::Refunded, true),
        ("expired", RiftOrderStatus::Expired, false),
        ("frozen", RiftOrderStatus::Frozen, true),
        ("settling", RiftOrderStatus::Unknown("settling".to_string()), true),
    ] {
        let state = parse_order_state(&format!(r#"{{"id":"o1","status":"{text}"}}"#)).expect(text);
        assert_eq!(state.status, status, "{text}");
        assert_eq!(state.status.funds_moved(), moved, "{text}");
        assert_eq!(state.amount_out, None);
    }
    let delivered = parse_order_state(
        r#"{"id":"o1","status":"DELIVERED","amount_out":"3699.353418","payout_tx":"0xpayout","refund_address":"0xus"}"#,
    )
    .expect("delivered");
    assert_eq!(delivered.status, RiftOrderStatus::Delivered);
    assert_eq!(delivered.amount_out.as_deref(), Some("3699.353418"));
    assert_eq!(delivered.payout_tx.as_deref(), Some("0xpayout"));
}

#[test]
fn a_status_code_decides_the_failure_and_the_body_explains_it() {
    let body = r#"{"message":"to_address failed screening"}"#;
    assert!(
        matches!(classify_failure(403, None, body), RiftError::Screened(text) if text.contains("failed screening"))
    );
    assert!(matches!(classify_failure(410, None, "{}"), RiftError::QuoteExpired(_)));
    assert!(
        matches!(classify_failure(422, None, r#"{"error":"no venue"}"#), RiftError::NoVenue(text) if text.contains("no venue"))
    );
    assert_eq!(
        classify_failure(429, Some(7), ""),
        RiftError::RateLimited {
            retry_after_secs: Some(7)
        }
    );
    for status in [400, 401, 404, 409] {
        assert!(
            matches!(classify_failure(status, None, "bad"), RiftError::Rejected(_)),
            "{status}"
        );
    }
    for status in [500, 502, 503] {
        assert!(
            matches!(classify_failure(status, None, ""), RiftError::Unreachable(_)),
            "{status}"
        );
    }
    assert!(matches!(classify_failure(302, None, ""), RiftError::Malformed(_)));
}

#[test]
fn a_plain_text_body_is_kept_short_in_the_message() {
    let long = "x".repeat(1000);
    let RiftError::Rejected(text) = classify_failure(400, None, &long) else {
        panic!("rejected");
    };
    assert!(text.len() < 400, "{}", text.len());
}

#[test]
fn the_client_needs_a_parsable_base_url() {
    assert!(super::RiftClient::new("not a url", None).is_err());
    assert!(super::RiftClient::new("https://api.rift.trade/", Some(" ".to_string())).is_ok());
}

#[test]
fn the_client_refuses_plain_http_except_to_this_machine() {
    let key = || Some("secret".to_string());
    for accepted in [
        "https://api.rift.trade",
        "http://127.0.0.1:23010",
        "http://localhost:23010",
        "http://[::1]:23010",
    ] {
        assert!(super::RiftClient::new(accepted, key()).is_ok(), "{accepted}");
    }
    for refused in [
        "http://api.rift.trade",
        "http://10.0.0.5:8080",
        "http://localhost.evil.example",
        "ftp://api.rift.trade",
    ] {
        let error = super::RiftClient::new(refused, key()).err().expect(refused);
        assert!(error.contains("must use https"), "{refused}: {error}");
        // The refusal is the same without a key: the payout address is in the request too.
        assert!(super::RiftClient::new(refused, None).is_err(), "{refused}");
    }
}

fn btc_to_usdt(amount_in: &str) -> RiftQuoteRequest {
    RiftQuoteRequest {
        from: RiftAsset::new("bitcoin", "btc"),
        to: RiftAsset::new("ethereum", "usdt"),
        amount_in: amount_in.to_string(),
        venues: RiftVenueLists::default(),
    }
}

/// The client returns an answer only when it belongs to the request. An answer
/// for another quote or order must not reach the plan or the steps.
#[test]
fn a_quote_for_another_pair_or_amount_is_malformed() {
    let quote = || parse_quote(QUOTE).expect("quote");
    // Trailing zeros are the same amount.
    assert!(matched_quote(&btc_to_usdt("0.05"), quote()).is_ok());
    assert!(matched_quote(&btc_to_usdt("0.0500"), quote()).is_ok());

    let other_amount = matched_quote(&btc_to_usdt("0.06"), quote()).unwrap_err();
    assert!(matches!(other_amount, RiftError::Malformed(_)), "{other_amount}");
    let mut other_pair = btc_to_usdt("0.05");
    other_pair.to = RiftAsset::new("ethereum", "usdc");
    assert!(matches!(matched_quote(&other_pair, quote()), Err(RiftError::Malformed(_))));
    assert!(matches!(
        matched_quote(&btc_to_usdt("not a number"), quote()),
        Err(RiftError::Malformed(_))
    ));
}

/// `ORDER` with one field replaced by the given JSON value.
fn order_with(field: &str, value: serde_json::Value) -> String {
    let mut order: serde_json::Value = serde_json::from_str(ORDER).expect("order json");
    order[field] = value;
    order.to_string()
}

#[test]
fn an_order_with_one_wrong_term_is_malformed_and_names_the_term() {
    use serde_json::json;
    let request = order_request();
    // The payout address differs in case only, so it is the same address.
    assert!(matched_order(&request, parse_order(ORDER).expect("order")).is_ok());

    for (field, value, named) in [
        ("quote_id", json!("q2"), "quote"),
        ("deposit_address", json!(" "), "no deposit address"),
        ("from", json!("ethereum.dai"), "the pair"),
        ("to", json!("ethereum.usdc"), "the pair"),
        ("amount_in", json!("100.01"), "the amount"),
        ("to_address", json!("0x9999999999999999999999999999999999999999"), "the payout address"),
        ("refund_address", json!("0x9999999999999999999999999999999999999999"), "the refund address"),
        ("refund_address", json!(null), "the refund address"),
        ("status", json!("funded"), "the status"),
        ("status", json!("expired"), "the status"),
    ] {
        let answer = parse_order(&order_with(field, value.clone())).expect("parsed");
        let error = matched_order(&request, answer).expect_err(field);
        assert!(
            matches!(error, RiftError::Malformed(ref text) if text.contains(named)),
            "{field}={value}: {error}"
        );
    }
}

#[test]
fn an_order_answer_without_its_terms_is_malformed() {
    for field in ["from", "to", "to_address"] {
        let mut order: serde_json::Value = serde_json::from_str(ORDER).expect("order json");
        order.as_object_mut().expect("object").remove(field);
        assert!(
            matches!(parse_order(&order.to_string()), Err(RiftError::Malformed(_))),
            "{field}"
        );
    }
}

#[test]
fn only_hex_and_bech32_addresses_compare_without_case() {
    let mut request = order_request();
    // A base58 address is case-sensitive, so another case is another address.
    request.to_address = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT".to_string();
    let exact = order_with("to_address", serde_json::json!("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"));
    assert!(matched_order(&request, parse_order(&exact).expect("order")).is_ok());
    let other_case = order_with("to_address", serde_json::json!("1boatslrhtknngkdxeeobr76b53lettpyt"));
    assert!(matched_order(&request, parse_order(&other_case).expect("order")).is_err());

    request.to_address = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq".to_string();
    let upper = order_with("to_address", serde_json::json!("BC1QAR0SRRR7XFKVY5L643LYDNW9RE59GTZZWF5MDQ"));
    assert!(matched_order(&request, parse_order(&upper).expect("order")).is_ok());
}

#[test]
fn a_status_of_another_order_is_malformed() {
    let state = || parse_order_state(r#"{"id":"o1","status":"delivered","payout_tx":"0xpayout"}"#).expect("state");
    assert!(matched_order_state("o1", state()).is_ok());
    let other = matched_order_state("o2", state()).unwrap_err();
    assert!(matches!(other, RiftError::Malformed(ref text) if text.contains("o1") && text.contains("o2")), "{other}");
}
