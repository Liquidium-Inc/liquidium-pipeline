//! Rift's wire format, read into the connector types. Amounts stay decimal
//! text; Rift sends them as text in `formatted` mode, and a number is accepted too.
use liquidium_pipeline_connectors::backend::rift_backend::{
    RiftAsset, RiftError, RiftOrder, RiftOrderState, RiftOrderStatus, RiftQuote,
};
use serde::Deserialize;

/// A decimal amount as Rift may send it: text in `formatted` mode, or a number.
#[derive(Deserialize)]
#[serde(untagged)]
enum TextOrNumber {
    Text(String),
    Number(serde_json::Number),
}

impl TextOrNumber {
    fn into_text(self) -> String {
        match self {
            Self::Text(text) => text,
            Self::Number(number) => number.to_string(),
        }
    }
}

fn text_or_number<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Ok(TextOrNumber::deserialize(deserializer)?.into_text())
}

fn optional_text_or_number<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Ok(Option::<TextOrNumber>::deserialize(deserializer)?.map(TextOrNumber::into_text))
}

#[derive(Deserialize)]
struct QuoteWire {
    id: String,
    from: String,
    to: String,
    #[serde(deserialize_with = "text_or_number")]
    amount_in: String,
    #[serde(deserialize_with = "text_or_number")]
    estimated_amount_out: String,
    #[serde(default, deserialize_with = "optional_text_or_number")]
    estimated_execution_cost: Option<String>,
    expires_at: String,
    #[serde(default)]
    route: Vec<RouteStepWire>,
}

#[derive(Deserialize)]
struct RouteStepWire {
    #[serde(default)]
    venue: Option<String>,
}

#[derive(Deserialize)]
struct OrderWire {
    id: String,
    quote_id: String,
    deposit_address: String,
    #[serde(deserialize_with = "text_or_number")]
    amount_in: String,
    deposit_deadline: String,
    status: String,
}

#[derive(Deserialize)]
struct OrderStateWire {
    id: String,
    status: String,
    #[serde(default, deserialize_with = "optional_text_or_number")]
    amount_out: Option<String>,
    #[serde(default)]
    payout_tx: Option<String>,
    #[serde(default)]
    refund_address: Option<String>,
}

/// `chain.symbol` as Rift wrote it.
fn asset(text: &str) -> Result<RiftAsset, RiftError> {
    match text.split_once('.') {
        Some((chain, symbol)) if !chain.is_empty() && !symbol.is_empty() => Ok(RiftAsset::new(chain, symbol)),
        _ => Err(RiftError::Malformed(format!("Rift asset `{text}` is not chain.symbol"))),
    }
}

fn malformed(what: &str, error: serde_json::Error) -> RiftError {
    RiftError::Malformed(format!("Rift {what} could not be read: {error}"))
}

pub(super) fn parse_quote(body: &str) -> Result<RiftQuote, RiftError> {
    let wire: QuoteWire = serde_json::from_str(body).map_err(|error| malformed("quote", error))?;
    Ok(RiftQuote {
        id: wire.id,
        from: asset(&wire.from)?,
        to: asset(&wire.to)?,
        amount_in: wire.amount_in,
        estimated_amount_out: wire.estimated_amount_out,
        estimated_execution_cost: wire.estimated_execution_cost.unwrap_or_else(|| "0".to_string()),
        expires_at: wire.expires_at,
        venues: wire.route.into_iter().filter_map(|step| step.venue).collect(),
    })
}

pub(super) fn parse_order(body: &str) -> Result<RiftOrder, RiftError> {
    let wire: OrderWire = serde_json::from_str(body).map_err(|error| malformed("order", error))?;
    Ok(RiftOrder {
        id: wire.id,
        quote_id: wire.quote_id,
        deposit_address: wire.deposit_address,
        amount_in: wire.amount_in,
        deposit_deadline: wire.deposit_deadline,
        status: RiftOrderStatus::from_api(&wire.status),
    })
}

pub(super) fn parse_order_state(body: &str) -> Result<RiftOrderState, RiftError> {
    let wire: OrderStateWire = serde_json::from_str(body).map_err(|error| malformed("order state", error))?;
    Ok(RiftOrderState {
        id: wire.id,
        status: RiftOrderStatus::from_api(&wire.status),
        amount_out: wire.amount_out,
        payout_tx: wire.payout_tx,
        refund_address: wire.refund_address,
    })
}

/// Rift's own message in an error body, when it has one.
fn error_message(body: &str) -> String {
    #[derive(Deserialize)]
    struct ErrorWire {
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        error: Option<String>,
        #[serde(default)]
        detail: Option<String>,
    }
    serde_json::from_str::<ErrorWire>(body)
        .ok()
        .and_then(|wire| wire.message.or(wire.error).or(wire.detail))
        .unwrap_or_else(|| body.trim().chars().take(300).collect())
}

/// The typed failure for an HTTP status Rift answered with. The decision is
/// the status code; the body only explains it.
pub(super) fn classify_failure(status: u16, retry_after_secs: Option<u64>, body: &str) -> RiftError {
    let message = error_message(body);
    match status {
        403 => RiftError::Screened(format!("Rift screening refused an address: {message}")),
        410 => RiftError::QuoteExpired(format!("Rift quote expired: {message}")),
        422 => RiftError::NoVenue(format!("no Rift venue could quote: {message}")),
        429 => RiftError::RateLimited { retry_after_secs },
        400 | 401 | 404 | 409 => RiftError::Rejected(format!("Rift refused the request ({status}): {message}")),
        500..=599 => RiftError::Unreachable(format!("Rift could not serve the request ({status}): {message}")),
        other => RiftError::Malformed(format!("Rift answered with an unexpected status {other}: {message}")),
    }
}
