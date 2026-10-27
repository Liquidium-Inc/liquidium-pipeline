//! Classifies MEXC API errors: which refusals are definite, which answers
//! may hide an executed request, and which only mean "try another alias".
use super::*;

/// MEXC answers a history read for a coin it does not list with 700004,
/// "mandatory parameter 'coin' ... malformed". Seen live on 2026-10-26.
const MEXC_MALFORMED_PARAM_RAW_CODE: i32 = 700004;
/// MEXC refuses a withdrawal under the coin's minimum with 10254. Its text
/// changes with the language, so only the code is read.
const MEXC_WITHDRAW_BELOW_MIN_RAW_CODE: i32 = 10254;

pub(super) fn format_mexc_api_error(err: &v3::ApiError) -> String {
    match err {
        v3::ApiError::ErrorResponse(resp) => match &resp._extend {
            Some(extra) => format!("code={:?} msg={} extend={}", resp.code, resp.msg, extra),
            None => format!("code={:?} msg={}", resp.code, resp.msg),
        },
        v3::ApiError::ReqwestError(err) => match err.status() {
            Some(status) => format!("status={} err={}", status, err),
            None => format!("err={}", err),
        },
        other => other.to_string(),
    }
}

/// True when the request never reached MEXC. A timeout does not count: the
/// request may have arrived and been acted on.
pub(super) fn is_unreachable(err: &v3::ApiError) -> bool {
    matches!(err, v3::ApiError::ReqwestError(inner) if inner.is_connect())
}

pub(super) fn is_bad_symbol(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::BadSymbol
    )
}

/// Reports whether MEXC rejected the order because the funds it already shows
/// in the account are not tradable yet.
///
/// `Oversold` is the sell-side form (base asset short) and `InsufficientPosition`
/// the buy-side one (quote asset short). Both appear while a credited deposit is
/// still settling into the matching engine, which is a wait, not a failure.
pub(super) fn is_pending_settlement(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp)
            if resp.code == v3::ErrorCode::Oversold || resp.code == v3::ErrorCode::InsufficientPosition
    )
}

pub(super) fn is_below_withdraw_minimum(err: &v3::ApiError) -> bool {
    matches!(err, v3::ApiError::ErrorResponse(resp) if resp.raw_code == MEXC_WITHDRAW_BELOW_MIN_RAW_CODE)
}

pub(super) fn is_coin_missing(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::CurrencyDoesNotExist
    )
}

/// True when a history read of a coin alias failed because MEXC does not
/// list that alias, so no withdrawal can exist under it.
pub(super) fn is_unlisted_alias(err: &v3::ApiError) -> bool {
    is_coin_missing(err)
        || matches!(err, v3::ApiError::ErrorResponse(resp) if resp.raw_code == MEXC_MALFORMED_PARAM_RAW_CODE)
}

/// True when MEXC refused the request before it acted on it. Transport
/// errors, 5xx answers, unreadable replies and "processing" codes leave the
/// outcome open, and MEXC documents 5xx as possibly executed.
pub(super) fn is_definite_rejection(err: &v3::ApiError) -> bool {
    use v3::ErrorCode::*;
    match err {
        v3::ApiError::ErrorResponse(resp) => !matches!(
            resp.code,
            InternalError
                | ServiceUnavailable
                | GatewayTimeout
                | TransferOperationProcessing
                | AssetFlowWritingFailedPleaseTryAgain
                | InternalSystemErrorPleaseContactSupport
        ),
        v3::ApiError::MalformedRequest
        | v3::ApiError::WebApplicationFirewallViolated
        | v3::ApiError::RateLimitExceeded
        | v3::ApiError::SerdeUrlencodedError(_)
        | v3::ApiError::SignQueryError(_) => true,
        _ => false,
    }
}

/// MEXC refused the IOC order type itself (30041), so nothing was placed. Any
/// other refusal, such as a price outside the band, must not become a market order.
pub(super) fn is_ioc_refusal(err: &v3::ApiError) -> bool {
    matches!(
        err,
        v3::ApiError::ErrorResponse(resp) if resp.code == v3::ErrorCode::CurrentOrderTypeCanNotPlaceOrder
    )
}

pub(super) fn is_order_missing_lookup_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("order does not exist")
        || msg.contains("unknown order")
        || msg.contains("-2013")
        || msg.contains("code=-2013")
}
