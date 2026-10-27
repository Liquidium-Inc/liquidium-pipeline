//! Payload encoding and the send clock that every sender shares.

/// Unix time in nanoseconds, the unit ICP ledgers use for `created_at_time`.
pub(super) fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

/// The saved form of a send payload, shared by every sender.
pub fn encode_payload<T: serde::Serialize>(payload: &T) -> Result<String, String> {
    serde_json::to_string(payload).map_err(|error| format!("encode outbound payload: {error}"))
}

pub fn decode_payload<T: serde::de::DeserializeOwned>(payload: &str) -> Result<T, String> {
    serde_json::from_str(payload).map_err(|error| format!("decode outbound payload: {error}"))
}
