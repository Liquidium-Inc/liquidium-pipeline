//! Maps MEXC withdraw history records to a withdraw status and fee.
use super::*;

pub(super) fn from_mexc_raw(s: &str) -> WithdrawStatus {
    let normalized = s.trim().to_ascii_uppercase();
    match normalized.as_str() {
        // Text statuses seen from MEXC.
        // Every in-flight name the MEXC client renders. An accepted withdrawal
        // must never read as absent, or an uncertain request could be repeated.
        "APPLY" | "AUDITING" | "WAIT" | "PENDING" | "PROCESSING" | "WAIT_PACKAGING" | "WAIT_CONFIRM" | "MANUAL" => {
            WithdrawStatus::Pending
        }
        "SUCCESS" | "FINISHED" | "DONE" => WithdrawStatus::Completed,
        "FAILED" | "FAIL" => WithdrawStatus::Failed,
        "CANCEL" | "CANCELED" => WithdrawStatus::Canceled,
        // Numeric statuses from MEXC withdraw history:
        // 1 Apply, 2 Auditing, 3 Wait, 4 Processing, 5 WaitPackaging,
        // 6 WaitConfirm, 7 Success, 8 Failed, 9 Cancel, 10 Manual.
        // We collapse them into our generic status model.
        "1" | "2" | "3" | "4" | "5" | "6" | "10" => WithdrawStatus::Pending,
        "7" => WithdrawStatus::Completed,
        "8" => WithdrawStatus::Failed,
        "9" => WithdrawStatus::Canceled,
        _ => WithdrawStatus::Unknown,
    }
}

fn has_non_empty_text(value: Option<&str>) -> bool {
    match value {
        Some(text) => !text.trim().is_empty(),
        None => false,
    }
}

pub(super) fn withdraw_status_from_mexc_record(status: &str, tx_id: Option<&str>, trans_hash: Option<&str>) -> WithdrawStatus {
    let mapped = from_mexc_raw(status);
    let has_chain_tx = has_non_empty_text(tx_id) || has_non_empty_text(trans_hash);
    match mapped {
        WithdrawStatus::Completed if !has_chain_tx => {
            // A completed status without chain txid is still in-flight from settlement perspective.
            WithdrawStatus::Pending
        }
        WithdrawStatus::Unknown if has_chain_tx => {
            // Some MEXC responses use numeric or undocumented status values even when
            // a chain transaction id/hash is already present. Treat those as completed.
            WithdrawStatus::Completed
        }
        other => other,
    }
}

pub(super) fn parse_mexc_transaction_fee(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = trimmed.parse::<f64>().ok()?;
    if !parsed.is_finite() || parsed < 0.0 {
        return None;
    }
    Some(parsed)
}
