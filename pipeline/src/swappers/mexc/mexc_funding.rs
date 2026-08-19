//! Moving funds on and off MEXC: the deposit address for an asset and
//! network, the free balance, the withdrawal and its status.
//!
//! MEXC spells assets and networks several ways (`ckUSDT` / `CKUSDT`, `ETH` /
//! `ERC20`, a ck-asset's own name as its ICP network), so each call tries the
//! candidate spellings in a fixed order. Withdrawal statuses arrive as text
//! or numbers and are folded into the shared status model here.

use super::*;

/// Default record limit for withdrawal history lookups.
const DEFAULT_WITHDRAW_HISTORY_LIMIT: u32 = 50;
/// MEXC's code for a withdrawal under the asset's minimum. The code is what
/// is matched, because the message alongside it is localized and reworded.
const MEXC_WITHDRAW_BELOW_MIN_RAW_CODE: i64 = 10254;

/// Reads the `raw_code:` MEXC's SDK renders into its error text.
fn parse_raw_code(err: &str) -> Option<i64> {
    let value = err.split("raw_code:").nth(1)?.trim_start();
    let end = value
        .find(|ch: char| !ch.is_ascii_digit() && ch != '-')
        .unwrap_or(value.len());
    value.get(..end)?.parse().ok()
}

/// Names a failed MEXC withdrawal by what it means for the funds, once, here.
fn classify_withdraw_error(message: String) -> CexWithdrawError {
    if parse_raw_code(&message) == Some(MEXC_WITHDRAW_BELOW_MIN_RAW_CODE) {
        CexWithdrawError::BelowMinimum(message)
    } else {
        CexWithdrawError::Other(message)
    }
}

fn from_mexc_raw(s: &str) -> WithdrawStatus {
    let normalized = s.trim().to_ascii_uppercase();
    match normalized.as_str() {
        // Text statuses seen from MEXC.
        "WAIT" | "PENDING" | "PROCESSING" => WithdrawStatus::Pending,
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

fn withdraw_status_from_mexc_record(status: &str, tx_id: Option<&str>, trans_hash: Option<&str>) -> WithdrawStatus {
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

fn parse_mexc_transaction_fee(raw: &str) -> Option<f64> {
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

fn mexc_network_candidates(asset: &str, network: &str) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    let mut push_unique = |value: String| {
        if !candidates.iter().any(|item| item.eq_ignore_ascii_case(&value)) {
            candidates.push(value);
        }
    };

    let network_norm = network.trim().to_ascii_uppercase();
    let asset_norm = asset.trim().to_ascii_uppercase();
    if asset_norm.is_empty() {
        if !network_norm.is_empty() {
            push_unique(network_norm);
        }
        return candidates;
    }

    // Special-case: native ICP asset on ICP network should just be "ICP".
    if asset_norm == "ICP" && network.eq_ignore_ascii_case("icp") {
        push_unique("ICP".to_string());
        return candidates;
    }

    let asset_no_ck = asset_norm.strip_prefix("CK").unwrap_or(&asset_norm);
    let ck_asset = format!("CK{}", asset_no_ck);

    // For ck-assets, prefer CK network names only (avoid leaking base symbol like BTC/USDT as a network).
    if asset_norm.starts_with("CK") {
        push_unique(ck_asset.clone());
    }

    // Always include the explicit network if provided.
    if !network_norm.is_empty() {
        push_unique(network_norm.clone());
    }

    if is_evm_network_alias(&network_norm) {
        push_unique("ERC20".to_string());
    }

    // If requested network is ICP, include ICP and CK-asset network names.
    if network.eq_ignore_ascii_case("icp") {
        push_unique("ICP".to_string());
        push_unique(ck_asset);
    }

    candidates
}

fn is_evm_network_alias(network_norm: &str) -> bool {
    network_norm == "ETH" || network_norm == "ETHEREUM" || network_norm.starts_with("EVM")
}

fn mexc_withdraw_network(asset: &str, network: &str) -> String {
    let asset_norm = asset.trim().to_ascii_uppercase();
    if asset_norm.starts_with("CK") && !asset_norm.is_empty() {
        return asset_norm;
    }

    if network.eq_ignore_ascii_case("icp") {
        let asset_norm = asset.trim().to_ascii_uppercase();
        if !asset_norm.is_empty() {
            return asset_norm;
        }
    }

    let network_norm = network.trim().to_ascii_uppercase();
    if is_evm_network_alias(&network_norm) {
        return "ERC20".to_string();
    }

    network.to_string()
}

fn mexc_deposit_asset_candidates(asset: &str) -> Vec<String> {
    let asset_trimmed = asset.trim();
    if asset_trimmed.is_empty() {
        return vec![];
    }

    let asset_upper = asset_trimmed.to_ascii_uppercase();
    let has_ck_prefix = asset_upper.starts_with("CK");
    let asset_no_ck = asset_upper.strip_prefix("CK").unwrap_or(&asset_upper);

    let mut candidates = Vec::new();

    if has_ck_prefix {
        // only ck variants
        candidates.push(format!("CK{}", asset_no_ck));
        candidates.push(format!("ck{}", asset_no_ck));
    } else {
        // normal asset + ck variants
        candidates.push(asset_trimmed.to_string());
        candidates.push(asset_upper.clone());
        candidates.push(format!("CK{}", asset_upper));
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

impl MexcClient {
    pub(super) async fn deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let ex = self.inner.lock().await;

        let candidates = mexc_network_candidates(asset, network);
        let asset_candidates = mexc_deposit_asset_candidates(asset);
        let mut last_err: Option<String> = None;
        let mut last_available: Option<Vec<String>> = None;

        let mut network_attempts: Vec<Option<String>> = candidates.iter().cloned().map(Some).collect();
        network_attempts.push(None);

        for coin in &asset_candidates {
            for net in &network_attempts {
                let res = match ex.get_deposit_address(coin.to_string(), net.as_deref()).await {
                    Ok(res) => res,
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                };

                if res.is_empty() {
                    last_err = Some(format!(
                        "no deposit addresses returned for coin={} network={:?}",
                        coin, net
                    ));
                    continue;
                }

                let addr = res.iter().find(|item| {
                    let item_network = item.network.to_ascii_uppercase();
                    candidates.iter().any(|cand| item_network.contains(cand))
                });

                if let Some(v) = addr {
                    return Ok(DepositAddress {
                        asset: asset.to_string(),
                        network: v.network.clone(),
                        address: v.address.clone(),
                        tag: v.memo.clone(),
                    });
                }

                last_available = Some(res.iter().map(|item| item.network.clone()).collect());
                last_err = Some(format!(
                    "address not found for coin={} network={:?} candidates={:?} available={:?}",
                    coin, net, candidates, last_available
                ));
            }
        }

        Err(format!(
            "address not found for asset={} network={} candidates={:?} asset_candidates={:?} available={:?} err={}",
            asset,
            network,
            candidates,
            asset_candidates,
            last_available.unwrap_or_default(),
            last_err.unwrap_or_else(|| "no deposit address candidates matched".to_string())
        ))
    }

    pub(super) async fn free_balance(&self, asset: &str) -> Result<f64, String> {
        let ex = self.inner.lock().await;

        let res = ex.account_information().await.map_err(|e| mexc_error_message(&e))?;

        let asset_norm = asset.to_ascii_uppercase();
        let balance = match res
            .balances
            .iter()
            .find(|item| item.asset.to_ascii_uppercase() == asset_norm)
            .cloned()
        {
            Some(b) => b,
            None => {
                debug!(
                    "[mexc] balance not found for asset={}, available={:?}",
                    asset,
                    res.balances.iter().map(|item| item.asset.clone()).collect::<Vec<_>>()
                );
                AccountBalance {
                    asset: asset.to_string(),
                    free: Decimal::ZERO,
                    locked: Decimal::ZERO,
                }
            }
        };

        balance
            .free
            .to_f64()
            .ok_or_else(|| "could not convert balance to f64".to_string())
    }

    pub(super) async fn submit_withdrawal(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, CexWithdrawError> {
        let ex = self.inner.lock().await;
        let push_unique = |list: &mut Vec<String>, value: String| {
            if !list.iter().any(|item| item.eq_ignore_ascii_case(&value)) {
                list.push(value);
            }
        };

        let asset_upper = asset.to_ascii_uppercase();
        let asset_no_ck = asset_upper.strip_prefix("CK").unwrap_or(&asset_upper);
        let mut candidates = Vec::new();
        // Prefer native symbol and its CK-prefixed form first.
        push_unique(&mut candidates, asset_upper.clone());
        push_unique(&mut candidates, format!("CK{}", asset_no_ck));
        push_unique(&mut candidates, asset_no_ck.to_string());
        push_unique(&mut candidates, asset.to_string());

        let mut network_candidates = mexc_network_candidates(asset, network);
        let network_mapped = mexc_withdraw_network(asset, network);
        let mut ordered_networks = Vec::new();
        push_unique(&mut ordered_networks, network_mapped.clone());
        for cand in network_candidates.drain(..) {
            push_unique(&mut ordered_networks, cand);
        }
        if ordered_networks.is_empty() {
            push_unique(&mut ordered_networks, network_mapped.clone());
        }
        let network_candidates = ordered_networks;

        info!(
            "Withdraw request coin={} network_candidates={:?} amount={} address={}",
            asset, network_candidates, amount, address
        );

        let mut last_err: Option<String> = None;
        let mut hard_err: Option<String> = None;
        let mut res = None;
        for coin in &candidates {
            for net in &network_candidates {
                match ex
                    .withdraw(WithdrawRequest {
                        address: address.to_string(),
                        amount: amount.to_string(),
                        coin: coin.to_string(),
                        memo: None,
                        network: Some(net.clone()),
                        remark: None,
                        withdraw_order_id: None,
                    })
                    .await
                {
                    Ok(ok) => {
                        res = Some(ok);
                        break;
                    }
                    Err(e) => {
                        if is_coin_missing(&e) {
                            last_err = Some(e.to_string());
                            continue;
                        }
                        last_err = Some(e.to_string());
                        if hard_err.is_none() {
                            hard_err = last_err.clone();
                        }
                    }
                }
            }
            if res.is_some() {
                break;
            }
        }

        let res = match res {
            Some(res) => res,
            None => {
                return Err(classify_withdraw_error(
                    hard_err.or(last_err).unwrap_or_else(|| "withdraw failed".to_string()),
                ));
            }
        };

        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network_mapped,
            amount,
            txid: None,
            internal_id: Some(res.id),
        })
    }

    pub(super) async fn withdrawal_status(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        let ex = self.inner.lock().await;
        let records = ex
            .withdraw_history(WithdrawHistoryRequest {
                coin: Some(coin.to_string()),
                status: None,
                limit: Some(DEFAULT_WITHDRAW_HISTORY_LIMIT),
                start_time: None,
                end_time: None,
            })
            .await
            .map_err(|e| mexc_error_message(&e))?;

        let rec = records
            .into_iter()
            .find(|r| r.id == withdraw_id || r.withdraw_order_id.as_deref() == Some(withdraw_id));

        let rec = match rec {
            Some(r) => r,
            None => {
                return Ok(WithdrawStatusSnapshot {
                    status: WithdrawStatus::Unknown,
                    txid: None,
                    transaction_fee: None,
                });
            }
        };

        let status =
            withdraw_status_from_mexc_record(rec.status.as_str(), rec.tx_id.as_deref(), rec.trans_hash.as_deref());
        let txid = rec
            .tx_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .or_else(|| {
                rec.trans_hash
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
            });
        let transaction_fee = parse_mexc_transaction_fee(rec.transaction_fee.as_str());
        if transaction_fee.is_none() && !rec.transaction_fee.trim().is_empty() {
            warn!(
                "mexc withdraw_history returned unparseable transaction_fee='{}' for coin={} withdraw_id={} status={}",
                rec.transaction_fee, coin, withdraw_id, rec.status
            );
        }
        if transaction_fee.is_none() && matches!(status, WithdrawStatus::Completed) {
            warn!(
                "mexc withdraw_history missing transaction fee for completed withdraw: coin={} withdraw_id={} status={} txid_present={}",
                coin,
                withdraw_id,
                rec.status,
                txid.is_some()
            );
        }

        Ok(WithdrawStatusSnapshot {
            status,
            txid,
            transaction_fee,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The finalizer writes a leg off as dust on this variant, so it has to come
    /// from the code MEXC sends and not from the words around it, which MEXC
    /// localizes and rewords.
    #[test]
    fn a_withdrawal_under_the_minimum_is_named_by_its_code_not_its_wording() {
        assert!(matches!(
            classify_withdraw_error(
                r#"Error response: ErrorResponse { code: InvalidResponse, raw_code: 10254, msg: "localized or changed text", _extend: None }"#.to_string()
            ),
            CexWithdrawError::BelowMinimum(_)
        ));
        assert!(matches!(
            classify_withdraw_error("Withdrawal shall not be less than the Min amount of:0.00002".to_string()),
            CexWithdrawError::Other(_)
        ));
        assert!(matches!(
            classify_withdraw_error("exchange error raw_code: 10007".to_string()),
            CexWithdrawError::Other(_)
        ));
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
}
