//! MEXC names for our assets and networks: market symbols, coin aliases and
//! the network labels its deposit and withdraw endpoints use.

pub(super) fn normalize_market_symbol(market: &str) -> String {
    market.replace(['/', '_', '-'], "").to_ascii_uppercase()
}

/// Splits an asset symbol into its upper-case form and its native symbol
/// without the `CK` prefix. Every MEXC alias builder starts from this pair.
fn ck_split(asset: &str) -> (String, String) {
    let upper = asset.trim().to_ascii_uppercase();
    let native = upper.strip_prefix("CK").unwrap_or(&upper).to_string();
    (upper, native)
}

pub(super) fn push_unique(candidates: &mut Vec<String>, value: String) {
    if !candidates.iter().any(|item| item.eq_ignore_ascii_case(&value)) {
        candidates.push(value);
    }
}

pub(super) fn mexc_withdraw_coin_candidates(asset: &str) -> Vec<String> {
    let (upper, native) = ck_split(asset);
    let mut candidates = vec![upper];
    push_unique(&mut candidates, format!("CK{native}"));
    push_unique(&mut candidates, native);
    candidates
}

pub(super) fn mexc_network_candidates(asset: &str, network: &str) -> Vec<String> {
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

    let (_, asset_no_ck) = ck_split(&asset_norm);
    let ck_asset = format!("CK{asset_no_ck}");

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

/// MEXC labels deposit networks either as a bare code (`CKBTC`) or as a
/// display name with the code in parentheses (`Solana(SOL)`). A plain
/// substring test would let `ETH` match `MEGAETH` or `CKETH`.
pub(super) fn deposit_network_matches(item_network: &str, candidates: &[String]) -> bool {
    let item_network = item_network.trim().to_ascii_uppercase();
    candidates.iter().any(|cand| {
        let cand = cand.to_ascii_uppercase();
        item_network == cand || item_network.ends_with(&format!("({cand})"))
    })
}

fn is_evm_network_alias(network_norm: &str) -> bool {
    network_norm == "ETH" || network_norm == "ETHEREUM" || network_norm.starts_with("EVM")
}

pub(super) fn mexc_withdraw_network(asset: &str, network: &str) -> String {
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

pub(super) fn mexc_deposit_asset_candidates(asset: &str) -> Vec<String> {
    let asset_trimmed = asset.trim();
    if asset_trimmed.is_empty() {
        return vec![];
    }
    let (upper, native) = ck_split(asset_trimmed);
    // Order matters: the requested asset comes before its CK alias, otherwise
    // a native ETH deposit could resolve to IC custody. A ck asset keeps only
    // its ck forms so the base symbol never leaks as a coin.
    let ordered = if upper.starts_with("CK") {
        vec![format!("CK{native}"), format!("ck{native}")]
    } else {
        vec![asset_trimmed.to_string(), upper.clone(), format!("CK{upper}")]
    };
    let mut candidates = Vec::new();
    for candidate in ordered {
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    candidates
}
