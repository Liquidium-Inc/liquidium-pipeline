//! Moving funds on and off Kraken: deposit and withdrawal method resolution,
//! the funding preflight a route must pass before collateral is sent, the
//! withdrawal itself and its status.
//!
//! Kraken has no single notion of a network: withdrawal methods report one,
//! deposit methods do not, and the same chain is spelled several ways across
//! method names. Every rule for telling them apart is here, with the live
//! examples that motivated it in the tests.

use super::*;

/// Withdrawals read per status poll before falling back to the whole history.
const KRAKEN_WITHDRAWAL_RECENT_PAGE: i64 = 20;
/// The most Kraken returns for one `WithdrawStatus` request, over a 90-day
/// window. The typed response carries no cursor, so this is the whole reach.
const KRAKEN_WITHDRAWAL_HISTORY_MAX: i64 = 500;

impl KrakenClient {
    async fn resolve_deposit(
        &self,
        asset: &str,
        network: &str,
    ) -> Result<(KrakenFundingMethod, String, Option<String>), String> {
        let api_asset = api_asset(asset);
        let method = unique_method(
            self.api.deposit_methods(&api_asset).await.map_err(api_read_error)?,
            asset,
            network,
            "deposit",
        )?;
        let addresses = self
            .api
            .deposit_addresses(&api_asset, &method.method)
            .await
            .map_err(api_read_error)?;
        match addresses.as_slice() {
            [address] => Ok((method, address.address.clone(), address.tag.clone())),
            [] => Err(format!(
                "Kraken has no existing {asset} deposit address for network {network}"
            )),
            _ => Err(format!(
                "Kraken returned ambiguous {asset} deposit addresses for network {network}"
            )),
        }
    }

    async fn resolve_withdrawal(
        &self,
        asset: &str,
        network: &str,
        destination: &str,
    ) -> Result<(KrakenFundingMethod, String), String> {
        let api_asset = api_asset(asset);
        let method = unique_method(
            self.api.withdrawal_methods(&api_asset).await.map_err(api_read_error)?,
            asset,
            network,
            "withdrawal",
        )?;
        let matches = self
            .api
            .withdrawal_addresses(&api_asset)
            .await
            .map_err(api_read_error)?
            .into_iter()
            .filter(|entry| {
                entry.verified && entry.method == method.method && addresses_match(&entry.address, destination)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [entry] => Ok((method, entry.key.clone())),
            [] => Err(format!(
                "Kraken destination is not a verified {asset} withdrawal address for network {network}"
            )),
            _ => Err(format!("Kraken withdrawal destination for {asset} is ambiguous")),
        }
    }

    pub(super) async fn preflight_funding_route(&self, preflight: &FundingRoutePreflight) -> Result<(), String> {
        ensure_positive(preflight.deposit_amount)?;
        ensure_positive(preflight.withdraw_amount)?;
        let (deposit_method, _, deposit_tag) = self
            .resolve_deposit(&preflight.deposit_asset, &preflight.deposit_network)
            .await?;

        if preflight.deposit_network.eq_ignore_ascii_case("ICP")
            && deposit_tag.as_deref().is_some_and(|tag| !tag.trim().is_empty())
        {
            return Err(format!(
                "Kraken {} deposit on ICP requires a memo/tag that the transfer layer cannot preserve",
                preflight.deposit_asset
            ));
        }

        let deposit_amount =
            Decimal::from_f64(preflight.deposit_amount).ok_or_else(|| "invalid Kraken deposit amount".to_string())?;

        if deposit_amount < deposit_method.minimum {
            return Err(format!(
                "Kraken deposit amount {deposit_amount} is below {} minimum {}",
                deposit_method.method, deposit_method.minimum
            ));
        }

        let (withdraw_method, _) = self
            .resolve_withdrawal(
                &preflight.withdraw_asset,
                &preflight.withdraw_network,
                &preflight.withdraw_address,
            )
            .await?;

        let withdraw_amount = Decimal::from_f64(preflight.withdraw_amount)
            .ok_or_else(|| "invalid Kraken withdrawal amount".to_string())?;

        if withdraw_amount < withdraw_method.minimum {
            return Err(format!(
                "Kraken withdrawal amount {withdraw_amount} is below {} minimum {}",
                withdraw_method.method, withdraw_method.minimum
            ));
        }
        Ok(())
    }

    pub(super) async fn deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let (_, address, tag) = self.resolve_deposit(asset, network).await?;
        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.to_string(),
            address,
            tag,
        })
    }

    pub(super) async fn submit_withdrawal(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, CexWithdrawError> {
        ensure_positive(amount)?;
        let (method, key) = self.resolve_withdrawal(asset, network, address).await?;
        let amount_decimal = Decimal::from_f64(amount).ok_or_else(|| "invalid Kraken withdrawal amount".to_string())?;
        // Kraken returns no code for this: the amount is refused here, before
        // submission, so this is where the refusal gets its name.
        if amount_decimal < method.minimum {
            return Err(CexWithdrawError::BelowMinimum(format!(
                "Kraken withdrawal amount {amount_decimal} is below {} minimum {}",
                method.method, method.minimum
            )));
        }
        let ref_id = self
            .api
            .withdraw(&api_asset(asset), &key, address, amount_decimal)
            .await
            .map_err(api_submit_error)?;
        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some(ref_id),
        })
    }

    pub(super) async fn withdrawal_status(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        // Polled every worker cycle for a withdrawal that is nearly always among
        // the newest, so the recent page is read first and the whole history
        // only when it is not there.
        let asset = api_asset(coin);
        let mut withdrawal = self
            .api
            .withdrawals(&asset, KRAKEN_WITHDRAWAL_RECENT_PAGE)
            .await
            .map_err(api_read_error)?
            .into_iter()
            .find(|item| item.ref_id == withdraw_id);
        if withdrawal.is_none() {
            withdrawal = self
                .api
                .withdrawals(&asset, KRAKEN_WITHDRAWAL_HISTORY_MAX)
                .await
                .map_err(api_read_error)?
                .into_iter()
                .find(|item| item.ref_id == withdraw_id);
        }
        let Some(withdrawal) = withdrawal else {
            return Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Unknown,
                txid: None,
                transaction_fee: None,
            });
        };
        let status = match withdrawal.status {
            KrakenTransferStatus::Pending => WithdrawStatus::Pending,
            KrakenTransferStatus::Completed => WithdrawStatus::Completed,
            KrakenTransferStatus::Failed => WithdrawStatus::Failed,
        };
        Ok(WithdrawStatusSnapshot {
            status,
            txid: withdrawal.tx_id,
            transaction_fee: Some(decimal_f64(withdrawal.fee, "withdrawal fee")?),
        })
    }
}

/// Compares a stored withdrawal address against the destination we intend to
/// send to.
///
/// Hex addresses carry no information in their case: EIP-55 checksumming is a
/// display convention, so Kraken storing an address lowercased while the
/// pipeline holds the checksummed form describes one address, not two. Matching
/// those byte-for-byte rejects a destination the operator did verify, and the
/// failure surfaces only after the collateral has been seized.
///
/// Every other address format is compared exactly. Base58 in particular encodes
/// distinct values in `A` and `a`, so case-folding it would risk matching an
/// address that was never verified.
fn addresses_match(stored: &str, destination: &str) -> bool {
    let hex_address = |value: &str| {
        value.len() > 2 && value[..2].eq_ignore_ascii_case("0x") && value[2..].chars().all(|c| c.is_ascii_hexdigit())
    };
    if hex_address(stored) && hex_address(destination) {
        return stored.eq_ignore_ascii_case(destination);
    }
    stored == destination
}

/// Kraken's own label for a chain this pipeline names, when it has one.
///
/// Used to compare against `KrakenFundingMethod::network` exactly. A method
/// *name* cannot identify a chain: `Tether USD (SPL)` contains "eth" because
/// "Tether" does, and `Ethereum (Polygon)` contains "ethereum" while settling on
/// Polygon. Matching either by substring selects the wrong chain or several.
fn kraken_network_label(requested: &str) -> Option<&'static str> {
    match requested.trim().to_ascii_uppercase().as_str() {
        "ETH" | "ETHEREUM" | "ERC20" => Some("ethereum"),
        "BTC" | "BITCOIN" => Some("bitcoin"),
        _ => None,
    }
}

/// Chain names that appear inside a method name for the chain the pipeline asked
/// for, used only when Kraken reports no network field.
///
/// Deposit methods carry no network, so the name is the only signal. Kraken
/// spells the same chain several ways -- `Ether (Hex)`, `Tether USD (ERC20)`,
/// `USDC - Ethereum (Unified)` -- so each accepted spelling is listed rather
/// than inferred, and anything unlisted does not match.
fn method_name_names_chain(requested: &str, method_name: &str) -> bool {
    let name = method_name.to_ascii_lowercase();
    let Some(label) = kraken_network_label(requested) else {
        // Assets whose chain this pipeline does not translate, such as ICP.
        return name.contains(&requested.to_ascii_lowercase());
    };
    match label {
        "ethereum" => {
            // `Ether (Hex)` is Kraken's native ETH deposit; `erc20` covers the
            // token spellings. A chain qualifier means a different rollup.
            let ethereum_spelling = name.starts_with("ether") || name.contains("erc20") || name.contains("ethereum");
            ethereum_spelling && !OTHER_CHAIN_QUALIFIERS.iter().any(|other| name.contains(other))
        }
        "bitcoin" => name.contains("bitcoin") && !name.contains("lightning") && !name.contains("kbtc"),
        _ => false,
    }
}

/// Chain qualifiers that disqualify an otherwise Ethereum-looking method name.
///
/// Kraken names rollup methods after the token plus the rollup, so a name can
/// contain an Ethereum spelling while settling elsewhere entirely.
const OTHER_CHAIN_QUALIFIERS: [&str; 12] = [
    "polygon",
    "optimism",
    "arbitrum",
    "base",
    "unichain",
    "ink",
    "linea",
    "zksync",
    "sei",
    "avalanche",
    "solana",
    "tron",
];

fn network_matches(asset: &str, requested: &str, method: &KrakenFundingMethod) -> bool {
    let _ = asset;
    // Withdrawal methods report the settlement network, which is the only field
    // that identifies a chain unambiguously. Compare it exactly.
    if let Some(expected) = kraken_network_label(requested)
        && let Some(network) = method.network.as_deref()
    {
        return network.trim().eq_ignore_ascii_case(expected);
    }
    // Deposit methods report no network, leaving only the method name.
    if let Some(network) = method.network.as_deref() {
        return network.to_ascii_lowercase().contains(&requested.to_ascii_lowercase());
    }
    method_name_names_chain(requested, &method.method)
}

fn unique_method(
    methods: Vec<KrakenFundingMethod>,
    asset: &str,
    network: &str,
    operation: &str,
) -> Result<KrakenFundingMethod, String> {
    let matches = methods
        .into_iter()
        .filter(|method| network_matches(asset, network, method))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [method] => Ok(method.clone()),
        [] => Err(format!(
            "Kraken does not support {operation} for {asset} on network {network}"
        )),
        _ => Err(format!(
            "Kraken {operation} method for {asset} on network {network} is ambiguous"
        )),
    }
}

#[cfg(test)]
#[path = "kraken_funding_tests.rs"]
mod tests;
