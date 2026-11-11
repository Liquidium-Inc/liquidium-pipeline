//! Moving funds on and off Kraken: deposit and withdrawal method lookup, the
//! route check before funds move, the withdrawal itself and its status.
//!
//! Withdrawal methods report a network and deposit methods do not, and one
//! chain has several spellings in method names. The rules that tell them apart
//! live here, with the live names that shaped them in the tests.

use liquidium_pipeline_connectors::backend::cex_backend::{
    DepositAddress, FundingRoute, FundingRouteError, WithdrawError, WithdrawStatus, WithdrawStatusSnapshot,
    WithdrawTerms, WithdrawalReceipt,
};
use rust_decimal::Decimal;

use super::api::{KrakenApiError, KrakenFundingMethod, KrakenTransferStatus};
use super::client::{KrakenClient, api_asset, decimal_f64, exactly_one, to_decimal};

/// Withdrawals read per status poll before the whole history is read.
pub(super) const WITHDRAWAL_RECENT_PAGE: i64 = 20;
/// The most Kraken returns for one status request, over 90 days.
pub(super) const WITHDRAWAL_HISTORY_MAX: i64 = 500;

fn unavailable(error: KrakenApiError) -> FundingRouteError {
    FundingRouteError::Unavailable(error.to_string())
}

impl KrakenClient {
    /// The deposit method, address and memo for an asset on a network.
    async fn resolve_deposit(
        &self,
        asset: &str,
        network: &str,
    ) -> Result<(KrakenFundingMethod, String, Option<String>), FundingRouteError> {
        let api_asset = api_asset(asset);
        let methods = self.deposit_methods(&api_asset).await.map_err(unavailable)?;
        let method = unique_method(methods, asset, network, "deposit").map_err(FundingRouteError::Unsupported)?;
        let addresses = self
            .deposit_addresses(&api_asset, &method.method)
            .await
            .map_err(unavailable)?;
        // Kraken keeps up to five addresses per coin, and each one credits the
        // account. Only an address that never expires is safe to send to later.
        let address = addresses.into_iter().find(|address| address.permanent).ok_or_else(|| {
            FundingRouteError::Unsupported(format!(
                "Kraken has no non-expiring {asset} deposit address for network {network}"
            ))
        })?;
        Ok((method, address.address, address.tag))
    }

    /// The withdrawal method, and the key of the verified address `destination`.
    async fn resolve_withdrawal(
        &self,
        asset: &str,
        network: &str,
        destination: &str,
    ) -> Result<(KrakenFundingMethod, String), FundingRouteError> {
        let (method, verified) = self.withdrawal_method_and_addresses(asset, network).await?;
        let matches = verified
            .into_iter()
            .filter(|(address, _)| addresses_match(address, destination))
            .collect::<Vec<_>>();
        let (_, key) = exactly_one(
            matches,
            || {
                FundingRouteError::Unsupported(format!(
                    "{destination} is not a verified Kraken {asset} withdrawal address for network {network}"
                ))
            },
            || {
                FundingRouteError::Unsupported(format!(
                    "Kraken has several verified {asset} withdrawal addresses matching {destination}"
                ))
            },
        )?;
        Ok((method, key))
    }

    /// The withdrawal method for an asset on a network, with its verified
    /// addresses as `(address, key)`.
    async fn withdrawal_method_and_addresses(
        &self,
        asset: &str,
        network: &str,
    ) -> Result<(KrakenFundingMethod, Vec<(String, String)>), FundingRouteError> {
        let api_asset = api_asset(asset);
        let methods = self.withdrawal_methods(&api_asset).await.map_err(unavailable)?;
        let method = unique_method(methods, asset, network, "withdrawal").map_err(FundingRouteError::Unsupported)?;
        let verified = self
            .withdrawal_addresses(&api_asset)
            .await
            .map_err(unavailable)?
            .into_iter()
            .filter(|entry| entry.verified && entry.method == method.method)
            .map(|entry| (entry.address, entry.key))
            .collect();
        Ok((method, verified))
    }

    /// The deposit must meet its minimum and carry no memo, and the
    /// destination must be a verified withdrawal address.
    pub(super) async fn check_route(&self, route: &FundingRoute) -> Result<(), FundingRouteError> {
        let (method, _, tag) = self
            .resolve_deposit(&route.deposit_asset, &route.deposit_network)
            .await?;
        // An ICP transfer cannot carry Kraken's memo, so the deposit would not be credited.
        if route.deposit_network.eq_ignore_ascii_case("ICP") && tag.as_deref().is_some_and(|tag| !tag.trim().is_empty())
        {
            return Err(FundingRouteError::Unsupported(format!(
                "Kraken {} deposit on ICP needs a memo that an ICP transfer cannot carry",
                route.deposit_asset
            )));
        }
        let deposit_amount =
            to_decimal(route.deposit_amount, "deposit amount").map_err(FundingRouteError::Unsupported)?;
        if deposit_amount < method.minimum {
            return Err(FundingRouteError::BelowMinimum(format!(
                "Kraken deposit amount {deposit_amount} is below the {} minimum {}",
                method.method, method.minimum
            )));
        }
        self.resolve_withdrawal(&route.withdraw_asset, &route.withdraw_network, &route.withdraw_address)
            .await?;
        Ok(())
    }

    pub(super) async fn deposit_address(&self, asset: &str, network: &str) -> Result<DepositAddress, String> {
        let (_, address, tag) = self
            .resolve_deposit(asset, network)
            .await
            .map_err(|error| error.to_string())?;
        Ok(DepositAddress {
            asset: asset.to_string(),
            network: network.to_string(),
            address,
            tag,
        })
    }

    /// The method minimum, and the fee Kraken quotes for it. The fee is fixed
    /// per method, so any verified address and the minimum amount read it.
    pub(super) async fn withdraw_terms(&self, asset: &str, network: &str) -> Result<WithdrawTerms, String> {
        let (method, verified) = self
            .withdrawal_method_and_addresses(asset, network)
            .await
            .map_err(|error| error.to_string())?;
        let Some((_, key)) = verified.first() else {
            return Err(format!(
                "Kraken has no verified {asset} withdrawal address for network {network}"
            ));
        };
        let probe = if method.minimum > Decimal::ZERO {
            method.minimum
        } else {
            Decimal::ONE
        };
        let fee = self
            .api
            .withdrawal_fee(&api_asset(asset), key, probe)
            .await
            .map_err(|error| error.to_string())?;
        Ok(WithdrawTerms {
            fee: decimal_f64(fee, "withdrawal fee")?,
            minimum: decimal_f64(method.minimum, "withdrawal minimum")?,
        })
    }

    pub(super) async fn submit_withdrawal(
        &self,
        asset: &str,
        network: &str,
        address: &str,
        amount: f64,
    ) -> Result<WithdrawalReceipt, WithdrawError> {
        let amount_decimal = to_decimal(amount, "withdrawal amount").map_err(WithdrawError::Refused)?;
        // Nothing is sent before these reads succeed. A failed read waits like an
        // outage, and a route Kraken cannot serve is a refusal.
        let (method, key) = self
            .resolve_withdrawal(asset, network, address)
            .await
            .map_err(|error| match error {
                FundingRouteError::Unavailable(message) => WithdrawError::Unreachable(message),
                other => WithdrawError::Refused(other.to_string()),
            })?;
        // Kraken refuses a small amount with no code, so the minimum is checked here.
        if amount_decimal < method.minimum {
            return Err(WithdrawError::BelowMinimum(format!(
                "Kraken withdrawal amount {amount_decimal} is below the {} minimum {}",
                method.method, method.minimum
            )));
        }
        let ref_id = self
            .api
            .withdraw(&api_asset(asset), &key, address, amount_decimal)
            .await
            .map_err(|error| match error {
                KrakenApiError::Rejected(message) => WithdrawError::Refused(message),
                KrakenApiError::Unreachable(message) => WithdrawError::Unreachable(message),
                KrakenApiError::Transport(message) => {
                    WithdrawError::Ambiguous(format!("Kraken {asset} withdrawal may have been sent: {message}"))
                }
            })?;
        Ok(WithdrawalReceipt {
            asset: asset.to_string(),
            network: network.to_string(),
            amount,
            txid: None,
            internal_id: Some(ref_id),
        })
    }

    /// The status of the withdrawal with Kraken's `ref_id`. A withdrawal is
    /// nearly always among the newest, so the recent page is read first.
    pub(super) async fn withdrawal_status(
        &self,
        coin: &str,
        withdraw_id: &str,
    ) -> Result<WithdrawStatusSnapshot, String> {
        let asset = api_asset(coin);
        let mut withdrawal = None;
        for limit in [WITHDRAWAL_RECENT_PAGE, WITHDRAWAL_HISTORY_MAX] {
            withdrawal = self
                .api
                .withdrawals(&asset, limit)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|item| item.ref_id == withdraw_id);
            if withdrawal.is_some() {
                break;
            }
        }
        let Some(withdrawal) = withdrawal else {
            return Ok(WithdrawStatusSnapshot {
                status: WithdrawStatus::Unknown,
                txid: None,
                transaction_fee: None,
            });
        };
        Ok(WithdrawStatusSnapshot {
            status: match withdrawal.status {
                KrakenTransferStatus::Pending => WithdrawStatus::Pending,
                KrakenTransferStatus::Completed => WithdrawStatus::Completed,
                KrakenTransferStatus::Failed => WithdrawStatus::Failed,
            },
            txid: withdrawal.tx_id,
            transaction_fee: Some(decimal_f64(withdrawal.fee, "withdrawal fee")?),
        })
    }
}

/// Hex addresses are case-free (EIP-55 is display only), so they compare
/// without case. Base58 encodes different values in `A` and `a`, so every
/// other format compares exactly (liquidation 1615).
pub(super) fn addresses_match(stored: &str, destination: &str) -> bool {
    let hex_address = |value: &str| {
        value.len() > 2 && value[..2].eq_ignore_ascii_case("0x") && value[2..].chars().all(|c| c.is_ascii_hexdigit())
    };
    if hex_address(stored) && hex_address(destination) {
        return stored.eq_ignore_ascii_case(destination);
    }
    stored == destination
}

/// A chain this pipeline names that Kraken labels. A method name cannot
/// identify a chain: `Tether USD (SPL)` contains "eth".
#[derive(Clone, Copy)]
enum KnownChain {
    Ethereum,
    Bitcoin,
}

impl KnownChain {
    fn from_network(requested: &str) -> Option<Self> {
        match requested.trim().to_ascii_uppercase().as_str() {
            "ETH" | "ETHEREUM" | "ERC20" => Some(Self::Ethereum),
            "BTC" | "BITCOIN" => Some(Self::Bitcoin),
            _ => None,
        }
    }

    /// Kraken's network label for the chain.
    fn kraken_label(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Bitcoin => "bitcoin",
        }
    }
}

/// Rollup and chain names that disqualify an Ethereum-looking method name.
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

/// Whether a method name names the requested chain, for methods that report
/// no network. Each accepted spelling is listed, and anything else does not match.
fn method_name_names_chain(requested: &str, method_name: &str) -> bool {
    let name = method_name.to_ascii_lowercase();
    match KnownChain::from_network(requested) {
        // Assets whose chain this pipeline does not translate, such as ICP.
        None => name.contains(&requested.to_ascii_lowercase()),
        Some(KnownChain::Ethereum) => {
            let ethereum_spelling = name.starts_with("ether") || name.contains("erc20") || name.contains("ethereum");
            ethereum_spelling && !OTHER_CHAIN_QUALIFIERS.iter().any(|other| name.contains(other))
        }
        Some(KnownChain::Bitcoin) => name.contains("bitcoin") && !name.contains("lightning") && !name.contains("kbtc"),
    }
}

fn network_matches(requested: &str, method: &KrakenFundingMethod) -> bool {
    match (KnownChain::from_network(requested), method.network.as_deref()) {
        // The reported network identifies the chain exactly.
        (Some(chain), Some(network)) => network.trim().eq_ignore_ascii_case(chain.kraken_label()),
        (None, Some(network)) => network.to_ascii_lowercase().contains(&requested.to_ascii_lowercase()),
        (_, None) => method_name_names_chain(requested, &method.method),
    }
}

pub(super) fn unique_method(
    methods: Vec<KrakenFundingMethod>,
    asset: &str,
    network: &str,
    operation: &str,
) -> Result<KrakenFundingMethod, String> {
    let matches = methods
        .into_iter()
        .filter(|method| network_matches(network, method))
        .collect::<Vec<_>>();
    exactly_one(
        matches,
        || format!("Kraken has no {operation} method for {asset} on network {network}"),
        || format!("Kraken has several {operation} methods for {asset} on network {network}"),
    )
}
