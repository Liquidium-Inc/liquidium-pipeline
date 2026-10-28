//! Deposit and withdrawal addresses on the ledgers the exchange uses: native ICP
//! account ids, ICRC accounts and the per-network deposit destination.
use super::*;

impl<C> CexVenueFinalizer<C>
where
    C: CexBackend,
{
    pub(super) fn is_native_icp_token(token: &ChainToken) -> bool {
        match token {
            ChainToken::Icp { ledger, symbol, .. } => {
                symbol.eq_ignore_ascii_case("ICP")
                    && Principal::from_text(ICP_LEDGER_PRINCIPAL).is_ok_and(|icp_ledger| *ledger == icp_ledger)
            }
            _ => false,
        }
    }

    fn principal_default_account_id_hex(owner: Principal) -> String {
        AccountIdentifier::new(&owner, &Subaccount([0; 32])).to_hex()
    }

    fn validate_icp_account_id_hex(address: &str, context: &str) -> Result<String, String> {
        let normalized = address.trim().to_ascii_lowercase();
        AccountIdentifier::from_hex(&normalized).map_err(|err| {
            format!(
                "invalid native ICP ledger account id '{}' for {}: {}",
                address, context, err
            )
        })?;
        Ok(normalized)
    }

    pub(super) fn native_icp_direct_withdraw_address(&self, requested_destination: &str) -> Result<String, String> {
        let requested_destination = requested_destination.trim();
        if requested_destination.is_empty() {
            return Ok(Self::principal_default_account_id_hex(self.liquidator_principal));
        }
        if let Ok(account_id) = Self::validate_icp_account_id_hex(requested_destination, "exchange withdraw") {
            return Ok(account_id);
        }

        let account = crate::finalizers::bridge_planner::parse_icp_account_like(requested_destination)?;
        let subaccount = Subaccount(account.subaccount.unwrap_or([0; 32]));
        Ok(AccountIdentifier::new(&account.owner, &subaccount).to_hex())
    }

    pub(super) fn direct_deposit_destination_account(
        token: &ChainToken,
        planned_network: &str,
        address: &str,
    ) -> Result<ChainAccount, String> {
        let destination = address.trim();
        if destination.is_empty() {
            return Err(format!(
                "invalid exchange deposit address for network '{}': address is empty",
                planned_network
            ));
        }

        match token {
            ChainToken::Icp { .. } => {
                if !planned_network.eq_ignore_ascii_case("ICP") {
                    return Err(format!(
                        "unsupported exchange deposit network '{}' for ICP asset {}; expected ICP",
                        planned_network,
                        token.symbol()
                    ));
                }
                if Self::is_native_icp_token(token) {
                    let account_id_hex = Self::validate_icp_account_id_hex(destination, "exchange ICP deposit")?;
                    return Ok(ChainAccount::IcpLedger(account_id_hex));
                }
                let owner = Principal::from_text(destination).map_err(|e| {
                    format!(
                        "invalid exchange ICP deposit address '{}' for network '{}': {e}",
                        destination, planned_network
                    )
                })?;
                Ok(ChainAccount::Icp(Account {
                    owner,
                    subaccount: None,
                }))
            }
            ChainToken::EvmNative { .. } | ChainToken::EvmErc20 { .. } => {
                if planned_network.eq_ignore_ascii_case("ICP") {
                    return Err(format!(
                        "unsupported exchange deposit network '{}' for EVM asset {}; expected EVM-compatible network",
                        planned_network,
                        token.symbol()
                    ));
                }
                let evm_address = destination.parse::<EvmAddress>().map_err(|e| {
                    format!(
                        "invalid exchange EVM deposit address '{}' for network '{}': {e}",
                        destination, planned_network
                    )
                })?;
                Ok(ChainAccount::Evm(evm_address.to_string()))
            }
            ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => Err(format!(
                "unsupported exchange deposit network '{}' for Solana asset {}; Solana settlement is not wired yet",
                planned_network,
                token.symbol()
            )),
        }
    }

    /// The address the exchange withdraws to: the bridge source for a bridged
    /// withdrawal, else the final destination.
    pub(in crate::finalizers::cex) fn planned_withdraw_address(&self, state: &CexState) -> Result<String, String> {
        let asset = <Self as BridgePlanner>::planned_withdraw_asset(state);
        let network = <Self as BridgePlanner>::planned_withdraw_network(state);
        if state.withdraw.bridge.withdraw_bridge_required {
            let route =
                Self::withdraw_route(state).ok_or_else(|| format!("no return bridge route for {asset}@{network}"))?;
            return self.resolve_bridge_source_address(route.source_chain);
        }
        self.direct_withdraw_destination(state, &asset, &network)
    }

    /// The address a direct withdrawal goes to, checked for its network.
    pub(super) fn direct_withdraw_destination(
        &self,
        state: &CexState,
        planned_asset: &str,
        planned_network: &str,
    ) -> Result<String, String> {
        let direct_destination = if planned_network.eq_ignore_ascii_case("ICP") {
            if Self::is_native_icp_token(&state.withdraw.withdraw_asset) {
                self.native_icp_direct_withdraw_address(&state.withdraw.withdraw_address)?
            } else {
                let raw = state.withdraw.withdraw_address.trim();
                if raw.is_empty() {
                    return Err(format!(
                        "missing ICP withdraw destination for {}@{}",
                        planned_asset, planned_network
                    ));
                }
                raw.to_string()
            }
        } else if planned_network.eq_ignore_ascii_case("ETH") || planned_network.to_ascii_lowercase().starts_with("evm")
        {
            let raw = state.withdraw.withdraw_address.trim();
            if raw.is_empty() {
                return Err(format!(
                    "missing EVM withdraw destination for {}@{}",
                    planned_asset, planned_network
                ));
            }
            raw.parse::<EvmAddress>()
                .map_err(|e| {
                    format!(
                        "invalid EVM withdraw destination '{}' for {}@{}: {e}",
                        raw, planned_asset, planned_network
                    )
                })?
                .to_string()
        } else {
            let raw = state.withdraw.withdraw_address.trim();
            if raw.is_empty() {
                return Err(format!(
                    "missing withdraw destination for {}@{}",
                    planned_asset, planned_network
                ));
            }
            raw.to_string()
        };
        Ok(direct_destination)
    }
}
