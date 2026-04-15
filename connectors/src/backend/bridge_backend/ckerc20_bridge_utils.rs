use alloy::primitives::{Address, FixedBytes};
use candid::Principal;
use icrc_ledger_types::icrc1::account::{Account, principal_to_subaccount};
use std::str::FromStr;

use crate::backend::bridge_backend::{
    BridgeDestination, BridgeDestinationKind, BridgeRequest, BridgeRouteKind, BridgeRouteSpec, resolve_route,
    validate_destination_for_route,
};

/// Encodes an ICP account into the `bytes32` principal/subaccount tuple expected by
/// ckETH helper contracts.
pub(super) fn destination_to_bytes32(account: &Account) -> (FixedBytes<32>, FixedBytes<32>) {
    let principal_bytes32: FixedBytes<32> = principal_to_subaccount(account.owner).into();
    let subaccount_bytes32: FixedBytes<32> = account.subaccount.unwrap_or([0u8; 32]).into();
    (principal_bytes32, subaccount_bytes32)
}

/// Ensures the request source EVM address matches the currently configured signer.
pub(super) fn ensure_source_matches_signer(source_address: &str, signer: Address) -> Result<(), String> {
    let source = source_address
        .parse::<Address>()
        .map_err(|e| format!("invalid source address '{source_address}': {e}"))?;
    if source != signer {
        return Err(format!(
            "source_address {} does not match signer {}",
            source_address, signer
        ));
    }
    Ok(())
}

/// Parses and validates the EVM token address from a bridge route definition.
pub(super) fn parse_evm_token_address(route: &BridgeRouteSpec) -> Result<Address, String> {
    let token = route.evm_token_address.ok_or_else(|| {
        format!(
            "route {}@{} -> {} has no EVM token address",
            route.source_asset, route.source_chain, route.target_asset
        )
    })?;
    token.parse::<Address>().map_err(|e| {
        format!(
            "invalid EVM token address '{}' for route {}@{} -> {}: {e}",
            token, route.source_asset, route.source_chain, route.target_asset
        )
    })
}

/// Parses and validates the ckERC20 ledger id from route metadata.
pub(super) fn parse_ckerc20_ledger_id(route: &BridgeRouteSpec) -> Result<Principal, String> {
    let ledger = route.ckerc20_ledger_id.ok_or_else(|| {
        format!(
            "route {}@{} -> {} has no ckERC20 ledger id",
            route.source_asset, route.source_chain, route.target_asset
        )
    })?;
    Principal::from_str(ledger).map_err(|e| {
        format!(
            "invalid ckERC20 ledger id '{}' for route {}@{} -> {}: {e}",
            ledger, route.source_asset, route.source_chain, route.target_asset
        )
    })
}

/// Parses a source ICP account from either textual `Account` form or principal text.
/// If only principal text is provided, `subaccount` is set to `None`.
pub(super) fn parse_source_icp_account(source_address: &str) -> Result<Account, String> {
    if let Ok(account) = Account::from_str(source_address.trim()) {
        return Ok(account);
    }
    if let Ok(owner) = Principal::from_str(source_address.trim()) {
        return Ok(Account {
            owner,
            subaccount: None,
        });
    }
    Err(format!(
        "invalid source ICP account '{}'; expected principal or Account text",
        source_address
    ))
}

/// Verifies reverse-route source account ownership and enforces principal-only source
/// accounts for bridge flow safety.
pub(super) fn ensure_source_matches_bridge_owner(
    source_account: &Account,
    bridge_owner: Principal,
) -> Result<(), String> {
    if source_account.owner != bridge_owner {
        return Err(format!(
            "source ICP account owner {} does not match configured bridge owner {}",
            source_account.owner, bridge_owner
        ));
    }
    if source_account.subaccount.is_some() {
        return Err(
            "bridge reverse route requires principal-only source account (subaccount must be None)".to_string(),
        );
    }
    Ok(())
}

/// Extracts an ICP destination account from a route-validated destination.
pub(super) fn expect_icp_destination<'a>(
    route: &BridgeRouteSpec,
    destination: &'a BridgeDestination,
) -> Result<&'a Account, String> {
    match destination {
        BridgeDestination::IcpAccount(account) => Ok(account),
        _ => Err(format!(
            "unsupported destination for {}@{} -> {}; expected {:?}",
            route.source_asset,
            route.source_chain,
            route.target_asset,
            BridgeDestinationKind::IcpAccount
        )),
    }
}

/// Extracts an EVM destination address from a route-validated destination.
pub(super) fn expect_evm_destination<'a>(
    route: &BridgeRouteSpec,
    destination: &'a BridgeDestination,
) -> Result<&'a Address, String> {
    match destination {
        BridgeDestination::EvmAddress(address) => Ok(address),
        _ => Err(format!(
            "unsupported destination for {}@{} -> {}; expected {:?}",
            route.source_asset,
            route.source_chain,
            route.target_asset,
            BridgeDestinationKind::EvmAddress
        )),
    }
}

/// Resolves route metadata for a bridge request and validates that the route is one of
/// the supported ckETH ERC-20 forward/reverse types.
pub(super) fn resolve_cketh_route_for_request(request: &BridgeRequest) -> Result<&'static BridgeRouteSpec, String> {
    let Some(route) = resolve_route(&request.asset, &request.source_chain, &request.target_asset) else {
        return Err(format!(
            "unsupported bridge route {}@{} -> {}; no route metadata found",
            request.asset, request.source_chain, request.target_asset
        ));
    };
    if route.route_kind != BridgeRouteKind::CkEthErc20Forward && route.route_kind != BridgeRouteKind::CkEthErc20Reverse
    {
        return Err(format!(
            "route {}@{} -> {} is not supported by CkErc20BridgeBackend",
            request.asset, request.source_chain, request.target_asset
        ));
    }
    validate_destination_for_route(route, &request.destination)?;
    if route.route_kind == BridgeRouteKind::CkEthErc20Forward {
        let _ = parse_evm_token_address(route)?;
    }
    if route.route_kind == BridgeRouteKind::CkEthErc20Reverse {
        let _ = parse_ckerc20_ledger_id(route)?;
    }
    Ok(route)
}
