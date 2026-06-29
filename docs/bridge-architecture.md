# Bridge Architecture (ckETH Minter)

This is a quick guide to the new bridge design.

## What changed

Before:
- Bridge flow was hardcoded for `USDC@ETH -> ckUSDC`.
- Backend was `CkUsdcBridgeBackend` and required `BRIDGE_USDC_ETH_TOKEN_ADDRESS`.

Now:
- Bridge flow is catalog-driven.
- Backend supports ckETH minter native and ERC-20 routes: `CkErc20BridgeBackend`.
- Supported catalog routes include:
  - native forward: `ETH@ETH -> ckETH`
  - native reverse: `ckETH@ICP -> ETH@ETH`
  - ERC-20 forward: `USDC@ETH -> ckUSDC`
  - ERC-20 reverse: `ckUSDC@ICP -> USDC@ETH`
- Token/ledger metadata comes from route catalog constants (not runtime env).
- Bridge ICP ownership is principal-only (`subaccount = None`).

## New core structures

Defined in `connectors/src/backend/bridge_backend/mod.rs`:

- `BridgeRouteKind`
  - `CkEthErc20Forward`
  - `CkEthErc20Reverse`
  - `EthToCkEth`
  - `CkEthToEth`
  - `BtcToCkBtc`
  - `CkBtcToBtc`

- `BridgeRouteSpec`
  - `source_asset`
  - `source_chain`
  - `target_asset`
  - `destination_kind`
  - `route_kind`
  - `evm_token_address`
  - `ckerc20_ledger_id`
  - `min_sweep_amount`

- `BridgeSweepRoute`
  - Runtime bridge route input: source/target + min threshold.

- Helpers
  - `resolve_route(source_asset, source_chain, target_asset)`
  - `resolve_cketh_forward_route_by_source(source_asset, source_chain)`
  - `resolve_cketh_reverse_route_by_source(source_asset, source_chain)`
  - `cketh_forward_routes()`
  - `cketh_reverse_routes()`

## Backend behavior

`connectors/src/backend/bridge_backend/ckerc20_bridge.rs`

- Resolves route + token/ledger metadata from catalog.
- Validates destination type using route spec.
- Enforces route-specific source invariants:
  - forward: request `source_address` must match EVM signer address.
  - reverse: request source ICP account owner must match bridge ICP owner principal and use no subaccount.
- Uses ckETH minter helper contract discovery (`get_minter_info`) as before.
- Native forward submit: calls `depositEth(bytes32, bytes32)` when available, or legacy `deposit(bytes32)` for owner-only destinations.
- ERC-20 forward submit: approves token and calls helper deposit method (`depositErc20` or native `deposit`).
- Native reverse submit: preflights ckETH amount + approve fee, approves ckETH, calls minter `withdraw_eth`.
- ERC-20 reverse submit: reads fee quote, preflights bridge ckETH fee balance, approves ckERC20 + ckETH, calls minter `withdraw_erc20`.

## Startup wiring

`pipeline/src/commands/liquidation_loop.rs`

- Builds one `CkErc20BridgeBackend`.
- Validates configured forward EVM routes use the single Ethereum RPC provider.
- Wires the backend into the MEXC finalizer for per-liquidation bridge submissions.
- CEX bridge submissions are serialized by source via `bridge_submit_lock`.

## Config changes

`pipeline/src/config.rs`

- Removed runtime dependency on `BRIDGE_USDC_ETH_TOKEN_ADDRESS`.
- Bridge ICP account model is owner-only (`bridge_ic_account()` always returns `subaccount = None`).
- Kept bridge namespace/signer/minter config intact (`bridge_evm_*`, `BRIDGE_CKETH_MINTER_CANISTER`, etc.).

## How to add another ckETH ERC-20 asset

Add one `BridgeRouteSpec` entry in the catalog with:
- desired `route_kind` (`CkEthErc20Forward` or `CkEthErc20Reverse`)
- `source_asset` / `target_asset` / `source_chain`
- `destination_kind`
- `evm_token_address` and/or `ckerc20_ledger_id` metadata
- desired `min_sweep_amount`

No planner/backend refactor is needed after that for standard ckERC20 routes.
