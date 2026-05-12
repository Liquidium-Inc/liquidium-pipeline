# Bridge Architecture (ckETH ERC-20)

This is a quick guide to the new bridge design.

## What changed

Before:
- Bridge flow was hardcoded for `USDC@ETH -> ckUSDC`.
- Backend was `CkUsdcBridgeBackend` and required `BRIDGE_USDC_ETH_TOKEN_ADDRESS`.
- Sweeper handled one fixed route.

Now:
- Bridge flow is catalog-driven.
- Backend is generic for ckETH ERC-20 routes: `CkErc20BridgeBackend`.
- Startup runs two serial sweepers:
  - forward: `USDC@ETH -> ckUSDC`
  - reverse: `ckUSDC@ICP -> USDC@ETH`
- Token/ledger metadata comes from route catalog constants (not runtime env).
- Bridge ICP ownership is principal-only (`subaccount = None`).

## New core structures

Defined in `connectors/src/backend/bridge_backend/mod.rs`:

- `BridgeRouteKind`
  - `CkEthErc20Forward`
  - `CkEthErc20Reverse`
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
  - Runtime sweeper input: source/target + min threshold.

- Helpers
  - `resolve_route(source_asset, source_chain, target_asset)`
  - `resolve_cketh_forward_route_by_source(source_asset, source_chain)`
  - `resolve_cketh_reverse_route_by_source(source_asset, source_chain)`
  - `cketh_forward_routes()`
  - `cketh_reverse_routes()`

## Generic backend behavior

`connectors/src/backend/bridge_backend/ckerc20_bridge.rs`

- Resolves route + token from catalog.
- Validates destination type using route spec.
- Enforces route-specific source invariants:
  - forward: request `source_address` must match EVM signer address.
  - reverse: request source ICP account owner must match bridge ICP owner principal and use no subaccount.
- Uses ckETH minter helper contract discovery (`get_minter_info`) as before.
- Forward submit: approves token and calls helper deposit method (`depositErc20` or native `deposit`).
- Reverse submit: reads fee quote, preflights bridge ckETH fee balance, approves ckUSDC + ckETH on ledgers, calls minter `withdraw_erc20`.

## Multi-asset sweeper behavior

`pipeline/src/stages/bridge_sweeper.rs`

- Each sweeper accepts `routes: Vec<BridgeSweepRoute>`.
- Each tick:
  - loops routes in order,
  - reads source balance per route,
  - applies `min_sweep_amount`,
  - resolves destination dynamically,
  - submits request.
- Routes are processed serially in one task (no parallel submits), so signer ordering remains deterministic.
- Per-route read/submit failures are logged and do not stop other routes in the same tick.

## Startup wiring

`pipeline/src/commands/liquidation_loop.rs`

- Builds one `CkErc20BridgeBackend`.
- Loads forward routes via `cketh_forward_routes()`.
- Loads reverse routes via `cketh_reverse_routes()`.
- Creates two `BridgeSweeper` instances:
  - forward source: bridge EVM address, destination resolver: liquidator ICP account.
  - reverse source: bridge ICP owner account text, destination resolver: EVM address (default: liquidator EVM).
- Starts both tasks and logs each route count.

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

No sweeper/backend refactor is needed after that.
