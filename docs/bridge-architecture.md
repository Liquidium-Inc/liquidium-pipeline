# Bridge Architecture (ckETH ERC-20, Forward-Only)

This is a quick guide to the new bridge design.

## What changed

Before:
- Bridge flow was hardcoded for `USDC@ETH -> ckUSDC`.
- Backend was `CkUsdcBridgeBackend` and required `BRIDGE_USDC_ETH_TOKEN_ADDRESS`.
- Sweeper handled one fixed route.

Now:
- Bridge flow is catalog-driven.
- Backend is generic for ckETH ERC-20 forward routes: `CkEthErc20BridgeBackend`.
- Sweeper supports multiple routes in one loop, processed serially (nonce-safe).
- Token address comes from route catalog constants (not runtime env).

## New core structures

Defined in `connectors/src/backend/bridge_backend/mod.rs`:

- `BridgeRouteKind`
  - `CkEthErc20Forward`
  - `BtcToCkBtc`
  - `CkBtcToBtc`

- `BridgeRouteSpec`
  - `source_asset`
  - `source_chain`
  - `target_asset`
  - `destination_kind`
  - `route_kind`
  - `evm_token_address`
  - `min_sweep_amount`

- `BridgeSweepRoute`
  - Runtime sweeper input: source/target + min threshold.

- Helpers
  - `resolve_route(source_asset, source_chain, target_asset)`
  - `resolve_cketh_forward_route_by_source(source_asset, source_chain)`
  - `cketh_forward_routes()`

## Generic backend behavior

`connectors/src/backend/bridge_backend/cketh_erc20.rs`

- Resolves route + token from catalog.
- Validates destination type using route spec.
- Enforces signer/source invariant:
  - request `source_address` must match provider signer address.
- Uses ckETH minter helper contract discovery (`get_minter_info`) as before.
- Approves token and calls helper deposit method (`depositErc20` or native `deposit`).

## Multi-asset sweeper behavior

`pipeline/src/stages/bridge_sweeper.rs`

- Accepts `routes: Vec<BridgeSweepRoute>`.
- Each tick:
  - loops routes in order,
  - reads source balance per route,
  - applies `min_sweep_amount`,
  - resolves destination dynamically,
  - submits request.
- Routes are processed serially in one task (no parallel submits), so nonce order is preserved for the shared bridge signer.
- Per-route read/submit failures are logged and do not stop other routes in the same tick.

## Startup wiring

`pipeline/src/commands/liquidation_loop.rs`

- Builds `CkEthErc20BridgeBackend`.
- Loads routes via `cketh_forward_routes()`.
- Creates one `BridgeSweeper` instance with all forward routes.
- Starts sweeper task and logs `bridge_route_count`.

## Config changes

`pipeline/src/config.rs`

- Removed runtime dependency on `BRIDGE_USDC_ETH_TOKEN_ADDRESS`.
- Kept bridge namespace/signer/minter config intact (`bridge_evm_*`, `BRIDGE_CKETH_MINTER_CANISTER`, etc.).

## How to add another ckETH ERC-20 asset

Add one `BridgeRouteSpec` entry in the catalog with:
- `route_kind = CkEthErc20Forward`
- new `source_asset` / `target_asset`
- `source_chain = "ETH"`
- `evm_token_address = Some("0x...")`
- desired `min_sweep_amount`

No sweeper/backend refactor is needed after that.
