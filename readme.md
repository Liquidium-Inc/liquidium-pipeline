# Liquidator Bot Framework for ICP

A modular, event-driven off-chain liquidation bot framework for [Internet Computer (ICP)](https://internetcomputer.org/) protocols.
Inspired by Artemis/MEV patterns and designed for permissionless, community-driven liquidations.

## Table of Contents

- [Features](#features)
- [At a Glance](#at-a-glance)
- [Quick Install](#quick-install)
- [Configuration](#configuration)
- [Identity Management](#identity-management)
- [Architecture Overview](#architecture-overview)
- [Multi-Venue Swap Pipeline](#multi-venue-swap-pipeline)
- [CLI Commands](#cli-commands)
- [Operations Runbook](#operations-runbook)
- [Developer Setup](#developer-setup)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Notes](#notes)
- [License](#license)

---

## Features

- **Pipeline Architecture** — Composable stages for discovery, strategy, execution, finalization, and export.
- **Async Rust** — Highly concurrent and efficient with Tokio runtime.
- **Multi-Chain** — Primary support for ICP with EVM (Arbitrum) integration.
- **Swap Execution** — Enabled-venue planning across ICPSwap and MEXC.
- **Extensible** — Add custom risk checks, strategies, swaps, or notification stages.
- **Permissionless** — Anyone can run it.
- **Multi-Account** — Separate liquidator, trader, and recovery identities for security.
- **CLI Interface** — Manage balances, funds, and identities.
- **Persistent State** — SQLite WAL ensures no double-liquidations and supports retries.

## At a Glance

- **Default venues:** `ENABLED_SWAP_VENUES=icpswap,mexc`; disable MEXC when credentials are not configured.
- **Current config precedence:** shell env vars > local `.env` > `~/.liquidium-pipeline/config.env`.
- **Required env vars (minimum):** `MNEMONIC_FILE`, `IC_URL`, `EVM_RPC_URL`, `LENDING_CANISTER`, `DEBT_ASSETS`, `COLLATERAL_ASSETS`.
- **New client env var:** `BRIDGE_CKETH_MINTER_CANISTER` (recommended to set explicitly; defaults to `sv3dd-oaaaa-aaaar-qacoa-cai` when unset/empty).
- **Primary operations:** `liquidator run`, `liquidator balance`, `liquidator withdraw`, `liquidator account show`.
- **Persistence:** SQLite WAL (`DB_PATH`) enables idempotent retries and resume-safe execution.

---

## Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/Liquidium-Inc/liquidium-pipeline/main/install.sh | bash
```

By default, this installs the latest released version (latest GitHub release tag).

Install a specific tag:

```bash
curl -fsSL https://raw.githubusercontent.com/Liquidium-Inc/liquidium-pipeline/main/install.sh | bash -s -- --tag v1.2.3
```

Install from a branch (dev/testing):

```bash
curl -fsSL https://raw.githubusercontent.com/Liquidium-Inc/liquidium-pipeline/main/install.sh | bash -s -- --branch main
```

This will:

- Clone/update the repo to `~/.liquidium-pipeline/repo`
- Build the liquidator binary in release mode
- Install it to `~/.local/bin/liquidator`
- Create `~/.liquidium-pipeline/config.env` if it doesn't exist (won't overwrite an existing file)

> Set `SKIP_RUST=true` before running to skip Rust installation if already present.

### Install Script Behavior (No Surprises)

- User-only install (no sudo) that keeps everything under `~/.liquidium-pipeline`
- Releases are stored in `~/.liquidium-pipeline/releases` and symlinked to `~/.local/bin/liquidator`
- Re-running the script updates the repo + binary, but **does not overwrite** your existing `config.env`
- You can customize with env/args: `TAG`, `BRANCH`, `BIN_NAME`, `INSTALL_DIR`, `SKIP_RUST`

### Upgrade and Compatibility

- Re-running the install command (without `--tag`/`--branch`) upgrades to the latest released tag.
- `config.env` is preserved across upgrades; new config keys should be added manually when needed.
- Legacy alias `CEX_BUY_INVERSE_OVESPEND_BPS` remains accepted, while `CEX_BUY_INVERSE_OVERSPEND_BPS` is canonical.

---

## Configuration

The bot loads configuration from (in order of precedence):

1. Environment variables (direct overrides)
2. `.env` in the current directory (optional overrides)
3. `~/.liquidium-pipeline/config.env` (user-level defaults)

### Override Rules (How Env "Overwrites" Work)

- Variables already set in your shell **always win**.
- `.env` overrides `~/.liquidium-pipeline/config.env`.
- `~/.liquidium-pipeline/config.env` is the default baseline created by the install script.

**One-off override example:**

```bash
IC_URL=https://icp-api.io liquidator run
```

### Required Configuration

```bash
# ICP Blockchain
IC_URL=https://ic0.app
LENDING_CANISTER=nja4y-2yaaa-aaaae-qddxa-cai
BRIDGE_CKETH_MINTER_CANISTER=sv3dd-oaaaa-aaaar-qacoa-cai

# EVM Blockchain
EVM_RPC_URL=https://ethereum-rpc.example
MEXC_DEFAULT_ROUTE_CHAIN_ID=1
# Optional Arbitrum override for an Arbitrum EVM_RPC_URL:
# MEXC_ROUTE_CHAIN_MAP=ETH=42161,ETHEREUM=42161,ARB=42161,ARBITRUM=42161

# Identity
MNEMONIC_FILE=~/.liquidium-pipeline/wallets/key

# Assets (comma-separated chain:address:symbol entries)
DEBT_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,icp:cngnf-vqaaa-aaaar-qag4q-cai:ckUSDT,icp:xevnm-gaaaa-aaaar-qafnq-cai:ckUSDC,icp:ss2fx-dyaaa-aaaar-qacoq-cai:ckETH,icp:ryjl3-tyaaa-aaaaa-aaaba-cai:ICP
COLLATERAL_ASSETS=icp:mxzaz-hqaaa-aaaar-qaada-cai:ckBTC,icp:cngnf-vqaaa-aaaar-qag4q-cai:ckUSDT,icp:xevnm-gaaaa-aaaar-qafnq-cai:ckUSDC,icp:ss2fx-dyaaa-aaaar-qacoq-cai:ckETH,icp:ryjl3-tyaaa-aaaaa-aaaba-cai:ICP

# Optional: only scan specific borrower principals (comma-separated). Set to "none" to disable.
OPPORTUNITY_ACCOUNT_FILTER=principal1,principal2
```

### Optional Configuration

```bash
# Optional operation flags
BUY_BAD_DEBT=false
DB_PATH=./wal.db
EXPORT_PATH=executions.csv
WATCHDOG_WEBHOOK=https://your-webhook-url.com/endpoint
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
BOT_NAME=prod-liquidator
LOW_BALANCE_THRESHOLDS=ckBTC=0.001,ckUSDT=100,ckUSDC=100,ICP=5,ETH=0.05
BALANCE_CHECK_EXCLUDE=ckETH,ETH
```

### Swap Configuration

```bash
# Ordered venues eligible for new plans. Omit to enable both by default.
ENABLED_SWAP_VENUES=icpswap,mexc

# CEX (MEXC) - Optional
CEX_MEXC_API_KEY=your_api_key
CEX_MEXC_API_SECRET=your_api_secret
MAX_ALLOWED_CEX_SLIPPAGE_BPS=200  # 2.00% in basis points

# Liquidation guard (bad debt only)
BAD_DEBT_COLLATERAL_SLIPPAGE_BPS=500  # 5.00% haircut used for min collateral
```

### Advanced CEX Tuning

```bash
# CEX trade slicing and execution controls
# Skip execution chunks below this USD notional (treat as dust)
CEX_MIN_EXEC_USD=1.1
# Per-slice impact target ratio of MAX_ALLOWED_CEX_SLIPPAGE_BPS
CEX_SLICE_TARGET_RATIO=0.7
# Arm adaptive buy fallback when truncation ratio is >= this value
CEX_BUY_TRUNCATION_TRIGGER_RATIO=0.25
# Max quote overspend allowed for inverse/base buy fallback (bps)
CEX_BUY_INVERSE_OVERSPEND_BPS=10
# Max inverse/base fallback retries per trade leg
CEX_BUY_INVERSE_MAX_RETRIES=1
# Enable/disable adaptive inverse/base fallback
CEX_BUY_INVERSE_ENABLED=true
# Retry backoff base and cap (seconds) for retryable CEX errors
CEX_RETRY_BASE_SECS=5
CEX_RETRY_MAX_SECS=120
# Minimum projected net edge required before executing on CEX (bps)
CEX_MIN_NET_EDGE_BPS=150
# Additional latency-risk haircut applied to projected edge (bps)
CEX_DELAY_BUFFER_BPS=75
# Estimated route fee haircut applied to projected edge (bps)
CEX_ROUTE_FEE_BPS=25
# Optional CSV market universe for MEXC hop discovery (`BASE_QUOTE` format)
CEX_MEXC_AVAILABLE_PAIRS=CKBTC_BTC,BTC_USDC,BTC_USDT,USDC_USDT,CKUSDT_USDT,ICP_USDT,ICP_USDC,ETH_USDT
# Max intermediate hops when searching configured pairs (0 disables hop fallback)
CEX_MEXC_MAX_HOPS=2
```

Quick reference:

| Parameter | What it controls |
|----------|-------------------|
| `CEX_MIN_EXEC_USD` | Dust floor per slice (below this, execution is skipped). |
| `CEX_SLICE_TARGET_RATIO` | How aggressive slice sizing is vs hard slippage cap. |
| `CEX_BUY_TRUNCATION_TRIGGER_RATIO` | When buy truncation is considered large enough to trigger fallback logic. |
| `CEX_BUY_INVERSE_OVERSPEND_BPS` | Safety cap for how much inverse/base buy mode may overspend. |
| `CEX_BUY_INVERSE_MAX_RETRIES` | Max fallback attempts per leg. |
| `CEX_BUY_INVERSE_ENABLED` | Master toggle for adaptive buy fallback. |
| `CEX_RETRY_BASE_SECS` | Initial retry delay after retryable CEX errors. |
| `CEX_RETRY_MAX_SECS` | Maximum retry delay cap. |
| `CEX_MIN_NET_EDGE_BPS` | Minimum projected edge needed before choosing CEX path. |
| `CEX_DELAY_BUFFER_BPS` | Extra haircut for execution-latency/price-move risk. |
| `CEX_ROUTE_FEE_BPS` | Fee haircut applied during route edge estimation. |
| `CEX_MEXC_AVAILABLE_PAIRS` | Configured market universe used for direct/hop route discovery. |
| `CEX_MEXC_MAX_HOPS` | Max intermediate hops allowed in configured-pair route search. |

Note: `CEX_BUY_INVERSE_OVESPEND_BPS` is still accepted as a legacy alias, but `CEX_BUY_INVERSE_OVERSPEND_BPS` is the canonical key.

#### `CEX_SLICE_TARGET_RATIO` Explained

`CEX_SLICE_TARGET_RATIO` controls how aggressive each CEX execution slice is.

The slicer computes:

`target_slice_bps = MAX_ALLOWED_CEX_SLIPPAGE_BPS * CEX_SLICE_TARGET_RATIO`

With:
- `MAX_ALLOWED_CEX_SLIPPAGE_BPS=200`
- `CEX_SLICE_TARGET_RATIO=0.7`

Target per slice becomes `140 bps`.

Meaning:
- Higher ratio (`0.9`) -> larger slices, fewer orders, more impact risk.
- Lower ratio (`0.4`) -> smaller slices, more orders, lower impact risk.

Important:
- This is a **sizing target**, not the hard reject limit.
- Hard rejection still uses `MAX_ALLOWED_CEX_SLIPPAGE_BPS`.

Examples when `MAX_ALLOWED_CEX_SLIPPAGE_BPS=200`:
- `CEX_SLICE_TARGET_RATIO=0.5` -> target `100 bps`
- `CEX_SLICE_TARGET_RATIO=0.7` -> target `140 bps`
- `CEX_SLICE_TARGET_RATIO=1.0` -> target `200 bps`

#### Impact Risk (What Can Still Go Wrong)

Even with slicing, execution still has market-impact and timing risk:

- **Book movement risk**: preview uses current orderbook, but fills happen slightly later.
- **Depth cliff risk**: one more level consumed can sharply worsen average price.
- **Thin-book risk**: soft target may find no chunk; only hard-cap fallback may be possible.
- **Precision/truncation risk**: exchange step-size and min-notional rules can reduce consumed size.
- **Retry drift risk**: after a retry, liquidity and prices may be different.

How config controls this risk:

- `MAX_ALLOWED_CEX_SLIPPAGE_BPS`: hard per-slice reject limit (safety brake).
- `CEX_SLICE_TARGET_RATIO`: softer sizing target below hard limit (execution smoothness).
- `CEX_MIN_EXEC_USD`: prevents low-notional micro-fills that usually have poor quality.
- `CEX_BUY_*`: controls adaptive buy fallback when quote-mode truncation leaves meaningful residual.

#### Route Resolution Order

For each `deposit_symbol -> withdraw_symbol`, route selection is deterministic:

1. Legacy special override (`CKBTC <-> CKUSDT`) when applicable.
2. Direct market probe (`A_B` sell, then `B_A` buy fallback).
3. Configured hop search over `CEX_MEXC_AVAILABLE_PAIRS` up to `CEX_MEXC_MAX_HOPS`.

Resolved legs are persisted in CEX state and reused across retries.

#### Execution Algorithm (Per Trade Leg)

For each leg, the finalizer runs a resumable slice loop:

1. Compute target slice impact:
   `target_slice_bps = MAX_ALLOWED_CEX_SLIPPAGE_BPS * CEX_SLICE_TARGET_RATIO`
2. Fetch orderbook for the leg market.
3. Estimate the largest chunk under `target_slice_bps` using binary search on simulated impact.
4. If no positive chunk passes soft target, try one-shot fallback:
   full remaining amount is accepted only if fillable and under the hard cap.
5. If chunk notional is below `CEX_MIN_EXEC_USD`, mark as dust and stop this leg.
6. Submit one market slice with deterministic `client_order_id` (WAL-safe resume/idempotency).
7. Use **actual** exchange fill amounts (`input_consumed`, `output_received`) for math.
8. Compute realized execution price and slippage:
   - sell: `exec_price = output_received / input_consumed`
   - buy: `exec_price = input_consumed / output_received`
   - sell slippage bps: `max(0, (preview_mid - exec_price) / preview_mid * 10000)`
   - buy slippage bps: `max(0, (exec_price - preview_mid) / preview_mid * 10000)`
9. If realized slippage > `MAX_ALLOWED_CEX_SLIPPAGE_BPS`, fail fast.
10. Otherwise, persist progress (`remaining_in`, `total_out`) and continue until consumed/dust.
11. On buy legs only, if truncation ratio is large and residual is executable, arm one inverse/base fallback retry (bounded by `CEX_BUY_INVERSE_MAX_RETRIES`).

Route summary metrics are updated from slice notional, and weighted slippage is tracked for post-trade reporting.

**Supported venues:**

- `icpswap` — native ICP input only.
- `mexc` — requires `CEX_MEXC_API_KEY` and `CEX_MEXC_API_SECRET` when enabled.
- Venue IDs are ordered, comma-separated, and validated at startup. `SWAPPER` is obsolete and ignored.

### Storage & Export

```bash
DB_PATH=./wal.db
EXPORT_PATH=executions.csv
BUY_BAD_DEBT=false  # Set to true to liquidate even if not profitable
```

### Monitoring

```bash
WATCHDOG_WEBHOOK=https://your-webhook-url.com/endpoint
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
BOT_NAME=prod-liquidator
LOW_BALANCE_THRESHOLDS=ckBTC=0.001,ckUSDT=100,ckUSDC=100,ICP=5,ETH=0.05
BALANCE_CHECK_EXCLUDE=ckETH,ETH
```

> `WATCHDOG_WEBHOOK`: if set, the bot sends POST requests with JSON payloads for monitoring and alerting (for example: Slack, Discord, or custom services).
> `SLACK_WEBHOOK_URL`: if set, the daemon sends Slack incoming-webhook alerts for main low balances, bridge ETH/ckETH low balance, lifecycle changes, and finalized liquidations.
> `BOT_NAME`: optional Slack label used in notifications so shared channels can identify which bot emitted the alert.
> `LOW_BALANCE_THRESHOLDS`: optional comma-separated symbol thresholds in token units, applied only when Slack monitoring is enabled with `SLACK_WEBHOOK_URL`. If `SLACK_WEBHOOK_URL` is not provided, no Slack low-balance alerts are sent even when `LOW_BALANCE_THRESHOLDS` is configured. Missing symbols use defaults; unknown symbols are monitored only when explicitly listed.
> `BALANCE_CHECK_EXCLUDE`: optional comma-separated symbols to exclude from main-account low-balance Slack alerts. Bridge balance alerts remain fixed to ETH and ckETH.

---

## Identity Management

The bot derives identities/addresses from one BIP39 mnemonic using two namespaces:

| Namespace | Role | Path / Derivation | Purpose |
|-----------|------|-------------------|---------|
| **Operational** (`account=0`) | Liquidator | `m/44'/60'/0'/0/0` | Main liquidation operations |
| **Operational** (`account=0`) | Trader | `m/44'/60'/0'/0/1` | Isolated swap execution |
| **Operational** (`account=0`) | Recovery | `RECOVERY_ACCOUNT` fixed subaccount | Fallback custody for failed flows |
| **Bridge** (`account=1`) | Bridge EVM signer | `m/44'/60'/1'/0/0` | Bridge source wallet signer |
| **Bridge** (`account=1`) | Bridge ICP owner | `m/44'/60'/1'/0/1` | Bridge-side ICP owner principal |
| **Bridge** (`account=1`) | Bridge BTC key/address | `m/84'/0'/1'/0/0` | Future BTC/ckBTC bridge routes |

### Account Structure Diagram

```text
Mnemonic
└── HD Key Tree
    ├── Operational Namespace (account = 0)
    │   ├── Liquidator (EVM signer)        m/44'/60'/0'/0/0
    │   ├── Liquidator (ICP principal)     m/44'/60'/0'/0/0
    │   ├── Trader (ICP principal)         m/44'/60'/0'/0/1
    │   └── Recovery account               owner + RECOVERY_ACCOUNT subaccount (0xBEEF...00)
    │       (recovery is a subaccount, not a separate HD index)
    │
    └── Bridge Namespace (account = 1)
        ├── Bridge EVM signer/address      m/44'/60'/1'/0/0
        ├── Bridge ICP owner principal     m/44'/60'/1'/0/1
        ├── Bridge BTC key/address         m/84'/0'/1'/0/0
        └── Bridge ICP account             owner-only (subaccount = None)
```

Current bridge/finalizer wiring:
- Forward source: derived `bridge_evm_address` (`ETH@ETH -> ckETH`, `USDC@ETH -> ckUSDC`)
- Reverse source: derived bridge ICP owner account (`ckETH@ICP -> ETH@ETH`, `ckUSDC@ICP -> USDC@ETH`)
- Destination: resolved per request in code (forward default: liquidator ICP principal; reverse default: liquidator EVM address)
- Routes: loaded from a code-level bridge catalog (`ckETH` native and ERC-20 routes); submissions are serialized per bridge source.
- Design details: `docs/bridge-architecture.md`

### Generate New Identities

```bash
liquidator account new
```

Creates a new mnemonic used to deterministically derive all operational and bridge roles.

### Show Existing Identities

```bash
liquidator account show
```

Displays a table of all identities with their principals and statuses.

---

## Architecture Overview

### Crate Structure

```
liquidium-pipeline/
├── core/        # Chain-agnostic types, tokens, balance service, RAY math
├── connectors/  # ICP/EVM backends, key derivation, canister calls
├── pipeline/    # Main app - stages, executors, finalizers, CLI
└── commons/     # Shared utilities and error types
```

### Pipeline Stages

```mermaid
stateDiagram-v2
    [*] --> OpportunityQuery
    OpportunityQuery --> StrategyBuild
    StrategyBuild --> ExecuteLiquidation
    ExecuteLiquidation --> WalRecord

    WalRecord --> SettlementWatch
    SettlementWatch --> MultiVenuePlan: collateral confirmed
    MultiVenuePlan --> VenueLegs: commit immutable plan
    VenueLegs --> VenueLegs: advance and persist each leg
    VenueLegs --> Finalize: all legs complete or recovered
    Finalize --> WalUpdate
    SettlementWatch --> Export: succeeded

    Finalize --> Export
    Export --> [*]
```

| Stage | Description |
|-------|-------------|
| **Opportunity Discovery** | Polls lending canister for at-risk positions |
| **Strategy Filter** | Filters opportunities by profitability and supported assets |
| **Liquidation Execution** | Calls `liquidate()` on lending canister, seizes collateral |
| **Swap Finalization** | Plans and executes enabled venue legs through one persisted orchestrator |
| **Export / Reporting** | Saves execution details to CSV |

Stages are implemented with `async-trait` for composability.

### Multi-Venue Swap Pipeline

The production finalizer quotes enabled venues through one amount-scoped adapter contract. Allocation policy, exchange mechanics, WAL orchestration, and result aggregation remain separate layers. See [Multi-Venue Swap Pipeline](docs/multi-venue-swap-pipeline.md) for the persisted model, `icpswap_first` policy, restart behavior, and extension contract.

### Swap Strategies

| Strategy | Description |
|----------|-------------|
| **ICPSwap first** | Uses ICPSwap below the impact cap and allocates executable overflow to enabled exchanges. |

### Retry & State Management

- **SQLite WAL** tracks liquidation state across restarts
- **Retryable failures** retry up to 5 times with exponential backoff
- **Idempotent operations** prevent double-liquidations

---

## CLI Commands

### Run the Liquidation Loop

```bash
liquidator run
# optional: custom control socket
liquidator run --sock-path /run/liquidator/ctl.sock
# optional: enable file sink at default path
liquidator run --log-file
# optional: enable file sink at custom path
liquidator run --log-file ./liquidator.log
# optional: explicitly disable file sink
liquidator run --no-log-file
```

Starts the foreground daemon loop (systemd-supervised) that continuously monitors and executes liquidations.
The control socket accepts `pause` / `resume` from attachable clients.
By default, `liquidator run` does not write a local log file.
Passing `--log-file` (without a value) enables file logging at the default path:
`<temp>/liquidator/liquidator.log` (for example, `/tmp/liquidator/liquidator.log` on Linux).
Passing `--log-file /custom/path.log` writes to that custom file.
`--log-file` and `--no-log-file` are mutually exclusive.

Default control socket path:
- Linux: `/run/liquidator/ctl.sock`
- non-Linux: `<temp>/liquidator/ctl.sock`

Linux note (non-systemd runs): `/run/liquidator` may not exist or may not be writable.
If you are not using systemd `RuntimeDirectory`, either create `/run/liquidator` with appropriate ownership/permissions, or pass a user-writable socket path via `--sock-path` (for example `--sock-path /tmp/liquidator/ctl.sock`).

### Start the TUI

```bash
liquidator tui
# optional: custom socket/unit/file source
liquidator tui --sock-path /run/liquidator/ctl.sock --unit-name liquidator.service --log-file /path/to/log
```

Launches an attachable terminal UI to **pause/resume** the daemon, view **WAL status**, **profits** (from `EXPORT_PATH`), **balances**, and run **withdrawals** (ICP tokens only).
Log source selection:
- If `--log-file` is provided, TUI tails that file on any OS.
- If running on Linux with no `--log-file`, TUI prefers `journalctl -u <unit-name>` (default unit: `liquidator.service`).
- If that unit is inactive on Linux, TUI may auto-tail `<temp>/liquidator/liquidator.log` if the file exists and was updated recently.
- On non-Linux systems without `--log-file`, TUI tails `<temp>/liquidator/liquidator.log` if present, otherwise it shows a no-log-source notice.

**Key bindings:**
- `r` — pause/resume
- `b` — refresh balances
- `p` — refresh profits
- `w` — withdraw (from balances)
- `d` — MEXC deposit address (from balances); in Withdraw panel, refresh deposit info
- `tab` — switch views
- `q` — quit

### Check Balances

```bash
liquidator balance
```

Displays **main**, **trader**, and **recovery** balances. Recovery balances are marked as "seized collateral (stale, pending withdrawal if swaps failed)".

### ICPSwap ICP → ckUSDC Test

Fetch a live quote without moving tokens:

```bash
liquidator icpswap --amount 0.1
```

Execute the quoted swap after reviewing the pool, fees, expected output, and interactive confirmation:

```bash
liquidator icpswap --amount 0.1 --execute
```

`--amount` is the maximum ICP debit, including the ICRC-1 transfer and pool-deposit fees. Executions use ICPSwap's manual `transfer → deposit → swap → withdraw` flow. The configured slippage is a hard cap: the first attempt uses a tighter limit, and up to three failed swaps are requoted with wider slippage without crossing the original minimum-output floor. A decoded slippage error retries immediately after balance reconciliation; an ambiguous swap retries only after its pool balances remain unchanged through the two-minute reconciliation window.

The command uses the configured liquidator identity and writes resumable checkpoints under `~/.liquidium-pipeline/icpswap-runs/`. The initial ICRC-1 transfer persists its `created_at_time` and ledger block index so an interrupted transfer can be retried with ledger deduplication. If an execution is interrupted or times out, resume it with:

```bash
liquidator icpswap --resume <RUN_ID>
```

Checkpoints from the removed `depositFromAndSwap` and `depositFrom → swap → withdraw` implementations are not supported and cannot be resumed.

From the repository, the equivalent smoke-test commands are:

```bash
cargo run -p liquidium-pipeline -- icpswap --amount 0.1
cargo run -p liquidium-pipeline -- icpswap --amount 0.1 --execute
cargo run -p liquidium-pipeline -- icpswap --resume <RUN_ID>
```

This is a real-funds mainnet smoke test and is intentionally excluded from automated CI. If the manual swap fails, the deposited ICP is withdrawn from the pool and confirmed back in the liquidator account.

### MEXC Smoke Swap + Withdraw

Dry-run preflight (no side effects):

```bash
liquidator mexc-smoke-swap-withdraw --amount-ckbtc 0.001
```

Live execution (swap + withdraw):

```bash
liquidator mexc-smoke-swap-withdraw \
  --amount-ckbtc 0.001 \
  --execute \
  --withdraw-address 0x1111111111111111111111111111111111111111 \
  --withdraw-network ETH
```

Live execution with final bridge step (USDC@ETH -> ckUSDC):

```bash
liquidator mexc-smoke-swap-withdraw \
  --amount-ckbtc 0.001 \
  --execute \
  --bridge-after-withdraw \
  --bridge-destination <ICP_ACCOUNT_OR_PRINCIPAL>
```

Behavior:
- Fixed smoke pair: `ckBTC -> USDC`.
- Uses existing free MEXC `CKBTC` balance (no on-chain deposit transfer step).
- Route resolution mirrors production MEXC finalizer logic.
- Without `--execute`, runs route/balance preflight only and exits.
- `--bridge-after-withdraw` adds a final bridge submission `USDC@ETH -> ckUSDC`.
- With bridge step enabled, withdraw destination is forced to the configured bridge EVM address.
- If `--bridge-destination` is omitted, bridge destination defaults to liquidator principal.

### MEXC Smoke Bridge + Swap + Withdraw

Dry-run preflight (no side effects):

```bash
liquidator mexc-smoke-bridge-swap-withdraw --amount-ckusdc 100
```

Live execution (bridge `ckUSDC@ICP -> USDC@ETH` to MEXC deposit, swap `USDC -> CKBTC`, withdraw `CKBTC`):

```bash
liquidator mexc-smoke-bridge-swap-withdraw \
  --amount-ckusdc 100 \
  --execute \
  --withdraw-network ICP
```

Behavior:
- Bridge source account is the configured bridge ICP owner account (principal-only).
- MEXC USDC/ETH deposit address is fetched dynamically per request.
- Waits for bridge source funding before submit, then waits for MEXC USDC balance credit.
- Swaps credited USDC to CKBTC using production route logic.
- Default withdraw destination is the liquidator principal; override with `--withdraw-address`.

### MEXC Smoke ckETH -> USDC -> ckUSDC

Dry-run preflight (no side effects):

```bash
liquidator mexc-smoke-bridge-swap-withdraw-cketh --amount-cketh 0.05
```

Live execution (bridge `ckETH@ICP -> ETH@ETH` to MEXC deposit, swap `ETH -> USDC`, withdraw to bridge source, then bridge `USDC@ETH -> ckUSDC@ICP`):

```bash
liquidator mexc-smoke-bridge-swap-withdraw-cketh --amount-cketh 0.05 --execute
```

Behavior:
- Uses the production MEXC finalizer bridge/trade/withdraw path and route discovery.
- Uses configured `CEX_MEXC_AVAILABLE_PAIRS` for market routing.
- Final bridged asset is `ckUSDC@ICP` to the liquidator principal account.

### Withdraw Funds

#### Interactive Wizard

```bash
liquidator withdraw
```

Launches an interactive wizard to select source account, asset, amount, and destination.

#### Non-Interactive (Flags)

```bash
liquidator withdraw \
  --source main \
  --destination <main|trader|recovery|ACCOUNT> \
  --asset <ASSET_SYMBOL|all> \
  --amount <DECIMAL|all>
```

**Options:**
- `--source`: `main`, `trader`, or `recovery`
- `--destination`: `main`, `trader`, `recovery`, or target ICP account/principal
- `--asset`: Asset symbol (e.g., `ckBTC`, `ckUSDT`, `ICP`) or `all`
- `--amount`: Specific amount or `all` for full balance

**Example:**
```bash
liquidator withdraw --source main --destination abc123-xyz --asset ckUSDT --amount all
```

---

## Operations Runbook

1. Verify account wiring and credentials:

   ```bash
   liquidator account show
   liquidator balance
   ```

2. Start the runner:

   ```bash
   ENABLED_SWAP_VENUES=icpswap,mexc liquidator run
   ```

3. Enable monitoring and inspect output artifacts:

   - Configure `WATCHDOG_WEBHOOK` for generic alerts and `SLACK_WEBHOOK_URL` for Slack alerts.
   - Check CSV exports at `EXPORT_PATH` and WAL state at `DB_PATH`.

4. Move funds operationally when needed:

   ```bash
   liquidator withdraw --source main --destination <main|trader|recovery|ACCOUNT> --asset <ASSET_SYMBOL|all> --amount <DECIMAL|all>
   ```

---

## Developer Setup

```bash
# Clone the repository
git clone https://github.com/Liquidium-Inc/liquidium-pipeline.git
cd liquidium-pipeline

# Build in release mode
cargo build --release

# Binary location
./target/release/liquidator
```

### Run Tests

```bash
cargo test
```

### Environment Logging

Set `RUST_LOG` for debug output:

```bash
RUST_LOG=debug liquidator run
```

For Grafana/Loki-friendly logs (no ASCII tables/spinner/banner), build with the `plain-logs` feature:

```bash
cargo build --release -p liquidium-pipeline --features plain-logs
RUST_LOG=info ./target/release/liquidator run
```

In `plain-logs` builds, interactive withdraw prompts are disabled; use non-interactive `liquidator withdraw --source ... --destination ... --asset ... --amount ...` flags.

### Daemon + systemd Example

Use the sample unit at `dev/liquidator.service`:

```ini
[Service]
RuntimeDirectory=liquidator
RuntimeDirectoryMode=0770
ExecStart=/home/liquidator/.local/bin/liquidator run --sock-path /run/liquidator/ctl.sock
Restart=always
```

Use an absolute binary path in `ExecStart` (systemd does not expand `~`).
If you installed with `install.sh`, the typical binary entrypoint is `~/.local/bin/liquidator` for that user; resolve it with `command -v liquidator` and place that absolute path in the unit.

Validate daemon logs:

```bash
journalctl -u liquidator.service -f -o short
```

Attach the TUI at any time:

```bash
liquidator tui --sock-path /run/liquidator/ctl.sock --unit-name liquidator.service
```

Convenience installer for Linux:

```bash
./dev/install-daemon.sh
```

`install-daemon.sh` requires sudo/root because it writes `/etc/systemd/system/*.service`,
may create the `liquidator` user/group, runs `systemctl daemon-reload`, enables the unit,
and restarts it.

This installs/updates `/etc/systemd/system/liquidator.service`, creates the
`liquidator` user/group if missing, runs `systemctl daemon-reload`, enables the
unit, and restarts it.

How binary path is handled:
- `install-daemon.sh` does **not** copy the binary into `/usr/local/bin`.
- It uses `--bin-path` if provided; otherwise it uses `command -v liquidator` and writes that absolute path into `ExecStart`.
- The separate `install.sh` script builds a release binary under `~/.liquidium-pipeline/releases/...` and symlinks `~/.local/bin/liquidator` to it.

Linux non-service mode with file tail:

```bash
liquidator run --log-file
liquidator tui
```

On macOS/dev, use file fallback explicitly:

```bash
liquidator run --log-file ./liquidator.log
liquidator tui --log-file ./liquidator.log
```

---

## Security

- Treat `MNEMONIC_FILE`, `CEX_MEXC_API_KEY`, and `CEX_MEXC_API_SECRET` as secrets; never commit them.
- Restrict CEX API keys to required permissions only, and rotate keys periodically.
- Keep mnemonic backups offline and access controlled.
- Prefer separate runtime users/environments for production bot instances.

## Troubleshooting

- `LENDING_CANISTER not configured` / `EVM_RPC_URL not configured`: confirm required env vars are set in `.env` or `config.env`.
- `Invalid source account` / destination parse errors during withdraw: use `main|trader|recovery` aliases or valid account/principal text.
- MEXC initialization failing while `mexc` is enabled: verify `CEX_MEXC_API_KEY` and `CEX_MEXC_API_SECRET`, or remove `mexc` from `ENABLED_SWAP_VENUES`.
- Noisy terminal output in containerized logging stacks: build and run with `--features plain-logs`.
- Missing diagnostic detail: rerun with `RUST_LOG=debug`.
- Runtime socket permission mismatch under systemd: run `systemctl daemon-reload` and restart `liquidator.service`; ensure `RuntimeDirectory=liquidator` and `RuntimeDirectoryMode=0770` are set on the active unit.
- `liquidator run` exits immediately saying systemd unit is already active: this is intentional duplicate-daemon protection; stop the unit first or use `liquidator tui` to attach.

## Notes

- Works with ICRC-1/ICRC-2 assets (ckBTC, ckUSDT, ICP, etc.)
- Identity/config can be system-wide or project-local
- Composable stages allow for custom liquidation strategies
- EVM support enables cross-chain liquidations (Arbitrum)

> Tip: use interactive wizards for manual operations and CLI flags for automation (cron jobs, scripts).

---

## License

MIT
