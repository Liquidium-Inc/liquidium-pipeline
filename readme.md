# 🧯 Liquidator Bot Framework for ICP

A modular, event-driven off-chain liquidation bot framework for [Internet Computer (ICP)](https://internetcomputer.org/) protocols.
Inspired by Artemis/MEV patterns and designed for permissionless, community-driven liquidations.

---

## ✨ Features

- 🔁 **Pipeline Architecture** — Composable stages for discovery, strategy, execution, finalization, and export.
- ⚡ **Async Rust** — Highly concurrent and efficient with Tokio runtime.
- 🔀 **Multi-Chain** — Primary support for ICP with EVM (Arbitrum) integration.
- 💱 **Flexible Swaps** — DEX (Kong), CEX (MEXC), or Hybrid strategies.
- 👷 **Extensible** — Add custom risk checks, strategies, swaps, or notification stages.
- 📦 **Permissionless** — Anyone can run it.
- 🔐 **Multi-Account** — Separate Liquidator, Trader, and Recovery identities for security.
- 🧪 **CLI Interface** — Manage balances, funds, and identities.
- 💾 **Persistent State** — SQLite WAL ensures no double-liquidations and supports retries.

---

## 📦 Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/Liquidium-Inc/liquidium-pipeline/main/install.sh | bash
```

This will:

- Clone/update the repo to `~/.liquidium-pipeline/repo`
- Build the liquidator binary in release mode
- Install it to `~/.local/bin/liquidator`
- Create `~/.liquidium-pipeline/config.env` if it doesn't exist

> 💡 Set `SKIP_RUST=true` before running to skip Rust installation if already present.

---

## ⚙️ Configuration

The bot loads configuration from (in order of precedence):

1. Environment variables (direct overrides)
2. `.env` in the current directory (optional overrides)
3. `~/.config/liquidator/config.env` (user-level defaults)

### Core Configuration

```bash
# ICP Blockchain
IC_URL=https://ic0.app
LENDING_CANISTER=nja4y-2yaaa-aaaae-qddxa-cai

# EVM Blockchain (Optional)
EVM_RPC_URL=https://arb1.arbitrum.io/rpc

# Identity
MNEMONIC_FILE=~/.liquidium-pipeline/wallets/key
# Or use PEM directly:
# IDENTITY_PEM=~/.config/liquidator/id.pem

# Assets (comma-separated principal:symbol pairs)
DEBT_ASSETS=principal1:ckBTC,principal2:ckUSDT,principal3:ICP
COLLATERAL_ASSETS=principal1:ckBTC,principal2:ckUSDT,principal3:ICP
```

### Swap Configuration

```bash
# Swap strategy: dex | cex | hybrid
SWAPPER=hybrid

# DEX (Kong)
KONG_SWAP_BACKEND=2ipq2-uqaaa-aaaar-qailq-cai
MAX_ALLOWED_DEX_SLIPPAGE=7500  # 0.75% in basis points

# CEX (MEXC) - Optional
CEX_LIST=mexc
CEX_MEXC_API_KEY=your_api_key
CEX_MEXC_API_SECRET=your_api_secret
```

### Storage & Export

```bash
DB_PATH=./wal.db
EXPORT_PATH=executions.csv
BUY_BAD_DEBT=false  # Set to true to liquidate even if not profitable
```

### Monitoring

```bash
WATCHDOG_WEBHOOK=https://your-webhook-url.com/endpoint
```

> 🔔 **WATCHDOG_WEBHOOK**: If set, the bot sends POST requests with JSON payloads for monitoring and alerting (e.g., Slack, Discord, or custom services).

---

## 🔑 Identity Management

The bot uses **three identities** derived from a BIP39 mnemonic for security:

| Identity | Purpose | Description |
|----------|---------|-------------|
| **Liquidator** | Main account | Initiates liquidations, receives collateral |
| **Trader** | Swap execution | Isolated swap operations (reduces MEV risk) |
| **Recovery** | Fallback | Catches failed collateral transfers |

### Generate New Identities

```bash
liquidator account new
```

Creates new Ed25519/Secp256k1 identities for all three roles.

### Show Existing Identities

```bash
liquidator account show
```

Displays a table of all identities with their principals and statuses.

---

## 🏗️ Architecture Overview

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
flowchart LR
    A[Opportunity Discovery] --> B[Strategy Filter]
    B --> C[Liquidation Execution]
    C --> D[Swap Finalization]
    D --> E[Export / Reporting]
```

| Stage | Description |
|-------|-------------|
| **Opportunity Discovery** | Polls lending canister for at-risk positions |
| **Strategy Filter** | Filters opportunities by profitability and supported assets |
| **Liquidation Execution** | Calls `liquidate()` on lending canister, seizes collateral |
| **Swap Finalization** | Swaps collateral via DEX/CEX/Hybrid strategy |
| **Export / Reporting** | Saves execution details to CSV |

Stages are implemented with `async-trait` for composability.

### Swap Strategies

| Strategy | Description |
|----------|-------------|
| **DEX** | Swaps via Kong on ICP. Retries with increasing slippage (0.75% → 5%). |
| **CEX** | Deposits to MEXC, swaps, and withdraws. |
| **Hybrid** | Tries DEX first, falls back to CEX on failure. |

### Retry & State Management

- **SQLite WAL** tracks liquidation state across restarts
- **Retryable failures** retry up to 5 times with exponential backoff
- **Idempotent operations** prevent double-liquidations

---

## 🧪 CLI Commands

### Run the Liquidation Loop

```bash
liquidator run
```

Starts the main liquidation loop that continuously monitors and executes liquidations.

### Check Balances

```bash
liquidator balance
```

Displays both **main** and **recovery** balances. Recovery balances are marked as "seized collateral (stale, pending withdrawal if swaps failed)".

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
  --destination <TO_PRINCIPAL> \
  --asset <ASSET_SYMBOL> \
  --amount <AMOUNT|all>
```

**Options:**
- `--source`: `main` or `recovery`
- `--destination`: Target principal
- `--asset`: Asset symbol (e.g., `ckBTC`, `ckUSDT`, `ICP`)
- `--amount`: Specific amount or `all` for full balance

**Example:**
```bash
liquidator withdraw --source main --destination abc123-xyz --asset ckUSDT --amount all
```

---

## 🛠️ Developer Setup

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

---

## 📝 Notes

- Works with ICRC-1/ICRC-2 assets (ckBTC, ckUSDT, ICP, etc.)
- Identity/config can be system-wide or project-local
- Composable stages allow for custom liquidation strategies
- EVM support enables cross-chain liquidations (Arbitrum)

> 💡 **Tip:** Use interactive wizards for manual operations or CLI flags for automation (cron jobs, scripts).

---

## 📄 License

MIT
