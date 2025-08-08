
# 🧯 Liquidator Bot Framework for ICP

A modular, event-driven off-chain liquidation bot framework for [Internet Computer (ICP)](https://internetcomputer.org/) protocols.  
Inspired by Artemis/MEV patterns and designed for permissionless, community-driven liquidations.

---

## ✨ Features

- 🔁 **Pipeline Architecture** — Composable stages for discovery, execution, and swaps.  
- ⚡ **Async Rust** — Highly concurrent and efficient.  
- 👷 **Extensible** — Add custom risk checks, strategies, swaps, or notification stages.  
- 📦 **Permissionless** — Anyone can run it.  
- 🧪 **CLI Interface** — Manage balances, funds, and identities.

---

## 📦 Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/Liquidium-Inc/liquidium-pipeline/main/install.sh | sudo bash
```
This will:
- Clone/update the repo
- Build the liquidator binary
- Install it to /usr/local/bin/liquidator
- Create ~/.config/liquidator/config.env if it doesn’t exist

⸻

⚙ Configuration

The bot loads configuration from:

1.	~/.config/liquidator/config.env (preferred, created automatically)

2.	.env in the current directory (optional overrides)

Example config.env:

```bash
IC_URL=https://ic0.app
IDENTITY_PEM=/home/youruser/.config/liquidator/id.pem
LENDING_CANISTER=ryjl3-tyaaa-aaaaa-aaaba-cai
EXPORT_PATH=executions.csv
BUY_BAD_DEBT=false

# Comma-separated principal:symbol
DEBT_ASSETS=principal1:BTC,principal2:ETH
COLLATERAL_ASSETS=principal3:ckBTC,principal4:ckETH
```

⸻

🔑 Identity Management

Generate a new Ed25519 identity:
```
liquidator account new
```

Show the liquidator principal:
```
liquidator account show
```
By default, identities are stored at:
```
~/.config/liquidator/id.pem
```
Change location by setting IDENTITY_PEM in config.env.

⸻

🏗️ Architecture Overview

Pipeline Stages

```mermaid
flowchart LR
    A[Opportunity Discovery] --> B[Liquidation Execution]
    B --> C[Asset Swap]
    C --> D[Reporting / Export]
```


- Opportunity Discovery → Polls ICP canisters for loans or positions eligible for liquidation.
- Liquidation Execution → Calls the canister to liquidate an at-risk position, seizing collateral.
- Asset Swap → Swaps seized collateral for a desired asset.
- Reporting / Export → Saves execution details to CSV or external systems.

Stages are implemented with async-trait for composability.

⸻

🧪 CLI Commands

Run loop:
```
liquidator run
```
Check balances:
```
liquidator balance
```
Withdraw funds:
```
liquidator withdraw <ASSET_PRINCIPAL> <AMOUNT> <TO_PRINCIPAL>
```
Show identity principal:
```
liquidator account show
```
Generate new identity:
```
liquidator account new
```

⸻

🛠 Developer Setup
```bash
git clone https://github.com/Liquidium-Inc/liquidium-pipeline.git

cd liquidium-pipeline

cargo build --release

Binary will be at:

target/release/liquidator

```
⸻

📝 Notes
- Works with ICRC-1 assets like ckBTC
- Identity/config can be system-wide or project-local
- Composable stages allow for custom liquidation strategies

⸻

📄 License

MIT
