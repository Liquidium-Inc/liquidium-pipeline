Sure — here’s a clean, copy-paste-ready README.md version:

⸻


# 🧯 Liquidator Bot Framework for ICP

A modular, event-driven offchain liquidation bot framework for [Internet Computer (ICP)](https://internetcomputer.org/) protocols.  
Inspired by Artemis/MEV patterns and designed for permissionless, community-driven liquidations.

---

## ✨ Features

- 🔁 **Pipeline Architecture**: Composable pipeline stages for opportunity discovery, execution, and asset swaps.
- ⚡ **Async/Concurrent**: Built with async Rust for efficiency and scalability.
- 👷 **Extensible**: Add custom modules for risk checks, strategy, swaps, notification, and more.
- 📦 **Open Source & Permissionless**: Anyone can run a bot, or contribute new pipeline stages.
- 🧪 **CLI Interface**: Check balances, withdraw funds, and manage identities with ease.

---

## 🏗️ Architecture Overview

### How It Works

The bot is structured as a **pipeline of stages**.  
Each stage is a Rust struct implementing the `PipelineStage` trait (via `async-trait`).  
Data flows through the pipeline as it is polled and processed in steps:

1. **Opportunity Discovery**  
   Polls ICP canisters for loans or positions eligible for liquidation.  
   → `PipelineStage<(), Vec<LiquidationOpportunity>>`

2. **Liquidation Execution**  
   Calls the canister to liquidate an at-risk position, seizing collateral.  
   → `PipelineStage<LiquidationOpportunity, ExecutionReceipt>`

3. **Swapping Assets**  
   Swaps seized collateral for a desired asset (e.g., via DEX).  
   → `PipelineStage<ExecutionReceipt, SwapResult>`

---

## 🧪 CLI Usage

### Setup

```bash
git clone <your-repo>
cd <your-repo>
cargo build --release
```

The binary will be located at:
```
target/release/liquidator
```
--- 
### Identity

The bot expects an identity PEM file at ./id.pem.
Generate a new one using the CLI (account new) or manually.

---

### Commands

#### 🔁 Run

Starts the liquidation loop.

```bash
./liquidator run
```
💰 Balance

Shows token balances for the configured wallet.

```bash
./liquidator balance
```

💸 Withdraw

Withdraw funds to a specified principal.
```bash
./liquidator withdraw <ASSET_PRINCIPAL> <AMOUNT> <TO_PRINCIPAL>
```
Example:
```bash
./liquidator withdraw ryjl3-tyaaa-aaaaa-aaaba-cai 0.01 q5wrv-piaaa-aaaag-qaagq-cai
```

🆔 Account Show

Displays the principal of the wallet.

```bash
./liquidator account show
```

🛠️ Account New

Generates a new identity (overwrites ./id.pem).

```bash
./liquidator account new
```

---
📝 Notes

	•	Compatible with ICRC-1 assets like ckBTC.
	•	Identity PEM path defaults to ./id.pem unless configured otherwise.
	•	Composable pipeline structure enables custom execution strategies.

---

📄 License

MIT