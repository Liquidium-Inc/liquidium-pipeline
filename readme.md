
A modular, event-driven offchain liquidation bot framework for [Internet Computer (ICP)](https://internetcomputer.org/) protocols.  
Inspired by Artemis/MEV patterns and designed for permissionless, community-driven liquidations.

---

## Features

- 🔁 **Pipeline Architecture**: Composable pipeline stages for opportunity discovery, execution, and asset swaps.
- ⚡ **Async/Concurrent**: Built with async Rust for efficiency and scalability.
- 👷 **Extensible**: Add custom modules for risk checks, strategy, swaps, notification, and more.
- 📦 **Open Source & Permissionless**: Anyone can run a bot, or contribute new pipeline stages.

---

# Architecture Overview

### **How It Works**

The bot is structured as a **pipeline of stages**.  
Each stage is a Rust struct implementing the `PipelineStage` trait (using `async-trait`).  
Data flows through the pipeline as it is polled and processed in steps:

1. **Opportunity Discovery:**  
   - Polls the ICP protocol canister for loans or positions eligible for liquidation.
   - (Implements `PipelineStage<(), Vec<LiquidationOpportunity>>`)
2. **Liquidation Execution:**  
   - Calls the canister to liquidate an at-risk position, seizing collateral.
   - (Implements `PipelineStage<LiquidationOpportunity, ExecutionReceipt>`)
3. **Swapping Assets:**  
   - Swaps the seized collateral for a desired asset (e.g., via DEX).
   - (Implements `PipelineStage<ExecutionReceipt, SwapResult>`)
