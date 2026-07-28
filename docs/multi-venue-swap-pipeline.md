# Multi-Venue Swap Pipeline

This document explains the production multi-venue swap-finalization pipeline.

Venue eligibility is configured with an ordered list, for example `ENABLED_SWAP_VENUES=icpswap,mexc`. There is no CEX/DEX/Hybrid routing mode.

## Goals

The pipeline is designed to:

- Quote every venue through one amount-scoped contract.
- Keep allocation policy separate from exchange mechanics.
- Split one seized-collateral amount into independently persisted venue legs.
- Resume each leg safely after process restarts.
- Add venues such as Kraken without changing the WAL schema or orchestrator.
- Preserve legacy in-progress MEXC and ICPSwap executions during rollout.

## Architecture at a Glance

```text
                         REPLACEABLE POLICY
              +------------------------------------+
              | icpswap_first                      |
              | future best-price / balanced policy|
              +------------------+-----------------+
                                 |
                                 v
+----------------------+   GENERIC MULTI-VENUE PIPELINE
| Successful           |   +-------------------------------+
| liquidation receipt  +-->| Allocation strategy           |
+----------------------+   |              |                |
                           |              v                |
                           | ENABLED_SWAP_VENUES             |
                           |              |                |
                           |              v                |
                           |       Venue registry           |
                           |        /     |      \          |
                           |       /      |       \         |
                           |      v       v        v        |
                           | ICPSwap    MEXC    Kraken ...   |
                           | adapter   adapter   adapter     |
                           |              |                |
                           |              v                |
                           | MultiVenueExecutionState       |
                           |              |                |
                           |              v                |
                           | WAL orchestrator               |
                           |              |                |
                           |              v                |
                           | Result aggregator              |
                           +--------------+----------------+
                                          |
                                          v
                           Profit snapshots / export / logs / TUI
```

The generic layer only deals with venue IDs, requests, quotes, leg state, and progress. It must not contain exchange-specific branches such as `if venue == "mexc"`.

## Layer Responsibilities

| Layer | Knows about | Must not own |
|---|---|---|
| Allocation strategy | Business policy, impact limits, minimum edge, overflow ordering | Deposits, orders, withdrawals, bridges, WAL writes |
| Venue registry | `venue_id -> MultiVenueAdapter` lookup and concurrent previews | Allocation policy |
| Venue adapter | Exchange APIs, route discovery, price-impact calculation, venue-local recovery | Another venue's state or the parent WAL status |
| WAL orchestrator | Plan commitment, leg ordering, persistence, restart precedence, parent outcome | ICPSwap pool math or MEXC order-book mechanics |
| Result aggregator | Combining completed leg amounts and execution records | Routing or rerouting |

This separation is what makes adding another exchange local: a Kraken adapter should not require changes to the persisted leg vector or WAL orchestration algorithm.

## Unified Venue Contract

Every exchange implements the same amount-scoped interface:

```rust
pub trait MultiVenueAdapter {
    fn venue_id(&self) -> &'static str;

    async fn preview(
        &self,
        request: &SwapRequest,
    ) -> Result<VenueRoutePreview, String>;

    async fn advance(
        &self,
        leg: &VenueLegState,
    ) -> Result<VenueLegProgress, String>;

    async fn recover(
        &self,
        leg: &VenueLegState,
    ) -> Result<VenueLegProgress, String>;
}
```

The boundary uses `ChainTokenAmount`. Exchange decimals, pool units, market symbols, order IDs, and bridge state stay inside the adapter.

```text
Allocation planner          Venue registry         Venue adapters
        |                          |                       |
        |-- preview(request) ----->|                       |
        |                          |--+ ICPSwap.preview() ->| choose pool
        |                          |  |                     | calculate X96 spot impact
        |                          |  +<-- quote + state ---|
        |                          |                       |
        |                          |--+ MEXC.preview() ---->| resolve markets
        |                          |  |                     | simulate order-book VWAP
        |                          |  +<-- quote + state ---|
        |                          |                       |
        |<-- ordered quote book ---|                       |
        |                          |                       |
        | validate quotes and allocate exact amounts       |

Strategy-selected venue subsets run concurrently. The common safe-ICPSwap path quotes ICPSwap first and avoids unnecessary exchange calls.
```

### Unified quote semantics

`SwapQuote` exposes the same planning fields for all venues:

- `mid_price`: the venue reference price before size impact.
- `exec_price`: expected amount-scoped execution price.
- `estimated_price_impact_bps`: adverse difference between reference and execution price.
- `receive_amount`: estimated output in the receive asset's native units.
- `legs`: venue route description for persistence and observability.

ICPSwap derives the reference output from pool `sqrtPriceX96` metadata and compares it with the canister quote. MEXC derives its reference from the best order-book side and compares it with the simulated VWAP. Execution drift is tracked separately from planning-time price impact.

## Current `icpswap_first` Policy

The first strategy intentionally expresses Liquidium's routing policy rather than pretending every strategy is best-price routing.

```text
Actual collateral_received
            |
            v
     Is input native ICP?
        /           \
      no             yes
      |               |
      v               v
Overflow-only      Quote full amount
venue plan         on ICPSwap
                      |
                      v
              Impact below 100 bps?
                  /          \
                yes           no
                |              |
                v              v
          All ICPSwap     Binary-search largest
                          confirmed-safe amount
                                |
                                v
                     Re-quote exact allocation
                     and exact venue remainder
                                |
                                v
                   Remainder meets CEX minimum?
                         /              \
                       yes               no
                       |                  |
                       v                  v
               ICPSwap + overflow   Re-quote full ICPSwap
                    split             /          \
                                  safe          unsafe
                                   |              |
                                   v              v
                            Full ICPSwap    Quote full overflow
                                                   |
                                           executable venue?
                                             /           \
                                           yes            no
                                            |              |
                                            v              v
                                    Full best overflow  Reject route
                       \                    /
                        +---------+--------+
                                |
                                v
                  Conservative edge >= 150 bps?
                         /              \
                       yes               no
                       |                  |
                       v                  v
               Commit immutable plan   Reject route
```

Important policy rules:

1. Use actual `collateral_received`, not the estimated pre-liquidation amount.
2. Only canonical native ICP is initially eligible for ICPSwap.
3. ICPSwap receives the full amount while its quoted impact is below 100 bps.
4. If the full quote is unsafe, search for the largest confirmed-safe ICPSwap allocation.
5. Binary search is bounded to 16 iterations and retains the last safe lower bound.
6. Exact final allocations are quoted again before commitment.
7. When the remainder is below `CEX_MIN_EXEC_USD`, use a refreshed full ICPSwap quote only if it remains below 100 bps; otherwise send the full amount to the best executable overflow venue or reject the route.
8. A better MEXC price does not reduce the policy's ICPSwap allocation.
9. The combined conservative output must satisfy the configured net-edge floor, currently 150 bps.
10. Invalid venue previews are discarded; valid venues remain eligible.
11. With ICPSwap alone, a full quote at or above 100 bps is rejected rather than forced.
12. Without ICPSwap, the best executable enabled overflow venue receives the full amount.

The strategy is replaceable. A future best-price strategy can make different allocation decisions while reusing the same adapters, persisted state, and orchestrator.

## Persisted Model

Multi-venue state is additive inside the existing `meta_json`; no SQLite schema migration is required.

```text
LiqMetaWrapper
├── receipt
├── meta                         legacy MEXC state
├── venue_execution              legacy ICPSwap state
└── meta_v2: FinalizerMetaV2?
    ├── version: 2
    ├── kind: multi_venue_swap
    └── state: MultiVenueExecutionState
        ├── plan: MultiVenueExecutionPlan
        ├── outcome: MultiVenueExecutionOutcome
        └── legs: Vec<VenueLegState>
            ├── leg_id: String
            ├── venue_id: String
            ├── request: SwapRequest
            ├── quote: VenueLegQuote
            ├── execution: VenueExecutionState
            ├── status: VenueLegStatus
            ├── result: Option<SwapExecution>
            └── last_error: Option<String>
```

Abbreviated structural example (not a complete deserializable fixture):

```json
{
  "version": 2,
  "kind": "multi_venue_swap",
  "state": {
    "plan": {
      "strategy_id": "icpswap_first",
      "allocation_reason": {
        "reason": "price_impact_split"
      }
    },
    "legs": [
      {
        "leg_id": "icpswap-0",
        "venue_id": "icpswap",
        "status": "planned"
      },
      {
        "leg_id": "mexc-1",
        "venue_id": "mexc",
        "status": "planned"
      }
    ],
    "outcome": {
      "status": "running"
    }
  }
}
```

The actual persisted legs also contain their complete request, quote, initialized venue state, optional result, and last error.

### Persistence invariants

- `version` must be supported.
- `leg_id` must be non-empty and unique.
- `venue_id` must be non-empty.
- `VenueLegState.venue_id` must match `VenueExecutionState.venue`.
- Leg pay allocations must sum exactly to `plan.total_pay`.
- Committed allocations are immutable.
- The initial `icpswap_first` strategy allows at most one leg per venue.
- Failed legs are never automatically rerouted.

## WAL Commitment and Execution

The orchestrator owns the parent lifecycle. Venue adapters return progress but never write the WAL or mark the liquidation successful.

```text
Multi-venue orchestrator          SQLite WAL             Venue adapter
          |                            |                       |
          |-- load row -------------->|                       |
          |<-- row + route metadata --|                       |
          |                            |                       |
          |  If no route is committed:                        |
          |  build + validate plan                            |
          |-- atomically persist plan and initialized legs -->|
          |                            |                       |
          |  For each leg in persisted vector order:          |
          |-------------------------------- advance(leg) ----->|
          |<---------------- state + status + result? ---------|
          |-- persist transition ---->|                       |
          |                            |                       |
          |  All legs complete/recovered?                     |
          |        yes: aggregate and mark parent successful  |
          |        no:  keep parent row resumable             |
          |  ambiguous custody: persist OperatorRequired      |
```

Restart state is loaded in this order:

1. `meta_v2`
2. Legacy `venue_execution`
3. Legacy MEXC `meta`
4. New planning only when no route has already been committed

Legacy in-progress rows remain on their legacy execution path and are not upgraded in flight.

## Venue Isolation

Each persisted leg is an independent state machine:

```text
                         +----------------------------+
                         | one venue-local transition |
                         v                            |
[start] --> Planned --> Running ----------------------+
                         |
                         +--> Completed         swap and custody complete
                         +--> Recovered         funds confirmed recovered
                         +--> OperatorRequired  custody is ambiguous
                         +--> FailedPermanent   permanent venue-local failure
```

An adapter may:

- Decode only its supplied leg.
- Update only its venue-specific execution state.
- Return a result only after its own leg completes.
- Reconcile only its own deposit, order, withdrawal, bridge, or refund state.

An adapter may not:

- Modify another leg.
- Change an allocation after commitment.
- Select a fallback venue.
- Mark the parent WAL row successful.

## Result Aggregation

Downstream consumers continue to receive one `SwapExecution`. The aggregator will:

- Sum all completed leg pay amounts.
- Sum all completed leg receive amounts.
- Concatenate execution legs in persisted venue-leg order.
- Preserve the aggregate for profit snapshots, CSV exports, logs, and TUI output.

One completed venue leg cannot close the parent row while another leg remains pending.

## Adding Another Venue

Adding Kraken should require the following work only:

1. Implement `MultiVenueAdapter` with stable `venue_id = "kraken"`.
2. Convert exact `SwapRequest` amounts into Kraken's internal decimal representation.
3. Return the normalized `SwapQuote` and initialized tagged `VenueExecutionState`.
4. Keep Kraken order IDs, deposits, withdrawals, and recovery state inside its leg.
5. Add the adapter factory and supported venue ID to startup registration.
6. Add `"kraken"` to `ENABLED_SWAP_VENUES` when it should receive new plans.
7. Add adapter contract tests and restart-boundary tests.

No change should be needed to:

- `FinalizerMetaV2`
- `MultiVenueExecutionState`
- `Vec<VenueLegState>`
- The generic WAL orchestrator
- Aggregate result construction

## Code Map

| Responsibility | Location |
|---|---|
| Versioned persisted envelope and leg vector | `pipeline/src/persistance/finalizer_meta_v2/` |
| Generic adapter contract | `pipeline/src/finalizers/multi_venue/multi_venue_adapter.rs` |
| Venue registry and concurrent quote book | `pipeline/src/finalizers/multi_venue/multi_venue_quote_book.rs` |
| `icpswap_first` allocation policy | `pipeline/src/finalizers/multi_venue/icpswap_first_planner.rs` |
| ICPSwap adapter | `pipeline/src/finalizers/icpswap/multi_venue_adapter.rs` |
| MEXC adapter | `pipeline/src/finalizers/mexc/mexc_multi_venue_adapter.rs` |
| MEXC amount-scoped preparation and quote support | `pipeline/src/finalizers/mexc/mexc_multi_venue_support.rs` |
| Shared legacy and multi-venue MEXC route quote | `pipeline/src/finalizers/mexc/mexc_route_preview.rs` |
| Existing MEXC CEX state machine | `pipeline/src/finalizers/mexc/mexc_finalizer.rs` |

## Implementation Status

| Stage | Status |
|---|---|
| Versioned `meta_v2` data model | Implemented |
| Generic venue contract and registry | Implemented |
| Pure `icpswap_first` allocation planner | Implemented |
| ICPSwap adapter | Implemented |
| MEXC adapter | Implemented |
| Generic multi-venue WAL orchestrator | Implemented |
| Aggregate result and legacy router removal | Implemented |
| Enabled-venue runtime selection | Implemented |
| Restart-boundary and full compatibility verification | Implemented |

## Rollout and Rollback

- No SQLite migration is required because `meta_v2` is stored inside existing JSON metadata.
- Older binaries do not understand the operational meaning of active `meta_v2` rows and cannot safely continue them.
- Before rolling back to a binary that does not understand `meta_v2`, drain or manually recover all active multi-venue rows.
- Before disabling a venue, finish or recover every unfinished committed leg for it. Startup rejects a disabled venue referenced by an unfinished `meta_v2` row.
