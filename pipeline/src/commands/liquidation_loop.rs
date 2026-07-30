use candid::{Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tracing::instrument;
use tracing::{info, warn};

use crate::{
    commands::liquidation_loop_helpers::{
        bootstrap_control_plane, console_ui_enabled, debt_asset_principals, debt_assets_as_text,
        ensure_runtime_file_permissions, print_banner, run_daemon_cycle_loop,
    },
    config::{Config, ConfigTrait},
    context::{PipelineContext, init_context},
    executors::basic::basic_executor::BasicExecutor,
    finalizers::{
        mexc::runtime::build_mexc_finalizer,
        multi_venue::{
            ICPSWAP_VENUE_ID, IcpswapFirstPlannerConfig, MEXC_VENUE_ID, MultiVenueAdapter, MultiVenueFinalizer,
        },
        profit_calculator::SimpleProfitCalculator,
    },
    liquidation::collateral_service::CollateralService,
    persistance::{
        FinalizerMetaPayload, MultiVenueExecutionOutcome, ResultStatus, VenueLegStatus, WalStore,
        sqlite::SqliteWalStore,
    },
    price_oracle::price_oracle::LiquidationPriceOracle,
    stages::{
        export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
        settlement_watcher::SettlementWatcher, simple_strategy::SimpleLiquidationStrategy,
    },
    swappers::icpswap::{identity::IcpswapExecutionIdentity, types::IcpswapExecutionState},
    wal::decode_receipt_wrapper,
    watchdog::{
        WatchdogEvent,
        balance_monitor::{
            DEFAULT_LOW_BALANCE_ALERT_COOLDOWN, LowBalanceMonitor, MonitoredBalanceAccount,
            balance_check_exclude_from_env,
        },
        slack_watchdog_from_env, slack_webhook_configured, webhook_watchdog_from_env,
    },
};
use ic_agent::Agent;
use liquidium_pipeline_core::{
    balance_service::BalanceService,
    tokens::{chain_token::ChainToken, token_registry::TokenRegistry},
};

const BRIDGE_CKETH_LEDGER_ID: &str = "ss2fx-dyaaa-aaaar-qacoq-cai";

fn disabled_venue_ids<'a>(
    outcome: &MultiVenueExecutionOutcome,
    venue_ids: impl IntoIterator<Item = &'a str>,
    enabled: &HashSet<&str>,
) -> Vec<String> {
    if matches!(
        outcome,
        MultiVenueExecutionOutcome::Completed | MultiVenueExecutionOutcome::Recovered
    ) {
        return Vec::new();
    }

    let mut disabled = Vec::new();
    for venue_id in venue_ids {
        if !enabled.contains(venue_id) && !disabled.iter().any(|existing| existing == venue_id) {
            disabled.push(venue_id.to_string());
        }
    }
    disabled
}

fn validate_icpswap_identity(identity: &IcpswapExecutionIdentity, configured_mnemonic: &str) -> Result<(), String> {
    identity.validate_and_derive(configured_mnemonic).map(|_| ())
}

async fn park_unresumable_committed_rows(
    db: &SqliteWalStore,
    enabled_venues: &[String],
    configured_icpswap_mnemonic: &str,
) -> Result<(), String> {
    let enabled: HashSet<&str> = enabled_venues.iter().map(String::as_str).collect();

    for row in db
        .list_unfinished_execution_rows()
        .map_err(|error| format!("failed to inspect unfinished venue executions: {error}"))?
    {
        let wrapper = match decode_receipt_wrapper(&row) {
            Ok(Some(wrapper)) => wrapper,
            Ok(None) => continue,
            Err(error) => {
                park_unresumable_row(
                    db,
                    &row.id,
                    format!("cannot inspect committed route because WAL metadata is malformed: {error}"),
                )
                .await?;
                continue;
            }
        };
        let Some(meta) = wrapper.meta_v2 else {
            continue;
        };
        let state = match meta.payload {
            FinalizerMetaPayload::MultiVenueSwap(state) => state,
            // Recovery sweeps have no venue adapter dependency. Their own
            // durable submission gate handles restart ambiguity.
            FinalizerMetaPayload::RecoverySweep(_) => continue,
        };
        let mut reasons = Vec::new();
        let disabled = disabled_venue_ids(
            &state.outcome,
            state.legs.iter().map(|leg| leg.venue_id.as_str()),
            &enabled,
        );
        if !disabled.is_empty() {
            reasons.push(format!("committed route uses disabled venues [{}]", disabled.join(",")));
        }

        // Every runnable or parked ICPSwap leg must still derive the principal
        // committed before its first side effect. Detect mnemonic or descriptor
        // drift at startup rather than waiting for the next execution cycle.
        for leg in &state.legs {
            if leg.venue_id != ICPSWAP_VENUE_ID
                || !enabled.contains(ICPSWAP_VENUE_ID)
                || matches!(
                    leg.status,
                    VenueLegStatus::Completed | VenueLegStatus::Recovered | VenueLegStatus::FailedPermanent
                )
            {
                continue;
            }
            let execution = match leg.execution.decode::<IcpswapExecutionState>(ICPSWAP_VENUE_ID) {
                Ok(Some(execution)) => execution,
                Ok(None) => {
                    reasons.push(format!("ICPSwap leg `{}` has no ICPSwap execution state", leg.leg_id));
                    continue;
                }
                Err(error) => {
                    reasons.push(format!(
                        "ICPSwap leg `{}` has malformed execution state: {error}",
                        leg.leg_id
                    ));
                    continue;
                }
            };
            if let Err(error) = validate_icpswap_identity(&execution.identity, configured_icpswap_mnemonic) {
                reasons.push(format!(
                    "ICPSwap leg `{}` derived principal cannot be reproduced: {error}",
                    leg.leg_id
                ));
            }
        }

        if !reasons.is_empty() {
            park_unresumable_row(db, &row.id, reasons.join("; ")).await?;
        }
    }

    Ok(())
}

/// Durably removes one incompatible row from automatic polling while keeping
/// its full WAL state visible in Executions for manual recovery.
async fn park_unresumable_row(db: &SqliteWalStore, row_id: &str, reason: String) -> Result<(), String> {
    warn!(
        liquidation_id = row_id,
        reason = %reason,
        "Committed swap route is unresumable; parking row and continuing startup"
    );
    db.update_failure(row_id, ResultStatus::Unresumable, reason, false)
        .await
        .map_err(|error| format!("failed to mark unfinished WAL row {row_id} unresumable: {error}"))
}

#[instrument(name = "liquidation.init", skip_all, err)]
async fn init(
    ctx: Arc<PipelineContext>,
) -> Result<
    (
        OpportunityFinder<Agent>,
        SimpleLiquidationStrategy<Config, TokenRegistry, CollateralService<LiquidationPriceOracle<Agent>>>,
        Arc<BasicExecutor<Agent, SqliteWalStore>>,
        Arc<ExportStage>,
        Arc<FinalizeStage<MultiVenueFinalizer, SqliteWalStore, SimpleProfitCalculator, Agent>>,
    ),
    String,
> {
    let config = ctx.config.clone();
    let agent = ctx.agent.clone();
    let registry = ctx.registry.clone();
    let db = Arc::new(SqliteWalStore::new(&config.db_path).map_err(|e| format!("could not connect to db: {e}"))?);
    park_unresumable_committed_rows(db.as_ref(), &config.enabled_swap_venues, &config.icpswap_mnemonic).await?;

    let tokens = debt_asset_principals(&registry);

    let mut executor = BasicExecutor::new(
        agent.clone(),
        Account {
            owner: config.liquidator_principal,
            subaccount: None,
        },
        config.lending_canister,
        db.clone(),
        ctx.approval_state.clone(),
    );

    executor
        .init(&tokens)
        .await
        .map_err(|e| format!("executor token init failed: {e}"))?;
    let executor = Arc::new(executor);

    let mut venue_adapters: Vec<Arc<dyn MultiVenueAdapter>> = Vec::new();
    for venue_id in &config.enabled_swap_venues {
        match venue_id.as_str() {
            ICPSWAP_VENUE_ID => venue_adapters.push(
                ctx.icpswap_finalizer
                    .clone()
                    .ok_or_else(|| "ICPSwap is enabled but its adapter was not initialized".to_string())?,
            ),
            MEXC_VENUE_ID => venue_adapters.push(build_mexc_finalizer(ctx.as_ref()).await?),
            _ => return Err(format!("unsupported enabled swap venue `{venue_id}`")),
        }
    }
    // Shared by the strategy's collateral sizing and the multi-venue planner's
    // quote guard, so both price a liquidation from the same oracle.
    let price_oracle = Arc::new(LiquidationPriceOracle::new(agent.clone(), config.lending_canister));

    let multi_venue_finalizer = Arc::new(
        MultiVenueFinalizer::new(
            venue_adapters,
            IcpswapFirstPlannerConfig {
                max_price_impact_bps: config.icpswap_max_price_impact_bps,
                max_search_iterations: config.icpswap_max_search_iterations,
                dust_fallback_max_price_impact_bps: config.icpswap_dust_fallback_max_price_impact_bps,
                cex_min_exec_usd: config.get_cex_min_exec_usd(),
                min_net_edge_bps: config.multi_venue_min_net_edge_bps,
                max_oracle_discount_bps: config.multi_venue_max_oracle_discount_bps,
                oracle_snapshot_max_age_secs: config.multi_venue_oracle_snapshot_max_age_secs,
                icpswap_test_allocation_usd: config.icpswap_test_allocation_usd,
            },
        )?
        .with_watchdog(slack_watchdog_from_env(DEFAULT_LOW_BALANCE_ALERT_COOLDOWN))
        .with_price_oracle(price_oracle.clone())
        .with_recovery_sweep(ctx.trader_transfers.actions(), config.get_recovery_account()),
    );

    // Profit calculator for expected/realized PnL
    let profit_calc = Arc::new(SimpleProfitCalculator); //todo implement real profit calculator

    // FinalizeStage wires WAL + finalizer + profit calculation
    let finalizer = Arc::new(
        FinalizeStage::new(
            db.clone(),
            multi_venue_finalizer,
            profit_calc,
            agent.clone(),
            config.lending_canister,
            config.cex_retry_base_secs,
            config.cex_retry_max_secs,
        )
        .with_watchdog(slack_watchdog_from_env(DEFAULT_LOW_BALANCE_ALERT_COOLDOWN)),
    );

    info!("Initializing searcher stage ...");
    let finder = OpportunityFinder::new(
        agent.clone(),
        config.lending_canister,
        config.opportunity_account_filter.clone(),
    );

    info!("Initializing liquidations stage ...");
    let collateral_service = Arc::new(CollateralService::new(price_oracle));

    let wd = webhook_watchdog_from_env(Duration::from_secs(300));
    wd.notify(WatchdogEvent::Heartbeat { stage: "Init" }).await;

    let strategy = SimpleLiquidationStrategy::new(
        config.clone(),
        registry.clone(),
        collateral_service.clone(),
        ctx.main_service.clone(),
        ctx.approval_state.clone(),
    )
    .with_watchdog(wd);

    let exporter = Arc::new(ExportStage {
        path: config.export_path.clone(),
    });

    Ok((finder, strategy, executor, exporter, finalizer))
}

fn bridge_low_balance_service(ctx: &PipelineContext) -> Arc<BalanceService> {
    let tokens = vec![
        ChainToken::EvmNative {
            chain: "eth".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        },
        ChainToken::Icp {
            ledger: Principal::from_text(BRIDGE_CKETH_LEDGER_ID).expect("valid ckETH ledger principal"),
            symbol: "ckETH".to_string(),
            decimals: 18,
            fee: Nat::from(0u8),
        },
    ];
    let registry = TokenRegistry::new(
        tokens
            .into_iter()
            .map(|token| (token.asset_id(), token))
            .collect::<HashMap<_, _>>(),
    );

    Arc::new(BalanceService::new(Arc::new(registry), ctx.bridge_service.accounts()))
}

pub async fn run_liquidation_loop(sock_path: PathBuf) {
    // Auditor note:
    // This function is the foreground daemon entrypoint. It does not fork/detach;
    // lifecycle is expected to be managed by an external supervisor (systemd).
    let ui_enabled = console_ui_enabled();
    if ui_enabled {
        print_banner();
    }

    let ctx = match init_context().await {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::error!("Failed to initialize pipeline context: {}", err);
            return;
        }
    };
    let ctx = Arc::new(ctx);
    let config = ctx.config.clone();

    if let Err(err) = ensure_runtime_file_permissions(&config.db_path, &config.export_path) {
        tracing::error!(
            db_path = %config.db_path,
            export_path = %config.export_path,
            "Startup filesystem preflight failed: {}",
            err
        );
        return;
    }

    if config.buy_bad_debt {
        info!(
            buy_bad_debt = true,
            "Bad debt mode enabled: liquidator may repay bad debt to restore solvency"
        );
        if ui_enabled {
            println!("====================================================================");
            println!("=                                                                  =");
            println!("=                   !!!  BAD DEBT MODE  !!!                        =");
            println!("=                                                                  =");
            println!("=  This bot WILL repay bad debt (you eat the loss).                =");
            println!("=  Use only if you intend to shore up protocol solvency.            =");
            println!("=                                                                  =");
            println!("====================================================================");
        }
        warn!("Continuing without interactive confirmation because BUY_BAD_DEBT is enabled.");
    } else {
        info!(
            buy_bad_debt = false,
            "Bad debt mode disabled: only collateral-backed liquidations will run"
        );
    }
    // Use main IC agent (liquidator identity) from context
    info!(
        liquidator_principal = %config.liquidator_principal.to_text(),
        "Agent initialized"
    );

    // Initialize components using shared pipeline context
    let (finder, strategy, executor, exporter, finalizer) = match init(ctx.clone()).await {
        Ok(stages) => stages,
        Err(err) => {
            tracing::error!("Failed to initialize pipeline stages: {}", err);
            return;
        }
    };

    let watcher_wal = match SqliteWalStore::new_with_busy_timeout(&config.db_path, 30_000) {
        Ok(wal) => Arc::new(wal),
        Err(err) => {
            tracing::error!("Failed to init watcher WAL: {}", err);
            return;
        }
    };

    // Settlement watcher is intentionally independent from pause/resume.
    // Even while paused, it can continue reconciling previously-started work.
    let watcher = SettlementWatcher::new(
        watcher_wal,
        ctx.agent.clone(),
        config.lending_canister,
        Duration::from_secs(3),
    );

    tokio::spawn(async move { watcher.run().await });

    let debt_asset_principals = debt_asset_principals(&ctx.registry);
    let debt_assets = debt_assets_as_text(&debt_asset_principals);

    info!(
        network = %config.ic_url,
        liquidator_principal = %config.liquidator_principal.to_text(),
        enabled_swap_venues = ?config.enabled_swap_venues,
        max_dex_slippage_bps = config.max_allowed_dex_slippage,
        max_cex_slippage_bps = config.max_allowed_cex_slippage_bps,
        buy_bad_debt = config.buy_bad_debt,
        "Startup configuration"
    );
    info!("Liquidator started; scanning for liquidation opportunities...");

    // Shared pause flag controlled by UDS commands.
    // `true` means: do not initiate new liquidations, but keep housekeeping alive.
    let paused = Arc::new(AtomicBool::new(false));
    let slack_watchdog = if slack_webhook_configured() {
        Some(slack_watchdog_from_env(DEFAULT_LOW_BALANCE_ALERT_COOLDOWN))
    } else {
        None
    };
    // The helper encapsulates:
    // - persisted paused/running state bootstrap
    // - UDS bind + serve
    // - state persistence on pause/resume transitions
    if let Err(err) = bootstrap_control_plane(&sock_path, &config.db_path, paused.clone(), slack_watchdog.clone()) {
        tracing::error!("{}", err);
        return;
    }
    info!(sock_path = %sock_path.display(), "Control plane ready");

    let liq_dog = webhook_watchdog_from_env(Duration::from_secs(300));
    if let Some(slack) = slack_watchdog.as_ref() {
        slack
            .notify(WatchdogEvent::Lifecycle {
                state: "started".to_string(),
                details: format!(
                    "Liquidator started on {}; scanning for liquidation opportunities.",
                    config.ic_url
                ),
            })
            .await;
    }

    let low_balance_monitor = slack_watchdog.as_ref().map(|slack| {
        Arc::new(LowBalanceMonitor::new(
            vec![
                MonitoredBalanceAccount {
                    label: "main",
                    service: ctx.main_service.clone(),
                    only_symbols: None,
                    exclude_symbols: balance_check_exclude_from_env(),
                },
                MonitoredBalanceAccount {
                    label: "bridge",
                    service: bridge_low_balance_service(&ctx),
                    only_symbols: None,
                    exclude_symbols: None,
                },
            ],
            slack.clone(),
        ))
    });

    // Steady-state operation is delegated to a helper to keep this entrypoint
    // focused on bootstrap wiring and lifecycle boundaries.
    run_daemon_cycle_loop(
        &finder,
        &strategy,
        &executor,
        &exporter,
        &finalizer,
        &liq_dog,
        slack_watchdog,
        low_balance_monitor,
        paused,
        &debt_assets,
        &debt_asset_principals,
        ui_enabled,
    )
    .await;
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopControl {
    Running,
    Paused,
    Stopping,
}

#[cfg(test)]
mod tests {
    use super::{disabled_venue_ids, park_unresumable_committed_rows, validate_icpswap_identity};
    use crate::commands::liquidation_loop_helpers::console_ui_enabled;
    use crate::{
        executors::executor::ExecutorRequest,
        persistance::sqlite::SqliteWalStore,
        persistance::{
            FINALIZER_META_V2_VERSION, FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper, LiqResultRecord,
            MultiVenueAllocationReason, MultiVenueExecutionOutcome, MultiVenueExecutionPlan, MultiVenueExecutionState,
            ResultStatus, VenueExecutionState, VenueLegQuote, VenueLegState, VenueLegStatus, WalStore,
        },
        stages::executor::{ExecutionReceipt, ExecutionStatus},
        swappers::{
            icpswap::{
                identity::IcpswapExecutionIdentity,
                transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
                types::{IcpswapExecutionPlan, IcpswapExecutionState},
            },
            model::SwapRequest,
        },
    };
    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use liquidium_pipeline_core::{
        tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount},
        types::protocol_types::LiquidationRequest,
    };
    use std::collections::HashSet;
    use std::io::IsTerminal;

    const COMMITTED_MNEMONIC: &str =
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    const DIFFERENT_MNEMONIC: &str = "legal winner thank year wave sausage worth useful legal winner thank yellow";

    fn startup_test_token(ledger: u8, symbol: &str, fee: u64) -> ChainToken {
        ChainToken::Icp {
            ledger: Principal::from_slice(&[ledger]),
            symbol: symbol.to_string(),
            decimals: 8,
            fee: Nat::from(fee),
        }
    }

    fn startup_test_row(id: &str, status: ResultStatus, mnemonic: &str) -> LiqResultRecord {
        let input = startup_test_token(1, "ICP", 10);
        let output = startup_test_token(2, "ckUSDC", 5);
        let pool_input = ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64));
        let input_fee = ChainTokenAmount::from_raw(input.clone(), Nat::from(10u64));
        let quoted_output = ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64));
        let output_fee = ChainTokenAmount::from_raw(output.clone(), Nat::from(5u64));
        let route = IcpswapExecutionPlan::new(
            Principal::from_slice(&[9]),
            Principal::from_slice(&[1]),
            Principal::from_slice(&[2]),
            Nat::from(3_000u64),
            pool_input,
            input_fee,
            quoted_output.clone(),
            output_fee,
            100,
        )
        .expect("ICPSwap route");
        let (identity, _) = IcpswapExecutionIdentity::derive(mnemonic, "1536").expect("execution identity");
        let child = Account {
            owner: identity.principal,
            subaccount: None,
        };
        let execution = IcpswapExecutionState::prepare(
            "icpswap-startup-test",
            route.clone(),
            identity,
            IcpswapFundingState::new(
                Account {
                    owner: Principal::from_slice(&[4]),
                    subaccount: None,
                },
                child,
                route.input_ledger_fee.clone(),
            ),
            IcpswapSettlementState {
                kind: None,
                destination: Account {
                    owner: Principal::from_slice(&[5]),
                    subaccount: None,
                },
                fee: route.output_ledger_fee.clone(),
                transfer: IcpswapLedgerTransferState::default(),
                interrupted_transfer: None,
                interrupted_observed_debit: None,
                recovery_credit: None,
                residual_dust: None,
            },
        )
        .expect("execution state");
        let allocation = ChainTokenAmount::from_raw(input.clone(), Nat::from(100_030u64));
        let receive = ChainTokenAmount::from_raw(output.clone(), Nat::from(119_990u64));
        let request = SwapRequest {
            pay_asset: input.asset_id(),
            pay_amount: allocation.clone(),
            receive_asset: output.asset_id(),
            receive_address: Some(Principal::from_slice(&[5]).to_text()),
            max_slippage_bps: Some(100),
            venue_hint: Some("icpswap".to_string()),
        };
        let leg = VenueLegState {
            leg_id: "icpswap-0".to_string(),
            venue_id: "icpswap".to_string(),
            request: request.clone(),
            quote: VenueLegQuote {
                pay_amount: allocation.clone(),
                estimated_receive: quoted_output.clone(),
                conservative_receive: receive.clone(),
                estimated_price_impact_bps: 50.0,
                route_id: "icpswap-route".to_string(),
            },
            execution: VenueExecutionState::new("icpswap", &execution).expect("tag execution"),
            status: VenueLegStatus::Running,
            result: None,
            last_error: None,
        };
        let meta_v2 = FinalizerMetaV2 {
            version: FINALIZER_META_V2_VERSION,
            payload: FinalizerMetaPayload::MultiVenueSwap(MultiVenueExecutionState {
                plan: MultiVenueExecutionPlan {
                    strategy_id: "icpswap_first".to_string(),
                    total_pay: allocation,
                    receive_asset: output.asset_id(),
                    debt_repaid: ChainTokenAmount::from_raw(output.clone(), Nat::from(100_000u64)),
                    allocation_reason: MultiVenueAllocationReason::SingleVenue {
                        venue_id: "icpswap".to_string(),
                    },
                    min_net_edge_bps: 150,
                    estimated_receive: quoted_output,
                    conservative_receive: receive,
                    combined_net_edge_bps: 175.0,
                    quoted_at: 123,
                },
                legs: vec![leg],
                outcome: MultiVenueExecutionOutcome::Running,
            }),
        };
        let receipt = ExecutionReceipt {
            request: ExecutorRequest {
                liquidation: LiquidationRequest {
                    borrower: Principal::from_slice(&[6]),
                    debt_pool_id: Principal::from_slice(&[7]),
                    collateral_pool_id: Principal::from_slice(&[8]),
                    debt_amount: Nat::from(100_000u64),
                    receiver_address: Principal::from_slice(&[4]),
                    buy_bad_debt: false,
                },
                swap_args: Some(request),
                debt_asset: output,
                collateral_asset: input,
                expected_profit: 1,
                ref_price: Nat::from(1u8),
                debt_ref_price: Nat::from(0u8),
                ref_price_at: 0,
                debt_approval_needed: false,
                min_collateral_amount: Nat::from(0u8),
            },
            liquidation_result: None,
            status: ExecutionStatus::Success,
            change_received: true,
        };
        let wrapper = LiqMetaWrapper {
            receipt,
            meta: Vec::new(),
            finalizer_decision: None,
            profit_snapshot: None,
            venue_execution: None,
            meta_v2: Some(meta_v2),
        };

        LiqResultRecord {
            id: id.to_string(),
            status,
            attempt: 0,
            error_count: 0,
            last_error: None,
            created_at: 1,
            updated_at: 1,
            meta_json: serde_json::to_string(&wrapper).expect("encode startup row"),
        }
    }

    fn set_startup_leg_status(row: &mut LiqResultRecord, status: VenueLegStatus) {
        let mut wrapper: LiqMetaWrapper = serde_json::from_str(&row.meta_json).expect("decode startup fixture");
        let meta = wrapper.meta_v2.as_mut().expect("multi-venue metadata");
        let FinalizerMetaPayload::MultiVenueSwap(state) = &mut meta.payload else {
            panic!("expected multi-venue state")
        };
        state.legs.first_mut().expect("ICPSwap leg").status = status;
        row.meta_json = serde_json::to_string(&wrapper).expect("encode startup fixture");
    }

    fn calc_console_ui_enabled(human_output: bool, stdout_is_tty: bool, stderr_is_tty: bool) -> bool {
        human_output && stdout_is_tty && stderr_is_tty
    }

    #[test]
    fn disables_console_ui_when_plain_logs_feature_is_enabled() {
        assert!(!calc_console_ui_enabled(false, true, true));
    }

    #[test]
    fn enables_console_ui_when_feature_disabled_and_both_terminals_present() {
        assert!(calc_console_ui_enabled(true, true, true));
    }

    #[test]
    fn disables_console_ui_when_stdout_is_not_tty() {
        assert!(!calc_console_ui_enabled(true, false, true));
    }

    #[test]
    fn disables_console_ui_when_stderr_is_not_tty() {
        assert!(!calc_console_ui_enabled(true, true, false));
    }

    #[test]
    fn console_ui_enabled_matches_helper_contract() {
        let actual = console_ui_enabled();
        let expected = calc_console_ui_enabled(
            crate::output::human_output_enabled(),
            std::io::stdout().is_terminal(),
            std::io::stderr().is_terminal(),
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn unfinished_plans_report_disabled_venues_once_in_leg_order() {
        let enabled = HashSet::from(["icpswap"]);
        let disabled = disabled_venue_ids(
            &MultiVenueExecutionOutcome::Running,
            ["icpswap", "mexc", "mexc", "kraken"],
            &enabled,
        );
        assert_eq!(disabled, vec!["mexc".to_string(), "kraken".to_string()]);
    }

    #[test]
    fn completed_plans_do_not_block_disabling_a_venue() {
        let enabled = HashSet::from(["icpswap"]);
        let disabled = disabled_venue_ids(&MultiVenueExecutionOutcome::Completed, ["mexc"], &enabled);
        assert!(disabled.is_empty());
    }

    #[test]
    fn startup_identity_check_rejects_a_different_configured_mnemonic() {
        let (identity, _) = IcpswapExecutionIdentity::derive(COMMITTED_MNEMONIC, "1536").expect("identity");

        validate_icpswap_identity(&identity, COMMITTED_MNEMONIC).expect("matching mnemonic");
        let error = validate_icpswap_identity(&identity, DIFFERENT_MNEMONIC)
            .expect_err("startup must reject a principal mismatch");

        assert!(error.contains("does not match the configured mnemonic"));
    }

    #[tokio::test]
    async fn startup_wal_accepts_a_reproducible_unfinished_icpswap_identity() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        db.upsert_result(startup_test_row(
            "valid-unfinished",
            ResultStatus::Enqueued,
            COMMITTED_MNEMONIC,
        ))
        .await
        .expect("seed unfinished row");

        park_unresumable_committed_rows(&db, &["icpswap".to_string()], COMMITTED_MNEMONIC)
            .await
            .expect("matching identity must resume");
        assert_eq!(
            db.get_result("valid-unfinished")
                .await
                .expect("read row")
                .expect("row")
                .status,
            ResultStatus::Enqueued
        );
    }

    #[tokio::test]
    async fn startup_wal_parks_an_unreproducible_identity_without_blocking_startup() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        db.upsert_result(startup_test_row(
            "mismatched-unfinished",
            ResultStatus::OperatorRequired,
            COMMITTED_MNEMONIC,
        ))
        .await
        .expect("seed unfinished row");

        park_unresumable_committed_rows(&db, &["icpswap".to_string()], DIFFERENT_MNEMONIC)
            .await
            .expect("mismatched row must not block startup");

        let parked = db
            .get_result("mismatched-unfinished")
            .await
            .expect("read row")
            .expect("parked row");
        assert_eq!(parked.status, ResultStatus::Unresumable);
        let reason = parked.last_error.expect("unresumable reason");
        assert!(reason.contains("icpswap-0"));
        assert!(reason.contains("cannot be reproduced"));
        assert!(db.get_pending(10).await.expect("pending rows").is_empty());
    }

    #[tokio::test]
    async fn startup_wal_parks_a_row_for_a_disabled_committed_venue() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        db.upsert_result(startup_test_row(
            "disabled-venue",
            ResultStatus::FailedRetryable,
            COMMITTED_MNEMONIC,
        ))
        .await
        .expect("seed unfinished row");

        park_unresumable_committed_rows(&db, &["mexc".to_string()], COMMITTED_MNEMONIC)
            .await
            .expect("disabled committed venue must not block startup");

        let parked = db
            .get_result("disabled-venue")
            .await
            .expect("read row")
            .expect("parked row");
        assert_eq!(parked.status, ResultStatus::Unresumable);
        assert!(parked.last_error.expect("reason").contains("disabled venues [icpswap]"));
    }

    #[tokio::test]
    async fn startup_wal_parks_malformed_metadata_without_blocking_startup() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        let mut row = startup_test_row("malformed-metadata", ResultStatus::InFlight, COMMITTED_MNEMONIC);
        row.meta_json = "not-json".to_string();
        db.upsert_result(row).await.expect("seed malformed row");

        park_unresumable_committed_rows(&db, &["icpswap".to_string(), "mexc".to_string()], COMMITTED_MNEMONIC)
            .await
            .expect("malformed historical row must not block startup");

        let parked = db
            .get_result("malformed-metadata")
            .await
            .expect("read row")
            .expect("parked row");
        assert_eq!(parked.status, ResultStatus::Unresumable);
        assert!(parked.last_error.expect("reason").contains("metadata is malformed"));
    }

    #[tokio::test]
    async fn startup_wal_ignores_a_terminal_row_with_an_old_icpswap_identity() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        db.upsert_result(startup_test_row(
            "completed-row",
            ResultStatus::Succeeded,
            COMMITTED_MNEMONIC,
        ))
        .await
        .expect("seed completed row");

        park_unresumable_committed_rows(&db, &["icpswap".to_string()], DIFFERENT_MNEMONIC)
            .await
            .expect("terminal rows no longer require identity reproduction");

        let stored = db
            .get_result("completed-row")
            .await
            .expect("read row")
            .expect("completed row");
        assert_eq!(stored.status, ResultStatus::Succeeded);
        assert_eq!(stored.last_error, None);
    }

    #[tokio::test]
    async fn startup_wal_skips_identity_checks_for_a_terminal_leg_in_a_runnable_parent() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        let mut row = startup_test_row("completed-leg", ResultStatus::Enqueued, COMMITTED_MNEMONIC);
        set_startup_leg_status(&mut row, VenueLegStatus::Completed);
        db.upsert_result(row).await.expect("seed completed leg");

        park_unresumable_committed_rows(&db, &["icpswap".to_string()], DIFFERENT_MNEMONIC)
            .await
            .expect("terminal leg must not require identity reproduction");

        assert_eq!(
            db.get_result("completed-leg")
                .await
                .expect("read row")
                .expect("completed leg row")
                .status,
            ResultStatus::Enqueued
        );
    }

    #[tokio::test]
    async fn startup_wal_parks_only_the_incompatible_row() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let db = SqliteWalStore::new(db_file.path().to_str().expect("database path")).expect("WAL store");
        db.upsert_result(startup_test_row(
            "compatible-row",
            ResultStatus::Enqueued,
            DIFFERENT_MNEMONIC,
        ))
        .await
        .expect("seed compatible row");
        db.upsert_result(startup_test_row(
            "incompatible-row",
            ResultStatus::Enqueued,
            COMMITTED_MNEMONIC,
        ))
        .await
        .expect("seed incompatible row");

        park_unresumable_committed_rows(&db, &["icpswap".to_string()], DIFFERENT_MNEMONIC)
            .await
            .expect("one incompatible row must not block startup");

        assert_eq!(
            db.get_result("compatible-row")
                .await
                .expect("read compatible row")
                .expect("compatible row")
                .status,
            ResultStatus::Enqueued
        );
        assert_eq!(
            db.get_result("incompatible-row")
                .await
                .expect("read incompatible row")
                .expect("incompatible row")
                .status,
            ResultStatus::Unresumable
        );
        let pending = db.get_pending(10).await.expect("pending rows");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "compatible-row");
    }

    #[tokio::test]
    async fn startup_wal_fails_if_an_incompatible_row_cannot_be_parked() {
        let db_file = tempfile::NamedTempFile::new().expect("temporary WAL");
        let path = db_file.path().to_str().expect("database path");
        let writer = SqliteWalStore::new(path).expect("writable WAL store");
        writer
            .upsert_result(startup_test_row(
                "incompatible-read-only-row",
                ResultStatus::Enqueued,
                COMMITTED_MNEMONIC,
            ))
            .await
            .expect("seed incompatible row");
        let reader = SqliteWalStore::new_read_only_with_busy_timeout(path, 5_000).expect("read-only WAL store");

        let error = park_unresumable_committed_rows(&reader, &["icpswap".to_string()], DIFFERENT_MNEMONIC)
            .await
            .expect_err("startup must fail when parking cannot be persisted");

        assert!(error.contains("failed to mark unfinished WAL row incompatible-read-only-row unresumable"));
        assert_eq!(
            writer
                .get_result("incompatible-read-only-row")
                .await
                .expect("read original row")
                .expect("original row")
                .status,
            ResultStatus::Enqueued
        );
    }
}
