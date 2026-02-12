use candid::Principal;
use indicatif::{ProgressBar, ProgressStyle};
use prettytable::{Cell, Row, Table, format};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{IsTerminal, stderr, stdout};
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tracing::{Instrument, info, info_span, warn};

use crate::config::Config;
use crate::executors::basic::basic_executor::BasicExecutor;
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::finalizers::{hybrid::hybrid_finalizer::HybridFinalizer, profit_calculator::SimpleProfitCalculator};
use crate::liquidation::collateral_service::CollateralService;
use crate::output::human_output_enabled;
use crate::persistance::sqlite::SqliteWalStore;
use crate::price_oracle::price_oracle::LiquidationPriceOracle;
use crate::stage::PipelineStage;
use crate::stages::executor::ExecutionStatus;
use crate::stages::{
    export::ExportStage, finalize::FinalizeStage, opportunity::OpportunityFinder,
    simple_strategy::SimpleLiquidationStrategy,
};
use crate::swappers::router::SwapRouter;
use crate::watchdog::{Watchdog, WatchdogEvent};
use anyhow::Context as _;
use futures::FutureExt;
use ic_agent::Agent;
use liquidium_pipeline_core::tokens::{
    chain_token::ChainToken,
    token_registry::{TokenRegistry, TokenRegistryTrait},
};

const FINDER_STAGE_TIMEOUT: Duration = Duration::from_secs(300);
const LIQUIDATION_CYCLE_TIMEOUT: Duration = Duration::from_secs(300);
const FINALIZER_STAGE_TIMEOUT: Duration = Duration::from_secs(300);
const EXPORT_STAGE_TIMEOUT: Duration = Duration::from_secs(20);
const WATCHDOG_STAGE_TIMEOUT: Duration = Duration::from_secs(10);
const REFRESH_ALLOWANCES_TIMEOUT: Duration = Duration::from_secs(45);
const SCAN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);
const PANIC_RECOVERY_DELAY: Duration = Duration::from_secs(1);
const TIMEOUT_RECOVERY_DELAY: Duration = Duration::from_secs(1);
const PAUSED_LOOP_DELAY: Duration = Duration::from_secs(2);
const RUNNING_LOOP_DELAY: Duration = Duration::from_secs(3);

/// Prints the startup banner when an interactive terminal is available.
pub(crate) fn print_banner() {
    println!(
        r#"
██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗
██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝
██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗  
██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝  
██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗
╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝
                                                        
          Liquidation Execution Engine
"#
    );
}

/// Console UI is enabled only for interactive terminals and non-plain logging mode.
pub(crate) fn console_ui_enabled() -> bool {
    human_output_enabled() && stdout().is_terminal() && stderr().is_terminal()
}

/// Spinner is used only when interactive output is enabled.
pub(crate) fn start_spinner(enabled: bool) -> Option<ProgressBar> {
    if !enabled {
        return None;
    }

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    spinner.enable_steady_tick(Duration::from_millis(100));
    Some(spinner)
}

fn set_spinner_message(spinner: &Option<ProgressBar>, paused: bool) {
    if let Some(s) = spinner.as_ref() {
        if paused {
            s.set_message("Daemon paused; finalizing queued work only...");
        } else {
            s.set_message("Scanning for liquidation opportunities...");
        }
    }
}

/// Structured per-outcome logs consumed by journald/OTEL and auditor tooling.
pub(crate) fn log_execution_results(results: &[LiquidationOutcome]) {
    let success_count = results
        .iter()
        .filter(|r| matches!(r.status, ExecutionStatus::Success))
        .count();

    info!(
        outcome_count = results.len(),
        success_count,
        failed_count = results.len() - success_count,
        "Liquidation outcomes finalized"
    );

    for r in results {
        let liquidation_id = r
            .execution_receipt
            .liquidation_result
            .as_ref()
            .map(|v| v.id.to_string())
            .unwrap_or_else(|| "n/a".to_string());

        let swap_status = r
            .finalizer_result
            .swap_result
            .as_ref()
            .map(|v| v.status.clone())
            .unwrap_or_else(|| "none".to_string());
        let status = r.status.description();

        info!(
            event = "liquidation_outcome",
            liquidation_id = %liquidation_id,
            borrower = %r.request.liquidation.borrower.to_text(),
            debt_asset = %r.request.debt_asset.symbol(),
            collateral_asset = %r.request.collateral_asset.symbol(),
            debt_repaid = %r.formatted_debt_repaid(),
            collateral_received = %r.formatted_received_collateral(),
            swap_output = %r.formatted_swap_output(),
            swapper = %r.formatted_swapper(),
            swap_status = %swap_status,
            status = %status,
            expected_profit = r.expected_profit,
            realized_profit = r.realized_profit,
            profit_delta = r.realized_profit - r.expected_profit,
            round_trip_secs = r.round_trip_secs.unwrap_or(-1),
            "Liquidation outcome"
        );
    }
}

/// Extract ICP debt ledgers once and reuse in all stages that need allowance refresh/finding.
pub(crate) fn debt_asset_principals(registry: &TokenRegistry) -> Vec<Principal> {
    registry
        .debt_assets()
        .iter()
        .filter_map(|(_, tok)| {
            if let ChainToken::Icp { ledger, .. } = tok {
                Some(*ledger)
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn debt_assets_as_text(principals: &[Principal]) -> Vec<String> {
    principals.iter().map(Principal::to_text).collect()
}

/// Verifies startup file-system access for critical runtime artifacts.
///
/// This preflight fails fast with explicit path+permission diagnostics so the
/// daemon does not enter a noisy retry loop when DB/export paths are invalid.
pub(crate) fn ensure_runtime_file_permissions(db_path: &str, export_path: &str) -> anyhow::Result<()> {
    ensure_parent_dir(Path::new(db_path), "DB_PATH")?;
    ensure_parent_dir(Path::new(export_path), "EXPORT_PATH")?;

    probe_rw_file(Path::new(db_path), "DB_PATH", false)?;
    probe_rw_file(Path::new(export_path), "EXPORT_PATH", true)?;
    Ok(())
}

fn ensure_parent_dir(path: &Path, label: &str) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(parent)
        .with_context(|| format!("create parent directory for {label}: {}", parent.display()))
}

fn probe_rw_file(path: &Path, label: &str, append: bool) -> anyhow::Result<()> {
    let mut opts = OpenOptions::new();
    opts.create(true).read(true).write(true);
    if append {
        opts.append(true);
    }

    match opts.open(path) {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            anyhow::bail!(
                "{label} is not writable: {} (permission denied; check file owner/group/mode and parent directory permissions)",
                path.display()
            );
        }
        Err(err) => Err(err).with_context(|| format!("open {label} at {}", path.display())),
    }
}

/// Boots and serves the UDS control plane used by attachable TUI clients.
///
/// Auditor notes:
/// - The daemon lifecycle state (paused/running) is persisted in sqlite so new
///   TUI sessions can recover a correct view without adding a "status" IPC call.
/// - Pause transitions are persisted only when a real state change occurs,
///   mirroring the transition semantics in `control_plane`.
/// - The server is fire-and-forget; failures after bootstrap are surfaced via
///   structured error logs while the daemon process keeps running.
pub(crate) fn bootstrap_control_plane(sock_path: &Path, db_path: &str, paused: Arc<AtomicBool>) -> anyhow::Result<()> {
    let control_state_store =
        Arc::new(SqliteWalStore::new_with_busy_timeout(db_path, 30_000).context("initialize control-state store")?);

    // On daemon start we explicitly persist "running" as the baseline state.
    // This prevents stale paused values from previous process lifetimes.
    control_state_store
        .set_daemon_paused(false)
        .context("persist initial daemon state")?;

    let control_listener = crate::control_plane::bind_control_listener(sock_path)
        .with_context(|| format!("initialize control socket at {}", sock_path.display()))?;

    let state_store = control_state_store.clone();
    tokio::spawn(async move {
        let on_transition = Arc::new(move |is_paused: bool| {
            if let Err(err) = state_store.set_daemon_paused(is_paused) {
                tracing::error!(paused = is_paused, "Failed to persist daemon pause state: {}", err);
            }
        });

        if let Err(err) =
            crate::control_plane::serve_bound_listener_with_hook(control_listener, paused, Some(on_transition)).await
        {
            tracing::error!("Control plane terminated: {}", err);
        }
    });

    Ok(())
}

/// Runs the steady-state daemon cycle.
///
/// Auditor notes:
/// - Paused mode suppresses only opportunity discovery/initiation.
/// - Finalization/export and housekeeping always run to keep WAL state convergent.
/// - The loop is intentionally infinite; process lifecycle is handled by supervisor.
/// - Stage timeouts and panic recovery keep the daemon responsive during partial
///   downstream outages or unexpected runtime faults.
pub(crate) async fn run_daemon_cycle_loop(
    finder: &OpportunityFinder<Agent>,
    strategy: &SimpleLiquidationStrategy<
        SwapRouter,
        Config,
        TokenRegistry,
        CollateralService<LiquidationPriceOracle<Agent>>,
    >,
    executor: &Arc<BasicExecutor<Agent, SqliteWalStore>>,
    exporter: &Arc<ExportStage>,
    finalizer: &Arc<FinalizeStage<HybridFinalizer<Config>, SqliteWalStore, SimpleProfitCalculator, Agent>>,
    liq_dog: &Arc<dyn Watchdog>,
    paused: Arc<AtomicBool>,
    debt_assets: &Vec<String>,
    debt_asset_principals: &[Principal],
    ui_enabled: bool,
) {
    let mut spinner = start_spinner(ui_enabled);
    let mut last_alive_log = Instant::now();
    let mut consecutive_timed_out_cycles: u32 = 0;

    loop {
        let cycle = run_single_daemon_cycle(
            finder,
            strategy,
            executor,
            exporter,
            finalizer,
            liq_dog,
            paused.as_ref(),
            debt_assets,
            debt_asset_principals,
            ui_enabled,
            &mut spinner,
            &mut last_alive_log,
        );

        let outcome = match AssertUnwindSafe(cycle).catch_unwind().await {
            Ok(outcome) => outcome,
            Err(_) => {
                tracing::error!("daemon cycle panicked; recovering and continuing");
                if let Some(s) = spinner.as_ref() {
                    s.set_message("Recovered from panic; resuming daemon loop...");
                }
                sleep(PANIC_RECOVERY_DELAY).await;
                continue;
            }
        };

        if outcome.had_timeout {
            consecutive_timed_out_cycles = consecutive_timed_out_cycles.saturating_add(1);
            if consecutive_timed_out_cycles.is_multiple_of(5) {
                warn!(
                    consecutive_timed_out_cycles,
                    "Repeated stage timeouts detected; daemon remains alive and retrying"
                );
            }
        } else {
            consecutive_timed_out_cycles = 0;
        }

        let delay = if outcome.had_timeout {
            TIMEOUT_RECOVERY_DELAY
        } else if outcome.is_paused {
            PAUSED_LOOP_DELAY
        } else {
            RUNNING_LOOP_DELAY
        };
        sleep(delay).await;
    }
}

struct CycleOutcome {
    is_paused: bool,
    had_timeout: bool,
}

async fn stage_with_timeout<T, F>(stage: &'static str, deadline: Duration, fut: F) -> Option<T>
where
    F: Future<Output = T>,
{
    match timeout(deadline, fut).await {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::error!(
                stage,
                timeout_ms = deadline.as_millis() as u64,
                "Stage timed out; skipping to keep daemon responsive"
            );
            None
        }
    }
}

async fn run_single_daemon_cycle(
    finder: &OpportunityFinder<Agent>,
    strategy: &SimpleLiquidationStrategy<
        SwapRouter,
        Config,
        TokenRegistry,
        CollateralService<LiquidationPriceOracle<Agent>>,
    >,
    executor: &Arc<BasicExecutor<Agent, SqliteWalStore>>,
    exporter: &Arc<ExportStage>,
    finalizer: &Arc<FinalizeStage<HybridFinalizer<Config>, SqliteWalStore, SimpleProfitCalculator, Agent>>,
    liq_dog: &Arc<dyn Watchdog>,
    paused: &AtomicBool,
    debt_assets: &Vec<String>,
    debt_asset_principals: &[Principal],
    ui_enabled: bool,
    spinner: &mut Option<ProgressBar>,
    last_alive_log: &mut Instant,
) -> CycleOutcome {
    let is_paused = paused.load(Ordering::Relaxed);
    let mut had_timeout = false;

    if is_paused {
        set_spinner_message(spinner, true);
    } else {
        set_spinner_message(spinner, false);
        if last_alive_log.elapsed() >= SCAN_HEARTBEAT_INTERVAL {
            info!("Liquidator running; scanning for liquidation opportunities...");
            *last_alive_log = Instant::now();
        }

        // Discovery + initiation are the only operations gated by pause.
        let opportunities =
            match stage_with_timeout("opportunity.finder", FINDER_STAGE_TIMEOUT, finder.process(debt_assets)).await {
                Some(Ok(opps)) => opps,
                Some(Err(err)) => {
                    warn!("Failed to find opportunities: {err}");
                    vec![]
                }
                None => {
                    had_timeout = true;
                    vec![]
                }
            };

        if !opportunities.is_empty() {
            sleep(Duration::from_secs(2)).await;
            if let Some(s) = spinner.as_ref() {
                s.finish_and_clear();
            }
            info!("Found {:?} opportunities", opportunities.len());

            let opp_count = opportunities.len();
            let cycle_future = async {
                let executions = strategy.process(&opportunities).await.unwrap_or_else(|e| {
                    info!("Strategy processing failed: {e}");
                    vec![]
                });

                if executions.is_empty() {
                    info!(
                        "Found {} opportunities but none executable yet; likely waiting for funds or liquidity",
                        opportunities.len()
                    );
                }

                let _ = executor.process(&executions).await.unwrap_or_else(|e| {
                    tracing::error!("Executor failed: {e}");
                    vec![]
                });
            }
            .instrument(info_span!("liquidation.cycle", opportunities = opp_count));

            if stage_with_timeout("liquidation.cycle.execute", LIQUIDATION_CYCLE_TIMEOUT, cycle_future)
                .await
                .is_none()
            {
                had_timeout = true;
            }
        }
    }

    // Finalization is intentionally always-on, even while paused, so queued
    // operations can complete and state can converge.
    let outcomes = match stage_with_timeout("finalizer", FINALIZER_STAGE_TIMEOUT, finalizer.process(&())).await {
        Some(Ok(results)) => results,
        Some(Err(err)) => {
            tracing::error!("Finalizer failed: {err}");
            vec![]
        }
        None => {
            had_timeout = true;
            vec![]
        }
    };

    if outcomes.is_empty() {
        set_spinner_message(spinner, is_paused);
    } else {
        match stage_with_timeout("exporter", EXPORT_STAGE_TIMEOUT, exporter.process(&outcomes)).await {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                warn!("Failed to export results: {}", err);
            }
            None => {
                had_timeout = true;
            }
        }
        log_execution_results(&outcomes);
        if ui_enabled {
            print_execution_results(outcomes.clone());
        }
        *spinner = start_spinner(ui_enabled);
        set_spinner_message(spinner, is_paused);
    }

    // Housekeeping stays active in both running and paused states so
    // monitoring and allowances remain fresh while initiation is suspended.
    if stage_with_timeout(
        "watchdog.heartbeat",
        WATCHDOG_STAGE_TIMEOUT,
        liq_dog.notify(WatchdogEvent::Heartbeat { stage: "Running" }),
    )
    .await
    .is_none()
    {
        had_timeout = true;
    }
    match stage_with_timeout(
        "allowance.refresh",
        REFRESH_ALLOWANCES_TIMEOUT,
        executor.refresh_allowances(debt_asset_principals),
    )
    .await
    {
        Some(Ok(())) => {}
        Some(Err(err)) => {
            warn!("Failed to refresh lending allowances: {}", err);
        }
        None => {
            had_timeout = true;
        }
    }

    CycleOutcome { is_paused, had_timeout }
}

pub(crate) fn print_execution_results(results: Vec<LiquidationOutcome>) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    table.set_titles(Row::new(vec![
        Cell::new("Realized (Δ)"),
        Cell::new("Expected"),
        Cell::new("Debt Repaid"),
        Cell::new("Collateral"),
        Cell::new("Swap Output"),
        Cell::new("Swap Status"),
        Cell::new("Swapper"),
        Cell::new("Round Trip (s)"),
        Cell::new("Status"),
    ]));

    for r in results {
        let (debt, collat) = (r.formatted_debt_repaid(), r.formatted_received_collateral());

        let (recv_amt, swap_status) = match &r.finalizer_result.swap_result {
            Some(sr) => (r.formatted_swap_output(), sr.status.clone()),
            None => ("-".to_string(), "-".to_string()),
        };

        let delta = r.realized_profit - r.expected_profit;
        let delta_cell = {
            let txt = format!("{} ({})", r.formatted_realized_profit(), r.formatted_profit_delta());
            match delta.cmp(&0) {
                std::cmp::Ordering::Greater => Cell::new(&txt).style_spec("Fg"),
                std::cmp::Ordering::Less => Cell::new(&txt).style_spec("Fr"),
                std::cmp::Ordering::Equal => Cell::new(&txt),
            }
        };

        let status_text = r.status.description();
        let status_cell = match &r.status {
            ExecutionStatus::Success => Cell::new(&status_text).style_spec("Fg"),
            _ => Cell::new(&status_text).style_spec("Fr"),
        };

        table.add_row(Row::new(vec![
            delta_cell,
            Cell::new(&r.formatted_expected_profit()),
            Cell::new(&debt),
            Cell::new(&collat),
            Cell::new(&recv_amt),
            Cell::new(&swap_status),
            Cell::new(&r.formatted_swapper()),
            Cell::new(&r.formatted_round_trip_secs()),
            status_cell,
        ]));
    }

    table.printstd();
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tempfile::TempDir;

    use tokio::time::sleep;

    use super::{ensure_runtime_file_permissions, stage_with_timeout};

    #[tokio::test]
    async fn stage_timeout_returns_none_when_future_exceeds_deadline() {
        let out = stage_with_timeout("test.stage", Duration::from_millis(10), async {
            sleep(Duration::from_millis(50)).await;
            42usize
        })
        .await;

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn stage_timeout_returns_value_when_future_finishes_in_time() {
        let out = stage_with_timeout("test.stage", Duration::from_millis(100), async { 7usize }).await;
        assert_eq!(out, Some(7));
    }

    #[test]
    fn runtime_file_preflight_creates_missing_parents_and_files() {
        let tmp = TempDir::new().expect("tmp");
        let db = tmp.path().join("state/wal.db");
        let export = tmp.path().join("exports/executions.csv");

        ensure_runtime_file_permissions(db.to_str().expect("db path"), export.to_str().expect("export path"))
            .expect("preflight should pass");

        assert!(db.is_file());
        assert!(export.is_file());
    }

    #[test]
    fn runtime_file_preflight_rejects_directory_target_for_db() {
        let tmp = TempDir::new().expect("tmp");
        let db_dir = tmp.path().join("dbdir");
        std::fs::create_dir_all(&db_dir).expect("mkdir");
        let export = tmp.path().join("exports/executions.csv");

        let err =
            ensure_runtime_file_permissions(db_dir.to_str().expect("db dir"), export.to_str().expect("export path"))
                .expect_err("directory path must fail");
        assert!(err.to_string().contains("DB_PATH"));
    }
}
