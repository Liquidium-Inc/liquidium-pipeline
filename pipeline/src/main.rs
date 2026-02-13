mod commands;
mod config;
mod context;
mod error;
mod executors;
mod finalizers;
mod liquidation;
mod output;
mod persistance;
mod price_oracle;
mod stage;
mod stages;
pub mod swappers;
mod wal;

mod approval_state;
mod control_plane;
mod tui;
mod utils;
mod watchdog;
use clap::{Parser, Subcommand};
use liquidium_pipeline_commons::env::load_env;
use liquidium_pipeline_commons::telemetry::{init_telemetry_from_env, init_telemetry_from_env_with_log_file};
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::time::{Duration, SystemTime};
#[cfg(target_os = "linux")]
use std::process::Command;

use crate::commands::liquidation_loop::run_liquidation_loop;

#[derive(Parser)]
#[command(name = "liquidator")]
#[command(about = "Liquidator Bot CLI to run liquidations, check balances, withdraw funds, and manage accounts.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // Starts the liquidation daemon loop
    Run {
        // Control socket path.
        #[arg(long)]
        sock_path: Option<PathBuf>,
        // Optional local log file.
        // `--log-file` (without a value) uses the default temp path.
        // `--log-file /path/to/file.log` uses the provided path.
        #[arg(long, num_args = 0..=1, value_name = "PATH", conflicts_with = "no_log_file")]
        log_file: Option<Option<PathBuf>>,
        // Disable local file logs explicitly.
        #[arg(long, conflicts_with = "log_file")]
        no_log_file: bool,
    },

    // Starts the interactive attachable TUI client.
    Tui {
        // Control socket path.
        #[arg(long)]
        sock_path: Option<PathBuf>,
        // Linux systemd unit for journalctl log source.
        #[arg(long, default_value = "liquidator.service")]
        unit_name: String,
        // Optional log file for TUI file-tail mode. If omitted and the default
        // log file exists, TUI tails it automatically.
        #[arg(long)]
        log_file: Option<PathBuf>,
    },

    // Shows wallet token balances
    Balance,

    // Test MEXC deposit address lookup
    MexcDepositAddress {
        // Asset symbol (e.g., ckUSDT, ckBTC)
        #[arg(long)]
        asset: String,
        // Network hint (e.g., ICP, CKUSDT). Defaults to ICP.
        #[arg(long)]
        network: Option<String>,
    },

    // Withdraws funds. Without flags, starts the interactive wizard.
    // With flags, performs a non-interactive withdrawal.
    Withdraw {
        // Source account: "main", "trader", or "recovery" (non-interactive)
        #[arg(long)]
        source: Option<String>,
        // Destination: "main", "trader", "recovery", or full Account string (non-interactive)
        #[arg(long)]
        destination: Option<String>,
        // Asset symbol (e.g., "ckUSDT") or "all" (non-interactive)
        #[arg(long)]
        asset: Option<String>,
        // Amount as decimal (respects token decimals) or "all" (non-interactive)
        #[arg(long)]
        amount: Option<String>,
    },

    // Account management commands
    Account {
        #[command(subcommand)]
        subcommand: AccountCommands,
    },
}

#[derive(Subcommand)]
enum AccountCommands {
    // Shows the wallet principal
    Show,

    // Generates a new identity or account key (implementation specific)
    New,
}

#[tokio::main]
async fn main() {
    load_env();
    let cli = Cli::parse();
    let running_under_systemd = is_systemd_service_process();

    if matches!(cli.command, Commands::Run { .. })
        && let Some(reason) = detect_manual_run_block_reason(running_under_systemd)
    {
        eprintln!("{reason}");
        return;
    }

    // Telemetry writes to stdout by default; in TUI mode it will corrupt the terminal.
    // The TUI sets up its own in-app log sink instead.
    let _telemetry_guard = match &cli.command {
        Commands::Tui { .. } => None,
        Commands::Run {
            log_file,
            no_log_file,
            ..
        } => {
            let mut telemetry_log_file = effective_run_log_file(log_file.clone(), *no_log_file);
            if let Some(path) = telemetry_log_file.as_deref()
                && let Err(err) = ensure_log_file_parent_exists(path)
            {
                eprintln!(
                    "Warning: cannot create log-file parent for {}: {err}. Falling back to stdout logs.",
                    path.display()
                );
                telemetry_log_file = None;
            }

            match init_telemetry_from_env_with_log_file(telemetry_log_file.as_deref()) {
                Ok(guard) => Some(guard),
                Err(err) => {
                    if let Some(path) = telemetry_log_file.as_deref() {
                        eprintln!(
                            "Failed to initialize telemetry with log file {}: {err}. Falling back to stdout logs.",
                            path.display()
                        );
                        match init_telemetry_from_env_with_log_file(None) {
                            Ok(guard) => Some(guard),
                            Err(stdout_err) => {
                                eprintln!("Failed to initialize telemetry fallback: {stdout_err}");
                                return;
                            }
                        }
                    } else {
                        eprintln!("Failed to initialize telemetry: {err}");
                        return;
                    }
                }
            }
        }
        _ => match init_telemetry_from_env() {
            Ok(guard) => Some(guard),
            Err(err) => {
                eprintln!("Failed to initialize telemetry: {err}");
                return;
            }
        },
    };

    match cli.command {
        Commands::Run { sock_path, .. } => {
            let sock_path = sock_path.unwrap_or_else(control_plane::default_sock_path);
            run_liquidation_loop(sock_path).await;
        }
        Commands::Tui {
            sock_path,
            unit_name,
            log_file,
        } => {
            let sock_path = sock_path.unwrap_or_else(control_plane::default_sock_path);
            let inferred_log_file = infer_tui_log_file(log_file, &unit_name);
            let opts = commands::tui::TuiOptions {
                sock_path,
                unit_name,
                log_file: inferred_log_file,
            };
            if let Err(err) = commands::tui::run(opts).await {
                eprintln!("TUI exited with error: {}", err);
            }
        }
        Commands::Balance => {
            if let Err(e) = commands::funds::funds().await {
                eprintln!("Balance check failed: {}", e);
            }
        }
        Commands::MexcDepositAddress { asset, network } => {
            if let Err(err) = commands::cex::mexc_deposit_address(&asset, network.as_deref()).await {
                eprintln!("MEXC deposit address failed: {}", err);
            }
        }
        Commands::Withdraw {
            source,
            destination,
            asset,
            amount,
        } => {
            let has_any = source.is_some() || destination.is_some() || asset.is_some() || amount.is_some();
            if has_any {
                // Validate that all required args are present
                match (
                    source.as_deref(),
                    destination.as_deref(),
                    asset.as_deref(),
                    amount.as_deref(),
                ) {
                    (Some(s), Some(d), Some(a), Some(am)) => {
                        commands::withdraw::withdraw_noninteractive(s, d, a, am).await;
                    }
                    _ => {
                        eprintln!(
                            "Missing flags. Required for non-interactive: --source <main|trader|recovery> --destination <main|trader|recovery|ACCOUNT> --asset <SYMBOL|all> --amount <DECIMAL|all>.\nRun without flags to use the interactive wizard."
                        );
                    }
                }
            } else {
                // Interactive wizard
                commands::withdraw::withdraw().await;
            }
        }
        Commands::Account { subcommand } => match subcommand {
            AccountCommands::Show => {
                commands::account::show().await;
            }
            AccountCommands::New => {
                commands::account::new().await;
            }
        },
    }
}

fn is_systemd_service_process() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::env::var_os("INVOCATION_ID").is_some() || std::env::var_os("JOURNAL_STREAM").is_some()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

fn effective_run_log_file(requested: Option<Option<PathBuf>>, no_log_file: bool) -> Option<PathBuf> {
    if no_log_file {
        return None;
    }

    match requested {
        Some(Some(path)) => Some(path),
        Some(None) => Some(control_plane::default_log_file_path()),
        None => None,
    }
}

fn infer_tui_log_file(requested: Option<PathBuf>, unit_name: &str) -> Option<PathBuf> {
    if requested.is_some() {
        return requested;
    }

    #[cfg(target_os = "linux")]
    {
        match is_systemd_unit_active(unit_name) {
            Ok(true) => return None,
            Ok(false) => {
                let candidate = control_plane::default_log_file_path();
                if should_infer_tui_log_file(&candidate) {
                    return Some(candidate);
                }
                return None;
            }
            Err(err) => {
                eprintln!(
                    "Warning: unable to query systemd unit state for '{unit_name}': {err}. Defaulting to journald."
                );
                return None;
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = unit_name;
    }

    let candidate = control_plane::default_log_file_path();
    if candidate.is_file() { Some(candidate) } else { None }
}

#[cfg(target_os = "linux")]
fn should_infer_tui_log_file(path: &std::path::Path) -> bool {
    const MAX_FILE_AGE: Duration = Duration::from_secs(15 * 60);

    if !path.is_file() {
        return false;
    }

    let modified = match std::fs::metadata(path).and_then(|meta| meta.modified()) {
        Ok(modified) => modified,
        Err(_) => return false,
    };

    match SystemTime::now().duration_since(modified) {
        Ok(age) => age <= MAX_FILE_AGE,
        // Clock skew or future timestamp: treat as active file.
        Err(_) => true,
    }
}

fn ensure_log_file_parent_exists(path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn detect_manual_run_block_reason(running_under_systemd: bool) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        if running_under_systemd {
            return None;
        }

        if !std::path::Path::new("/run/systemd/system").exists() {
            return None;
        }

        let unit_name = std::env::var("LIQUIDATOR_SYSTEMD_UNIT").unwrap_or_else(|_| "liquidator.service".to_string());
        match is_systemd_unit_active(&unit_name) {
            Ok(true) => Some(format!(
                "Refusing to start duplicate daemon: systemd unit '{unit_name}' is already active.\nUse `liquidator tui` to attach, or stop the unit first: sudo systemctl stop {unit_name}"
            )),
            Ok(false) => None,
            Err(err) => {
                eprintln!("Warning: unable to query systemd unit state for '{unit_name}': {err}");
                None
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = running_under_systemd;
        None
    }
}

#[cfg(target_os = "linux")]
fn is_systemd_unit_active(unit_name: &str) -> std::io::Result<bool> {
    let status = Command::new("systemctl")
        .args(["is-active", "--quiet", unit_name])
        .status()?;
    Ok(status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::fs::File;
    #[cfg(target_os = "linux")]
    use tempfile::TempDir;

    #[test]
    fn run_log_file_resolution_no_flags_disables_file_sink() {
        let resolved = effective_run_log_file(None, false);
        assert_eq!(resolved, None);
    }

    #[test]
    fn run_log_file_resolution_no_log_file_disables_file_sink() {
        let resolved = effective_run_log_file(Some(None), true);
        assert_eq!(resolved, None);
    }

    #[test]
    fn run_log_file_resolution_flag_without_value_uses_default() {
        let resolved = effective_run_log_file(Some(None), false);
        assert_eq!(resolved, Some(control_plane::default_log_file_path()));
    }

    #[test]
    fn run_log_file_resolution_custom_path() {
        let custom = PathBuf::from("/tmp/custom.log");
        let resolved = effective_run_log_file(Some(Some(custom.clone())), false);
        assert_eq!(resolved, Some(custom));
    }

    #[test]
    fn cli_parse_run_without_log_flags() {
        let parsed = Cli::try_parse_from(["liquidator", "run"]).expect("run should parse");
        match parsed.command {
            Commands::Run {
                log_file,
                no_log_file,
                ..
            } => {
                assert_eq!(log_file, None);
                assert!(!no_log_file);
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn cli_parse_run_log_file_flag_without_value() {
        let parsed =
            Cli::try_parse_from(["liquidator", "run", "--log-file"]).expect("run --log-file should parse");
        match parsed.command {
            Commands::Run { log_file, .. } => {
                assert_eq!(log_file, Some(None));
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn cli_parse_run_log_file_custom_path() {
        let parsed = Cli::try_parse_from(["liquidator", "run", "--log-file", "/tmp/x.log"])
            .expect("run --log-file /tmp/x.log should parse");
        match parsed.command {
            Commands::Run { log_file, .. } => {
                assert_eq!(log_file, Some(Some(PathBuf::from("/tmp/x.log"))));
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn cli_parse_run_conflicting_log_flags_fails() {
        let parsed = Cli::try_parse_from(["liquidator", "run", "--log-file", "--no-log-file"]);
        assert!(parsed.is_err(), "conflicting log flags should fail parsing");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn infer_tui_log_file_candidate_requires_existing_recent_file() {
        let tmp = TempDir::new().expect("tempdir");
        let missing = tmp.path().join("missing.log");
        assert!(!should_infer_tui_log_file(&missing));

        let existing = tmp.path().join("existing.log");
        File::create(&existing).expect("create log file");
        assert!(should_infer_tui_log_file(&existing));
    }
}
