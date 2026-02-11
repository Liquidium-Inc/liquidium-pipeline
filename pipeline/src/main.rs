mod commands;
mod config;
mod context;
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
mod tui;
mod utils;
mod watchdog;
use clap::{Parser, Subcommand};
use liquidium_pipeline_commons::env::load_env;
use liquidium_pipeline_commons::telemetry::init_telemetry_from_env;

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
    // Starts the liquidation bot loop
    Run,

    // Starts the interactive TUI (start/pause loop, view balances & profits)
    Tui,

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

    // Telemetry writes to stdout by default; in TUI mode it will corrupt the terminal.
    // The TUI sets up its own in-app log sink instead.
    let _telemetry_guard = match cli.command {
        Commands::Tui => None,
        _ => Some(init_telemetry_from_env().expect("Failed to initialize telemetry")),
    };

    match &cli.command {
        Commands::Run => {
            run_liquidation_loop().await;
        }
        Commands::Tui => {
            if let Err(err) = commands::tui::run().await {
                eprintln!("TUI exited with error: {}", err);
            }
        }
        Commands::Balance => {
            if let Err(e) = commands::funds::funds().await {
                eprintln!("Balance check failed: {}", e);
            }
        }
        Commands::MexcDepositAddress { asset, network } => {
            if let Err(err) = commands::cex::mexc_deposit_address(asset, network.as_deref()).await {
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
