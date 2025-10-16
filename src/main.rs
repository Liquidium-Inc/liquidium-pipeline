mod account;
mod commands;
mod config;
mod executors;
mod finalizers;
mod icrc_token;
mod liquidation;
mod persistance;
mod pipeline_agent;
mod price_oracle;
mod ray_math;
mod stage;
mod stages;
pub mod swappers;
mod types;
mod utils;
mod watchdog;
use clap::{Parser, Subcommand};

use commands::liquidation_loop::run_liquidation_loop;

#[derive(Parser)]
#[command(name = "liquidator")]
#[command(about = "Liquidator Bot CLI to run liquidations, check balances, withdraw funds, and manage accounts.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts the liquidation bot loop
    Run,

    /// Shows wallet token balances
    Balance,

    /// Withdraws funds. Without flags, starts the interactive wizard.
    /// With flags, performs a non-interactive withdrawal.
    Withdraw {
        /// Source account: "main" or "recovery" (non-interactive)
        #[arg(long)]
        source: Option<String>,
        /// Destination: "main" or full Account string (non-interactive)
        #[arg(long)]
        destination: Option<String>,
        /// Asset symbol (e.g., "ckUSDT") or "all" (non-interactive)
        #[arg(long)]
        asset: Option<String>,
        /// Amount as decimal (respects token decimals) or "all" (non-interactive)
        #[arg(long)]
        amount: Option<String>,
    },

    /// Account management commands
    Account {
        #[command(subcommand)]
        subcommand: AccountCommands,
    },
}

#[derive(Subcommand)]
enum AccountCommands {
    /// Shows the wallet principal
    Show,

    /// Generates a new identity or account key (implementation specific)
    New,
}

#[tokio::main]
async fn main() {
    // Parse CLI
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run => {
            run_liquidation_loop().await;
        }
        Commands::Balance => {
            commands::funds::funds().await;
        }
        Commands::Withdraw { source, destination, asset, amount } => {
            let has_any = source.is_some() || destination.is_some() || asset.is_some() || amount.is_some();
            if has_any {
                // Validate that all required args are present
                match (source.as_deref(), destination.as_deref(), asset.as_deref(), amount.as_deref()) {
                    (Some(s), Some(d), Some(a), Some(am)) => {
                        commands::withdraw::withdraw_noninteractive(s, d, a, am).await;
                    }
                    _ => {
                        eprintln!(
                            "Missing flags. Required for non-interactive: --source <main|recovery> --destination <main|ACCOUNT> --asset <SYMBOL|all> --amount <DECIMAL|all>.\nRun without flags to use the interactive wizard."
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
