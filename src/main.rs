mod account;
mod commands;
mod config;
mod executors;
mod icrc_token;
mod liquidation;
mod pipeline_agent;
mod price_oracle;
mod ray_math;
mod stage;
mod stages;
mod utils;
mod types;
mod watchdog;
mod finalizers;
pub mod swappers;
mod persistance;
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

    /// Withdraws funds to a specified address
    Withdraw {
        /// Asset principal ID (e.g. ckBTC ledger principal)
        asset: String,
        /// Amount to withdraw in human-readable units
        amount: String,
        /// Destination principal address
        to: String,
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
        Commands::Withdraw { asset, amount, to } => {
            commands::withdraw::withdraw(asset, amount, to).await;
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
