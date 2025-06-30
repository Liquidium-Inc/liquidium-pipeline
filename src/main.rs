mod account;
mod config;
mod executors;
mod icrc_token;
mod liquidation;
mod commands;
mod pipeline_agent;
mod price_oracle;
mod ray_math;
mod stage;
mod stages;
mod utils;


use clap::{Parser, Subcommand};

use commands::liquidation_loop::run_liquidation_loop;


#[derive(Parser)]
#[command(name = "liquidator")]
#[command(about = "Liquidator Bot CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run,
    Funds,
    Withdraw { asset: String, amount: String, to: String },
}

#[tokio::main]
async fn main() {
    // Parse CLI
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run => {
            run_liquidation_loop().await;
        }
        Commands::Funds => {
           commands::funds::funds().await;
        }
        Commands::Withdraw { asset, amount, to } => {
            println!("Withdrawing {} {} to {}", amount, asset, to);
            // TODO: Implement actual withdraw logic with account_service
        }
    }
}
