use crossterm::event::KeyEvent;

use crate::commands::liquidation_loop::LoopEvent;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ExecutionsSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Paste(String),
    Tick,
    Engine(LoopEvent),
    Wal(Result<WalSnapshot, String>),
    Balances(Result<BalancesSnapshot, String>),
    Profits(Result<ProfitsSnapshot, String>),
    Executions(Result<ExecutionsSnapshot, String>),
    Withdraw(Result<String, String>),
    Deposit(Result<DepositAddress, String>),
}
