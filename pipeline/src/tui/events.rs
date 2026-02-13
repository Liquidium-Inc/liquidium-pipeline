use crossterm::event::KeyEvent;

use crate::commands::liquidation_loop::LoopEvent;
use crate::error::AppResult;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ExecutionsSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Paste(String),
    Tick,
    Engine(LoopEvent),
    Wal(AppResult<WalSnapshot>),
    Balances(AppResult<BalancesSnapshot>),
    Profits(AppResult<ProfitsSnapshot>),
    Executions(AppResult<ExecutionsSnapshot>),
    Withdraw(AppResult<String>),
    Deposit(AppResult<DepositAddress>),
}
