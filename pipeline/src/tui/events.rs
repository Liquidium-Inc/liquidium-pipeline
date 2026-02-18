use crossterm::event::KeyEvent;

use crate::error::AppError;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ExecutionsSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Paste(String),
    Tick,
    AppendLogLines(Vec<String>),
    PrependLogLines(Vec<String>),
    DaemonPaused(Result<bool, AppError>),
    Wal(Result<WalSnapshot, AppError>),
    Balances(Result<BalancesSnapshot, AppError>),
    Profits(Result<ProfitsSnapshot, AppError>),
    Executions(Result<ExecutionsSnapshot, AppError>),
    Withdraw(Result<String, AppError>),
    Deposit(Result<DepositAddress, AppError>),
}
