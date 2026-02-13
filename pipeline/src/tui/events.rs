use crossterm::event::KeyEvent;

use crate::error::AppResult;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ExecutionsSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Paste(String),
    Tick,
    AppendLogLines(Vec<String>),
    PrependLogLines(Vec<String>),
    DaemonPaused(AppResult<bool>),
    Wal(AppResult<WalSnapshot>),
    Balances(AppResult<BalancesSnapshot>),
    Profits(AppResult<ProfitsSnapshot>),
    Executions(AppResult<ExecutionsSnapshot>),
    Withdraw(AppResult<String>),
    Deposit(AppResult<DepositAddress>),
}
