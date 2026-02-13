use crossterm::event::KeyEvent;

use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ExecutionsSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Paste(String),
    Tick,
    AppendLogLines(Vec<String>),
    PrependLogLines(Vec<String>),
    DaemonPaused(Result<bool, String>),
    Wal(Result<WalSnapshot, String>),
    Balances(Result<BalancesSnapshot, String>),
    Profits(Result<ProfitsSnapshot, String>),
    Executions(Result<ExecutionsSnapshot, String>),
    Withdraw(Result<String, String>),
    Deposit(Result<DepositAddress, String>),
}
