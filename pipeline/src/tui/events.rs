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
    /// A deposit-address reply for the lookup with this id.
    Deposit {
        request: u64,
        result: Result<DepositAddress, String>,
    },
}
