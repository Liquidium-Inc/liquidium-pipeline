use crossterm::event::KeyEvent;

use crate::commands::liquidation_loop::LoopEvent;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;

use super::app::{BalancesSnapshot, ProfitsSnapshot, WalSnapshot};

pub(super) enum UiEvent {
    Input(KeyEvent),
    Tick,
    Engine(LoopEvent),
    Wal(Result<WalSnapshot, String>),
    Balances(Result<BalancesSnapshot, String>),
    Profits(Result<ProfitsSnapshot, String>),
    Withdraw(Result<String, String>),
    Deposit(Result<DepositAddress, String>),
}
