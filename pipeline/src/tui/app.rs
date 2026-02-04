use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use chrono::{DateTime, Local};

use crate::commands::liquidation_loop::LoopControl;
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::persistance::ResultStatus;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;
use liquidium_pipeline_core::tokens::asset_id::AssetId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Tab {
    Dashboard,
    Balances,
    Profits,
    Logs,
}

impl Tab {
    pub(super) fn all() -> &'static [Tab] {
        &[Tab::Dashboard, Tab::Balances, Tab::Profits, Tab::Logs]
    }

    pub(super) fn title(self) -> &'static str {
        match self {
            Tab::Dashboard => "Dashboard",
            Tab::Balances => "Balances",
            Tab::Profits => "Profits",
            Tab::Logs => "Logs",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct BalanceRowData {
    pub(super) asset: AssetId,
    pub(super) main: String,
    pub(super) trader: String,
    pub(super) recovery: String,
}

#[derive(Clone, Debug)]
pub(super) struct BalancesSnapshot {
    pub(super) rows: Vec<BalanceRowData>,
    pub(super) at: DateTime<Local>,
}

#[derive(Clone, Debug)]
pub(super) struct ProfitBySymbol {
    pub(super) symbol: String,
    pub(super) count: usize,
    pub(super) realized: i128,
    pub(super) expected: i128,
    pub(super) decimals: Option<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct ProfitsSnapshot {
    pub(super) rows: Vec<ProfitBySymbol>,
    pub(super) at: DateTime<Local>,
}

#[derive(Clone, Debug)]
pub(super) struct RecentOutcome {
    pub(super) at: DateTime<Local>,
    pub(super) outcome: LiquidationOutcome,
}

#[derive(Clone, Debug, Default)]
pub(super) struct WalCounts {
    pub(super) enqueued: i64,
    pub(super) inflight: i64,
    pub(super) succeeded: i64,
    pub(super) failed_retryable: i64,
    pub(super) failed_permanent: i64,
    pub(super) waiting_collateral: i64,
    pub(super) waiting_profit: i64,
    pub(super) total: i64,
}

impl WalCounts {
    pub(super) fn from_map(map: &HashMap<ResultStatus, i64>) -> Self {
        let mut out = Self::default();
        out.enqueued = *map.get(&ResultStatus::Enqueued).unwrap_or(&0);
        out.inflight = *map.get(&ResultStatus::InFlight).unwrap_or(&0);
        out.succeeded = *map.get(&ResultStatus::Succeeded).unwrap_or(&0);
        out.failed_retryable = *map.get(&ResultStatus::FailedRetryable).unwrap_or(&0);
        out.failed_permanent = *map.get(&ResultStatus::FailedPermanent).unwrap_or(&0);
        out.waiting_collateral = *map.get(&ResultStatus::WaitingCollateral).unwrap_or(&0);
        out.waiting_profit = *map.get(&ResultStatus::WaitingProfit).unwrap_or(&0);
        out.total = out.enqueued
            + out.inflight
            + out.succeeded
            + out.failed_retryable
            + out.failed_permanent
            + out.waiting_collateral
            + out.waiting_profit;
        out
    }
}

#[derive(Clone, Debug)]
pub(super) struct WalSnapshot {
    pub(super) counts: WalCounts,
    pub(super) at: DateTime<Local>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WithdrawAccountKind {
    Main,
    Trader,
    Recovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WithdrawDestinationKind {
    Main,
    Trader,
    Recovery,
    Manual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WithdrawField {
    Source,
    Destination,
    ManualDestination,
    Asset,
    Amount,
    Submit,
}

pub(super) struct WithdrawState {
    pub(super) field: WithdrawField,
    pub(super) source: WithdrawAccountKind,
    pub(super) destination: WithdrawDestinationKind,
    pub(super) manual_destination: String,
    pub(super) asset_idx: usize,
    pub(super) amount: String,

    pub(super) editing: Option<WithdrawField>,
    pub(super) edit_backup: Option<String>,
    pub(super) in_flight: bool,
    pub(super) last_result: Option<Result<String, String>>,
}

impl WithdrawState {
    fn new() -> Self {
        Self {
            field: WithdrawField::Source,
            source: WithdrawAccountKind::Recovery,
            destination: WithdrawDestinationKind::Main,
            manual_destination: String::new(),
            asset_idx: 0,
            amount: "all".to_string(),
            editing: None,
            edit_backup: None,
            in_flight: false,
            last_result: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BalancesPanel {
    None,
    Withdraw,
    Deposit,
}

#[derive(Default)]
pub(super) struct DepositState {
    pub(super) in_flight: bool,
    pub(super) asset: Option<AssetId>,
    pub(super) network: Option<String>,
    pub(super) at: Option<DateTime<Local>>,
    pub(super) last: Option<Result<DepositAddress, String>>,
}

pub(super) struct App {
    pub(super) tab: Tab,
    pub(super) should_quit: bool,
    pub(super) engine: LoopControl,
    pub(super) logs: VecDeque<String>,
    pub(super) last_outcomes: usize,
    pub(super) last_error: Option<String>,
    pub(super) last_tick: Instant,

    pub(super) wal: Option<WalSnapshot>,
    pub(super) wal_error: Option<String>,

    pub(super) balances: Option<BalancesSnapshot>,
    pub(super) balances_error: Option<String>,
    pub(super) balances_selected: usize,

    pub(super) profits: Option<ProfitsSnapshot>,
    pub(super) profits_error: Option<String>,

    pub(super) recent_outcomes: VecDeque<RecentOutcome>,

    pub(super) balances_panel: BalancesPanel,
    pub(super) deposit: DepositState,

    pub(super) withdraw_assets: Vec<AssetId>,
    pub(super) withdraw: WithdrawState,
}

impl App {
    pub(super) fn new(withdraw_assets: Vec<AssetId>) -> Self {
        Self {
            tab: Tab::Dashboard,
            should_quit: false,
            engine: LoopControl::Paused,
            logs: VecDeque::with_capacity(500),
            last_outcomes: 0,
            last_error: None,
            last_tick: Instant::now(),

            wal: None,
            wal_error: None,

            balances: None,
            balances_error: None,
            balances_selected: 0,

            profits: None,
            profits_error: None,

            recent_outcomes: VecDeque::with_capacity(200),

            balances_panel: BalancesPanel::None,
            deposit: DepositState::default(),

            withdraw_assets,
            withdraw: WithdrawState::new(),
        }
    }

    pub(super) fn push_log(&mut self, line: impl Into<String>) {
        const MAX: usize = 500;
        let line = line.into();
        for part in line.split(|c| c == '\n' || c == '\r') {
            let part = part.trim_end();
            if part.is_empty() {
                continue;
            }
            if self.logs.len() >= MAX {
                self.logs.pop_front();
            }
            self.logs.push_back(part.to_string());
        }
    }

    pub(super) fn next_tab(&mut self) {
        let tabs = Tab::all();
        let idx = tabs.iter().position(|t| *t == self.tab).unwrap_or(0);
        self.tab = tabs[(idx + 1) % tabs.len()];
    }

    pub(super) fn prev_tab(&mut self) {
        let tabs = Tab::all();
        let idx = tabs.iter().position(|t| *t == self.tab).unwrap_or(0);
        self.tab = tabs[(idx + tabs.len() - 1) % tabs.len()];
    }
}
