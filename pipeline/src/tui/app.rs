use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use chrono::{DateTime, Local};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

use crate::commands::liquidation_loop::LoopControl;
use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::persistance::ResultStatus;
use liquidium_pipeline_connectors::backend::cex_backend::DepositAddress;
use liquidium_pipeline_core::tokens::asset_id::AssetId;

#[derive(Clone, Debug)]
pub(super) struct ConfigSummary {
    pub(super) ic_url: String,
    pub(super) liquidator_principal: String,
    pub(super) trader_principal: String,
    pub(super) evm_address: String,
    pub(super) swapper_mode: String,
    pub(super) max_dex_slippage_bps: u32,
    pub(super) max_cex_slippage_bps: u32,
    pub(super) buy_bad_debt: bool,
    pub(super) opportunity_filter: Vec<String>,
    pub(super) db_path: String,
    pub(super) export_path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Tab {
    Dashboard,
    Balances,
    Profits,
    Executions,
    Logs,
}

impl Tab {
    pub(super) fn all() -> &'static [Tab] {
        &[Tab::Dashboard, Tab::Balances, Tab::Profits, Tab::Executions, Tab::Logs]
    }

    pub(super) fn title(self) -> &'static str {
        match self {
            Tab::Dashboard => "Dashboard",
            Tab::Balances => "Balances",
            Tab::Profits => "Profits",
            Tab::Executions => "Executions",
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
pub(super) struct ExecutionRowData {
    pub(super) liq_id: String,
    pub(super) status: ResultStatus,
    pub(super) attempt: i32,
    pub(super) error_count: i32,
    pub(super) last_error: Option<String>,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
    pub(super) meta_json: String,
}

#[derive(Clone, Debug)]
pub(super) struct ExecutionsSnapshot {
    pub(super) rows: Vec<ExecutionRowData>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UiFocus {
    Tabs,
    Logs,
    ExecutionsTable,
    ExecutionsDetails,
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
    pub(super) config: ConfigSummary,
    pub(super) bad_debt_confirmed: bool,
    pub(super) bad_debt_confirm_input: Option<String>,

    pub(super) tab: Tab,
    pub(super) ui_focus: UiFocus,
    pub(super) should_quit: bool,
    pub(super) engine: LoopControl,
    pub(super) logs: VecDeque<String>,
    pub(super) logs_scroll: u16,
    pub(super) logs_scroll_x: u16,
    pub(super) logs_follow: bool,
    pub(super) logs_g_pending: bool,
    pub(super) logs_scroll_active: bool,
    pub(super) logs_content_width: usize,
    pub(super) dashboard_logs_scroll: u16,
    pub(super) dashboard_logs_scroll_x: u16,
    pub(super) dashboard_logs_follow: bool,
    pub(super) dashboard_logs_g_pending: bool,
    pub(super) dashboard_logs_scroll_active: bool,
    pub(super) dashboard_logs_content_width: usize,
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

    pub(super) executions: Option<ExecutionsSnapshot>,
    pub(super) executions_error: Option<String>,
    pub(super) executions_selected: usize,
    pub(super) executions_details_scroll: u16,

    pub(super) recent_outcomes: VecDeque<RecentOutcome>,

    pub(super) balances_panel: BalancesPanel,
    pub(super) deposit: DepositState,

    pub(super) withdraw_assets: Vec<AssetId>,
    pub(super) withdraw: WithdrawState,
}

impl App {
    pub(super) fn new(withdraw_assets: Vec<AssetId>, config: ConfigSummary) -> Self {
        Self {
            bad_debt_confirmed: !config.buy_bad_debt,
            bad_debt_confirm_input: None,
            config,

            tab: Tab::Dashboard,
            ui_focus: UiFocus::Tabs,
            should_quit: false,
            engine: LoopControl::Paused,
            logs: VecDeque::with_capacity(500),
            logs_scroll: 0,
            logs_scroll_x: 0,
            logs_follow: true,
            logs_g_pending: false,
            logs_scroll_active: false,
            logs_content_width: 1,
            dashboard_logs_scroll: 0,
            dashboard_logs_scroll_x: 0,
            dashboard_logs_follow: true,
            dashboard_logs_g_pending: false,
            dashboard_logs_scroll_active: false,
            dashboard_logs_content_width: 1,
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

            executions: None,
            executions_error: None,
            executions_selected: 0,
            executions_details_scroll: 0,

            recent_outcomes: VecDeque::with_capacity(200),

            balances_panel: BalancesPanel::None,
            deposit: DepositState::default(),

            withdraw_assets,
            withdraw: WithdrawState::new(),
        }
    }

    pub(super) fn push_log(&mut self, line: impl Into<String>) {
        const MAX: usize = 500;
        let bump_logs_scroll = self.logs_scroll_active && !self.logs_follow;
        let bump_dashboard_scroll = self.dashboard_logs_scroll_active && !self.dashboard_logs_follow;
        let line = line.into();
        for part in line.split(['\n', '\r']) {
            let part = part.trim_end();
            if part.is_empty() {
                continue;
            }
            if self.logs.len() >= MAX {
                self.logs.pop_front();
            }
            self.logs.push_back(part.to_string());
            if bump_logs_scroll {
                let rows = super::views::logs::wrapped_row_count_for_entry(part, self.logs_content_width)
                    .min(usize::from(u16::MAX)) as u16;
                self.logs_scroll = self.logs_scroll.saturating_add(rows);
            }
            if bump_dashboard_scroll {
                let rows = super::views::logs::wrapped_row_count_for_entry(part, self.dashboard_logs_content_width)
                    .min(usize::from(u16::MAX)) as u16;
                self.dashboard_logs_scroll = self.dashboard_logs_scroll.saturating_add(rows);
            }
        }
    }

    pub(super) fn update_log_viewport_widths(&mut self, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(3), Constraint::Min(0)])
            .split(area);
        let body = chunks[2];
        self.logs_content_width = body.width.saturating_sub(2) as usize;

        let dashboard_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(body);
        let dashboard_top = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(67), Constraint::Percentage(33)])
            .split(dashboard_chunks[0]);
        self.dashboard_logs_content_width = dashboard_top[0].width.saturating_sub(2) as usize;
    }

    pub(super) fn next_tab(&mut self) {
        let tabs = Tab::all();
        let idx = tabs.iter().position(|t| *t == self.tab).unwrap_or(0);
        self.tab = tabs[(idx + 1) % tabs.len()];
        self.ui_focus = UiFocus::Tabs;
        self.logs_scroll_active = false;
        self.dashboard_logs_scroll_active = false;
        self.logs_g_pending = false;
        self.dashboard_logs_g_pending = false;
        self.logs_follow = true;
        self.dashboard_logs_follow = true;
        self.logs_scroll = 0;
        self.dashboard_logs_scroll = 0;
        self.executions_details_scroll = 0;
    }

    pub(super) fn prev_tab(&mut self) {
        let tabs = Tab::all();
        let idx = tabs.iter().position(|t| *t == self.tab).unwrap_or(0);
        self.tab = tabs[(idx + tabs.len() - 1) % tabs.len()];
        self.ui_focus = UiFocus::Tabs;
        self.logs_scroll_active = false;
        self.dashboard_logs_scroll_active = false;
        self.logs_g_pending = false;
        self.dashboard_logs_g_pending = false;
        self.logs_follow = true;
        self.dashboard_logs_follow = true;
        self.logs_scroll = 0;
        self.dashboard_logs_scroll = 0;
        self.executions_details_scroll = 0;
    }
}
