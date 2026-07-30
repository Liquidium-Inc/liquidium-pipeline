use chrono::{Local, TimeZone};
use liquidium_pipeline_core::tokens::chain_token_amount::ChainTokenAmount;
use liquidium_pipeline_core::types::protocol_types::{LiquidationStatus, TransferStatus, TxStatus};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap};
use serde::Deserialize;
use serde_json::Value;

use crate::finalizers::liquidation_outcome::LiquidationOutcome;
use crate::persistance::{
    FinalizerMetaPayload, FinalizerMetaV2, LiqMetaWrapper, ResultStatus, VenueExecutionState, VenueLegState,
    WalProfitSnapshot,
};
use crate::stages::executor::{ExecutionReceipt, ExecutionStatus};
use crate::swappers::icpswap::{VENUE_ID as ICPSWAP_VENUE_ID, identity::IcpswapExecutionIdentity};
use crate::wal::liq_id_from_receipt;

use super::super::app::{App, ExecutionRowData, UiFocus};

pub(super) fn draw_executions(f: &mut Frame<'_>, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    if let Some(exec) = &app.executions {
        lines.push(Line::from(format!("Updated @ {}", exec.at.format("%H:%M:%S"))));
    } else {
        lines.push(Line::from("No executions yet (loading WAL)."));
    }
    lines.push(Line::from(
        "Focus: Down -> table, Enter/Right -> details, Left/Esc -> table, Esc -> tabs",
    ));

    if let Some(wal) = &app.wal {
        lines.push(Line::from(format!(
            "WAL: inflight={} wait={} unresumable={} ok={} fail={} total={}",
            wal.counts.inflight,
            wal.counts.waiting_collateral + wal.counts.waiting_profit,
            wal.counts.unresumable,
            wal.counts.succeeded,
            wal.counts.failed_retryable + wal.counts.failed_permanent,
            wal.counts.total
        )));
    } else if let Some(err) = &app.wal_error {
        lines.push(Line::from(Span::styled(
            format!("WAL error: {}", err),
            Style::default().fg(Color::Red),
        )));
    }

    if let Some(err) = &app.executions_error {
        lines.push(Line::from(Span::styled(
            format!("Executions error: {}", err),
            Style::default().fg(Color::Red),
        )));
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(lines.len() as u16 + 2), Constraint::Min(0)])
        .split(area);

    let header = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Executions"))
        .wrap(Wrap { trim: false });
    f.render_widget(header, layout[0]);

    let Some(exec) = &app.executions else {
        return;
    };

    let now = Local::now().timestamp();

    let rows = exec.rows.iter().enumerate().map(|(idx, r)| {
        let status = status_label(r.status);
        let status_cell = Cell::new(status).style(status_style(r.status));

        let age_secs = now.saturating_sub(r.updated_at);
        let updated = format_age(age_secs);

        let profit_cell = profit_cell_for_row(r, app);

        let last_error = r
            .last_error
            .as_deref()
            .map(|e| truncate(e, 34))
            .unwrap_or_else(|| "-".to_string());

        let mut row = Row::new(vec![
            status_cell,
            Cell::new(truncate(&r.liq_id, 14)),
            Cell::new(r.attempt.to_string()),
            Cell::new(r.error_count.to_string()),
            Cell::new(updated),
            profit_cell,
            Cell::new(last_error),
        ]);

        if idx == app.executions_selected {
            row = row.style(Style::default().bg(Color::DarkGray));
        }
        row
    });

    let header = Row::new(vec![
        Cell::new("Status"),
        Cell::new("Liq ID"),
        Cell::new("Att"),
        Cell::new("Err"),
        Cell::new("Age"),
        Cell::new("Profit"),
        Cell::new("Last error"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let body_area = layout[1];
    let body_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(body_area);

    let selected = exec.rows.get(app.executions_selected).or_else(|| exec.rows.first());

    let details_block = {
        let title = if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            "Execution Details (j/k PgUp/PgDn Home/End)"
        } else {
            "Execution Details"
        };
        let mut block = Block::default().borders(Borders::ALL).title(title);
        if matches!(app.ui_focus, UiFocus::ExecutionsDetails) {
            block = block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
        }
        block
    };

    if let Some(row) = selected {
        let lines = build_execution_details(row, app);
        let height = body_layout[1].height.saturating_sub(2) as usize;
        let content_width = body_layout[1].width.saturating_sub(2) as usize;
        let wrapped_lines = estimate_wrapped_lines(&lines, content_width);
        let max_scroll = wrapped_lines.saturating_sub(height) as u16;
        let scroll = app.executions_details_scroll.min(max_scroll);
        let details_widget = Paragraph::new(lines)
            .block(details_block)
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(details_widget, body_layout[1]);
    } else {
        let details_widget = Paragraph::new("No selection.")
            .block(details_block)
            .wrap(Wrap { trim: false });
        f.render_widget(details_widget, body_layout[1]);
    }

    let mut table_block = Block::default().borders(Borders::ALL).title("WAL");
    if matches!(app.ui_focus, UiFocus::ExecutionsTable) {
        table_block = table_block.border_style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    }

    if exec.rows.is_empty() {
        let w = Paragraph::new("No WAL entries yet.").block(table_block);
        f.render_widget(w, body_layout[0]);
        return;
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(16),
            Constraint::Min(0),
        ],
    )
    .header(header)
    .block(table_block);

    f.render_widget(table, body_layout[0]);
}

fn status_label(status: ResultStatus) -> &'static str {
    match status {
        ResultStatus::Enqueued => "enqueued",
        ResultStatus::InFlight => "in-flight",
        ResultStatus::Succeeded => "succeeded",
        ResultStatus::FailedRetryable => "retry",
        ResultStatus::FailedPermanent => "failed",
        ResultStatus::WaitingCollateral => "wait-col",
        ResultStatus::WaitingProfit => "wait-prof",
        ResultStatus::OperatorRequired => "operator",
        ResultStatus::Unresumable => "unresumable",
    }
}

fn status_style(status: ResultStatus) -> Style {
    match status {
        ResultStatus::Succeeded => Style::default().fg(Color::Green),
        ResultStatus::FailedPermanent => Style::default().fg(Color::Red),
        ResultStatus::FailedRetryable => Style::default().fg(Color::Yellow),
        ResultStatus::InFlight => Style::default().fg(Color::Cyan),
        ResultStatus::WaitingCollateral | ResultStatus::WaitingProfit => Style::default().fg(Color::Yellow),
        ResultStatus::OperatorRequired => Style::default().fg(Color::Magenta),
        ResultStatus::Unresumable => Style::default().fg(Color::Red),
        ResultStatus::Enqueued => Style::default().fg(Color::DarkGray),
    }
}

fn format_age(age_secs: i64) -> String {
    if age_secs < 0 {
        return "0s".to_string();
    }
    if age_secs < 60 {
        return format!("{}s", age_secs);
    }
    if age_secs < 3600 {
        return format!("{}m", age_secs / 60);
    }
    format!("{}h", age_secs / 3600)
}

fn truncate(s: &str, max: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max {
        return s.to_string();
    }
    if max <= 3 {
        return s.chars().take(max).collect();
    }
    let take = max - 3;
    let mut truncated: String = s.chars().take(take).collect();
    truncated.push_str("...");
    truncated
}

fn estimate_wrapped_lines(lines: &[Line<'_>], content_width: usize) -> usize {
    if content_width == 0 {
        return lines.len();
    }

    lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(content_width))
        .sum()
}

fn build_execution_details(row: &ExecutionRowData, app: &App) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    push_section_title(&mut lines, "WAL Row");
    lines.push(Line::from(format!("Liq ID: {}", row.liq_id)));
    lines.push(Line::from(format!(
        "Status: {} | attempt: {} | errors: {}",
        status_label(row.status),
        row.attempt,
        row.error_count
    )));
    lines.push(Line::from(format!(
        "Created: {} | updated: {}",
        format_ts(row.created_at),
        format_ts(row.updated_at)
    )));

    if let Some(err) = row.last_error.as_deref() {
        lines.push(Line::from(format!("Last error: {}", err)));
    }
    if let Some(outcome) = latest_outcome_for(app, &row.liq_id)
        && let Some(reason) = outcome.finalizer_result.reason.as_deref()
    {
        lines.push(Line::from(format!("Finalizer reason: {}", reason)));
    }

    append_receipt_from_meta(&mut lines, &row.meta_json);

    lines
}

fn push_section_title(lines: &mut Vec<Line<'static>>, title: &str) {
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(Span::styled(
        title.to_string(),
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
}

fn append_receipt_from_meta(lines: &mut Vec<Line<'static>>, raw: &str) {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        push_section_title(lines, "Execution");
        lines.push(Line::from("<no receipt metadata in WAL row>"));
        return;
    }

    match serde_json::from_str::<LiqMetaWrapper>(raw) {
        Ok(wrapper) => {
            append_receipt_lines(lines, &wrapper.receipt);
            append_finalizer_decision(lines, wrapper.finalizer_decision.as_ref());
            append_multi_venue_summary(lines, wrapper.meta_v2.as_ref());
            append_meta_summary(lines, &wrapper.meta);
        }
        Err(wrapper_err) => match serde_json::from_str::<ExecutionReceipt>(raw) {
            Ok(receipt) => {
                append_receipt_lines(lines, &receipt);
                push_section_title(lines, "Metadata");
                lines.push(Line::from(Span::styled(
                    "Legacy WAL payload: receipt present, wrapper/finalizer decision missing".to_string(),
                    Style::default().fg(Color::Yellow),
                )));
            }
            Err(receipt_err) => {
                if let Ok(value) = serde_json::from_str::<Value>(raw) {
                    push_section_title(lines, "Metadata");
                    lines.push(Line::from(Span::styled(
                        format!(
                            "meta_json unexpected shape (wrapper_err={}, receipt_err={})",
                            wrapper_err, receipt_err
                        ),
                        Style::default().fg(Color::Yellow),
                    )));
                    lines.push(Line::from(format!(
                        "JSON type: {}",
                        match value {
                            Value::Null => "null",
                            Value::Bool(_) => "bool",
                            Value::Number(_) => "number",
                            Value::String(_) => "string",
                            Value::Array(_) => "array",
                            Value::Object(_) => "object",
                        }
                    )));
                    return;
                }

                push_section_title(lines, "Metadata");
                lines.push(Line::from(Span::styled(
                    format!(
                        "meta_json parse error (wrapper_err={}, receipt_err={})",
                        wrapper_err, receipt_err
                    ),
                    Style::default().fg(Color::Red),
                )));
                lines.push(Line::from(format!("Raw preview: {}", truncate(raw, 220))));
            }
        },
    }
}

/// Displays the identity that actually signs each persisted ICPSwap leg. This
/// is the account operators need when checking isolated balances or recovering
/// a specific liquidation.
fn append_multi_venue_summary(lines: &mut Vec<Line<'static>>, meta: Option<&FinalizerMetaV2>) {
    let Some(meta) = meta else {
        return;
    };
    let FinalizerMetaPayload::MultiVenueSwap(state) = &meta.payload;

    push_section_title(lines, "Multi-Venue Execution");
    lines.push(Line::from(format!(
        "Strategy: {} | outcome: {:?}",
        state.plan.strategy_id, state.outcome
    )));
    lines.push(Line::from(format!(
        "Allocation: {:?} | conservative edge: {:.2} bps",
        state.plan.allocation_reason, state.plan.combined_net_edge_bps
    )));

    let last = state.legs.len().saturating_sub(1);
    for (index, leg) in state.legs.iter().enumerate() {
        append_venue_leg_summary(lines, leg, index == last);
        if leg.venue_id == ICPSWAP_VENUE_ID {
            match icpswap_account_principal(&leg.execution) {
                Ok(principal) => lines.push(Line::from(format!("   Execution principal: {principal}"))),
                Err(error) => lines.push(Line::from(Span::styled(
                    format!("   Execution principal: <unavailable: {}>", truncate(&error, 140)),
                    Style::default().fg(Color::Yellow),
                ))),
            }
        }
    }
}

fn append_venue_leg_summary(lines: &mut Vec<Line<'static>>, leg: &VenueLegState, is_last: bool) {
    let branch = if is_last { "└─" } else { "├─" };
    let stage = super::dashboard::venue_leg_stage(leg);
    let style = super::dashboard::venue_leg_style(leg.status);
    lines.push(Line::from(vec![
        Span::styled(
            format!("{branch} {}", super::dashboard::venue_display_name(&leg.venue_id)),
            style.add_modifier(Modifier::BOLD),
        ),
        Span::raw(" · "),
        Span::styled(stage, style),
    ]));
    lines.push(Line::from(format!(
        "   Allocation: {}",
        leg.request.pay_amount.formatted()
    )));
    lines.push(Line::from(format!(
        "   Quote: expected {} | conservative {} | impact {:.2} bps",
        leg.quote.estimated_receive.formatted(),
        leg.quote.conservative_receive.formatted(),
        leg.quote.estimated_price_impact_bps
    )));
    lines.push(Line::from(format!("   Route: {}", leg.quote.route_id)));

    if let Some(result) = &leg.result {
        let mut received = leg.quote.estimated_receive.clone();
        received.value = result.receive_amount.clone();
        lines.push(Line::from(Span::styled(
            format!("   Realized: {} | status: {}", received.formatted(), result.status),
            Style::default().fg(Color::Green),
        )));
    }
    if let Some(error) = leg.last_error.as_deref() {
        lines.push(Line::from(Span::styled(
            format!("   Last error: {error}"),
            Style::default().fg(Color::Red),
        )));
    }
}

#[derive(Deserialize)]
struct IcpswapIdentityView {
    identity: IcpswapExecutionIdentity,
}

/// Decodes only the stable identity portion of the venue state. Ignoring the
/// remaining fields keeps the TUI useful while execution-state details evolve.
fn icpswap_account_principal(execution: &VenueExecutionState) -> Result<String, String> {
    execution
        .decode::<IcpswapIdentityView>(ICPSWAP_VENUE_ID)?
        .map(|state| state.identity.principal.to_text())
        .ok_or_else(|| "execution state is tagged for a different venue".to_string())
}

fn append_receipt_lines(lines: &mut Vec<Line<'static>>, receipt: &ExecutionReceipt) {
    push_section_title(lines, "Request");
    let req = &receipt.request;
    lines.push(Line::from(format!("Borrower: {}", req.liquidation.borrower.to_text())));
    lines.push(Line::from(format!(
        "Debt pool: {}",
        req.liquidation.debt_pool_id.to_text()
    )));
    lines.push(Line::from(format!(
        "Collateral pool: {}",
        req.liquidation.collateral_pool_id.to_text()
    )));
    lines.push(Line::from(format!(
        "Debt amount request: {}",
        ChainTokenAmount::from_raw(req.debt_asset.clone(), req.liquidation.debt_amount.clone()).formatted()
    )));
    lines.push(Line::from(format!("Bad debt mode: {}", req.liquidation.buy_bad_debt)));
    lines.push(Line::from(format!(
        "Swap requested: {}",
        if req.swap_args.is_some() { "yes" } else { "no" }
    )));
    if let Some(swap) = req.swap_args.as_ref() {
        lines.push(Line::from(format!(
            "Swap route: {}:{} -> {}:{}",
            swap.pay_asset.chain, swap.pay_asset.symbol, swap.receive_asset.chain, swap.receive_asset.symbol
        )));
    }
    lines.push(Line::from(format!(
        "Debt asset: {} ({})",
        req.debt_asset.symbol(),
        req.debt_asset.chain()
    )));
    lines.push(Line::from(format!(
        "Collateral asset: {} ({})",
        req.collateral_asset.symbol(),
        req.collateral_asset.chain()
    )));
    lines.push(Line::from(format!(
        "Expected profit: {}",
        format_profit(
            req.expected_profit,
            req.debt_asset.decimals(),
            req.debt_asset.symbol().as_str()
        )
    )));
    lines.push(Line::from(format!(
        "Debt approval needed: {}",
        req.debt_approval_needed
    )));

    push_section_title(lines, "Execution");
    lines.push(Line::from(format!(
        "Executor status: {}",
        format_execution_status(&receipt.status)
    )));
    lines.push(Line::from(format!("Change received: {}", receipt.change_received)));

    if let Some(liq) = &receipt.liquidation_result {
        push_section_title(lines, "Liquidation Result");
        lines.push(Line::from(format!("Liquidation ID: {}", liq.id)));
        lines.push(Line::from(format!("Timestamp: {}", format_ts(liq.timestamp as i64))));
        lines.push(Line::from(format!(
            "Liquidation status: {}",
            format_liquidation_status(&liq.status)
        )));
        lines.push(Line::from(format!(
            "Debt repaid: {}",
            ChainTokenAmount::from_raw(req.debt_asset.clone(), liq.amounts.debt_repaid.clone()).formatted()
        )));
        lines.push(Line::from(format!(
            "Collateral received: {}",
            ChainTokenAmount::from_raw(req.collateral_asset.clone(), liq.amounts.collateral_received.clone())
                .formatted()
        )));
        lines.push(Line::from(format!(
            "Change transfer: {}",
            format_tx_status(&liq.change_tx)
        )));
        lines.push(Line::from(format!(
            "Collateral transfer: {}",
            format_tx_status(&liq.collateral_tx)
        )));
    } else {
        push_section_title(lines, "Liquidation Result");
        lines.push(Line::from("<missing liquidation_result>"));
    }
}

fn append_finalizer_decision(
    lines: &mut Vec<Line<'static>>,
    decision: Option<&crate::persistance::FinalizerDecisionSnapshot>,
) {
    push_section_title(lines, "Hybrid Decision Snapshot");
    let Some(decision) = decision else {
        lines.push(Line::from("<not persisted yet>"));
        return;
    };

    lines.push(Line::from(format!(
        "Mode: {} | chosen: {}",
        decision.mode, decision.chosen
    )));
    lines.push(Line::from(format!("Reason: {}", decision.reason)));
    lines.push(Line::from(format!(
        "Min required edge: {:.2} bps",
        decision.min_required_bps
    )));
    lines.push(Line::from(format!(
        "DEX preview: gross={:?} net={:?}",
        decision.dex_preview_gross_bps, decision.dex_preview_net_bps
    )));
    lines.push(Line::from(format!(
        "CEX preview: gross={:?} net={:?}",
        decision.cex_preview_gross_bps, decision.cex_preview_net_bps
    )));
    lines.push(Line::from(format!("Decision time: {}", format_ts(decision.ts))));
}

fn append_meta_summary(lines: &mut Vec<Line<'static>>, meta: &[u8]) {
    push_section_title(lines, "Internal State");
    if meta.is_empty() {
        lines.push(Line::from("State bytes: <empty>"));
        return;
    }

    lines.push(Line::from(format!("State bytes length: {}", meta.len())));
    if let Ok(text) = std::str::from_utf8(meta) {
        lines.push(Line::from("Encoding: utf8"));
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            let mut emitted = false;
            if let Some(step) = value.get("step").and_then(Value::as_str) {
                lines.push(Line::from(format!("CEX step: {}", step)));
                emitted = true;
            }
            if let Some(last_error) = value.get("last_error").and_then(Value::as_str)
                && !last_error.trim().is_empty()
            {
                lines.push(Line::from(format!("CEX last error: {}", last_error)));
                emitted = true;
            }
            if let Some(withdraw_id) = value
                .get("withdraw_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                lines.push(Line::from(format!("CEX withdraw id: {}", withdraw_id)));
                emitted = true;
            }
            if let Some(deposit_bridge_id) = value
                .get("deposit_bridge_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                lines.push(Line::from(format!("Bridge deposit txid: {}", deposit_bridge_id)));
                emitted = true;
            }
            if let Some(withdraw_bridge_id) = value
                .get("withdraw_bridge_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                lines.push(Line::from(format!("Bridge withdraw txid: {}", withdraw_bridge_id)));
                emitted = true;
            }
            if !emitted {
                lines.push(Line::from(format!("State preview: {}", truncate(text, 180))));
            }
            return;
        }

        lines.push(Line::from(format!("State preview: {}", truncate(text, 180))));
        return;
    }

    lines.push(Line::from("Encoding: binary"));
}

fn format_execution_status(status: &ExecutionStatus) -> String {
    status.description()
}

fn format_liquidation_status(status: &LiquidationStatus) -> String {
    match status {
        LiquidationStatus::Success => "success".to_string(),
        LiquidationStatus::FailedLiquidation(err) => format!("failed liquidation ({err})"),
        LiquidationStatus::CollateralTransferFailed(err) => format!("collateral transfer failed ({err})"),
        LiquidationStatus::ChangeTransferFailed(err) => format!("change transfer failed ({err})"),
        LiquidationStatus::InflowProcessed => "inflow processed".to_string(),
        LiquidationStatus::CoreExecuted => "core executed".to_string(),
    }
}

fn format_transfer_status(status: &TransferStatus) -> String {
    match status {
        TransferStatus::Pending => "pending".to_string(),
        TransferStatus::Success => "success".to_string(),
        TransferStatus::Failed(err) => format!("failed ({err})"),
    }
}

fn format_tx_status(tx: &TxStatus) -> String {
    let status = format_transfer_status(&tx.status);
    let tx_id = tx.tx_id.as_deref().unwrap_or("-");
    format!("{status} | tx_id={tx_id}")
}

fn format_ts(secs: i64) -> String {
    if let Some(dt) = Local.timestamp_opt(secs, 0).single() {
        return dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }
    secs.to_string()
}

fn profit_cell_for_row(row: &ExecutionRowData, app: &App) -> Cell<'static> {
    if let Some(snapshot) = profit_snapshot_from_meta(&row.meta_json)
        && let Some((formatted, style)) = profit_display_from_snapshot(&snapshot)
    {
        return Cell::new(formatted).style(style);
    }

    if let Some(outcome) = latest_outcome_for(app, &row.liq_id) {
        let delta = outcome.realized_profit - outcome.expected_profit;
        let style = profit_style_from_delta(delta);
        return Cell::new(outcome.formatted_realized_profit()).style(style);
    }

    if let Some((expected, decimals, symbol)) = expected_profit_from_meta(&row.meta_json) {
        let style = profit_style_from_delta(expected);
        let formatted = format_profit(expected, decimals, &symbol);
        return Cell::new(formatted).style(style);
    }

    Cell::new("-").style(Style::default().fg(Color::DarkGray))
}

fn profit_snapshot_from_meta(raw: &str) -> Option<WalProfitSnapshot> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return None;
    }
    serde_json::from_str::<LiqMetaWrapper>(raw)
        .ok()
        .and_then(|wrapper| wrapper.profit_snapshot)
}

fn profit_display_from_snapshot(snapshot: &WalProfitSnapshot) -> Option<(String, Style)> {
    let expected = snapshot.expected_profit_raw.parse::<i128>().ok()?;
    let symbol = snapshot.debt_symbol.as_str();
    let decimals = snapshot.debt_decimals;

    if let Some(realized_raw) = snapshot.realized_profit_raw.as_deref()
        && let Ok(realized) = realized_raw.parse::<i128>()
    {
        let style = profit_style_from_delta(realized - expected);
        return Some((format_profit(realized, decimals, symbol), style));
    }

    let style = profit_style_from_delta(expected);
    Some((format_profit(expected, decimals, symbol), style))
}

fn latest_outcome_for<'a>(app: &'a App, liq_id: &str) -> Option<&'a LiquidationOutcome> {
    for r in app.recent_outcomes.iter().rev() {
        if let Ok(outcome_id) = liq_id_from_receipt(&r.outcome.execution_receipt) {
            if outcome_id == liq_id {
                return Some(&r.outcome);
            }
        }
    }
    None
}

fn expected_profit_from_meta(raw: &str) -> Option<(i128, u8, String)> {
    let receipt = extract_receipt(raw)?;
    let expected = receipt.request.expected_profit;
    let decimals = receipt.request.debt_asset.decimals();
    let symbol = receipt.request.debt_asset.symbol().to_string();
    Some((expected, decimals, symbol))
}

fn extract_receipt(raw: &str) -> Option<ExecutionReceipt> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return None;
    }
    if let Ok(wrapper) = serde_json::from_str::<LiqMetaWrapper>(raw) {
        return Some(wrapper.receipt);
    }
    if let Ok(receipt) = serde_json::from_str::<ExecutionReceipt>(raw) {
        return Some(receipt);
    }
    None
}

fn profit_style_from_delta(delta: i128) -> Style {
    match delta.cmp(&0) {
        std::cmp::Ordering::Greater => Style::default().fg(Color::Green),
        std::cmp::Ordering::Less => Style::default().fg(Color::Red),
        std::cmp::Ordering::Equal => Style::default().fg(Color::DarkGray),
    }
}

fn format_profit(amount: i128, decimals: u8, symbol: &str) -> String {
    let scaled = (amount as f64) / 10f64.powi(decimals as i32);
    format!("{scaled} {symbol}")
}

#[cfg(test)]
mod tests {
    use super::{
        WalProfitSnapshot, append_meta_summary, append_venue_leg_summary, icpswap_account_principal,
        profit_display_from_snapshot, status_label, truncate,
    };
    use crate::{
        persistance::{ResultStatus, VenueExecutionState, VenueLegQuote, VenueLegState, VenueLegStatus},
        swappers::icpswap::{
            identity::IcpswapExecutionIdentity,
            transfer_state::{IcpswapFundingState, IcpswapLedgerTransferState, IcpswapSettlementState},
            types::{IcpswapExecutionPlan, IcpswapExecutionState},
        },
        swappers::model::SwapRequest,
    };
    use candid::{Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use ratatui::style::Color;
    use ratatui::text::Line;

    fn lines_as_text(lines: &[Line<'_>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| line.spans.iter().map(|span| span.content.as_ref()).collect::<String>())
            .collect()
    }

    #[test]
    fn unresumable_status_is_visible_in_executions() {
        assert_eq!(status_label(ResultStatus::Unresumable), "unresumable");
    }

    #[test]
    fn execution_details_show_venue_branch_stage_quote_route_and_error() {
        let pay_token = ChainToken::EvmNative {
            chain: "ICP".to_string(),
            symbol: "ICP".to_string(),
            decimals: 2,
            fee: Nat::from(1u8),
        };
        let receive_token = ChainToken::EvmNative {
            chain: "ICP".to_string(),
            symbol: "ckUSDC".to_string(),
            decimals: 2,
            fee: Nat::from(1u8),
        };
        let pay_amount = ChainTokenAmount::from_raw(pay_token.clone(), Nat::from(720u64));
        let receive_amount = ChainTokenAmount::from_raw(receive_token.clone(), Nat::from(810u64));
        let leg = VenueLegState {
            leg_id: "mexc-0".to_string(),
            venue_id: "mexc".to_string(),
            request: SwapRequest {
                pay_asset: pay_token.asset_id(),
                pay_amount: pay_amount.clone(),
                receive_asset: receive_token.asset_id(),
                receive_address: None,
                max_slippage_bps: Some(100),
                venue_hint: Some("mexc".to_string()),
            },
            quote: VenueLegQuote {
                pay_amount,
                estimated_receive: receive_amount.clone(),
                conservative_receive: receive_amount,
                estimated_price_impact_bps: 12.5,
                route_id: "ICP_USDC".to_string(),
            },
            execution: VenueExecutionState {
                venue: "mexc".to_string(),
                state: serde_json::json!({ "cex": { "step": "WithdrawPending" } }),
            },
            status: VenueLegStatus::Running,
            result: None,
            last_error: Some("waiting for bridge".to_string()),
        };

        let mut lines = Vec::new();
        append_venue_leg_summary(&mut lines, &leg, true);
        let text = lines_as_text(&lines);

        assert_eq!(text[0], "└─ MEXC · withdraw pending");
        assert!(text.iter().any(|line| line == "   Allocation: ICP: 7.20"));
        assert!(text.iter().any(|line| line.contains("impact 12.50 bps")));
        assert!(text.iter().any(|line| line == "   Route: ICP_USDC"));
        assert!(text.iter().any(|line| line == "   Last error: waiting for bridge"));
    }

    #[test]
    fn decoded_icpswap_view_shows_the_execution_account_principal() {
        let input = ChainToken::Icp {
            ledger: Principal::from_slice(&[1]),
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10u64),
        };
        let output = ChainToken::Icp {
            ledger: Principal::from_slice(&[2]),
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(5u64),
        };
        let plan = IcpswapExecutionPlan::new(
            Principal::from_slice(&[9]),
            Principal::from_slice(&[1]),
            Principal::from_slice(&[2]),
            Nat::from(3_000u64),
            ChainTokenAmount::from_raw(input.clone(), Nat::from(100_000u64)),
            ChainTokenAmount::from_raw(input, Nat::from(10u64)),
            ChainTokenAmount::from_raw(output.clone(), Nat::from(120_000u64)),
            ChainTokenAmount::from_raw(output, Nat::from(5u64)),
            100,
        )
        .expect("ICPSwap plan");
        let (identity, _) = IcpswapExecutionIdentity::derive(
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            "1536",
        )
        .expect("derive execution identity");
        let principal = identity.principal;
        let child = Account {
            owner: principal,
            subaccount: None,
        };
        let state = IcpswapExecutionState::prepare(
            "icpswap-tui-test",
            plan.clone(),
            identity,
            IcpswapFundingState::new(
                Account {
                    owner: Principal::from_slice(&[4]),
                    subaccount: None,
                },
                child,
                plan.input_ledger_fee.clone(),
            ),
            IcpswapSettlementState {
                kind: None,
                destination: Account {
                    owner: Principal::from_slice(&[5]),
                    subaccount: None,
                },
                fee: plan.output_ledger_fee.clone(),
                transfer: IcpswapLedgerTransferState::default(),
                interrupted_transfer: None,
                interrupted_observed_debit: None,
                recovery_credit: None,
                residual_dust: None,
            },
        )
        .expect("prepare persisted execution state");
        let execution = VenueExecutionState::new("icpswap", &state).expect("encode execution state");

        assert_eq!(
            icpswap_account_principal(&execution).expect("decode principal"),
            principal.to_text()
        );
    }

    #[test]
    fn truncate_no_change_when_within_limit() {
        assert_eq!(truncate("abcdef", 6), "abcdef");
        assert_eq!(truncate("abcdef", 7), "abcdef");
    }

    #[test]
    fn truncate_max_le_3_takes_chars_without_panic() {
        assert_eq!(truncate("é漢🧪abc", 0), "");
        assert_eq!(truncate("é漢🧪abc", 1), "é");
        assert_eq!(truncate("é漢🧪abc", 2), "é漢");
        assert_eq!(truncate("é漢🧪abc", 3), "é漢🧪");
    }

    #[test]
    fn truncate_max_gt_3_appends_three_dots() {
        assert_eq!(truncate("abcdef", 4), "a...");
        assert_eq!(truncate("abcdef", 5), "ab...");
    }

    #[test]
    fn truncate_handles_multibyte_utf8_without_panic() {
        assert_eq!(truncate("é漢🧪abc", 4), "é...");
        assert_eq!(truncate("é漢🧪abc", 5), "é漢...");
    }

    #[test]
    fn profit_snapshot_prefers_realized_and_colors_by_delta() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "1000".to_string(),
            realized_profit_raw: Some("1200".to_string()),
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        let (text, style) = profit_display_from_snapshot(&snapshot).expect("display should parse");
        assert_eq!(text, "12 ckUSDT");
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn profit_snapshot_falls_back_to_expected_when_realized_missing() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "-250".to_string(),
            realized_profit_raw: None,
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        let (text, style) = profit_display_from_snapshot(&snapshot).expect("display should parse");
        assert_eq!(text, "-2.5 ckUSDT");
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn malformed_profit_snapshot_returns_none() {
        let snapshot = WalProfitSnapshot {
            expected_profit_raw: "not-a-number".to_string(),
            realized_profit_raw: Some("1200".to_string()),
            debt_symbol: "ckUSDT".to_string(),
            debt_decimals: 2,
            updated_at: 0,
        };

        assert!(profit_display_from_snapshot(&snapshot).is_none());
    }

    #[test]
    fn append_meta_summary_shows_both_bridge_txids_when_present() {
        let mut lines = Vec::new();
        let meta = br#"{"deposit_bridge_id":"0xabc123","withdraw_bridge_id":"0xdef456"}"#;
        append_meta_summary(&mut lines, meta);

        let text = lines_as_text(&lines);
        assert!(text.iter().any(|line| line == "Bridge deposit txid: 0xabc123"));
        assert!(text.iter().any(|line| line == "Bridge withdraw txid: 0xdef456"));
        assert!(!text.iter().any(|line| line.starts_with("State preview: ")));
    }

    #[test]
    fn append_meta_summary_shows_only_deposit_bridge_txid_when_only_deposit_is_present() {
        let mut lines = Vec::new();
        let meta = br#"{"deposit_bridge_id":"0xabc123"}"#;
        append_meta_summary(&mut lines, meta);

        let text = lines_as_text(&lines);
        assert!(text.iter().any(|line| line == "Bridge deposit txid: 0xabc123"));
        assert!(!text.iter().any(|line| line.starts_with("Bridge withdraw txid: ")));
        assert!(!text.iter().any(|line| line.starts_with("State preview: ")));
    }

    #[test]
    fn append_meta_summary_shows_only_withdraw_bridge_txid_when_only_withdraw_is_present() {
        let mut lines = Vec::new();
        let meta = br#"{"withdraw_bridge_id":"ic-withdraw:12:34"}"#;
        append_meta_summary(&mut lines, meta);

        let text = lines_as_text(&lines);
        assert!(
            text.iter()
                .any(|line| line == "Bridge withdraw txid: ic-withdraw:12:34")
        );
        assert!(!text.iter().any(|line| line.starts_with("Bridge deposit txid: ")));
        assert!(!text.iter().any(|line| line.starts_with("State preview: ")));
    }

    #[test]
    fn append_meta_summary_without_bridge_txids_preserves_state_preview_fallback() {
        let mut lines = Vec::new();
        let meta = br#"{"unrelated":"value"}"#;
        append_meta_summary(&mut lines, meta);

        let text = lines_as_text(&lines);
        assert!(text.iter().any(|line| line.starts_with("State preview: ")));
        assert!(!text.iter().any(|line| line.starts_with("Bridge deposit txid: ")));
        assert!(!text.iter().any(|line| line.starts_with("Bridge withdraw txid: ")));
    }
}
