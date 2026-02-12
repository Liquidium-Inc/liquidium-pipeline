use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
#[cfg(target_os = "linux")]
use chrono::{SecondsFormat, TimeZone};
use tokio::sync::mpsc;

use super::events::UiEvent;

#[cfg(target_os = "linux")]
use serde_json::Value;
#[cfg(target_os = "linux")]
use std::process::Stdio;

pub(super) const LOG_BUFFER_MAX: usize = 5000;
const INITIAL_HISTORY_WINDOW_SECS: i64 = 300;
const INITIAL_HISTORY_MAX_LINES: usize = 400;
const OLDER_PAGE_LINES: usize = 300;
const FILE_HISTORY_CHUNK_BYTES: u64 = 256 * 1024;
const FILE_FOLLOW_POLL_MS: u64 = 400;
const FILE_ERROR_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
enum LogSource {
    #[cfg(target_os = "linux")]
    Journalctl {
        unit_name: String,
    },
    FileTail(PathBuf),
    Notice(String),
}

#[derive(Debug, Clone, Copy)]
enum LogControlMsg {
    LoadOlder,
}

#[derive(Clone)]
pub(super) struct LogControlHandle {
    tx: mpsc::Sender<LogControlMsg>,
}

impl LogControlHandle {
    pub(super) fn request_older(&self) {
        let _ = self.tx.try_send(LogControlMsg::LoadOlder);
    }
}

pub(super) fn start_log_source(
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    unit_name: String,
    log_file: Option<PathBuf>,
) -> LogControlHandle {
    let (control_tx, control_rx) = mpsc::channel::<LogControlMsg>(1);

    match choose_log_source(unit_name, log_file) {
        #[cfg(target_os = "linux")]
        LogSource::Journalctl { unit_name } => spawn_journalctl(ui_tx, unit_name, control_rx),
        LogSource::FileTail(path) => spawn_file_tail(ui_tx, path, control_rx),
        LogSource::Notice(msg) => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![msg]));
        }
    }

    LogControlHandle { tx: control_tx }
}

fn choose_log_source(unit_name: String, log_file: Option<PathBuf>) -> LogSource {
    if let Some(path) = log_file {
        return LogSource::FileTail(path);
    }

    #[cfg(target_os = "linux")]
    {
        LogSource::Journalctl { unit_name }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = unit_name;
        LogSource::Notice("logs unavailable (set --log-file)".to_string())
    }
}

pub(super) fn describe_log_source(unit_name: &str, log_file: Option<&Path>) -> String {
    match choose_log_source(unit_name.to_string(), log_file.map(Path::to_path_buf)) {
        #[cfg(target_os = "linux")]
        LogSource::Journalctl { unit_name } => format!("Log source unit: {}", unit_name),
        LogSource::FileTail(path) => format!("Log source file: {}", path.display()),
        LogSource::Notice(msg) => format!("Log source: {}", msg),
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct JournalRecord {
    line: String,
    cursor: Option<String>,
}

#[cfg(target_os = "linux")]
fn journal_initial_args(unit_name: &str) -> Vec<String> {
    vec![
        "-u".to_string(),
        unit_name.to_string(),
        "--since".to_string(),
        "5 minutes ago".to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--no-pager".to_string(),
    ]
}

#[cfg(target_os = "linux")]
fn journal_follow_args(unit_name: &str) -> Vec<String> {
    vec![
        "-f".to_string(),
        "-u".to_string(),
        unit_name.to_string(),
        "-n".to_string(),
        "0".to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--no-pager".to_string(),
    ]
}

#[cfg(target_os = "linux")]
fn journal_older_args(unit_name: &str, before_cursor: &str) -> Vec<String> {
    vec![
        "-u".to_string(),
        unit_name.to_string(),
        "--before-cursor".to_string(),
        before_cursor.to_string(),
        "-n".to_string(),
        OLDER_PAGE_LINES.to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--no-pager".to_string(),
    ]
}

#[cfg(target_os = "linux")]
fn journal_latest_cursor_args(unit_name: &str) -> Vec<String> {
    vec![
        "-u".to_string(),
        unit_name.to_string(),
        "-n".to_string(),
        "1".to_string(),
        "-o".to_string(),
        "json".to_string(),
        "--no-pager".to_string(),
    ]
}

#[cfg(target_os = "linux")]
fn spawn_journalctl(
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    unit_name: String,
    mut control_rx: mpsc::Receiver<LogControlMsg>,
) {
    tokio::spawn(async move {
        let mut oldest_cursor: Option<String> = None;
        let mut has_more_older = false;
        let mut no_more_notice_sent = false;

        if let Some(records) = run_journalctl_batch(&ui_tx, &journal_initial_args(&unit_name), "history").await {
            if !records.is_empty() {
                oldest_cursor = records.first().and_then(|r| r.cursor.clone());
                let lines: Vec<String> = records.into_iter().map(|r| r.line).collect();
                let _ = ui_tx.send(UiEvent::AppendLogLines(lines));
            }
        }

        if oldest_cursor.is_none()
            && let Some(records) = run_journalctl_batch(&ui_tx, &journal_latest_cursor_args(&unit_name), "cursor").await
        {
            oldest_cursor = records.first().and_then(|r| r.cursor.clone());
        }
        has_more_older = oldest_cursor.is_some();

        {
            let follow_ui_tx = ui_tx.clone();
            let follow_args = journal_follow_args(&unit_name);
            tokio::spawn(async move {
                run_journalctl_follow(follow_ui_tx, follow_args).await;
            });
        }

        while let Some(msg) = control_rx.recv().await {
            match msg {
                LogControlMsg::LoadOlder => {
                    let Some(cursor) = oldest_cursor.clone() else {
                        has_more_older = false;
                        continue;
                    };
                    if !has_more_older {
                        if !no_more_notice_sent {
                            no_more_notice_sent = true;
                            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![
                                "logs: reached oldest available history".to_string(),
                            ]));
                        }
                        continue;
                    }

                    match run_journalctl_batch(&ui_tx, &journal_older_args(&unit_name, &cursor), "older").await {
                        Some(records) if !records.is_empty() => {
                            oldest_cursor = records.first().and_then(|r| r.cursor.clone()).or(oldest_cursor);
                            has_more_older = oldest_cursor.is_some();
                            no_more_notice_sent = false;
                            let lines: Vec<String> = records.into_iter().map(|r| r.line).collect();
                            let _ = ui_tx.send(UiEvent::PrependLogLines(lines));
                        }
                        Some(_) => {
                            has_more_older = false;
                            if !no_more_notice_sent {
                                no_more_notice_sent = true;
                                let _ = ui_tx.send(UiEvent::AppendLogLines(vec![
                                    "logs: reached oldest available history".to_string(),
                                ]));
                            }
                        }
                        None => {}
                    }
                }
            }
        }
    });
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn spawn_journalctl(
    _ui_tx: mpsc::UnboundedSender<UiEvent>,
    _unit_name: String,
    _control_rx: mpsc::Receiver<LogControlMsg>,
) {
}

#[cfg(target_os = "linux")]
async fn run_journalctl_batch(
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    args: &[String],
    mode: &'static str,
) -> Option<Vec<JournalRecord>> {
    use tokio::process::Command;

    let output = match Command::new("journalctl").args(args).output().await {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![
                "logs unavailable: journalctl not found".to_string(),
            ]));
            return None;
        }
        Err(err) => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
                "logs unavailable: failed to start journalctl ({err})",
            )]));
            return None;
        }
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines().filter(|line| !line.trim().is_empty()) {
        let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
            "journalctl {mode} stderr: {line}"
        )]));
    }
    if !output.status.success() {
        let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
            "journalctl {mode} exited with status {}",
            output.status
        )]));
    }

    let mut records = Vec::new();
    for raw in String::from_utf8_lossy(&output.stdout).lines() {
        if raw.trim().is_empty() {
            continue;
        }
        records.push(parse_journal_record(raw).unwrap_or_else(|_| JournalRecord {
            line: raw.to_string(),
            cursor: None,
        }));
    }
    Some(records)
}

#[cfg(target_os = "linux")]
async fn run_journalctl_follow(ui_tx: mpsc::UnboundedSender<UiEvent>, args: Vec<String>) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    let mut child = match Command::new("journalctl")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![
                "logs unavailable: journalctl not found".to_string(),
            ]));
            return;
        }
        Err(err) => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
                "logs unavailable: failed to start journalctl ({err})",
            )]));
            return;
        }
    };

    let stdout_task = child.stdout.take().map(|stdout| {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(raw)) = lines.next_line().await {
                if raw.trim().is_empty() {
                    continue;
                }
                let record = parse_journal_record(&raw).unwrap_or_else(|_| JournalRecord {
                    line: raw,
                    cursor: None,
                });
                let _ = ui_tx.send(UiEvent::AppendLogLines(vec![record.line]));
            }
        })
    });

    let stderr_task = child.stderr.take().map(|stderr| {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
                    "journalctl follow stderr: {line}"
                )]));
            }
        })
    });

    match child.wait().await {
        Ok(status) if !status.success() => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
                "journalctl follow exited with status {status}",
            )]));
        }
        Ok(_) => {}
        Err(err) => {
            let _ = ui_tx.send(UiEvent::AppendLogLines(vec![format!(
                "journalctl follow wait failed: {err}",
            )]));
        }
    }

    if let Some(task) = stdout_task {
        let _ = task.await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
}

#[cfg(target_os = "linux")]
fn parse_journal_record(raw: &str) -> Result<JournalRecord, String> {
    let value: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let cursor = value.get("__CURSOR").and_then(Value::as_str).map(ToOwned::to_owned);

    let message = value.get("MESSAGE").map(journal_value_to_text).unwrap_or_default();
    let timestamp = value
        .get("__REALTIME_TIMESTAMP")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(format_micros_timestamp);
    let level = value
        .get("PRIORITY")
        .and_then(Value::as_str)
        .and_then(|p| p.parse::<u8>().ok())
        .map(priority_to_level)
        .unwrap_or("INFO");

    let line = if let Some(ts) = timestamp {
        format!("{ts} {level:<5} {message}")
    } else {
        format!("{level:<5} {message}")
    };
    Ok(JournalRecord { line, cursor })
}

#[cfg(target_os = "linux")]
fn journal_value_to_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(values) => decode_journal_byte_array(values)
            .unwrap_or_else(|| values.iter().map(journal_value_to_text).collect::<Vec<_>>().join(" ")),
        Value::Object(_) => value.to_string(),
    }
}

#[cfg(target_os = "linux")]
fn decode_journal_byte_array(values: &[Value]) -> Option<String> {
    let mut bytes = Vec::with_capacity(values.len());
    for value in values {
        let n = value.as_u64()?;
        if n > u8::MAX as u64 {
            return None;
        }
        bytes.push(n as u8);
    }

    // journald JSON arrays for binary fields may be NUL-terminated.
    while bytes.last().copied() == Some(0) {
        bytes.pop();
    }

    Some(String::from_utf8_lossy(&bytes).to_string())
}

#[cfg(target_os = "linux")]
fn priority_to_level(priority: u8) -> &'static str {
    match priority {
        0..=3 => "ERROR",
        4 => "WARN",
        5 | 6 => "INFO",
        _ => "DEBUG",
    }
}

#[cfg(target_os = "linux")]
fn format_micros_timestamp(micros: i64) -> Option<String> {
    if micros < 0 {
        return None;
    }
    let secs = micros / 1_000_000;
    let nanos = (micros % 1_000_000) as u32 * 1_000;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[derive(Debug, Clone)]
struct PositionedLine {
    start: u64,
    text: String,
}

#[derive(Debug, Clone, Default)]
struct FileTailState {
    follow_offset: u64,
    oldest_offset: u64,
    has_more_older: bool,
    no_more_notice_sent: bool,
}

fn spawn_file_tail(
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    path: PathBuf,
    mut control_rx: mpsc::Receiver<LogControlMsg>,
) {
    tokio::task::spawn_blocking(move || {
        let mut last_error_message: Option<String> = None;
        let mut last_error_sent_at: Option<Instant> = None;
        let mut state = match initialize_file_state(&path) {
            Ok((state, lines)) => {
                if !lines.is_empty() {
                    let _ = ui_tx.send(UiEvent::AppendLogLines(lines));
                }
                state
            }
            Err(err) => {
                emit_throttled_file_error(
                    &ui_tx,
                    &mut last_error_message,
                    &mut last_error_sent_at,
                    format!("log tail error ({}): {}", path.display(), err),
                );
                FileTailState::default()
            }
        };

        loop {
            while let Ok(msg) = control_rx.try_recv() {
                match msg {
                    LogControlMsg::LoadOlder => match load_older_file_lines(&path, &mut state) {
                        Ok(lines) if !lines.is_empty() => {
                            let _ = ui_tx.send(UiEvent::PrependLogLines(lines));
                        }
                        Ok(_) => {
                            if !state.no_more_notice_sent {
                                state.no_more_notice_sent = true;
                                let _ = ui_tx.send(UiEvent::AppendLogLines(vec![
                                    "logs: reached oldest available history".to_string(),
                                ]));
                            }
                        }
                        Err(err) => {
                            emit_throttled_file_error(
                                &ui_tx,
                                &mut last_error_message,
                                &mut last_error_sent_at,
                                format!("log backfill error ({}): {}", path.display(), err),
                            );
                        }
                    },
                }
            }

            match read_follow_file_lines(&path, &mut state) {
                Ok(lines) if !lines.is_empty() => {
                    let _ = ui_tx.send(UiEvent::AppendLogLines(lines));
                }
                Ok(_) => {}
                Err(err) => {
                    emit_throttled_file_error(
                        &ui_tx,
                        &mut last_error_message,
                        &mut last_error_sent_at,
                        format!("log tail error ({}): {}", path.display(), err),
                    );
                }
            }

            thread::sleep(Duration::from_millis(FILE_FOLLOW_POLL_MS));
        }
    });
}

fn emit_throttled_file_error(
    ui_tx: &mpsc::UnboundedSender<UiEvent>,
    last_error_message: &mut Option<String>,
    last_error_sent_at: &mut Option<Instant>,
    message: String,
) {
    let now = Instant::now();
    let same_error = last_error_message.as_deref().is_some_and(|m| m == message);
    let within_backoff = last_error_sent_at
        .as_ref()
        .is_some_and(|sent| now.duration_since(*sent) < FILE_ERROR_BACKOFF);
    if same_error && within_backoff {
        return;
    }

    *last_error_message = Some(message.clone());
    *last_error_sent_at = Some(now);
    let _ = ui_tx.send(UiEvent::AppendLogLines(vec![message]));
}

fn initialize_file_state(path: &Path) -> std::io::Result<(FileTailState, Vec<String>)> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(FILE_HISTORY_CHUNK_BYTES);
    let positioned = read_positioned_lines(path, start, len)?;
    let initial = select_initial_file_history(positioned);

    let oldest_offset = initial.first().map(|line| line.start).unwrap_or(len);
    let has_more_older = oldest_offset > 0 && len > 0;
    let lines = initial.into_iter().map(|line| line.text).collect();

    Ok((
        FileTailState {
            follow_offset: len,
            oldest_offset,
            has_more_older,
            no_more_notice_sent: false,
        },
        lines,
    ))
}

fn select_initial_file_history(mut lines: Vec<PositionedLine>) -> Vec<PositionedLine> {
    if lines.is_empty() {
        return lines;
    }

    let cutoff = Utc::now() - chrono::Duration::seconds(INITIAL_HISTORY_WINDOW_SECS);
    let has_parseable = lines.iter().any(|line| parse_log_timestamp(&line.text).is_some());
    if has_parseable {
        if let Some(first_recent) = lines
            .iter()
            .position(|line| parse_log_timestamp(&line.text).is_some_and(|ts| ts >= cutoff))
        {
            lines = lines.split_off(first_recent);
            trim_positioned_lines_to_latest(&mut lines, INITIAL_HISTORY_MAX_LINES);
            return lines;
        }
        return Vec::new();
    }

    trim_positioned_lines_to_latest(&mut lines, INITIAL_HISTORY_MAX_LINES);
    lines
}

fn read_follow_file_lines(path: &Path, state: &mut FileTailState) -> std::io::Result<Vec<String>> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();

    if len < state.follow_offset {
        state.follow_offset = 0;
        state.oldest_offset = state.oldest_offset.min(len);
        state.has_more_older = state.oldest_offset > 0;
    }
    if len == state.follow_offset {
        return Ok(Vec::new());
    }

    let positioned = read_positioned_lines(path, state.follow_offset, len)?;
    state.follow_offset = len;
    if state.oldest_offset > len {
        state.oldest_offset = len;
        state.has_more_older = state.oldest_offset > 0;
    }
    Ok(positioned.into_iter().map(|line| line.text).collect())
}

fn load_older_file_lines(path: &Path, state: &mut FileTailState) -> std::io::Result<Vec<String>> {
    if !state.has_more_older || state.oldest_offset == 0 {
        state.has_more_older = false;
        return Ok(Vec::new());
    }

    let start = state.oldest_offset.saturating_sub(FILE_HISTORY_CHUNK_BYTES);
    let mut positioned = read_positioned_lines(path, start, state.oldest_offset)?;
    if positioned.is_empty() {
        state.has_more_older = false;
        return Ok(Vec::new());
    }

    trim_positioned_lines_to_latest(&mut positioned, OLDER_PAGE_LINES);
    state.oldest_offset = positioned.first().map(|line| line.start).unwrap_or(0);
    state.has_more_older = state.oldest_offset > 0;
    state.no_more_notice_sent = false;

    Ok(positioned.into_iter().map(|line| line.text).collect())
}

fn read_positioned_lines(path: &Path, start: u64, end: u64) -> std::io::Result<Vec<PositionedLine>> {
    if end <= start {
        return Ok(Vec::new());
    }

    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let end = end.min(len);
    if end <= start {
        return Ok(Vec::new());
    }

    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file);
    let mut current_offset = start;

    if start > 0 {
        let mut file = reader.into_inner();
        file.seek(SeekFrom::Start(start - 1))?;
        let mut prev = [0u8; 1];
        file.read_exact(&mut prev)?;
        file.seek(SeekFrom::Start(start))?;
        reader = BufReader::new(file);
        if prev[0] != b'\n' {
            let mut discard = String::new();
            let skipped = reader.read_line(&mut discard)?;
            current_offset = current_offset.saturating_add(skipped as u64);
        }
    }

    let mut out = Vec::new();
    loop {
        if current_offset >= end {
            break;
        }

        let line_start = current_offset;
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        current_offset = current_offset.saturating_add(n as u64);

        if line_start >= end {
            break;
        }

        out.push(PositionedLine {
            start: line_start,
            text: line.trim_end_matches(['\r', '\n']).to_string(),
        });
    }
    Ok(out)
}

fn trim_positioned_lines_to_latest(lines: &mut Vec<PositionedLine>, keep: usize) {
    if lines.len() > keep {
        let drop_count = lines.len() - keep;
        lines.drain(0..drop_count);
    }
}

fn parse_log_timestamp(line: &str) -> Option<DateTime<Utc>> {
    let token = line.split_whitespace().next()?;
    DateTime::parse_from_rfc3339(token)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use chrono::{SecondsFormat, Utc};
    use tempfile::TempDir;

    use super::{
        FileTailState, LogSource, choose_log_source, initialize_file_state, load_older_file_lines,
        read_follow_file_lines,
    };

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_prefers_explicit_log_file() {
        let tmp = TempDir::new().expect("tmp");
        let explicit = tmp.path().join("explicit.log");
        let src = choose_log_source("liquidator.service".to_string(), Some(explicit.clone()));
        assert_eq!(src, LogSource::FileTail(explicit));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_defaults_to_journald_without_log_file() {
        let src = choose_log_source("liquidator.service".to_string(), None);
        assert_eq!(
            src,
            LogSource::Journalctl {
                unit_name: "liquidator.service".to_string()
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn journald_args_are_expected() {
        let initial = super::journal_initial_args("liquidator.service");
        assert!(initial.iter().any(|v| v == "--since"));
        assert!(initial.iter().any(|v| v == "5 minutes ago"));
        assert!(initial.iter().any(|v| v == "json"));

        let follow = super::journal_follow_args("liquidator.service");
        assert!(follow.windows(2).any(|pair| pair == ["-n", "0"]));

        let older = super::journal_older_args("liquidator.service", "cursor123");
        assert!(older.windows(2).any(|pair| pair == ["--before-cursor", "cursor123"]));
        assert!(older.windows(2).any(|pair| pair == ["-n", "300"]));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn journald_message_byte_array_decodes_to_text() {
        let value = serde_json::json!([27, 91, 51, 50, 109, 73, 78, 70, 79, 27, 91, 48, 109, 32, 111, 107, 0]);
        let text = super::journal_value_to_text(&value);
        assert!(text.contains("INFO"));
        assert!(text.contains("ok"));
        assert!(!text.contains("27 91"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_prefers_explicit_log_file() {
        let tmp = TempDir::new().expect("tmp");
        let explicit = tmp.path().join("explicit.log");
        std::fs::write(&explicit, "line\n").expect("write");
        let src = choose_log_source("ignored".to_string(), Some(explicit.clone()));
        assert_eq!(src, LogSource::FileTail(explicit));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_uses_notice_when_log_file_missing() {
        let src = choose_log_source("ignored".to_string(), None);
        assert_eq!(src, LogSource::Notice("logs unavailable (set --log-file)".to_string()));
    }

    #[test]
    fn file_initial_history_prefers_last_five_minutes_when_timestamped() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");

        let old = (Utc::now() - chrono::Duration::minutes(10)).to_rfc3339_opts(SecondsFormat::Millis, true);
        let recent = (Utc::now() - chrono::Duration::minutes(2)).to_rfc3339_opts(SecondsFormat::Millis, true);
        let latest = (Utc::now() - chrono::Duration::seconds(20)).to_rfc3339_opts(SecondsFormat::Millis, true);
        std::fs::write(
            &path,
            format!("{old} INFO old\n{recent} INFO recent\n{latest} INFO latest\n"),
        )
        .expect("write");

        let (_state, initial) = initialize_file_state(&path).expect("init");
        assert_eq!(initial.len(), 2);
        assert!(initial[0].contains("recent"));
        assert!(initial[1].contains("latest"));
    }

    #[test]
    fn file_initial_history_falls_back_to_latest_lines_without_timestamps() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");

        let mut file = std::fs::File::create(&path).expect("create");
        for i in 0..500 {
            writeln!(file, "line-{i}").expect("write");
        }

        let (_state, initial) = initialize_file_state(&path).expect("init");
        assert_eq!(initial.len(), 400);
        assert_eq!(initial.first().expect("first"), "line-100");
        assert_eq!(initial.last().expect("last"), "line-499");
    }

    #[test]
    fn file_older_paging_preserves_order_and_handles_boundaries() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");

        let mut file = std::fs::File::create(&path).expect("create");
        for i in 0..1000 {
            writeln!(file, "line-{i:04}").expect("write");
        }

        let (mut state, initial) = initialize_file_state(&path).expect("init");
        assert_eq!(initial.len(), 400);
        assert_eq!(initial.first().expect("first"), "line-0600");
        assert_eq!(initial.last().expect("last"), "line-0999");

        let older = load_older_file_lines(&path, &mut state).expect("older");
        assert_eq!(older.len(), 300);
        assert_eq!(older.first().expect("first"), "line-0300");
        assert_eq!(older.last().expect("last"), "line-0599");
    }

    #[test]
    fn file_follow_reads_appends_and_handles_truncation() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");
        std::fs::write(&path, "old\n").expect("write old");

        let (mut state, _initial) = initialize_file_state(&path).expect("init");

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open")
            .write_all(b"new1\nnew2\n")
            .expect("append");
        let appended = read_follow_file_lines(&path, &mut state).expect("append read");
        assert_eq!(appended, vec!["new1".to_string(), "new2".to_string()]);

        std::fs::write(&path, "reset\n").expect("truncate+write");
        let after_truncate = read_follow_file_lines(&path, &mut state).expect("truncate read");
        assert_eq!(after_truncate, vec!["reset".to_string()]);
    }

    #[test]
    fn no_more_older_returns_empty() {
        let mut state = FileTailState {
            follow_offset: 0,
            oldest_offset: 0,
            has_more_older: false,
            no_more_notice_sent: false,
        };
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");
        std::fs::write(&path, "only\n").expect("write");
        let older = load_older_file_lines(&path, &mut state).expect("older");
        assert!(older.is_empty());
    }
}
