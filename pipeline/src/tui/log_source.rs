use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc;

use super::events::UiEvent;

#[cfg(target_os = "linux")]
use std::process::Stdio;

const MAX_INITIAL_FILE_HISTORY_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
enum LogSource {
    #[cfg(target_os = "linux")]
    Journalctl {
        unit_name: String,
    },
    FileTail(PathBuf),
    Notice(String),
}

#[derive(Default)]
struct FileTailState {
    offset: u64,
    initialized: bool,
}

pub(super) fn start_log_source(ui_tx: mpsc::UnboundedSender<UiEvent>, unit_name: String, log_file: Option<PathBuf>) {
    match choose_log_source(unit_name, log_file) {
        #[cfg(target_os = "linux")]
        LogSource::Journalctl { unit_name } => spawn_journalctl(ui_tx, unit_name),
        LogSource::FileTail(path) => spawn_file_tail(ui_tx, path),
        LogSource::Notice(msg) => {
            let _ = ui_tx.send(UiEvent::LogLine(msg));
        }
    }
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

#[cfg(target_os = "linux")]
fn spawn_journalctl(ui_tx: mpsc::UnboundedSender<UiEvent>, unit_name: String) {
    spawn_journalctl_stream(
        ui_tx.clone(),
        vec![
            "-u".to_string(),
            unit_name.clone(),
            "--since".to_string(),
            "1 hour ago".to_string(),
            "-o".to_string(),
            "short".to_string(),
            "--no-pager".to_string(),
        ],
        "history",
    );

    spawn_journalctl_stream(
        ui_tx,
        vec![
            "-f".to_string(),
            "-u".to_string(),
            unit_name,
            "-o".to_string(),
            "short".to_string(),
            "--no-pager".to_string(),
        ],
        "follow",
    );
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn spawn_journalctl(_ui_tx: mpsc::UnboundedSender<UiEvent>, _unit_name: String) {}

#[cfg(target_os = "linux")]
fn spawn_journalctl_stream(ui_tx: mpsc::UnboundedSender<UiEvent>, args: Vec<String>, mode: &'static str) {
    tokio::spawn(async move {
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
                let _ = ui_tx.send(UiEvent::LogLine("logs unavailable: journalctl not found".to_string()));
                return;
            }
            Err(err) => {
                let _ = ui_tx.send(UiEvent::LogLine(format!(
                    "logs unavailable: failed to start journalctl ({err})"
                )));
                return;
            }
        };

        let stdout_task = child.stdout.take().map(|stdout| {
            let ui_tx = ui_tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let _ = ui_tx.send(UiEvent::LogLine(line));
                }
            })
        });

        let stderr_task = child.stderr.take().map(|stderr| {
            let ui_tx = ui_tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let _ = ui_tx.send(UiEvent::LogLine(format!("journalctl {mode} stderr: {line}")));
                }
            })
        });

        match child.wait().await {
            Ok(status) if !status.success() => {
                let _ = ui_tx.send(UiEvent::LogLine(format!(
                    "journalctl {mode} exited with status {status}"
                )));
            }
            Ok(_) => {}
            Err(err) => {
                let _ = ui_tx.send(UiEvent::LogLine(format!("journalctl {mode} wait failed: {err}")));
            }
        }

        if let Some(task) = stdout_task {
            let _ = task.await;
        }
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
    });
}

fn spawn_file_tail(ui_tx: mpsc::UnboundedSender<UiEvent>, path: PathBuf) {
    tokio::task::spawn_blocking(move || {
        let mut state = FileTailState::default();
        loop {
            match read_new_lines(&path, &mut state) {
                Ok(lines) => {
                    for line in lines {
                        let _ = ui_tx.send(UiEvent::LogLine(line));
                    }
                }
                Err(err) => {
                    let _ = ui_tx.send(UiEvent::LogLine(format!(
                        "log tail error ({}): {}",
                        path.display(),
                        err
                    )));
                }
            }
            thread::sleep(Duration::from_millis(400));
        }
    });
}

fn read_new_lines(path: &Path, state: &mut FileTailState) -> std::io::Result<Vec<String>> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut skip_partial_line = false;

    if !state.initialized {
        state.offset = len.saturating_sub(MAX_INITIAL_FILE_HISTORY_BYTES);
        state.initialized = true;
        if state.offset > 0 {
            file.seek(SeekFrom::Start(state.offset - 1))?;
            let mut prev = [0u8; 1];
            file.read_exact(&mut prev)?;
            skip_partial_line = prev[0] != b'\n';
        }
    }

    if len < state.offset {
        state.offset = 0;
        skip_partial_line = false;
    }

    if len == state.offset {
        return Ok(vec![]);
    }

    file.seek(SeekFrom::Start(state.offset))?;
    let mut reader = BufReader::new(file);
    if skip_partial_line {
        let mut discard = String::new();
        let skipped = reader.read_line(&mut discard)?;
        state.offset = state.offset.saturating_add(skipped as u64);
    }

    let mut out = Vec::new();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        state.offset = state.offset.saturating_add(n as u64);
        out.push(line.trim_end_matches(['\r', '\n']).to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use tempfile::TempDir;

    use super::{FileTailState, LogSource, choose_log_source, read_new_lines};

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
    fn file_tail_reads_appends_and_handles_truncation() {
        let tmp = TempDir::new().expect("tmp");
        let path = tmp.path().join("liquidator.log");
        std::fs::write(&path, "old\n").expect("write old");

        let mut state = FileTailState::default();
        let initial = read_new_lines(&path, &mut state).expect("initial read");
        assert_eq!(initial, vec!["old".to_string()]);

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open")
            .write_all(b"new1\nnew2\n")
            .expect("append");
        let appended = read_new_lines(&path, &mut state).expect("append read");
        assert_eq!(appended, vec!["new1".to_string(), "new2".to_string()]);

        std::fs::write(&path, "reset\n").expect("truncate+write");
        let after_truncate = read_new_lines(&path, &mut state).expect("truncate read");
        assert_eq!(after_truncate, vec!["reset".to_string()]);
    }
}
