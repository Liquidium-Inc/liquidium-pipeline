use std::io;

use anyhow::Context as _;
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode};
use crossterm::{ExecutableCommand, execute};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;

use crate::commands::liquidation_loop::LoopEvent;

use super::events::UiEvent;

pub(super) struct TerminalGuard;

impl TerminalGuard {
    pub(super) fn enter() -> anyhow::Result<Self> {
        enter_with_ops(
            || enable_raw_mode().context("enable raw mode"),
            || execute!(io::stdout(), EnterAlternateScreen, EnableBracketedPaste).context("enter alternate screen"),
            || {
                let _ = disable_raw_mode();
                let _ = io::stdout().execute(DisableBracketedPaste);
                let _ = io::stdout().execute(LeaveAlternateScreen);
            },
        )
    }
}

fn enter_with_ops(
    enable_raw: impl FnOnce() -> anyhow::Result<()>,
    enter_alt: impl FnOnce() -> anyhow::Result<()>,
    cleanup: impl FnOnce(),
) -> anyhow::Result<TerminalGuard> {
    enable_raw()?;
    if let Err(err) = enter_alt() {
        cleanup();
        return Err(err);
    }
    Ok(TerminalGuard)
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(DisableBracketedPaste);
        let _ = io::stdout().execute(LeaveAlternateScreen);
    }
}

#[derive(Clone)]
struct TuiMakeWriter {
    ui_tx: mpsc::UnboundedSender<UiEvent>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TuiMakeWriter {
    type Writer = TuiWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TuiWriter {
            ui_tx: self.ui_tx.clone(),
            buf: Vec::new(),
        }
    }
}

struct TuiWriter {
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    buf: Vec<u8>,
}

impl TuiWriter {
    fn emit(&mut self) {
        if self.buf.is_empty() {
            return;
        }

        let buf = std::mem::take(&mut self.buf);
        let Ok(s) = String::from_utf8(buf) else {
            return;
        };

        for line in s.lines() {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            let _ = self.ui_tx.send(UiEvent::Engine(LoopEvent::Log(line.to_string())));
        }
    }
}

impl io::Write for TuiWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit();
        Ok(())
    }
}

impl Drop for TuiWriter {
    fn drop(&mut self) {
        self.emit();
    }
}

pub(super) fn init_tui_tracing(ui_tx: mpsc::UnboundedSender<UiEvent>) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_ansi(false)
        .with_writer(TuiMakeWriter { ui_tx });

    let subscriber = tracing_subscriber::registry().with(env_filter).with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).context("set tracing subscriber")?;

    // Bridge `log` records into `tracing`.
    let _ = tracing_log::LogTracer::builder().init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use anyhow::anyhow;

    use super::enter_with_ops;

    #[test]
    fn enable_raw_failure_returns_error() {
        let enter_called = AtomicBool::new(false);
        let cleanup_called = AtomicBool::new(false);
        let res = enter_with_ops(
            || Err(anyhow!("raw fail")),
            || {
                enter_called.store(true, Ordering::SeqCst);
                Ok(())
            },
            || {
                cleanup_called.store(true, Ordering::SeqCst);
            },
        );
        assert!(res.is_err());
        assert!(!enter_called.load(Ordering::SeqCst));
        assert!(!cleanup_called.load(Ordering::SeqCst));
    }

    #[test]
    fn enter_alt_failure_runs_cleanup() {
        let cleanup_called = AtomicBool::new(false);
        let res = enter_with_ops(
            || Ok(()),
            || Err(anyhow!("alt fail")),
            || {
                cleanup_called.store(true, Ordering::SeqCst);
            },
        );
        assert!(res.is_err());
        assert!(cleanup_called.load(Ordering::SeqCst));
    }

    #[test]
    fn success_returns_guard_without_cleanup() {
        let cleanup_called = AtomicBool::new(false);
        let res = enter_with_ops(
            || Ok(()),
            || Ok(()),
            || {
                cleanup_called.store(true, Ordering::SeqCst);
            },
        );
        assert!(res.is_ok());
        let guard = res.expect("guard");
        std::mem::forget(guard);
        assert!(!cleanup_called.load(Ordering::SeqCst));
    }
}
