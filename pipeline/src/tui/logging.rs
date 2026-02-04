use std::io;

use anyhow::Context as _;
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
        enable_raw_mode().context("enable raw mode")?;
        execute!(io::stdout(), EnterAlternateScreen).context("enter alternate screen")?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
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
