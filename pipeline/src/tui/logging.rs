use std::io;

use anyhow::Context as _;
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode};
use crossterm::{ExecutableCommand, execute};

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
