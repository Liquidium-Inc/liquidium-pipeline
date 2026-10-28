//! The reconcile panel of the Executions tab. It sends the same requests as
//! `liquidator reconcile` over the daemon's control socket.
use std::path::Path;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::time::timeout;

use super::app::App;
use crate::control_plane::send_reconcile;
use crate::outbound::driver::OperatorDecision;
use crate::operator::{ReconcileAction, ReconcileRequest};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INPUT_LEN: usize = 256;

/// Why a request has no confirmed reply.
#[derive(Debug)]
enum NoReply {
    /// The daemon refused the request, or the request never reached it.
    Refused(String),
    /// The daemon did not answer in time. It may still apply the request.
    TimedOut,
}

impl NoReply {
    fn message(&self) -> String {
        match self {
            Self::Refused(error) => error.clone(),
            Self::TimedOut => format!(
                "the daemon did not answer within {}s; it may still apply the request, so show the row before you answer again",
                RECONCILE_TIMEOUT.as_secs()
            ),
        }
    }
}

/// The log line for an answer. Only a reply from the daemon proves a refusal.
fn outcome(reply: &Result<String, NoReply>) -> String {
    match reply {
        Ok(_) => "released".to_string(),
        Err(NoReply::Refused(error)) => format!("refused: {error}"),
        Err(no_reply @ NoReply::TimedOut) => format!("not confirmed: {}", no_reply.message()),
    }
}

/// An open reconcile panel for one execution row.
#[derive(Clone, Debug)]
pub(super) struct ReconcilePanel {
    pub(super) liq_id: String,
    pub(super) input: String,
    /// The last report or error from the daemon.
    pub(super) reply: Result<String, String>,
}

impl ReconcilePanel {
    pub(super) fn type_text(&mut self, text: impl IntoIterator<Item = char>) {
        for c in text {
            if self.input.len() >= MAX_INPUT_LEN {
                break;
            }
            if !c.is_control() {
                self.input.push(c);
            }
        }
    }
}

/// Opens the panel on a row and loads the row's report.
pub(super) async fn open_reconcile(app: &mut App, sock_path: &Path, liq_id: String) {
    let request = ReconcileRequest {
        liq_id: liq_id.clone(),
        leg_id: None,
        action: ReconcileAction::Show,
    };
    let reply = send(sock_path, &request).await.map_err(|no_reply| no_reply.message());
    app.reconcile = Some(ReconcilePanel {
        liq_id,
        input: String::new(),
        reply,
    });
}

/// Handles a key while the panel is open. Letters go to the input, so a
/// receipt can contain any of them; only Esc closes the panel.
pub(super) async fn handle_reconcile_key(app: &mut App, key: KeyEvent, sock_path: &Path) {
    let Some(panel) = app.reconcile.as_mut() else {
        return;
    };
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => app.reconcile = None,
        (KeyCode::Enter, _) => {
            let line = std::mem::take(&mut panel.input);
            let request = parse_reconcile_input(&panel.liq_id, &line);
            let reply = match &request {
                Ok(request) => send(sock_path, request).await,
                Err(error) => Err(NoReply::Refused(error.clone())),
            };
            let logged = request
                .is_ok_and(|request| request.action != ReconcileAction::Show)
                .then(|| outcome(&reply));
            panel.reply = reply.map_err(|no_reply| no_reply.message());
            if let Some(outcome) = logged {
                let liq_id = panel.liq_id.clone();
                app.push_log(format!("reconcile {liq_id}: `{}` {outcome}", line.trim()));
            }
        }
        (KeyCode::Backspace, _) => {
            panel.input.pop();
        }
        (KeyCode::Char(c), KeyModifiers::NONE | KeyModifiers::SHIFT) => panel.type_text([c]),
        _ => {}
    }
}

async fn send(sock_path: &Path, request: &ReconcileRequest) -> Result<String, NoReply> {
    match timeout(RECONCILE_TIMEOUT, send_reconcile(sock_path, request)).await {
        Ok(reply) => reply.map_err(NoReply::Refused),
        Err(_) => Err(NoReply::TimedOut),
    }
}

/// Reads `show`, `release`, `absent` or `landed <receipt>`, each optionally
/// followed by `leg <id>`. An empty line shows the row again.
fn parse_reconcile_input(liq_id: &str, line: &str) -> Result<ReconcileRequest, String> {
    let mut words = line.split_whitespace();
    let action = match words.next() {
        None | Some("show") => ReconcileAction::Show,
        Some("release") => ReconcileAction::Release,
        Some("absent") => ReconcileAction::Answer(OperatorDecision::Absent),
        Some("landed") => {
            let receipt = words.next().ok_or("landed needs a receipt: landed <receipt>")?;
            ReconcileAction::Answer(OperatorDecision::Landed {
                receipt: receipt.to_string(),
            })
        }
        Some(other) => {
            return Err(format!(
                "unknown action `{other}`; use show, release, absent or landed <receipt>"
            ));
        }
    };
    let leg_id = match (words.next(), words.next(), words.next()) {
        (None, _, _) => None,
        (Some("leg"), Some(id), None) => Some(id.to_string()),
        _ => return Err("only `leg <id>` may follow the action".to_string()),
    };
    Ok(ReconcileRequest {
        liq_id: liq_id.to_string(),
        leg_id,
        action,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(line: &str) -> Result<(ReconcileAction, Option<String>), String> {
        parse_reconcile_input("1720", line).map(|request| (request.action, request.leg_id))
    }

    /// Review finding 34: the daemon may still apply an answer it did not
    /// confirm in time, so the log must not call it refused.
    #[test]
    fn an_unconfirmed_answer_is_not_logged_as_refused() {
        assert_eq!(outcome(&Ok("ok".to_string())), "released");
        assert_eq!(
            outcome(&Err(NoReply::Refused("row 7 is Enqueued, not parked".to_string()))),
            "refused: row 7 is Enqueued, not parked"
        );
        let timed_out = outcome(&Err(NoReply::TimedOut));
        assert!(!timed_out.contains("refused"), "{timed_out}");
        assert!(timed_out.contains("may still apply"), "{timed_out}");
    }

    #[test]
    fn panel_input_maps_to_the_cli_actions() {
        assert_eq!(action("").unwrap(), (ReconcileAction::Show, None));
        assert_eq!(action("show").unwrap(), (ReconcileAction::Show, None));
        assert_eq!(action("release").unwrap(), (ReconcileAction::Release, None));
        assert_eq!(
            action("absent leg mexc-1").unwrap(),
            (
                ReconcileAction::Answer(OperatorDecision::Absent),
                Some("mexc-1".to_string())
            )
        );
        // Receipts are case sensitive and pass through unchanged.
        assert_eq!(
            action("  landed 4xFk9Q  ").unwrap(),
            (
                ReconcileAction::Answer(OperatorDecision::Landed {
                    receipt: "4xFk9Q".to_string()
                }),
                None
            )
        );
    }

    #[test]
    fn unclear_panel_input_sends_nothing() {
        for line in ["landed", "absnt", "absent extra", "release leg", "landed a leg b c"] {
            assert!(action(line).is_err(), "{line}");
        }
    }
}
