//! The reconcile panel of the Executions tab. It sends the same requests as
//! `liquidator reconcile` over the daemon's control socket.
use std::path::Path;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::time::timeout;

use super::app::App;
use crate::control_plane::send_reconcile;
use crate::finalizers::outbound::OperatorDecision;
use crate::operator::{ReconcileAction, ReconcileRequest};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INPUT_LEN: usize = 256;

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
    let reply = send(sock_path, &request).await;
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
            panel.reply = match &request {
                Ok(request) => send(sock_path, request).await,
                Err(error) => Err(error.clone()),
            };
            if let Ok(request) = request
                && request.action != ReconcileAction::Show
            {
                let outcome = match &panel.reply {
                    Ok(_) => "released".to_string(),
                    Err(error) => format!("refused: {error}"),
                };
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

async fn send(sock_path: &Path, request: &ReconcileRequest) -> Result<String, String> {
    match timeout(RECONCILE_TIMEOUT, send_reconcile(sock_path, request)).await {
        Ok(reply) => reply,
        Err(_) => Err(format!(
            "the daemon did not answer within {}s",
            RECONCILE_TIMEOUT.as_secs()
        )),
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
