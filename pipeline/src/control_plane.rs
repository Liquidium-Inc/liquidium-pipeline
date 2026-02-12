//! Control-plane IPC over Unix Domain Sockets.
//!
//! Security model:
//! - The socket path defaults to `/run/liquidator/ctl.sock` on Linux and to
//!   `<temp>/liquidator/ctl.sock` on non-Linux Unix systems.
//! - Existing socket files at the configured path are treated as stale and
//!   removed before bind.
//! - On Linux, the socket file is forced to mode `0660`.
//! - Directory mode `0770` is applied best-effort only on dedicated leaf dirs
//!   (`/run/liquidator` and `<temp>/liquidator`), never on global dirs such as
//!   `/run` or `/tmp`.
//!
//! Protocol:
//! - Client sends exactly one line: `pause\n` or `resume\n`.
//! - Server replies with exactly one line: `ok\n` or `err:<reason>\n`.
//! - Connection is closed after the reply.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, bail};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

const MAX_COMMAND_LEN: usize = 32;
type TransitionHook = Arc<dyn Fn(bool) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    Pause,
    Resume,
}

impl ControlCommand {
    fn as_wire(self) -> &'static str {
        match self {
            Self::Pause => "pause\n",
            Self::Resume => "resume\n",
        }
    }
}

pub fn default_sock_path() -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        PathBuf::from("/run/liquidator/ctl.sock")
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::temp_dir().join("liquidator").join("ctl.sock")
    }
}

pub fn default_log_file_path() -> PathBuf {
    std::env::temp_dir().join("liquidator").join("liquidator.log")
}

pub fn bind_control_listener(sock_path: &Path) -> anyhow::Result<UnixListener> {
    prepare_socket_path(sock_path)?;
    let listener = match UnixListener::bind(sock_path) {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::AddrInUse => {
            bail!(
                "bind control socket {} failed: address already in use (another daemon may already be running)",
                sock_path.display()
            );
        }
        Err(err) => {
            return Err(err).with_context(|| format!("bind control socket {}", sock_path.display()));
        }
    };
    harden_socket_permissions(sock_path)?;
    Ok(listener)
}

pub async fn serve_bound_listener(listener: UnixListener, paused: Arc<AtomicBool>) -> anyhow::Result<()> {
    serve_bound_listener_with_hook(listener, paused, None).await
}

pub async fn serve_bound_listener_with_hook(
    listener: UnixListener,
    paused: Arc<AtomicBool>,
    on_transition: Option<TransitionHook>,
) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await.context("accept control client")?;
        let paused = paused.clone();
        let on_transition = on_transition.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, paused, on_transition).await {
                tracing::warn!("control client handling failed: {err}");
            }
        });
    }
}

#[allow(dead_code)]
pub async fn run_control_server(sock_path: PathBuf, paused: Arc<AtomicBool>) -> anyhow::Result<()> {
    let listener = bind_control_listener(&sock_path)?;
    tracing::info!("control socket ready at {}", sock_path.display());
    serve_bound_listener(listener, paused).await
}

pub async fn send_control_command(sock_path: &Path, cmd: ControlCommand) -> Result<(), String> {
    let mut stream = UnixStream::connect(sock_path)
        .await
        .map_err(|e| format!("connect {} failed: {e}", sock_path.display()))?;

    stream
        .write_all(cmd.as_wire().as_bytes())
        .await
        .map_err(|e| format!("send command failed: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush command failed: {e}"))?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .await
        .map_err(|e| format!("read response failed: {e}"))?;
    if n == 0 {
        return Err("empty response from control server".to_string());
    }

    if line.trim() == "ok" {
        Ok(())
    } else {
        Err(line.trim().to_string())
    }
}

fn prepare_socket_path(sock_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = sock_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create socket dir {}", parent.display()))?;
        maybe_harden_parent_permissions(parent)?;
    }

    // Treat any previous file at path as stale. We intentionally ignore errors here:
    // bind() will produce the authoritative outcome.
    let _ = fs::remove_file(sock_path);

    if sock_path.exists() {
        bail!("control socket path is occupied: {}", sock_path.display());
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn maybe_harden_parent_permissions(parent: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp_leaf = std::env::temp_dir().join("liquidator");
    let should_harden = parent == Path::new("/run/liquidator") || parent == temp_leaf.as_path();
    if should_harden && let Err(err) = fs::set_permissions(parent, fs::Permissions::from_mode(0o770)) {
        tracing::warn!("failed to chmod 0770 {}: {}", parent.display(), err);
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn maybe_harden_parent_permissions(parent: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp_leaf = std::env::temp_dir().join("liquidator");
        if parent == temp_leaf.as_path()
            && let Err(err) = fs::set_permissions(parent, fs::Permissions::from_mode(0o770))
        {
            tracing::warn!("failed to chmod 0770 {}: {}", parent.display(), err);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn harden_socket_permissions(sock_path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(sock_path, fs::Permissions::from_mode(0o660))
        .with_context(|| format!("chmod 0660 {}", sock_path.display()))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn harden_socket_permissions(_sock_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

async fn handle_client(
    stream: UnixStream,
    paused: Arc<AtomicBool>,
    on_transition: Option<TransitionHook>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut line = String::new();
    let reader = BufReader::new(read_half);
    let mut limited = reader.take((MAX_COMMAND_LEN + 1) as u64);
    let bytes = limited.read_line(&mut line).await.context("read control command")?;

    let response = if bytes == 0 {
        "err:empty\n".to_string()
    } else if bytes > MAX_COMMAND_LEN || !line.ends_with('\n') {
        "err:invalid command\n".to_string()
    } else {
        let normalized = line.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            "err:empty\n".to_string()
        } else {
            match normalized.as_str() {
                "pause" => {
                    let was_paused = paused.swap(true, Ordering::SeqCst);
                    if !was_paused {
                        tracing::info!("daemon paused: liquidation initiation suspended");
                        if let Some(cb) = &on_transition {
                            cb(true);
                        }
                    }
                    "ok\n".to_string()
                }
                "resume" => {
                    let was_paused = paused.swap(false, Ordering::SeqCst);
                    if was_paused {
                        tracing::info!("daemon resumed: liquidation initiation enabled");
                        if let Some(cb) = &on_transition {
                            cb(false);
                        }
                    }
                    "ok\n".to_string()
                }
                _ => "err:invalid command\n".to_string(),
            }
        }
    };

    write_half
        .write_all(response.as_bytes())
        .await
        .context("write control response")?;
    write_half.shutdown().await.context("shutdown control stream")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::net as std_uds;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    use super::{ControlCommand, bind_control_listener, run_control_server, send_control_command};

    fn sock_path(tmp: &TempDir) -> std::path::PathBuf {
        tmp.path().join("ctl.sock")
    }

    fn skip_if_uds_unsupported(tmp: &TempDir) -> bool {
        let probe = tmp.path().join("probe.sock");
        match std_uds::UnixListener::bind(&probe) {
            Ok(listener) => {
                drop(listener);
                let _ = fs::remove_file(&probe);
                false
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping test: unix sockets are not permitted in this environment");
                true
            }
            Err(err) => panic!("unexpected unix socket bind failure: {err}"),
        }
    }

    async fn wait_for_socket(path: &Path) {
        for _ in 0..100 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("socket did not appear at {}", path.display());
    }

    async fn send_raw_command(path: &Path, cmd: &str) -> String {
        let mut stream = UnixStream::connect(path).await.expect("connect");
        stream.write_all(cmd.as_bytes()).await.expect("write");
        stream.shutdown().await.expect("shutdown");

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read");
        line
    }

    #[test]
    fn bind_removes_stale_socket_file() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);

        let stale = std_uds::UnixListener::bind(&path).expect("create stale socket");
        drop(stale);
        assert!(path.exists());

        let listener = bind_control_listener(&path).expect("bind after stale cleanup");
        drop(listener);
        assert!(path.exists());
    }

    #[test]
    fn bind_rejects_non_socket_path() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);

        fs::create_dir_all(&path).expect("create dir at socket path");

        let err = bind_control_listener(&path).expect_err("expected bind failure");
        assert!(err.to_string().contains("occupied"));
    }

    #[tokio::test]
    async fn pause_and_resume_commands_update_shared_flag() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let paused = Arc::new(AtomicBool::new(false));

        let server = tokio::spawn(run_control_server(path.clone(), paused.clone()));
        wait_for_socket(&path).await;

        send_control_command(&path, ControlCommand::Pause).await.expect("pause");
        assert!(paused.load(Ordering::SeqCst));

        send_control_command(&path, ControlCommand::Resume)
            .await
            .expect("resume");
        assert!(!paused.load(Ordering::SeqCst));

        server.abort();
    }

    #[tokio::test]
    async fn invalid_command_returns_error_reply() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let paused = Arc::new(AtomicBool::new(false));

        let server = tokio::spawn(run_control_server(path.clone(), paused));
        wait_for_socket(&path).await;

        let reply = send_raw_command(&path, "bad\n").await;
        assert_eq!(reply.trim(), "err:invalid command");

        server.abort();
    }

    #[tokio::test]
    async fn empty_command_returns_error_reply() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let paused = Arc::new(AtomicBool::new(false));

        let server = tokio::spawn(run_control_server(path.clone(), paused));
        wait_for_socket(&path).await;

        let reply = send_raw_command(&path, "\n").await;
        assert_eq!(reply.trim(), "err:empty");

        server.abort();
    }

    #[tokio::test]
    async fn oversized_command_returns_error_reply() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let paused = Arc::new(AtomicBool::new(false));

        let server = tokio::spawn(run_control_server(path.clone(), paused));
        wait_for_socket(&path).await;

        let too_long = format!("{}\n", "a".repeat(64));
        let reply = send_raw_command(&path, &too_long).await;
        assert_eq!(reply.trim(), "err:invalid command");

        server.abort();
    }

    #[tokio::test]
    async fn concurrent_clients_are_handled() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let paused = Arc::new(AtomicBool::new(false));

        let server = tokio::spawn(run_control_server(path.clone(), paused.clone()));
        wait_for_socket(&path).await;

        let mut tasks = Vec::new();
        for i in 0..24 {
            let path = path.clone();
            tasks.push(tokio::spawn(async move {
                let cmd = if i % 2 == 0 {
                    ControlCommand::Pause
                } else {
                    ControlCommand::Resume
                };
                send_control_command(&path, cmd).await
            }));
        }

        for t in tasks {
            t.await.expect("join").expect("command");
        }

        send_control_command(&path, ControlCommand::Pause)
            .await
            .expect("server still responsive");
        assert!(paused.load(Ordering::SeqCst));

        server.abort();
    }

    #[tokio::test]
    async fn client_maps_error_reply() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let listener = std_uds::UnixListener::bind(&path).expect("bind mock");

        let server = tokio::task::spawn_blocking(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0_u8; 32];
            let _ = std::io::Read::read(&mut stream, &mut buf).expect("read");
            std::io::Write::write_all(&mut stream, b"err:mock failure\n").expect("write");
        });

        let err = send_control_command(&path, ControlCommand::Pause)
            .await
            .expect_err("expected error");
        assert_eq!(err, "err:mock failure");
        server.await.expect("join");
    }
}
