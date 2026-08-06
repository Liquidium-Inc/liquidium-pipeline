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

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context as _, bail};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::sleep;

const MAX_COMMAND_LEN: usize = 32;

/// Backoff between `accept` retries after a transient failure.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Consecutive transient `accept` failures tolerated before giving up. At
/// [`ACCEPT_RETRY_DELAY`] this is roughly ten seconds of sustained failure,
/// which no longer looks transient.
const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 100;

type TransitionHook = Arc<dyn Fn(bool) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    Pause,
    Resume,
}

/// Process-lifetime ownership of one liquidation-intake database.
///
/// The lock file remains on disk after shutdown, but the OS lock itself is
/// released automatically when the process exits, including after a crash.
/// Keeping the file handle alive is what keeps this daemon authoritative.
#[derive(Debug)]
pub struct DaemonInstanceLock {
    _file: File,
    path: PathBuf,
}

impl DaemonInstanceLock {
    pub fn path(&self) -> &Path {
        &self.path
    }
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

/// Claims exclusive ownership of the intake database and control socket.
///
/// This must run before opening either SQLite database or recovering
/// `Submitting` intents. The database-scoped lock prevents a second daemon
/// from bypassing exclusivity with a different `--sock-path`; the pre-bound
/// socket additionally prevents an older daemon using the same control path
/// from overlapping startup.
pub fn acquire_daemon_instance(
    liquidations_db_path: &Path,
    sock_path: &Path,
) -> anyhow::Result<(DaemonInstanceLock, UnixListener)> {
    let instance_lock = acquire_intake_db_lock(liquidations_db_path)?;
    let listener = bind_control_listener(sock_path)
        .with_context(|| format!("claim daemon control socket at {}", sock_path.display()))?;
    Ok((instance_lock, listener))
}

fn acquire_intake_db_lock(liquidations_db_path: &Path) -> anyhow::Result<DaemonInstanceLock> {
    let lock_path = daemon_lock_path(liquidations_db_path)?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let file = options
        .open(&lock_path)
        .with_context(|| format!("open daemon lock file {}", lock_path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(DaemonInstanceLock {
            _file: file,
            path: lock_path,
        }),
        Err(TryLockError::WouldBlock) => bail!(
            "another liquidation daemon is already using intake database {} (lock: {})",
            liquidations_db_path.display(),
            lock_path.display()
        ),
        Err(TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("lock daemon instance at {}", lock_path.display()))
        }
    }
}

fn daemon_lock_path(liquidations_db_path: &Path) -> anyhow::Result<PathBuf> {
    let canonical_target = if liquidations_db_path.exists() {
        liquidations_db_path
            .canonicalize()
            .with_context(|| format!("canonicalize intake database {}", liquidations_db_path.display()))?
    } else {
        let parent = liquidations_db_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).with_context(|| format!("create intake database directory {}", parent.display()))?;
        let canonical_parent = parent
            .canonicalize()
            .with_context(|| format!("canonicalize intake database directory {}", parent.display()))?;
        let file_name = liquidations_db_path.file_name().ok_or_else(|| {
            anyhow::anyhow!(
                "liquidation intake database path has no file name: {}",
                liquidations_db_path.display()
            )
        })?;
        canonical_parent.join(file_name)
    };

    let mut lock_name = OsString::from(canonical_target.as_os_str());
    lock_name.push(".daemon.lock");
    Ok(PathBuf::from(lock_name))
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

/// Serves the control socket until the listener becomes unusable.
///
/// Auditor notes: `accept` can fail for reasons that leave the listener
/// perfectly valid — a client vanishing mid-handshake, or the process hitting
/// its descriptor limit. Propagating those would permanently disable
/// pause/resume for the lifetime of the daemon, so they are retried with a
/// backoff and only a sustained run of failures is treated as fatal.
pub async fn serve_bound_listener_with_hook(
    listener: UnixListener,
    paused: Arc<AtomicBool>,
    on_transition: Option<TransitionHook>,
) -> anyhow::Result<()> {
    let mut consecutive_errors: u32 = 0;

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => {
                consecutive_errors = 0;
                stream
            }
            Err(err) if is_transient_accept_error(&err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                if consecutive_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                    return Err(err).with_context(|| {
                        format!("accept control client failed {consecutive_errors} times consecutively")
                    });
                }
                tracing::warn!(
                    consecutive_errors,
                    "accept control client failed transiently; retrying: {err}"
                );
                sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
            Err(err) => return Err(err).context("accept control client"),
        };

        let paused = paused.clone();
        let on_transition = on_transition.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, paused, on_transition).await {
                tracing::warn!("control client handling failed: {err}");
            }
        });
    }
}

/// Classifies `accept` failures that leave the listener usable.
///
/// `EMFILE`/`ENFILE` have no `ErrorKind` variant and surface as
/// `Uncategorized`, so they are matched on errno instead; both values are
/// identical on Linux and macOS.
fn is_transient_accept_error(err: &std::io::Error) -> bool {
    const EMFILE: i32 = 24;
    const ENFILE: i32 = 23;

    matches!(
        err.kind(),
        ErrorKind::ConnectionAborted | ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::OutOfMemory
    ) || matches!(err.raw_os_error(), Some(EMFILE) | Some(ENFILE))
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

    if sock_path.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            use std::os::unix::net::UnixStream as StdUnixStream;

            let meta = fs::symlink_metadata(sock_path)
                .with_context(|| format!("inspect existing control socket path {}", sock_path.display()))?;
            if !meta.file_type().is_socket() {
                bail!(
                    "control socket path is occupied by non-socket file: {}",
                    sock_path.display()
                );
            }

            match StdUnixStream::connect(sock_path) {
                Ok(_) => {
                    bail!(
                        "control socket already in use at {} (another daemon may already be running)",
                        sock_path.display()
                    );
                }
                Err(err) if is_stale_socket_connect_error(&err) => match fs::remove_file(sock_path) {
                    Ok(_) => {}
                    Err(remove_err) if remove_err.kind() == ErrorKind::NotFound => {}
                    Err(remove_err) => {
                        return Err(remove_err)
                            .with_context(|| format!("remove stale control socket {}", sock_path.display()));
                    }
                },
                Err(err) => {
                    return Err(err).with_context(|| format!("probe existing control socket {}", sock_path.display()));
                }
            }
        }
    }

    if sock_path.exists() {
        bail!("control socket path is occupied: {}", sock_path.display());
    }

    Ok(())
}

#[cfg(unix)]
fn is_stale_socket_connect_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::ConnectionRefused | ErrorKind::ConnectionReset | ErrorKind::NotConnected | ErrorKind::TimedOut
    )
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

    use super::{
        ControlCommand, acquire_daemon_instance, acquire_intake_db_lock, bind_control_listener,
        is_transient_accept_error, run_control_server, send_control_command,
    };

    fn sock_path(tmp: &TempDir) -> std::path::PathBuf {
        tmp.path().join("ctl.sock")
    }

    /// Failures that leave the listener usable must not end the accept loop:
    /// doing so would disable pause/resume for the rest of the process lifetime
    /// while liquidations keep running.
    #[test]
    fn recoverable_accept_failures_are_classified_transient() {
        for kind in [
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::OutOfMemory,
        ] {
            assert!(
                is_transient_accept_error(&std::io::Error::new(kind, "transient")),
                "{kind:?} should be retried"
            );
        }

        // EMFILE / ENFILE: descriptor exhaustion, surfaced only as an errno.
        for errno in [24, 23] {
            assert!(
                is_transient_accept_error(&std::io::Error::from_raw_os_error(errno)),
                "errno {errno} should be retried"
            );
        }
    }

    #[test]
    fn unrecoverable_accept_failures_are_classified_fatal() {
        for kind in [
            std::io::ErrorKind::InvalidInput,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::NotConnected,
        ] {
            assert!(
                !is_transient_accept_error(&std::io::Error::new(kind, "fatal")),
                "{kind:?} should not be retried"
            );
        }
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

    #[tokio::test]
    async fn bind_removes_stale_socket_file() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);

        let stale = std_uds::UnixListener::bind(&path).expect("bind stale");
        drop(stale);
        assert!(path.exists(), "stale socket file should remain after drop");

        let listener = bind_control_listener(&path).expect("bind should remove stale socket");
        drop(listener);
        assert!(path.exists(), "socket should be present after successful bind");
    }

    #[tokio::test]
    async fn bind_rejects_non_socket_path() {
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
    async fn bind_rejects_active_socket_path() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let path = sock_path(&tmp);
        let _active = std_uds::UnixListener::bind(&path).expect("bind active socket");

        let err = bind_control_listener(&path).expect_err("expected active-socket bind failure");
        let msg = err.to_string();
        assert!(
            msg.contains("already in use") || msg.contains("another daemon"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn intake_database_lock_rejects_a_second_holder_and_releases_on_drop() {
        let tmp = TempDir::new().expect("tmp");
        let db_path = tmp.path().join("liquidations.db");

        let first = acquire_intake_db_lock(&db_path).expect("first daemon lock");
        assert_eq!(
            first.path(),
            tmp.path()
                .canonicalize()
                .expect("canonical temp directory")
                .join("liquidations.db.daemon.lock")
        );

        let error = acquire_intake_db_lock(&db_path).expect_err("second daemon must be rejected");
        assert!(
            error.to_string().contains("another liquidation daemon"),
            "unexpected error: {error:#}"
        );

        drop(first);
        acquire_intake_db_lock(&db_path).expect("lock should be released when daemon exits");
    }

    #[cfg(unix)]
    #[test]
    fn intake_database_lock_canonicalizes_symlink_aliases() {
        use std::os::unix::fs::symlink;

        let tmp = TempDir::new().expect("tmp");
        let db_path = tmp.path().join("liquidations.db");
        let db_alias = tmp.path().join("liquidations-alias.db");
        fs::write(&db_path, b"").expect("create intake database");
        symlink(&db_path, &db_alias).expect("create intake database alias");

        let _first = acquire_intake_db_lock(&db_path).expect("first daemon lock");
        let error = acquire_intake_db_lock(&db_alias).expect_err("canonical alias must share the same lock");

        assert!(
            error.to_string().contains("another liquidation daemon"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn same_database_is_rejected_even_with_a_different_socket() {
        let tmp = TempDir::new().expect("tmp");
        if skip_if_uds_unsupported(&tmp) {
            return;
        }
        let db_path = tmp.path().join("liquidations.db");
        let first_socket = tmp.path().join("first.sock");
        let second_socket = tmp.path().join("second.sock");

        let _first = acquire_daemon_instance(&db_path, &first_socket).expect("first daemon");
        let error = acquire_daemon_instance(&db_path, &second_socket).expect_err("second daemon must be rejected");

        assert!(
            error.to_string().contains("another liquidation daemon"),
            "unexpected error: {error:#}"
        );
        assert!(
            !second_socket.exists(),
            "rejected daemon must not bind its alternate socket"
        );
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
