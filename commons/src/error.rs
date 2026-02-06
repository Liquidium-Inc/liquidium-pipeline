use std::error::Error;
use std::fmt;

/// Stable error codes shared across the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    Unknown = 0,
    ConfigMissingEnv = 1_000,
    ConfigInvalidPrincipal = 1_001,
    ConfigReadMnemonic = 1_002,
    ConfigDeriveIdentity = 1_003,
    ConfigDeriveEvmKey = 1_004,
    ConfigMissingCexCredentials = 1_005,
    CoreUnknownAsset = 2_000,
    CoreAccountBackend = 2_001,
    CoreAccountCache = 2_002,
    CoreTransferBackend = 2_100,
    ConnectorBackend = 3_000,
    ConnectorEnv = 3_001,
    PipelineWal = 4_000,
    PipelineStage = 4_001,
    PipelineSwap = 4_002,
}

impl ErrorCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}({})", self.as_u16())
    }
}

/// Trait for errors that expose a stable error code.
pub trait CodedError: Error {
    fn code(&self) -> ErrorCode;

    fn retriable(&self) -> bool {
        false
    }
}

/// Helper error type for external sources that only provide strings.
#[derive(Debug)]
pub struct ExternalError(pub String);

impl fmt::Display for ExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ExternalError {}

impl From<String> for ExternalError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ExternalError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Formats a coded error with its numeric identifier for user-facing logs.
pub fn format_with_code<E>(err: &E) -> String
where
    E: CodedError + fmt::Display,
{
    format!("{} (code={})", err, err.code().as_u16())
}
