use liquidium_pipeline_commons::error::{CodedError, ErrorCode, ExternalError, format_with_code};
use thiserror::Error;

pub type ConnectorResult<T> = Result<T, ConnectorError>;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("missing env var {var}")]
    MissingEnv {
        var: &'static str,
        #[source]
        source: std::env::VarError,
    },
    #[error("invalid input: {message}")]
    InvalidInput { message: String },
    #[error("backend error")]
    Backend {
        #[source]
        source: ExternalError,
    },
}

impl ConnectorError {
    pub fn backend<E>(err: E) -> Self
    where
        E: Into<ExternalError>,
    {
        ConnectorError::Backend {
            source: err.into(),
        }
    }
}

impl From<ConnectorError> for ExternalError {
    fn from(value: ConnectorError) -> Self {
        ExternalError(value.to_string())
    }
}

impl From<ConnectorError> for String {
    fn from(value: ConnectorError) -> Self {
        format_with_code(&value)
    }
}

impl CodedError for ConnectorError {
    fn code(&self) -> ErrorCode {
        match self {
            ConnectorError::MissingEnv { .. } => ErrorCode::ConnectorEnv,
            ConnectorError::InvalidInput { .. } => ErrorCode::ConnectorEnv,
            ConnectorError::Backend { .. } => ErrorCode::ConnectorBackend,
        }
    }
}
