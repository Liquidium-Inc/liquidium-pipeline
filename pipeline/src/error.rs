use liquidium_pipeline_commons::error::{CodedError, ErrorCode, ExternalError};
use liquidium_pipeline_connectors::error::ConnectorError;
use liquidium_pipeline_core::error::CoreError;
use thiserror::Error;

use crate::config::ConfigError;

pub type PipelineResult<T> = Result<T, PipelineError>;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error("wal error")]
    Wal {
        #[source]
        source: ExternalError,
    },
    #[error("{message}")]
    Stage { code: ErrorCode, message: String },
}

impl PipelineError {
    pub fn wal<E>(err: E) -> Self
    where
        E: Into<ExternalError>,
    {
        PipelineError::Wal { source: err.into() }
    }

    pub fn stage(code: ErrorCode, message: impl Into<String>) -> Self {
        PipelineError::Stage {
            code,
            message: message.into(),
        }
    }
}

impl CodedError for PipelineError {
    fn code(&self) -> ErrorCode {
        match self {
            PipelineError::Config(err) => err.code(),
            PipelineError::Core(err) => err.code(),
            PipelineError::Connector(err) => err.code(),
            PipelineError::Wal { .. } => ErrorCode::PipelineWal,
            PipelineError::Stage { code, .. } => *code,
        }
    }
}
