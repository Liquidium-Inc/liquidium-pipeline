use liquidium_pipeline_commons::error::{CodedError, ErrorCode, ExternalError, format_with_code};
use thiserror::Error;

use crate::tokens::asset_id::AssetId;
use crate::tokens::chain_token::ChainToken;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    TokenRegistry(#[from] TokenRegistryError),
    #[error(transparent)]
    Account(#[from] AccountError),
    #[error(transparent)]
    Transfer(#[from] TransferError),
}

impl CodedError for CoreError {
    fn code(&self) -> ErrorCode {
        match self {
            CoreError::TokenRegistry(err) => err.code(),
            CoreError::Account(err) => err.code(),
            CoreError::Transfer(err) => err.code(),
        }
    }
}

impl From<CoreError> for String {
    fn from(value: CoreError) -> Self {
        format_with_code(&value)
    }
}

#[derive(Debug, Error)]
pub enum TokenRegistryError {
    #[error("unknown asset id: {asset_id}")]
    UnknownAsset { asset_id: AssetId },
}

impl CodedError for TokenRegistryError {
    fn code(&self) -> ErrorCode {
        ErrorCode::PipelineCore
    }
}

impl From<TokenRegistryError> for String {
    fn from(value: TokenRegistryError) -> Self {
        format_with_code(&value)
    }
}

#[derive(Debug, Error)]
pub enum AccountError {
    #[error("account backend error")]
    Backend {
        #[source]
        source: ExternalError,
    },
    #[error("account cache poisoned")]
    CachePoisoned,
    #[error("unsupported token: {token}")]
    UnsupportedToken { token: String },
}

impl AccountError {
    pub fn backend<E>(err: E) -> Self
    where
        E: Into<ExternalError>,
    {
        AccountError::Backend { source: err.into() }
    }
}

impl CodedError for AccountError {
    fn code(&self) -> ErrorCode {
        ErrorCode::PipelineCore
    }
}

impl From<AccountError> for String {
    fn from(value: AccountError) -> Self {
        format_with_code(&value)
    }
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("transfer backend error")]
    Backend {
        #[source]
        source: ExternalError,
    },
    #[error("unknown asset: {asset_id}")]
    UnknownAsset { asset_id: AssetId },
    #[error("unsupported token: {token:?}")]
    UnsupportedToken { token: ChainToken },
    #[error("unsupported destination: {details}")]
    UnsupportedDestination { details: String },
}

impl TransferError {
    pub fn backend<E>(err: E) -> Self
    where
        E: Into<ExternalError>,
    {
        TransferError::Backend { source: err.into() }
    }
}

impl CodedError for TransferError {
    fn code(&self) -> ErrorCode {
        ErrorCode::PipelineCore
    }
}

impl From<TransferError> for String {
    fn from(value: TransferError) -> Self {
        format_with_code(&value)
    }
}
