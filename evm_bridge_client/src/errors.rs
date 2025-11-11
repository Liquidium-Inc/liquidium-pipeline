// Error types for EVM operations

#[derive(thiserror::Error, Debug)]
pub enum EvmError {
    #[error("rpc: {0}")]
    Rpc(String),

    #[error("signing: {0}")]
    Signing(String),

    #[error("revert: {0}")]
    Revert(String),

    #[error("timeout")]
    Timeout,

    #[error("dropped: {0}")]
    Dropped(String),

    #[error("reorg")]
    Reorg,

    #[error("other: {0}")]
    Other(String),
}

impl From<alloy::transports::RpcError<alloy::transports::TransportErrorKind>> for EvmError {
    fn from(e: alloy::transports::RpcError<alloy::transports::TransportErrorKind>) -> Self {
        EvmError::Rpc(e.to_string())
    }
}

impl From<alloy::contract::Error> for EvmError {
    fn from(e: alloy::contract::Error) -> Self {
        EvmError::Other(e.to_string())
    }
}

impl From<alloy::primitives::ruint::ParseError> for EvmError {
    fn from(e: alloy::primitives::ruint::ParseError) -> Self {
        EvmError::Other(e.to_string())
    }
}

impl From<alloy::hex::FromHexError> for EvmError {
    fn from(e: alloy::hex::FromHexError) -> Self {
        EvmError::Other(e.to_string())
    }
}
