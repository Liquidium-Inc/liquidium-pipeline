use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorDef {
    pub code: u32,
    pub name: &'static str,
    pub message: &'static str,
}

pub mod error_codes {
    use super::ErrorDef;

    // 2xxx: validation and lookup errors
    pub const INVALID_INPUT: ErrorDef = ErrorDef {
        code: 2001,
        name: "INVALID_INPUT",
        message: "Invalid input",
    };
    pub const NOT_FOUND: ErrorDef = ErrorDef {
        code: 2002,
        name: "NOT_FOUND",
        message: "Resource not found",
    };
    pub const UNSUPPORTED: ErrorDef = ErrorDef {
        code: 2003,
        name: "UNSUPPORTED",
        message: "Unsupported operation",
    };

    // 3xxx: external/backend call errors
    pub const EXTERNAL_CALL_FAILED: ErrorDef = ErrorDef {
        code: 3001,
        name: "EXTERNAL_CALL_FAILED",
        message: "External call failed",
    };
    pub const ENCODE_ERROR: ErrorDef = ErrorDef {
        code: 3002,
        name: "ENCODE_ERROR",
        message: "Encoding failed",
    };
    pub const DECODE_ERROR: ErrorDef = ErrorDef {
        code: 3003,
        name: "DECODE_ERROR",
        message: "Decoding failed",
    };

    // 4xxx: configuration and state errors
    pub const CONFIG_ERROR: ErrorDef = ErrorDef {
        code: 4001,
        name: "CONFIG_ERROR",
        message: "Configuration error",
    };

    // 5xxx: domain execution errors
    pub const INSUFFICIENT_FUNDS: ErrorDef = ErrorDef {
        code: 5001,
        name: "INSUFFICIENT_FUNDS",
        message: "Insufficient funds",
    };
    pub const TRANSFER_FAILED: ErrorDef = ErrorDef {
        code: 5002,
        name: "TRANSFER_FAILED",
        message: "Transfer failed",
    };
    pub const SWAP_FAILED: ErrorDef = ErrorDef {
        code: 5003,
        name: "SWAP_FAILED",
        message: "Swap failed",
    };

    // 8xxx: persistence/serialization errors
    pub const PERSISTENCE_ERROR: ErrorDef = ErrorDef {
        code: 8001,
        name: "PERSISTENCE_ERROR",
        message: "Persistence error",
    };
    pub const SERIALIZATION_ERROR: ErrorDef = ErrorDef {
        code: 8002,
        name: "SERIALIZATION_ERROR",
        message: "Serialization error",
    };

    // 9xxx: internal/generic errors
    pub const INTERNAL_ERROR: ErrorDef = ErrorDef {
        code: 9001,
        name: "INTERNAL_ERROR",
        message: "Internal error",
    };
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppError {
    pub code: u32,
    pub name: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn from_def(def: ErrorDef) -> Self {
        Self {
            code: def.code,
            name: def.name.to_string(),
            message: def.message.to_string(),
            context: None,
        }
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn contains(&self, needle: &str) -> bool {
        self.name.contains(needle)
            || self.message.contains(needle)
            || self.context.as_ref().map(|item| item.contains(needle)).unwrap_or(false)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(context) = &self.context {
            write!(
                formatter,
                "{}({}): {} ({})",
                self.name, self.code, self.message, context
            )
        } else {
            write!(formatter, "{}({}): {}", self.name, self.code, self.message)
        }
    }
}

impl std::error::Error for AppError {}

impl From<ErrorDef> for AppError {
    fn from(value: ErrorDef) -> Self {
        Self::from_def(value)
    }
}

impl From<String> for AppError {
    fn from(value: String) -> Self {
        Self::from_def(error_codes::INTERNAL_ERROR).with_context(value)
    }
}

impl From<&str> for AppError {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

impl PartialEq<String> for AppError {
    fn eq(&self, other: &String) -> bool {
        if let Some(context) = &self.context {
            context == other
        } else {
            &self.message == other
        }
    }
}

impl PartialEq<&str> for AppError {
    fn eq(&self, other: &&str) -> bool {
        if let Some(context) = &self.context {
            context == other
        } else {
            self.message == *other
        }
    }
}
