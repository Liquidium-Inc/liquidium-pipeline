use async_trait::async_trait;

#[derive(thiserror::Error, Debug)]
pub enum SecretError {
    #[error("secret not found: {0}")]
    NotFound(String),
    #[error("secret backend error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn get_secret(&self, key: &str) -> Result<String, SecretError>;
}

pub struct EnvSecretStore;

#[async_trait::async_trait]
impl SecretStore for EnvSecretStore {
    async fn get_secret(&self, key: &str) -> Result<String, SecretError> {
        std::env::var(key).map_err(|_| SecretError::NotFound(key.to_string()))
    }
}
