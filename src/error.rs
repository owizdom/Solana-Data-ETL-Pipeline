use thiserror::Error;

pub type Result<T> = std::result::Result<T, ETLError>;

#[derive(Error, Debug)]
pub enum ETLError {
    #[error("RPC error: {0}")]
    RPC(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

