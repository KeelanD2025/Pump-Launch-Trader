use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuantError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("time parse error: {0}")]
    Time(#[from] time::error::Parse),
    #[error("config error: {0}")]
    Config(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("budget denied: {0}")]
    BudgetDenied(String),
}

pub type Result<T> = std::result::Result<T, QuantError>;
