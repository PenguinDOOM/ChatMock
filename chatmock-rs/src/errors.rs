use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid environment variable {name}: {value}")]
    InvalidEnvVar { name: &'static str, value: String },

    #[error("invalid bind address: {0}")]
    InvalidBindAddress(String),

    #[error("RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL requires RESPONSES_WEBSOCKET_UPSTREAM")]
    StatefulRequiresWebsocketUpstream,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
