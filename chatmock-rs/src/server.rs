use std::net::SocketAddr;

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{Cli, Command, RuntimeConfig},
    errors::AppError,
};

#[derive(Debug, Clone)]
struct AppState {
    bind_address: SocketAddr,
}

#[derive(Debug, Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
    bind_address: String,
}

pub fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

pub async fn run_cli(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::Serve(args) => {
            let config = RuntimeConfig::from_sources(args)?;
            run(config).await
        }
    }
}

pub async fn run(config: RuntimeConfig) -> Result<(), AppError> {
    let listener = bind(&config).await?;
    serve(listener).await
}

pub async fn bind(config: &RuntimeConfig) -> Result<TcpListener, AppError> {
    let addr = validated_bind_address(config)?;
    Ok(TcpListener::bind(addr).await?)
}

fn validated_bind_address(config: &RuntimeConfig) -> Result<SocketAddr, AppError> {
    // RuntimeConfig can also be constructed directly in tests or future callers, so
    // binding keeps the startup validation gate instead of assuming from_sources().
    config.validate()?;
    Ok(config.socket_addr()?)
}

pub async fn serve(listener: TcpListener) -> Result<(), AppError> {
    let address = listener.local_addr()?;
    let app = app(address);
    axum::serve(listener, app).await?;
    Ok(())
}

fn app(bind_address: SocketAddr) -> Router {
    Router::new()
        .route("/health", get(health))
        .with_state(AppState { bind_address })
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        bind_address: state.bind_address.to_string(),
    })
}

pub struct RunningServer {
    pub address: SocketAddr,
    task: JoinHandle<Result<(), AppError>>,
}

impl RunningServer {
    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn spawn_server(config: RuntimeConfig) -> Result<RunningServer, AppError> {
    let listener = bind(&config).await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { serve(listener).await });
    Ok(RunningServer { address, task })
}

#[cfg(test)]
mod tests {
    use super::bind;
    use crate::{AppError, ConfigError, RuntimeConfig};

    #[tokio::test]
    async fn bind_rejects_invalid_programmatic_stateful_config() {
        let config = RuntimeConfig {
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        };

        let err = bind(&config)
            .await
            .expect_err("bind should validate config");
        assert!(matches!(
            err,
            AppError::Config(ConfigError::StatefulRequiresWebsocketUpstream)
        ));
    }
}
