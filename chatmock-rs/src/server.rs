use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use reqwest::Client;
use serde::Serialize;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{Cli, Command, RuntimeConfig},
    errors::AppError,
    prompts::{read_prompt_text, PromptLookup},
    protocol::ResponsesConfig,
    websocket::registry::RetainedUpstreamWebsocketRegistry,
    websocket::upstream::{ResponsesWebsocketConnector, ResponsesWebsocketConnectorFn},
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) bind_address: SocketAddr,
    pub(crate) responses_config: ResponsesConfig,
    pub(crate) reasoning_compat: String,
    pub(crate) expose_reasoning_models: bool,
    pub(crate) http_client: Client,
    pub(crate) responses_websocket_connector: Option<ResponsesWebsocketConnector>,
    pub(crate) responses_websocket_registry: Option<
        Arc<RetainedUpstreamWebsocketRegistry<crate::websocket::upstream::SharedUpstreamWebsocket>>,
    >,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppState")
            .field("bind_address", &self.bind_address)
            .field("responses_config", &self.responses_config)
            .field("reasoning_compat", &self.reasoning_compat)
            .field("expose_reasoning_models", &self.expose_reasoning_models)
            .field("http_client", &self.http_client)
            .field(
                "responses_websocket_connector",
                &self.responses_websocket_connector,
            )
            .field(
                "responses_websocket_registry",
                &self.responses_websocket_registry,
            )
            .finish()
    }
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
    serve_with_config(listener, config, None).await
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
    serve_with_config(listener, RuntimeConfig::default(), None).await
}

async fn serve_with_config(
    listener: TcpListener,
    config: RuntimeConfig,
    responses_websocket_connector: Option<ResponsesWebsocketConnector>,
) -> Result<(), AppError> {
    let address = listener.local_addr()?;
    let app = app(build_app_state(
        address,
        &config,
        responses_websocket_connector,
    ));
    axum::serve(listener, app).await?;
    Ok(())
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(crate::routes::openai_router())
        .with_state(state)
}

fn build_app_state(
    bind_address: SocketAddr,
    config: &RuntimeConfig,
    responses_websocket_connector: Option<ResponsesWebsocketConnector>,
) -> AppState {
    let responses_config = default_responses_config();
    let responses_websocket_registry = if config.responses_websocket_upstream_stateful
        && responses_websocket_connector.is_some()
    {
        Some(Arc::new(RetainedUpstreamWebsocketRegistry::new(64)))
    } else {
        None
    };
    AppState {
        bind_address,
        responses_config,
        reasoning_compat: "think-tags".to_string(),
        expose_reasoning_models: false,
        http_client: Client::new(),
        responses_websocket_connector,
        responses_websocket_registry,
    }
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        bind_address: state.bind_address.to_string(),
    })
}

fn default_responses_config() -> ResponsesConfig {
    let lookup = PromptLookup {
        repo_root: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR"))),
        module_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        meipass_dir: env::var_os("_MEIPASS").map(PathBuf::from),
        cwd: env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    };
    let base_instructions = read_prompt_text("prompt.md", &lookup);
    let gpt5_codex_instructions =
        read_prompt_text("prompt_gpt5_codex.md", &lookup).or_else(|| base_instructions.clone());

    ResponsesConfig {
        base_instructions,
        gpt5_codex_instructions,
        ..ResponsesConfig::default()
    }
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
    let task = tokio::spawn(async move { serve_with_config(listener, config, None).await });
    Ok(RunningServer { address, task })
}

pub async fn spawn_server_with_websocket_connector(
    config: RuntimeConfig,
    connector: std::sync::Arc<ResponsesWebsocketConnectorFn>,
) -> Result<RunningServer, AppError> {
    let listener = bind(&config).await?;
    let address = listener.local_addr()?;
    let connector = ResponsesWebsocketConnector::new(connector);
    let task =
        tokio::spawn(async move { serve_with_config(listener, config, Some(connector)).await });
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
