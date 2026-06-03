use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use reqwest::Client;
use serde::Serialize;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::RuntimeConfig,
    errors::AppError,
    prompts::{read_prompt_text, PromptLookup},
    protocol::ResponsesConfig,
    websocket::registry::RetainedUpstreamWebsocketRegistry,
    websocket::upstream::{ResponsesWebsocketConnector, ResponsesWebsocketConnectorFn},
};

const DEBUG_MODEL_ENV: &str = "CHATGPT_LOCAL_DEBUG_MODEL";
const FAST_MODE_ENV: &str = "CHATGPT_LOCAL_FAST_MODE";
const REASONING_EFFORT_ENV: &str = "CHATGPT_LOCAL_REASONING_EFFORT";
const REASONING_SUMMARY_ENV: &str = "CHATGPT_LOCAL_REASONING_SUMMARY";
const ENABLE_WEB_SEARCH_ENV: &str = "CHATGPT_LOCAL_ENABLE_WEB_SEARCH";
const REASONING_COMPAT_ENV: &str = "CHATGPT_LOCAL_REASONING_COMPAT";
const EXPOSE_REASONING_MODELS_ENV: &str = "CHATGPT_LOCAL_EXPOSE_REASONING_MODELS";

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
    let http_client = Client::new();
    let responses_websocket_connector = responses_websocket_connector.or_else(|| {
        config.responses_websocket_upstream.then(|| {
            crate::websocket::upstream::live_responses_websocket_connector(http_client.clone())
        })
    });
    let app = app(build_app_state(
        address,
        &config,
        http_client,
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
    http_client: Client,
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
        reasoning_compat: read_string_env(REASONING_COMPAT_ENV)
            .unwrap_or_else(|| "think-tags".to_string())
            .to_ascii_lowercase(),
        expose_reasoning_models: read_bool_env(EXPOSE_REASONING_MODELS_ENV),
        http_client,
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
        debug_model: read_string_env(DEBUG_MODEL_ENV),
        base_instructions,
        gpt5_codex_instructions,
        reasoning_effort: read_string_env(REASONING_EFFORT_ENV)
            .unwrap_or_else(|| "medium".to_string())
            .to_ascii_lowercase(),
        reasoning_summary: read_string_env(REASONING_SUMMARY_ENV)
            .unwrap_or_else(|| "auto".to_string())
            .to_ascii_lowercase(),
        default_web_search: read_bool_env(ENABLE_WEB_SEARCH_ENV),
        fast_mode: read_bool_env(FAST_MODE_ENV),
        ..ResponsesConfig::default()
    }
}

fn read_string_env(name: &str) -> Option<String> {
    env::var(name).ok()
}

fn read_bool_env(name: &str) -> bool {
    matches!(
        env::var(name)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase()),
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on")
    )
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
    use std::ffi::OsString;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Mutex, OnceLock};

    const SERVER_ENV_VARS: &[&str] = &[
        "CHATGPT_LOCAL_DEBUG_MODEL",
        "CHATGPT_LOCAL_FAST_MODE",
        "CHATGPT_LOCAL_REASONING_EFFORT",
        "CHATGPT_LOCAL_REASONING_SUMMARY",
        "CHATGPT_LOCAL_ENABLE_WEB_SEARCH",
        "CHATGPT_LOCAL_REASONING_COMPAT",
        "CHATGPT_LOCAL_EXPOSE_REASONING_MODELS",
    ];

    struct EnvVarGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvVarGuard {
        fn capture(names: &[&'static str]) -> Self {
            Self {
                saved: names
                    .iter()
                    .map(|name| (*name, std::env::var_os(name)))
                    .collect(),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

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

    #[test]
    fn default_responses_config_uses_phase6_env_overrides() {
        let _guard = env_lock().lock().expect("env lock");
        let _saved = EnvVarGuard::capture(SERVER_ENV_VARS);
        std::env::set_var("CHATGPT_LOCAL_DEBUG_MODEL", "gpt-5.4-debug");
        std::env::set_var("CHATGPT_LOCAL_FAST_MODE", "yes");
        std::env::set_var("CHATGPT_LOCAL_REASONING_EFFORT", "xhigh");
        std::env::set_var("CHATGPT_LOCAL_REASONING_SUMMARY", "detailed");
        std::env::set_var("CHATGPT_LOCAL_ENABLE_WEB_SEARCH", "true");

        let config = super::default_responses_config();

        assert_eq!(config.debug_model.as_deref(), Some("gpt-5.4-debug"));
        assert!(config.fast_mode);
        assert_eq!(config.reasoning_effort, "xhigh");
        assert_eq!(config.reasoning_summary, "detailed");
        assert!(config.default_web_search);
    }

    #[test]
    fn build_app_state_uses_reasoning_env_overrides() {
        let _guard = env_lock().lock().expect("env lock");
        let _saved = EnvVarGuard::capture(SERVER_ENV_VARS);
        std::env::set_var("CHATGPT_LOCAL_REASONING_COMPAT", "legacy");
        std::env::set_var("CHATGPT_LOCAL_EXPOSE_REASONING_MODELS", "true");

        let state = super::build_app_state(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
            &RuntimeConfig::default(),
            reqwest::Client::new(),
            None,
        );

        assert_eq!(state.reasoning_compat, "legacy");
        assert!(state.expose_reasoning_models);
    }
}
