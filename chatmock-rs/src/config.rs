use std::env;
use std::net::{SocketAddr, ToSocketAddrs};

use clap::{Args, Parser, Subcommand};

use crate::errors::ConfigError;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8000;
const HOST_ENV: &str = "CHATMOCK_HOST";
const PORT_ENV: &str = "CHATMOCK_PORT";
const RESPONSES_WEBSOCKET_UPSTREAM_ENV: &str = "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM";
const RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL_ENV: &str =
    "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL";

#[derive(Debug, Parser)]
#[command(name = "chatmock-rs")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Login(LoginArgs),
    Serve(ServeArgs),
    Info(InfoArgs),
}

#[derive(Debug, Clone, Args, Default)]
pub struct LoginArgs {
    #[arg(long)]
    pub no_browser: bool,

    #[arg(long)]
    pub verbose: bool,
}

#[derive(Debug, Clone, Args, Default)]
pub struct InfoArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args, Default)]
pub struct ServeArgs {
    #[arg(long)]
    pub host: Option<String>,

    #[arg(long)]
    pub port: Option<u16>,

    #[arg(long, default_value_t = false)]
    pub responses_websocket_upstream: bool,

    #[arg(
        long = "no-responses-websocket-upstream",
        default_value_t = false,
        conflicts_with = "responses_websocket_upstream"
    )]
    pub no_responses_websocket_upstream: bool,

    #[arg(long, default_value_t = false)]
    pub responses_websocket_upstream_stateful: bool,

    #[arg(
        long = "no-responses-websocket-upstream-stateful",
        default_value_t = false,
        conflicts_with = "responses_websocket_upstream_stateful"
    )]
    pub no_responses_websocket_upstream_stateful: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub host: String,
    pub port: u16,
    pub responses_websocket_upstream: bool,
    pub responses_websocket_upstream_stateful: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            responses_websocket_upstream: false,
            responses_websocket_upstream_stateful: false,
        }
    }
}

impl RuntimeConfig {
    pub fn from_sources(args: ServeArgs) -> Result<Self, ConfigError> {
        let host = args
            .host
            .or_else(|| env::var(HOST_ENV).ok())
            .unwrap_or_else(|| DEFAULT_HOST.to_string());
        let port = match args.port {
            Some(port) => port,
            None => read_port_env(PORT_ENV)?.unwrap_or(DEFAULT_PORT),
        };
        let responses_websocket_upstream = resolve_bool_flag(
            args.responses_websocket_upstream,
            args.no_responses_websocket_upstream,
            RESPONSES_WEBSOCKET_UPSTREAM_ENV,
        )?;
        let responses_websocket_upstream_stateful = resolve_bool_flag(
            args.responses_websocket_upstream_stateful,
            args.no_responses_websocket_upstream_stateful,
            RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL_ENV,
        )?;

        let config = Self {
            host,
            port,
            responses_websocket_upstream,
            responses_websocket_upstream_stateful,
        };

        config.validate()?;
        Ok(config)
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        let bind = format!("{}:{}", self.host, self.port);
        bind.to_socket_addrs()
            .map_err(|_| ConfigError::InvalidBindAddress(bind.clone()))?
            .next()
            .ok_or(ConfigError::InvalidBindAddress(bind))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.responses_websocket_upstream_stateful && !self.responses_websocket_upstream {
            return Err(ConfigError::StatefulRequiresWebsocketUpstream);
        }

        Ok(())
    }
}

fn resolve_bool_flag(
    enabled: bool,
    disabled: bool,
    env_name: &'static str,
) -> Result<bool, ConfigError> {
    if enabled {
        return Ok(true);
    }
    if disabled {
        return Ok(false);
    }
    read_bool_env(env_name)
}

fn read_port_env(name: &'static str) -> Result<Option<u16>, ConfigError> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidEnvVar { name, value })
        })
        .transpose()
}

fn read_bool_env(name: &'static str) -> Result<bool, ConfigError> {
    match env::var(name) {
        Ok(value) => parse_bool_like(name, &value),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(value)) => Err(ConfigError::InvalidEnvVar {
            name,
            value: value.to_string_lossy().into_owned(),
        }),
    }
}

fn parse_bool_like(name: &'static str, value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        _ => Err(ConfigError::InvalidEnvVar {
            name,
            value: value.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeConfig;
    use crate::errors::ConfigError;

    #[test]
    fn stateful_requires_websocket_upstream() {
        let config = RuntimeConfig {
            responses_websocket_upstream: false,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        };

        let err = config.validate().expect_err("validation should fail");
        assert!(matches!(
            err,
            ConfigError::StatefulRequiresWebsocketUpstream
        ));
    }
}
