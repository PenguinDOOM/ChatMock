use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::{Mutex, OnceLock};

use chatmock_rs::auth::{write_auth_file, AuthFile, AuthTokens};
use chatmock_rs::config::{Cli, Command, LoginArgs, RuntimeConfig, ServeArgs};
use clap::{CommandFactory, Parser};
use serde_json::json;
use tempfile::TempDir;

const CLI_ENV_VARS: &[&str] = &[
    "CHATGPT_LOCAL_HOME",
    "CODEX_HOME",
    "HOME",
    "USERPROFILE",
    "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM",
    "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL",
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

#[test]
fn cli_parses_login_info_and_serve_subcommands() {
    let login = Cli::try_parse_from(["chatmock-rs", "login", "--no-browser", "--verbose"])
        .expect("login cli should parse");
    assert!(matches!(
        login.command,
        Command::Login(args) if args.no_browser && args.verbose
    ));

    let info =
        Cli::try_parse_from(["chatmock-rs", "info", "--json"]).expect("info cli should parse");
    assert!(matches!(info.command, Command::Info(args) if args.json));

    let serve = Cli::try_parse_from([
        "chatmock-rs",
        "serve",
        "--responses-websocket-upstream",
        "--responses-websocket-upstream-stateful",
    ])
    .expect("serve cli should parse");
    assert!(matches!(
        serve.command,
        Command::Serve(args)
            if args.responses_websocket_upstream
                && args.responses_websocket_upstream_stateful
    ));
}

#[test]
fn serve_help_lists_boolean_positive_and_negative_flags() {
    let help = Cli::command()
        .find_subcommand_mut("serve")
        .expect("serve subcommand")
        .render_help()
        .to_string();

    assert!(help.contains("--responses-websocket-upstream"));
    assert!(help.contains("--no-responses-websocket-upstream"));
    assert!(help.contains("--responses-websocket-upstream-stateful"));
    assert!(help.contains("--no-responses-websocket-upstream-stateful"));
}

#[test]
fn login_help_is_available() {
    let help = Cli::command()
        .find_subcommand_mut("login")
        .expect("login subcommand")
        .render_help()
        .to_string();

    assert!(help.contains("--no-browser"));
    assert!(help.contains("--verbose"));
}

#[test]
fn login_run_reports_resolved_home_and_flags() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    let temp = TempDir::new().expect("tempdir");
    std::env::set_var("CHATGPT_LOCAL_HOME", temp.path());
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("HOME");
    std::env::remove_var("USERPROFILE");

    let mut output = Vec::new();
    chatmock_rs::login::run_with_writer(
        LoginArgs {
            no_browser: true,
            verbose: true,
        },
        &mut output,
        |_| Ok(()),
    )
    .expect("login should run");

    let output = String::from_utf8(output).expect("utf8 output");
    assert!(output.contains("Auth home: "));
    assert!(output.contains(&temp.path().display().to_string()));
    assert!(output.contains("Browser launch disabled."));
    assert!(output.contains("Verbose logging enabled."));
    assert!(!output.contains("not implemented yet"));
}

#[test]
fn login_run_browser_enabled_uses_injected_opener_and_prints_auth_url() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    let temp = TempDir::new().expect("tempdir");
    std::env::set_var("CHATGPT_LOCAL_HOME", temp.path());
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("HOME");
    std::env::remove_var("USERPROFILE");

    let mut output = Vec::new();
    let mut opened_url = None;
    chatmock_rs::login::run_with_writer(
        LoginArgs {
            no_browser: false,
            verbose: false,
        },
        &mut output,
        |auth_url| {
            opened_url = Some(auth_url.to_string());
            Ok(())
        },
    )
    .expect("login should run");

    let output = String::from_utf8(output).expect("utf8 output");
    let opened_url = opened_url.expect("browser opener should be called");

    assert!(output.contains("Opened the login URL in your browser."));
    assert!(output.contains("Navigate to:\n"));
    assert!(output.contains(&opened_url));
}

#[test]
fn login_run_browser_open_failure_reports_error_and_auth_url() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    let temp = TempDir::new().expect("tempdir");
    std::env::set_var("CHATGPT_LOCAL_HOME", temp.path());
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("HOME");
    std::env::remove_var("USERPROFILE");

    let mut output = Vec::new();
    chatmock_rs::login::run_with_writer(
        LoginArgs {
            no_browser: false,
            verbose: false,
        },
        &mut output,
        |_| Err(std::io::Error::other("browser failed")),
    )
    .expect("login should run");

    let output = String::from_utf8(output).expect("utf8 output");

    assert!(output.contains("Failed to open browser automatically: browser failed"));
    assert!(output.contains("Navigate to:\n"));
    assert!(output.contains("https://"));
}

#[test]
fn runtime_config_uses_env_fallback_for_websocket_flags() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    std::env::set_var("CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM", "yes");
    std::env::set_var(
        "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL",
        "true",
    );

    let config = RuntimeConfig::from_sources(ServeArgs::default()).expect("config from env");

    assert!(config.responses_websocket_upstream);
    assert!(config.responses_websocket_upstream_stateful);
}

#[test]
fn runtime_config_explicit_negative_flags_override_env() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    std::env::set_var("CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM", "true");
    std::env::set_var(
        "CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL",
        "true",
    );

    let config = RuntimeConfig::from_sources(ServeArgs {
        no_responses_websocket_upstream: true,
        no_responses_websocket_upstream_stateful: true,
        ..ServeArgs::default()
    })
    .expect("config from cli override");

    assert!(!config.responses_websocket_upstream);
    assert!(!config.responses_websocket_upstream_stateful);
}

#[test]
fn info_json_reads_auth_from_env_home() {
    let _guard = env_lock().lock().expect("env lock");
    let _saved = EnvVarGuard::capture(CLI_ENV_VARS);
    let temp = TempDir::new().expect("tempdir");
    std::env::set_var("CHATGPT_LOCAL_HOME", temp.path());
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("HOME");
    std::env::remove_var("USERPROFILE");

    let auth = AuthFile {
        openai_api_key: Some("sk-test".to_string()),
        tokens: Some(AuthTokens {
            access_token: Some("access-token".to_string()),
            account_id: Some("account-123".to_string()),
            id_token: Some("id-token".to_string()),
            refresh_token: Some("refresh-token".to_string()),
            extra: BTreeMap::from([(String::from("tenant"), json!("chatgpt"))]),
        }),
        last_refresh: Some(json!(12345)),
        extra: BTreeMap::new(),
    };
    write_auth_file(temp.path(), &auth).expect("write auth file");

    let output = chatmock_rs::info::read_auth_json_from_env().expect("info json");
    let actual: serde_json::Value = serde_json::from_str(&output).expect("json output");

    assert_eq!(actual["OPENAI_API_KEY"], json!("sk-test"));
    assert_eq!(actual["tokens"]["access_token"], json!("access-token"));
    assert_eq!(actual["tokens"]["account_id"], json!("account-123"));
    assert_eq!(actual["tokens"]["tenant"], json!("chatgpt"));
}
