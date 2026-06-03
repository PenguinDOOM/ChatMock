use std::{
    collections::BTreeMap,
    env,
    future::Future,
    io::{self, BufRead, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::SystemTime,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::random;
use reqwest::Url;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::runtime::{Builder, Handle};

use crate::{
    auth::{
        derive_account_id_from_id_token, format_system_time_as_iso8601, get_home_dir,
        write_auth_file, AuthFile, AuthTokens,
    },
    config::LoginArgs,
    errors::AppError,
};

const DEFAULT_CHATGPT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEFAULT_OAUTH_ISSUER: &str = "https://auth.openai.com";
const DEFAULT_LOGIN_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
const DEFAULT_LOGIN_BIND_HOST: &str = "127.0.0.1";
const LOGIN_CALLBACK_HOST: &str = "localhost";
const LOGIN_CALLBACK_PORT: u16 = 1455;
const LOGIN_CALLBACK_PATH: &str = "/auth/callback";
const LOGIN_SUCCESS_HTML: &str = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\" /><title>Login successful</title></head><body><h1>Login successful</h1><p>You can close this window and return to the terminal.</p></body></html>";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginConfig {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginSeed {
    pub code_verifier: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginSession {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub auth_url: String,
    pub code_verifier: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationCodeRequest {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub code: String,
    pub code_verifier: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeResult {
    pub api_key: Option<String>,
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
    pub account_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PastedRedirectError {
    #[error("invalid redirect URL: {0}")]
    InvalidUrl(String),

    #[error("redirect URL path was not /auth/callback")]
    WrongPath { received: String },

    #[error("redirect URL did not contain an auth code")]
    MissingCode,

    #[error("state mismatch")]
    StateMismatch {
        expected: String,
        received: Option<String>,
    },
}

#[derive(Debug, Error)]
pub enum LoginError {
    #[error("no OAuth client id configured")]
    MissingClientId,

    #[error("no redirect URL was provided")]
    MissingRedirectInput,

    #[error("invalid OAuth configuration: {0}")]
    InvalidConfig(String),

    #[error(transparent)]
    PastedRedirect(#[from] PastedRedirectError),

    #[error("invalid callback request: {0}")]
    InvalidCallbackRequest(String),

    #[error("callback listener was cancelled")]
    CallbackCancelled,

    #[error("token exchange failed: {0}")]
    TokenExchange(String),

    #[error(transparent)]
    Io(#[from] io::Error),
}

pub fn run(args: LoginArgs) -> Result<(), AppError> {
    let stdin = io::BufReader::new(io::stdin());
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    block_on_login(run_with_io(
        &args,
        stdin,
        &mut writer,
        exchange_code_live,
        open_browser_url,
    ))
    .map_err(into_app_error)
}

pub fn run_with_writer<F>(
    args: LoginArgs,
    writer: &mut dyn Write,
    mut open_browser: F,
) -> Result<(), AppError>
where
    F: FnMut(&str) -> io::Result<()>,
{
    let home_dir = resolve_home_dir_from_env();
    let config = login_config_from_env().map_err(into_app_error)?;
    let session = build_login_session(config, random_login_seed()).map_err(into_app_error)?;

    writeln!(writer, "Login flow ready.")?;
    writeln!(writer, "Auth home: {}", home_dir.display())?;
    if args.no_browser {
        writeln!(writer, "Browser launch disabled.")?;
    } else {
        report_browser_launch(writer, &session.auth_url, &mut open_browser)?;
    }
    if args.verbose {
        writeln!(writer, "Verbose logging enabled.")?;
    }
    writeln!(writer, "Navigate to:\n{}", session.auth_url)?;

    Ok(())
}

pub fn build_login_session(
    config: LoginConfig,
    seed: LoginSeed,
) -> Result<LoginSession, LoginError> {
    if config.client_id.trim().is_empty() {
        return Err(LoginError::MissingClientId);
    }
    if config.issuer.trim().is_empty() {
        return Err(LoginError::InvalidConfig(
            "issuer cannot be empty".to_string(),
        ));
    }
    if config.redirect_uri.trim().is_empty() {
        return Err(LoginError::InvalidConfig(
            "redirect URI cannot be empty".to_string(),
        ));
    }
    if seed.code_verifier.trim().is_empty() {
        return Err(LoginError::InvalidConfig(
            "code verifier cannot be empty".to_string(),
        ));
    }
    if seed.state.trim().is_empty() {
        return Err(LoginError::InvalidConfig(
            "state cannot be empty".to_string(),
        ));
    }

    let issuer = normalize_env_value(Some(config.issuer.as_str()))
        .ok_or_else(|| LoginError::InvalidConfig("issuer cannot be empty".to_string()))?;
    let client_id =
        normalize_env_value(Some(config.client_id.as_str())).ok_or(LoginError::MissingClientId)?;
    let redirect_uri = normalize_env_value(Some(config.redirect_uri.as_str()))
        .ok_or_else(|| LoginError::InvalidConfig("redirect URI cannot be empty".to_string()))?;
    let state = normalize_env_value(Some(seed.state.as_str()))
        .ok_or_else(|| LoginError::InvalidConfig("state cannot be empty".to_string()))?;
    let code_verifier = normalize_env_value(Some(seed.code_verifier.as_str()))
        .ok_or_else(|| LoginError::InvalidConfig("code verifier cannot be empty".to_string()))?;

    let code_challenge = build_pkce_code_challenge(&code_verifier);
    let authorize_url = format!("{}/oauth/authorize", issuer.trim_end_matches('/'));
    let auth_url = Url::parse_with_params(
        &authorize_url,
        [
            ("response_type", "code"),
            ("client_id", client_id.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "openid profile email offline_access"),
            ("code_challenge", code_challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
            ("state", state.as_str()),
        ],
    )
    .map_err(|error| LoginError::InvalidConfig(error.to_string()))?;

    Ok(LoginSession {
        issuer,
        client_id,
        redirect_uri,
        auth_url: auth_url.into(),
        code_verifier,
        state,
    })
}

pub fn parse_pasted_redirect_url(
    pasted_redirect_url: &str,
    expected_state: &str,
) -> Result<String, PastedRedirectError> {
    let url = Url::parse(pasted_redirect_url.trim())
        .map_err(|error| PastedRedirectError::InvalidUrl(error.to_string()))?;
    if url.path() != LOGIN_CALLBACK_PATH {
        return Err(PastedRedirectError::WrongPath {
            received: url.path().to_string(),
        });
    }

    let mut code = None;
    let mut state = None;
    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "code" if code.is_none() => code = Some(value.into_owned()),
            "state" if state.is_none() => state = Some(value.into_owned()),
            _ => {}
        }
    }

    let code = code.ok_or(PastedRedirectError::MissingCode)?;
    if state.as_deref() != Some(expected_state) {
        return Err(PastedRedirectError::StateMismatch {
            expected: expected_state.to_string(),
            received: state,
        });
    }

    Ok(code)
}

pub fn login_bind_host_from_env() -> String {
    env::var("CHATGPT_LOCAL_LOGIN_BIND")
        .ok()
        .and_then(|value| normalize_env_value(Some(&value)))
        .unwrap_or_else(|| DEFAULT_LOGIN_BIND_HOST.to_string())
}

pub fn wait_for_local_callback(
    bind_host: &str,
    expected_state: &str,
) -> Result<String, LoginError> {
    let cancel = Arc::new(AtomicBool::new(false));
    wait_for_local_callback_until_stopped(bind_host, expected_state, cancel)
}

pub async fn complete_login_with_exchange<F, Fut>(
    home_dir: &Path,
    session: &LoginSession,
    pasted_redirect_url: &str,
    exchange: F,
) -> Result<PathBuf, LoginError>
where
    F: FnOnce(AuthorizationCodeRequest) -> Fut,
    Fut: Future<Output = Result<ExchangeResult, LoginError>>,
{
    let code = parse_pasted_redirect_url(pasted_redirect_url, &session.state)?;
    let request = AuthorizationCodeRequest {
        issuer: session.issuer.clone(),
        client_id: session.client_id.clone(),
        redirect_uri: session.redirect_uri.clone(),
        code,
        code_verifier: session.code_verifier.clone(),
    };
    let exchanged = exchange(request).await?;

    let auth_file = AuthFile {
        openai_api_key: normalize_env_value(exchanged.api_key.as_deref()),
        tokens: Some(AuthTokens {
            access_token: Some(exchanged.access_token),
            account_id: Some(exchanged.account_id),
            id_token: Some(exchanged.id_token),
            refresh_token: Some(exchanged.refresh_token),
            extra: BTreeMap::new(),
        }),
        last_refresh: Some(Value::String(format_system_time_as_iso8601(
            SystemTime::now(),
        ))),
        extra: BTreeMap::new(),
    };

    write_auth_file(home_dir, &auth_file).map_err(LoginError::Io)
}

fn resolve_home_dir_from_env() -> PathBuf {
    let chatgpt_local_home = env_path("CHATGPT_LOCAL_HOME");
    let codex_home = env_path("CODEX_HOME");
    let user_home = env_path("USERPROFILE").or_else(|| env_path("HOME"));
    get_home_dir(
        chatgpt_local_home.as_deref(),
        codex_home.as_deref(),
        user_home.as_deref(),
    )
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}

async fn run_with_io<R, W, F, Fut>(
    args: &LoginArgs,
    reader: R,
    writer: &mut W,
    exchange: F,
    mut open_browser: impl FnMut(&str) -> io::Result<()>,
) -> Result<(), LoginError>
where
    R: BufRead + Send + 'static,
    W: Write,
    F: FnOnce(AuthorizationCodeRequest) -> Fut,
    Fut: Future<Output = Result<ExchangeResult, LoginError>>,
{
    let home_dir = resolve_home_dir_from_env();
    let session = build_login_session(login_config_from_env()?, random_login_seed())?;
    let bind_host = login_bind_host_from_env();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let (redirect_tx, redirect_rx) = mpsc::channel::<Result<String, LoginError>>();

    let callback_thread = {
        let bind_host = bind_host.clone();
        let expected_state = session.state.clone();
        let stop_requested = Arc::clone(&stop_requested);
        let redirect_tx = redirect_tx.clone();
        thread::spawn(move || {
            let result =
                wait_for_local_callback_until_stopped(&bind_host, &expected_state, stop_requested);
            if !matches!(result, Err(LoginError::CallbackCancelled)) {
                let _ = redirect_tx.send(result);
            }
        })
    };

    let pasted_state = session.state.clone();
    let pasted_redirect_tx = redirect_tx.clone();
    thread::spawn(move || {
        let mut reader = reader;
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(_) => {
                let pasted_redirect_url = line.trim().to_string();
                if pasted_redirect_url.is_empty() {
                    return;
                }

                let result = parse_pasted_redirect_url(&pasted_redirect_url, &pasted_state)
                    .map(|_| pasted_redirect_url)
                    .map_err(LoginError::from);
                let _ = pasted_redirect_tx.send(result);
            }
            Err(error) => {
                let _ = pasted_redirect_tx.send(Err(LoginError::Io(error)));
            }
        }
    });
    drop(redirect_tx);

    writeln!(writer, "Login flow ready.")?;
    writeln!(writer, "Auth home: {}", home_dir.display())?;
    if args.no_browser {
        writeln!(writer, "Browser launch disabled.")?;
    } else {
        report_browser_launch(writer, &session.auth_url, &mut open_browser)?;
    }
    if args.verbose {
        writeln!(writer, "Verbose logging enabled.")?;
    }
    writeln!(
        writer,
        "Starting local login server on http://localhost:1455"
    )?;
    writeln!(writer, "Navigate to:\n{}", session.auth_url)?;
    writeln!(
        writer,
        "If the browser can't reach this machine, paste the full redirect URL here and press Enter (or leave blank to keep waiting):"
    )?;
    writer.flush()?;

    let redirect_url = redirect_rx
        .recv()
        .map_err(|_| LoginError::MissingRedirectInput)??;

    stop_requested.store(true, Ordering::Relaxed);
    let _ = callback_thread.join();

    let path = complete_login_with_exchange(&home_dir, &session, &redirect_url, exchange).await?;
    writeln!(writer, "Saved auth to {}", path.display())?;
    Ok(())
}

fn login_config_from_env() -> Result<LoginConfig, LoginError> {
    let client_id = env::var("CHATGPT_LOCAL_CLIENT_ID")
        .ok()
        .and_then(|value| normalize_env_value(Some(&value)))
        .unwrap_or_else(|| DEFAULT_CHATGPT_CLIENT_ID.to_string());
    let issuer = env::var("CHATGPT_LOCAL_ISSUER")
        .ok()
        .and_then(|value| normalize_env_value(Some(&value)))
        .unwrap_or_else(|| DEFAULT_OAUTH_ISSUER.to_string());

    if client_id.trim().is_empty() {
        return Err(LoginError::MissingClientId);
    }

    Ok(LoginConfig {
        issuer,
        client_id,
        redirect_uri: DEFAULT_LOGIN_REDIRECT_URI.to_string(),
    })
}

fn random_login_seed() -> LoginSeed {
    LoginSeed {
        code_verifier: base64url_encode(&random::<[u8; 32]>()),
        state: base64url_encode(&random::<[u8; 32]>()),
    }
}

fn build_pkce_code_challenge(code_verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(code_verifier.as_bytes());
    let digest = hasher.finalize();
    URL_SAFE_NO_PAD.encode(digest)
}

fn base64url_encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

fn normalize_env_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn report_browser_launch<W, F>(
    writer: &mut W,
    auth_url: &str,
    open_browser: &mut F,
) -> io::Result<()>
where
    W: Write + ?Sized,
    F: FnMut(&str) -> io::Result<()>,
{
    match open_browser(auth_url) {
        Ok(()) => writeln!(writer, "Opened the login URL in your browser."),
        Err(error) => {
            writeln!(writer, "Failed to open browser automatically: {error}")?;
            writeln!(writer, "Open the following URL in your browser.")
        }
    }
}

fn open_browser_url(auth_url: &str) -> io::Result<()> {
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = std::process::Command::new("rundll32");
        command.arg("url.dll,FileProtocolHandler").arg(auth_url);
        command
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = std::process::Command::new("open");
        command.arg(auth_url);
        command
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = std::process::Command::new("xdg-open");
        command.arg(auth_url);
        command
    };

    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "browser opener exited with status {status}"
        )))
    }
}

fn into_app_error(error: LoginError) -> AppError {
    AppError::Io(io::Error::other(error.to_string()))
}

fn block_on_login<Fut>(future: Fut) -> Result<(), LoginError>
where
    Fut: Future<Output = Result<(), LoginError>>,
{
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(LoginError::Io)?
            .block_on(future),
    }
}

async fn exchange_code_live(
    request: AuthorizationCodeRequest,
) -> Result<ExchangeResult, LoginError> {
    let client = reqwest::Client::new();
    let token_url = format!("{}/oauth/token", request.issuer.trim_end_matches('/'));
    let payload: serde_json::Value = client
        .post(token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", request.code.as_str()),
            ("redirect_uri", request.redirect_uri.as_str()),
            ("client_id", request.client_id.as_str()),
            ("code_verifier", request.code_verifier.as_str()),
        ])
        .send()
        .await
        .map_err(|error| LoginError::TokenExchange(error.to_string()))?
        .error_for_status()
        .map_err(|error| LoginError::TokenExchange(error.to_string()))?
        .json()
        .await
        .map_err(|error| LoginError::TokenExchange(error.to_string()))?;

    let id_token = payload
        .get("id_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    let account_id = payload
        .get("account_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| derive_account_id_from_id_token(&id_token))
        .ok_or_else(|| {
            LoginError::TokenExchange("token response did not include an account id".to_string())
        })?;

    Ok(ExchangeResult {
        api_key: payload
            .get("OPENAI_API_KEY")
            .and_then(Value::as_str)
            .map(str::to_string),
        access_token: payload
            .get("access_token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                LoginError::TokenExchange("token response did not include access_token".to_string())
            })?,
        refresh_token: payload
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                LoginError::TokenExchange(
                    "token response did not include refresh_token".to_string(),
                )
            })?,
        id_token,
        account_id,
    })
}

fn wait_for_local_callback_until_stopped(
    bind_host: &str,
    expected_state: &str,
    stop_requested: Arc<AtomicBool>,
) -> Result<String, LoginError> {
    let listener = TcpListener::bind((bind_host, LOGIN_CALLBACK_PORT))?;
    listener.set_nonblocking(true)?;

    loop {
        if stop_requested.load(Ordering::Relaxed) {
            return Err(LoginError::CallbackCancelled);
        }

        match listener.accept() {
            Ok((stream, _)) => return handle_callback_connection(stream, expected_state),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(error) => return Err(LoginError::Io(error)),
        }
    }
}

fn handle_callback_connection(
    mut stream: TcpStream,
    expected_state: &str,
) -> Result<String, LoginError> {
    let mut request_line = String::new();
    {
        let mut reader = io::BufReader::new(&mut stream);
        reader.read_line(&mut request_line)?;
    }

    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| LoginError::InvalidCallbackRequest("missing HTTP method".to_string()))?;
    let request_target = parts
        .next()
        .ok_or_else(|| LoginError::InvalidCallbackRequest("missing request target".to_string()))?;

    if method != "GET" {
        write_http_response(
            &mut stream,
            404,
            "Not Found",
            "Not Found",
            "text/plain; charset=utf-8",
        )?;
        return Err(LoginError::InvalidCallbackRequest(format!(
            "unsupported callback method: {method}"
        )));
    }

    let redirect_url =
        format!("http://{LOGIN_CALLBACK_HOST}:{LOGIN_CALLBACK_PORT}{request_target}");
    match parse_pasted_redirect_url(&redirect_url, expected_state) {
        Ok(_) => {
            write_http_response(
                &mut stream,
                200,
                "OK",
                LOGIN_SUCCESS_HTML,
                "text/html; charset=utf-8",
            )?;
            Ok(redirect_url)
        }
        Err(error @ PastedRedirectError::WrongPath { .. }) => {
            write_http_response(
                &mut stream,
                404,
                "Not Found",
                "Not Found",
                "text/plain; charset=utf-8",
            )?;
            Err(LoginError::PastedRedirect(error))
        }
        Err(error) => {
            let message = error.to_string();
            write_http_response(
                &mut stream,
                400,
                "Bad Request",
                &message,
                "text/plain; charset=utf-8",
            )?;
            Err(LoginError::PastedRedirect(error))
        }
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    status_code: u16,
    reason: &str,
    body: &str,
    content_type: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status_code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}
