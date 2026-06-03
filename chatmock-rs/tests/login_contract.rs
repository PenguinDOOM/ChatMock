use std::{
    fs,
    io::{Read, Write},
    net::TcpStream,
    sync::{Mutex, OnceLock},
    thread,
    time::Duration,
};

use chatmock_rs::login::{
    build_login_session, complete_login_with_exchange, login_bind_host_from_env,
    parse_pasted_redirect_url, wait_for_local_callback, AuthorizationCodeRequest, ExchangeResult,
    LoginConfig, LoginSeed, PastedRedirectError,
};
use reqwest::Url;
use serde_json::Value;
use tempfile::TempDir;

fn login_test_guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}

fn connect_when_ready(bind_host: &str) -> TcpStream {
    let address = format!("{bind_host}:1455");
    for _ in 0..50 {
        match TcpStream::connect(&address) {
            Ok(stream) => return stream,
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }

    TcpStream::connect(address).expect("connect to login callback listener")
}

fn send_callback_request(
    bind_host: &str,
    request_target: &str,
) -> (String, Result<String, chatmock_rs::login::LoginError>) {
    let listener = thread::spawn({
        let bind_host = bind_host.to_string();
        move || wait_for_local_callback(&bind_host, "expected-state")
    });

    let mut stream = connect_when_ready(bind_host);
    write!(
        stream,
        "GET {request_target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write callback request");
    stream.flush().expect("flush callback request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read callback response");

    let result = listener.join().expect("join callback listener");
    (response, result)
}

#[test]
fn auth_url_contains_pkce_and_state() {
    let session = build_login_session(
        LoginConfig {
            issuer: "https://auth.openai.com".to_string(),
            client_id: "client-123".to_string(),
            redirect_uri: "http://localhost:1455/auth/callback".to_string(),
        },
        LoginSeed {
            code_verifier: "verifierverifierverifierverifierverifierver".to_string(),
            state: "state-value-123".to_string(),
        },
    )
    .expect("build session");

    assert_eq!(session.state, "state-value-123");
    assert_eq!(
        session.code_verifier,
        "verifierverifierverifierverifierverifierver"
    );

    let url = Url::parse(&session.auth_url).expect("auth url");
    let params = url
        .query_pairs()
        .into_owned()
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(
        url.as_str().split('?').next(),
        Some("https://auth.openai.com/oauth/authorize")
    );
    assert_eq!(
        params.get("response_type").map(String::as_str),
        Some("code")
    );
    assert_eq!(
        params.get("client_id").map(String::as_str),
        Some("client-123")
    );
    assert_eq!(
        params.get("redirect_uri").map(String::as_str),
        Some("http://localhost:1455/auth/callback")
    );
    assert_eq!(
        params.get("scope").map(String::as_str),
        Some("openid profile email offline_access")
    );
    assert_eq!(
        params.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert_eq!(
        params.get("id_token_add_organizations").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        params.get("codex_cli_simplified_flow").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        params.get("state").map(String::as_str),
        Some("state-value-123")
    );
    assert!(params
        .get("code_challenge")
        .is_some_and(|value| !value.is_empty()));
    assert!(!session.auth_url.contains(&session.code_verifier));
}

#[test]
fn pasted_redirect_rejects_missing_code() {
    let error = parse_pasted_redirect_url(
        "http://localhost:1455/auth/callback?state=expected-state",
        "expected-state",
    )
    .expect_err("missing code should fail");

    assert!(matches!(error, PastedRedirectError::MissingCode));
}

#[test]
fn pasted_redirect_rejects_wrong_path() {
    let error = parse_pasted_redirect_url(
        "http://localhost:1455/not-callback?code=auth-code-123&state=expected-state",
        "expected-state",
    )
    .expect_err("wrong path should fail");

    assert!(matches!(
        error,
        PastedRedirectError::WrongPath { received } if received == "/not-callback"
    ));
}

#[test]
fn pasted_redirect_rejects_mismatched_state() {
    let error = parse_pasted_redirect_url(
        "http://localhost:1455/auth/callback?code=auth-code-123&state=wrong-state",
        "expected-state",
    )
    .expect_err("state mismatch should fail");

    assert!(matches!(
        error,
        PastedRedirectError::StateMismatch {
            expected,
            received: Some(received),
        } if expected == "expected-state" && received == "wrong-state"
    ));
}

#[test]
fn login_bind_host_defaults_to_localhost() {
    let _guard = login_test_guard().lock().expect("login test guard");
    let original = std::env::var("CHATGPT_LOCAL_LOGIN_BIND").ok();
    std::env::remove_var("CHATGPT_LOCAL_LOGIN_BIND");

    let bind_host = login_bind_host_from_env();

    match original {
        Some(value) => std::env::set_var("CHATGPT_LOCAL_LOGIN_BIND", value),
        None => std::env::remove_var("CHATGPT_LOCAL_LOGIN_BIND"),
    }

    assert_eq!(bind_host, "127.0.0.1");
}

#[test]
fn callback_listener_accepts_auth_callback() {
    let _guard = login_test_guard().lock().expect("login test guard");
    let (response, result) = send_callback_request(
        "127.0.0.1",
        "/auth/callback?code=auth-code-123&state=expected-state",
    );

    assert!(response.starts_with("HTTP/1.1 200 OK") || response.starts_with("HTTP/1.0 200 OK"));
    assert_eq!(
        result.expect("callback result"),
        "http://localhost:1455/auth/callback?code=auth-code-123&state=expected-state"
    );
}

#[test]
fn callback_listener_rejects_missing_code() {
    let _guard = login_test_guard().lock().expect("login test guard");
    let (response, result) =
        send_callback_request("127.0.0.1", "/auth/callback?state=expected-state");

    assert!(response.starts_with("HTTP/1.1 400") || response.starts_with("HTTP/1.0 400"));
    assert!(matches!(
        result.expect_err("missing code should fail"),
        chatmock_rs::login::LoginError::PastedRedirect(PastedRedirectError::MissingCode)
    ));
}

#[test]
fn callback_listener_rejects_wrong_path() {
    let _guard = login_test_guard().lock().expect("login test guard");
    let (response, result) = send_callback_request(
        "127.0.0.1",
        "/wrong-path?code=auth-code-123&state=expected-state",
    );

    assert!(response.starts_with("HTTP/1.1 404") || response.starts_with("HTTP/1.0 404"));
    assert!(matches!(
        result.expect_err("wrong path should fail"),
        chatmock_rs::login::LoginError::PastedRedirect(PastedRedirectError::WrongPath { received })
        if received == "/wrong-path"
    ));
}

#[tokio::test]
async fn fake_provider_smoke_exchanges_and_persists_auth_json() {
    let auth_home = TempDir::new().expect("tempdir");
    let session = build_login_session(
        LoginConfig {
            issuer: "https://auth.openai.com".to_string(),
            client_id: "client-123".to_string(),
            redirect_uri: "http://localhost:1455/auth/callback".to_string(),
        },
        LoginSeed {
            code_verifier: "verifierverifierverifierverifierverifierver".to_string(),
            state: "expected-state".to_string(),
        },
    )
    .expect("build session");
    let seen_request = std::sync::Arc::new(std::sync::Mutex::new(None::<AuthorizationCodeRequest>));

    let written_path = complete_login_with_exchange(
        auth_home.path(),
        &session,
        "http://localhost:1455/auth/callback?code=auth-code-123&state=expected-state",
        {
            let seen_request = std::sync::Arc::clone(&seen_request);
            move |request| {
                let seen_request = std::sync::Arc::clone(&seen_request);
                async move {
                    *seen_request.lock().expect("request lock") = Some(request.clone());
                    Ok(ExchangeResult {
                        api_key: Some("sk-provider".to_string()),
                        access_token: "access-token-123".to_string(),
                        refresh_token: "refresh-token-123".to_string(),
                        id_token: "id-token-123".to_string(),
                        account_id: "account-123".to_string(),
                    })
                }
            }
        },
    )
    .await
    .expect("exchange and persist");

    assert_eq!(written_path, auth_home.path().join("auth.json"));

    let request = seen_request
        .lock()
        .expect("request lock")
        .clone()
        .expect("captured request");
    assert_eq!(request.issuer, "https://auth.openai.com");
    assert_eq!(request.client_id, "client-123");
    assert_eq!(request.redirect_uri, "http://localhost:1455/auth/callback");
    assert_eq!(request.code, "auth-code-123");
    assert_eq!(
        request.code_verifier,
        "verifierverifierverifierverifierverifierver"
    );

    let persisted: Value = serde_json::from_slice(&fs::read(&written_path).expect("read auth"))
        .expect("persisted auth json");
    assert_eq!(persisted["OPENAI_API_KEY"], "sk-provider");
    assert_eq!(persisted["tokens"]["access_token"], "access-token-123");
    assert_eq!(persisted["tokens"]["refresh_token"], "refresh-token-123");
    assert_eq!(persisted["tokens"]["id_token"], "id-token-123");
    assert_eq!(persisted["tokens"]["account_id"], "account-123");
    assert!(persisted["last_refresh"].as_str().is_some());
}
