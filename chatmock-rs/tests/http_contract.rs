use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::thread;
use std::time::Duration;

use chatmock_rs::{server, RuntimeConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex;

const SHUTDOWN_PATH: &str = "/__chatmock_fake_upstream_shutdown__";

struct FakeUpstreamServer {
    address: SocketAddr,
    requests: Arc<StdMutex<Vec<Value>>>,
    handle: Option<thread::JoinHandle<()>>,
}

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

impl FakeUpstreamServer {
    fn start() -> Self {
        let listener = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind fake upstream");
        let address = listener.local_addr().expect("local addr");
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);

        let handle = thread::spawn(move || {
            for incoming in listener.incoming() {
                let mut stream = match incoming {
                    Ok(stream) => stream,
                    Err(_) => break,
                };

                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 4096];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(count) => {
                            buffer.extend_from_slice(&chunk[..count]);
                            if find_header_end(&buffer).is_some() {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }

                let Some(header_end) = find_header_end(&buffer) else {
                    continue;
                };
                let headers_text = String::from_utf8_lossy(&buffer[..header_end]);
                let request_line = headers_text.lines().next().unwrap_or_default().to_string();
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();
                if path == SHUTDOWN_PATH {
                    break;
                }
                let mut content_length = 0_usize;
                for line in headers_text.lines().skip(1) {
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        content_length = value.trim().parse::<usize>().unwrap_or(0);
                    }
                }

                let body_start = header_end + 4;
                while buffer.len() < body_start + content_length {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(count) => buffer.extend_from_slice(&chunk[..count]),
                        Err(_) => return,
                    }
                }

                let body = &buffer[body_start
                    ..body_start + content_length.min(buffer.len().saturating_sub(body_start))];
                let request_json: Value = serde_json::from_slice(body).expect("request json");
                requests_for_thread
                    .lock()
                    .expect("lock requests")
                    .push(json!({
                        "path": path,
                        "json": request_json.clone(),
                    }));

                let input_text = collect_text_fragments(&request_json["input"]).join(" ");
                let (status_line, content_type, response_body) = if input_text
                    .contains("contract-upstream-error")
                {
                    (
                        "HTTP/1.1 502 Bad Gateway\r\n",
                        "text/plain",
                        "gateway meltdown before JSON. Authorization: Bearer auth-secret-123. Bearer bearer-secret-456. sk-live-super-secret-789. session_id=session-secret-abc. access_token=access-secret-def. token=token-secret-ghi.".to_string(),
                    )
                } else {
                    let events = if input_text.contains("contract-responses-backfill") {
                        vec![
                            json!({"type": "response.created", "response": {"id": "resp_contract_backfill", "object": "response", "status": "in_progress"}}),
                            json!({"type": "response.output_item.done", "item": {"type": "message", "role": "assistant", "id": "msg_contract_backfill", "content": [{"type": "output_text", "text": "assistant output"}]}}),
                            json!({"type": "response.completed", "response": {"id": "resp_contract_backfill", "object": "response", "status": "completed", "output": []}}),
                        ]
                    } else {
                        vec![
                            json!({"type": "response.output_text.delta", "delta": "hello from contract upstream"}),
                            json!({"type": "response.completed", "response": {"id": "resp_contract_chat"}}),
                        ]
                    };
                    let mut body = String::new();
                    for event in events {
                        body.push_str("data: ");
                        body.push_str(&event.to_string());
                        body.push_str("\n\n");
                    }
                    body.push_str("data: [DONE]\n\n");
                    ("HTTP/1.1 200 OK\r\n", "text/event-stream", body)
                };

                let response = format!(
                    "{status_line}Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        Self {
            address,
            requests,
            handle: Some(handle),
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    fn last_request(&self) -> Option<Value> {
        self.requests.lock().expect("lock requests").last().cloned()
    }
}

impl Drop for FakeUpstreamServer {
    fn drop(&mut self) {
        if let Ok(mut stream) = std::net::TcpStream::connect(self.address) {
            let shutdown_request = format!(
                "GET {SHUTDOWN_PATH} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                self.address
            );
            let _ = stream.write_all(shutdown_request.as_bytes());
            let _ = stream.flush();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn collect_text_fragments(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items.iter().flat_map(collect_text_fragments).collect(),
        Value::Object(map) => map
            .iter()
            .flat_map(|(key, item)| {
                if key == "text" {
                    item.as_str()
                        .map(|text| vec![text.to_string()])
                        .unwrap_or_default()
                } else {
                    collect_text_fragments(item)
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn contract_server_config() -> RuntimeConfig {
    RuntimeConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        ..RuntimeConfig::default()
    }
}

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn contract_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client")
}

fn upstream_env_guard() -> EnvVarGuard {
    EnvVarGuard::capture(&["CHATGPT_LOCAL_HOME", "CHATGPT_RESPONSES_URL"])
}

fn configure_upstream_env(fake_upstream: &FakeUpstreamServer, auth_home: &tempfile::TempDir) {
    std::env::set_var("CHATGPT_LOCAL_HOME", auth_home.path());
    std::env::set_var(
        "CHATGPT_RESPONSES_URL",
        format!("{}/backend-api/codex/responses", fake_upstream.url()),
    );
}

#[tokio::test]
async fn models_list_exposes_known_public_models() {
    let _guard = test_lock().lock().await;
    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");

    let response = reqwest::get(format!("{}/v1/models", server.base_url()))
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert_eq!(
        response.headers()["access-control-allow-methods"],
        "POST, GET, OPTIONS"
    );
    assert_eq!(
        response.headers()["access-control-allow-headers"],
        "Authorization, Content-Type, Accept"
    );
    let body: Value = response.json().await.expect("json body");
    let model_ids = body["data"]
        .as_array()
        .expect("model data array")
        .iter()
        .filter_map(|item| item["id"].as_str())
        .collect::<Vec<_>>();
    assert!(model_ids.contains(&"gpt-5.4"));
    assert!(model_ids.contains(&"gpt-5.4-mini"));
    assert!(model_ids.contains(&"gpt-5.3-codex-spark"));
}

#[tokio::test]
async fn completions_prompt_array_coerces_and_returns_text_completion_shape() {
    let _guard = test_lock().lock().await;
    let _env_guard = upstream_env_guard();
    let fake_upstream = FakeUpstreamServer::start();
    let auth_home = tempfile::TempDir::new().expect("auth tempdir");
    std::fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");
    configure_upstream_env(&fake_upstream, &auth_home);

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let response = client
        .post(format!("{}/v1/completions", server.base_url()))
        .json(&json!({
            "model": "gpt5.4-mini",
            "prompt": ["contract", "-prompt", "-array"]
        }))
        .send()
        .await
        .expect("completions request should succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    let body: Value = response.json().await.expect("completions json");
    assert_eq!(body["object"], "text_completion");
    assert_eq!(body["model"], "gpt5.4-mini");
    assert_eq!(body["choices"][0]["text"], "hello from contract upstream");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");

    let outbound = fake_upstream
        .last_request()
        .expect("captured outbound request");
    assert_eq!(outbound["path"], "/backend-api/codex/responses");
    assert_eq!(
        outbound["json"]["input"][0]["content"][0]["text"],
        "contract-prompt-array"
    );
}

#[tokio::test]
async fn completions_stream_uses_python_compatible_text_completion_chunk_object() {
    let _guard = test_lock().lock().await;
    let _env_guard = upstream_env_guard();
    let fake_upstream = FakeUpstreamServer::start();
    let auth_home = tempfile::TempDir::new().expect("auth tempdir");
    std::fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");
    configure_upstream_env(&fake_upstream, &auth_home);

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let response = client
        .post(format!("{}/v1/completions", server.base_url()))
        .json(&json!({
            "model": "gpt5.4-mini",
            "prompt": "contract-stream-object",
            "stream": true
        }))
        .send()
        .await
        .expect("stream request should succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = response.text().await.expect("stream body");
    assert!(body.contains("\"object\":\"text_completion.chunk\""));
    assert!(body.contains("\"text\":\"hello from contract upstream\""));
    assert!(body.contains("data: [DONE]"));
}

#[tokio::test]
async fn completions_missing_auth_returns_401_with_cors_headers() {
    let _guard = test_lock().lock().await;
    let _env_guard = EnvVarGuard::capture(&[
        "CHATGPT_LOCAL_HOME",
        "CHATGPT_RESPONSES_URL",
        "CODEX_HOME",
        "USERPROFILE",
        "HOME",
    ]);
    let isolated_home = tempfile::TempDir::new().expect("isolated auth home");
    let isolated_codex = tempfile::TempDir::new().expect("isolated codex home");
    std::env::set_var("CHATGPT_LOCAL_HOME", isolated_home.path());
    std::env::set_var("CODEX_HOME", isolated_codex.path());
    std::env::set_var("USERPROFILE", isolated_home.path());
    std::env::set_var("HOME", isolated_home.path());
    std::env::remove_var("CHATGPT_RESPONSES_URL");

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let response = client
        .post(format!("{}/v1/completions", server.base_url()))
        .json(&json!({
            "model": "gpt5.4-mini",
            "suffix": "contract-missing-auth"
        }))
        .send()
        .await
        .expect("missing auth request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert_eq!(
        response.headers()["access-control-allow-methods"],
        "POST, GET, OPTIONS"
    );
    assert_eq!(
        response.headers()["access-control-allow-headers"],
        "Authorization, Content-Type, Accept"
    );
    let body: Value = response.json().await.expect("missing auth json");
    assert_eq!(
        body["error"]["message"],
        "Missing ChatGPT credentials. Run 'python3 chatmock.py login' first."
    );
}

#[tokio::test]
async fn chat_completions_follow_minimal_http_contract() {
    let _guard = test_lock().lock().await;
    let _env_guard = upstream_env_guard();
    let fake_upstream = FakeUpstreamServer::start();
    let auth_home = tempfile::TempDir::new().expect("auth tempdir");
    std::fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");

    configure_upstream_env(&fake_upstream, &auth_home);

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let chat_response = client
        .post(format!("{}/v1/chat/completions", server.base_url()))
        .json(&json!({
            "model": "gpt5.4-mini",
            "messages": [{"role": "user", "content": "contract-chat-completions"}]
        }))
        .send()
        .await
        .expect("chat request should succeed");
    assert_eq!(chat_response.status(), reqwest::StatusCode::OK);
    let chat_body: Value = chat_response.json().await.expect("chat json");
    assert_eq!(
        chat_body["choices"][0]["message"]["content"],
        "hello from contract upstream"
    );
    assert_eq!(chat_body["model"], "gpt5.4-mini");
}

#[tokio::test]
async fn chat_completions_upstream_error_contract() {
    let _guard = test_lock().lock().await;
    let _env_guard = upstream_env_guard();
    let fake_upstream = FakeUpstreamServer::start();
    let auth_home = tempfile::TempDir::new().expect("auth tempdir");
    std::fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");
    configure_upstream_env(&fake_upstream, &auth_home);

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let error_response = client
        .post(format!("{}/v1/chat/completions", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "messages": [{"role": "user", "content": "contract-upstream-error"}]
        }))
        .send()
        .await
        .expect("error request should succeed");
    assert_eq!(error_response.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert_eq!(error_response.headers()["access-control-allow-origin"], "*");
    let error_body: Value = error_response.json().await.expect("error json");
    let error_message = error_body["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(error_message.contains("502"));
    assert!(error_message.contains("text/plain"));
    assert!(error_message.contains("gateway meltdown before JSON"));
    for secret in [
        "auth-secret-123",
        "bearer-secret-456",
        "sk-live-super-secret-789",
        "session-secret-abc",
        "access-secret-def",
        "token-secret-ghi",
    ] {
        assert!(!error_message.contains(secret));
    }
}

#[tokio::test]
async fn responses_follow_minimal_http_contract() {
    let _guard = test_lock().lock().await;
    let _env_guard = upstream_env_guard();
    let fake_upstream = FakeUpstreamServer::start();
    let auth_home = tempfile::TempDir::new().expect("auth tempdir");
    std::fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");
    configure_upstream_env(&fake_upstream, &auth_home);

    let server = server::spawn_server(contract_server_config())
        .await
        .expect("server should start");
    let client = contract_client();

    let responses_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt5.4-mini",
            "input": "contract-responses-backfill"
        }))
        .send()
        .await
        .expect("responses request should succeed");
    assert_eq!(responses_response.status(), reqwest::StatusCode::OK);
    let responses_body: Value = responses_response.json().await.expect("responses json");
    assert_eq!(responses_body["id"], "resp_contract_backfill");
    assert_eq!(responses_body["output"][0]["id"], "msg_contract_backfill");
    assert_eq!(
        responses_body["output"][0]["content"][0]["text"],
        "assistant output"
    );

    let outbound = fake_upstream
        .last_request()
        .expect("captured outbound request");
    assert_eq!(outbound["path"], "/backend-api/codex/responses");
    assert_eq!(outbound["json"]["model"], "gpt-5.4-mini");
    assert_eq!(outbound["json"]["store"], false);
    assert_eq!(
        outbound["json"]["input"],
        json!([
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "contract-responses-backfill"}]
            }
        ])
    );
}
