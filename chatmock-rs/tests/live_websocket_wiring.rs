use std::ffi::OsString;
use std::fs;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, OnceLock,
};

use chatmock_rs::{server, RuntimeConfig};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const TEST_ENV_VARS: &[&str] = &["CHATGPT_LOCAL_HOME", "CHATGPT_RESPONSES_URL"];

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

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct FakeUpstreamServer {
    base_http_url: String,
    connect_calls: Arc<AtomicUsize>,
    received_messages: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl FakeUpstreamServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake upstream");
        let address = listener.local_addr().expect("fake upstream addr");
        let connect_calls = Arc::new(AtomicUsize::new(0));
        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let connect_calls_for_task = Arc::clone(&connect_calls);
        let received_messages_for_task = Arc::clone(&received_messages);

        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept upstream socket");
            connect_calls_for_task.fetch_add(1, Ordering::SeqCst);
            let mut websocket = accept_async(stream).await.expect("accept websocket");

            let first_message = next_text_frame(&mut websocket).await;
            received_messages_for_task
                .lock()
                .expect("lock received messages")
                .push(first_message);
            websocket
                .send(Message::Text(
                    json!({
                        "type": "response.created",
                        "response": {
                            "id": "resp_live_turn_1",
                            "object": "response",
                            "status": "in_progress"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send first created event");
            websocket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_live_turn_1",
                            "object": "response",
                            "status": "completed",
                            "output": []
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send first completed event");

            let second_message = next_text_frame(&mut websocket).await;
            received_messages_for_task
                .lock()
                .expect("lock received messages")
                .push(second_message);
            websocket
                .send(Message::Text(
                    json!({
                        "type": "response.created",
                        "response": {
                            "id": "resp_live_turn_2",
                            "object": "response",
                            "status": "in_progress"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send second created event");
            websocket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_live_turn_2",
                            "object": "response",
                            "status": "completed",
                            "output": []
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send second completed event");
        });

        Self {
            base_http_url: format!("http://{address}"),
            connect_calls,
            received_messages,
            task,
        }
    }

    async fn finish(self) -> Vec<String> {
        timeout(Duration::from_secs(2), self.task)
            .await
            .expect("fake upstream task should finish")
            .expect("fake upstream task should succeed");
        self.received_messages
            .lock()
            .expect("lock received messages")
            .clone()
    }
}

async fn next_text_frame(
    websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> String {
    loop {
        let frame = timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("fake upstream frame timeout")
            .expect("fake upstream frame should exist")
            .expect("fake upstream frame should be ok");
        match frame {
            Message::Text(text) => return text.to_string(),
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(frame) => panic!("unexpected upstream close: {frame:?}"),
            Message::Frame(_) => continue,
        }
    }
}

fn write_auth_file(home: &TempDir) {
    fs::write(
        home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "account_id": "contract-account-id"
            }
        })
        .to_string(),
    )
    .expect("write auth file");
}

#[tokio::test]
async fn spawn_server_live_path_reuses_real_websocket_connector_for_stateful_follow_up() {
    let _env_lock = env_lock().lock().await;
    let _env_guard = EnvVarGuard::capture(TEST_ENV_VARS);
    let auth_home = TempDir::new().expect("temp auth dir");
    write_auth_file(&auth_home);

    let fake_upstream = FakeUpstreamServer::start().await;
    std::env::set_var("CHATGPT_LOCAL_HOME", auth_home.path());
    std::env::set_var(
        "CHATGPT_RESPONSES_URL",
        format!(
            "{}/backend-api/codex/responses",
            fake_upstream.base_http_url
        ),
    );

    let server = server::spawn_server(RuntimeConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        responses_websocket_upstream: true,
        responses_websocket_upstream_stateful: true,
    })
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should complete");

    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: Value = first_response.json().await.expect("first json body");
    assert_eq!(first_body["id"], "resp_live_turn_1");

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_live_turn_1"
        }))
        .send()
        .await
        .expect("second request should complete");

    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: Value = second_response.json().await.expect("second json body");
    assert_eq!(second_body["id"], "resp_live_turn_2");

    let connect_calls = Arc::clone(&fake_upstream.connect_calls);
    drop(server);

    let received_messages = fake_upstream.finish().await;
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    assert_eq!(received_messages.len(), 2);

    let first_outbound: Value =
        serde_json::from_str(&received_messages[0]).expect("first outbound json");
    let second_outbound: Value =
        serde_json::from_str(&received_messages[1]).expect("second outbound json");
    assert!(first_outbound.get("previous_response_id").is_none());
    assert_eq!(second_outbound["previous_response_id"], "resp_live_turn_1");
}
