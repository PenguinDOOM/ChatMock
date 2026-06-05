use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chatmock_rs::server;
use chatmock_rs::websocket::registry::{
    ResponsesWebsocketSessionCapacityError, ResponsesWebsocketSessionConflictError,
    ResponsesWebsocketSessionNotFoundError, RetainedUpstreamWebsocket,
    RetainedUpstreamWebsocketRegistry,
};
use chatmock_rs::websocket::upstream::{
    ResponsesWebsocketConnectContext, ScriptedUpstreamReceive, SharedUpstreamWebsocket,
};
use chatmock_rs::RuntimeConfig;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio::task::yield_now;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{protocol::frame::coding::CloseCode, Message},
};

#[derive(Debug)]
struct FakeRetainedUpstreamWebsocket {
    closed: bool,
    close_calls: usize,
}

impl FakeRetainedUpstreamWebsocket {
    fn close(&mut self) {
        self.closed = true;
        self.close_calls += 1;
    }

    fn mark_closed(&mut self) {
        self.closed = true;
    }
}

#[derive(Debug, Clone)]
struct SharedSocketHandle(Arc<Mutex<FakeRetainedUpstreamWebsocket>>);

impl SharedSocketHandle {
    fn lock(
        &self,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'_, FakeRetainedUpstreamWebsocket>> {
        self.0.lock()
    }
}

impl RetainedUpstreamWebsocket for SharedSocketHandle {
    fn close_socket(&self) {
        self.lock().expect("lock socket").close();
    }

    fn is_socket_closed(&self) -> bool {
        self.lock().expect("lock socket").closed
    }
}

fn make_socket() -> SharedSocketHandle {
    SharedSocketHandle(Arc::new(Mutex::new(FakeRetainedUpstreamWebsocket {
        closed: false,
        close_calls: 0,
    })))
}

fn websocket_url(base_url: &str) -> String {
    format!("ws://{}", base_url.trim_start_matches("http://"))
}

async fn next_text_message(
    websocket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    loop {
        let frame = timeout(Duration::from_secs(1), websocket.next())
            .await
            .expect("websocket frame timeout")
            .expect("websocket should stay open long enough")
            .expect("websocket frame should be ok");
        match frame {
            Message::Text(text) => return text.to_string(),
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => panic!("websocket closed before text frame"),
            Message::Frame(_) => continue,
        }
    }
}

async fn assert_websocket_stops(
    websocket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let frame = timeout(Duration::from_secs(1), websocket.next())
        .await
        .expect("websocket should close promptly");
    match frame {
        None => {}
        Some(Ok(Message::Close(_))) => {}
        Some(Ok(other)) => panic!("expected websocket close, got {other:?}"),
        Some(Err(error)) => panic!("expected websocket close, got error {error}"),
    }
}

fn first_sse_event(body: &str) -> Value {
    let event_line = body
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("sse event line");
    let payload = event_line.strip_prefix("data: ").expect("sse data prefix");
    serde_json::from_str(payload).expect("sse event json")
}

async fn wait_for_sent_messages(socket: &SharedUpstreamWebsocket, expected_len: usize) {
    timeout(Duration::from_secs(1), async {
        loop {
            if socket.scripted_sent_messages().await.len() >= expected_len {
                return;
            }
            yield_now().await;
        }
    })
    .await
    .expect("scripted websocket should send expected messages");
}

#[tokio::test]
async fn scripted_upstream_websocket_retains_close_code_and_reason() {
    let socket = SharedUpstreamWebsocket::scripted(vec![ScriptedUpstreamReceive::close(
        Some(CloseCode::Away),
        Some("backend drained".to_string()),
    )]);

    let message = socket.recv_text().await.expect("receive should succeed");
    let close = socket.close_metadata().expect("close metadata");

    assert!(message.is_none());
    assert_eq!(close.code, Some(CloseCode::Away));
    assert_eq!(close.reason.as_deref(), Some("backend drained"));
    assert!(socket.is_socket_closed());
}

#[test]
fn registry_promotes_first_turn_to_response_marker_and_reuses_it() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let created = Arc::new(Mutex::new(Vec::<SharedSocketHandle>::new()));

    let first = registry
        .acquire(None, {
            let created = Arc::clone(&created);
            move || {
                let socket = make_socket();
                created.lock().expect("lock created").push(socket.clone());
                Ok(socket)
            }
        })
        .expect("first lease");
    registry.release(first.clone(), true, Some("resp_fixed_1"));

    let second = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect("second lease");

    assert!(first.created);
    assert!(!second.created);
    assert!(Arc::ptr_eq(&first.upstream_ws.0, &second.upstream_ws.0));
    assert_eq!(created.lock().expect("lock created").len(), 1);

    registry.release(second, true, Some("resp_fixed_2"));
}

#[test]
fn registry_initial_lease_has_codex_style_metadata_and_reuses_it() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");

    assert!(first.created);
    assert_eq!(first.metadata.session_id, first.metadata.thread_id);
    assert_eq!(first.metadata.window_generation, 1);
    assert_eq!(first.metadata.last_response_id, None);
    assert_eq!(first.metadata.turn_state, None);

    let first_session_id = first.metadata.session_id.clone();
    let first_thread_id = first.metadata.thread_id.clone();
    registry.release(first, true, Some("resp_fixed_1"));

    let second = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect("second lease");

    assert!(!second.created);
    assert_eq!(second.metadata.session_id, first_session_id);
    assert_eq!(second.metadata.thread_id, first_thread_id);
    assert_eq!(second.metadata.window_generation, 1);
    assert_eq!(
        second.metadata.last_response_id.as_deref(),
        Some("resp_fixed_1")
    );

    registry.release(second, true, Some("resp_fixed_2"));
}

#[test]
fn registry_guard_can_store_upgrade_turn_state_without_changing_marker() {
    let registry = Arc::new(RetainedUpstreamWebsocketRegistry::new(2));
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");
    let mut guard = chatmock_rs::websocket::registry::RetainedUpstreamWebsocketLeaseGuard::new(
        Arc::clone(&registry),
        first,
    );
    guard.set_turn_state(Some("turn-state-1".to_string()));
    guard.mark_completed("resp_fixed_1");
    guard.release();

    let second = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect("second lease");
    assert_eq!(second.metadata.turn_state.as_deref(), Some("turn-state-1"));

    registry.release(second, true, Some("resp_fixed_2"));
}

#[test]
fn registry_rejects_same_marker_contention() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("seed lease");
    registry.release(first, true, Some("resp_fixed_1"));

    let lease = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect("existing lease");

    let error = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect_err("contention should fail");
    let conflict = error
        .downcast_ref::<ResponsesWebsocketSessionConflictError>()
        .expect("conflict error");
    assert_eq!(conflict.response_id, "resp_fixed_1");

    registry.release(lease, true, Some("resp_fixed_2"));
}

#[test]
fn registry_rejects_new_conversation_when_all_capacity_is_in_use() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");
    let second = registry
        .acquire(None, || Ok(make_socket()))
        .expect("second lease");

    let error = registry
        .acquire(None, || Ok(make_socket()))
        .expect_err("capacity should fail");
    assert!(error
        .downcast_ref::<ResponsesWebsocketSessionCapacityError>()
        .is_some());

    registry.release(first, false, None);
    registry.release(second, false, None);
}

#[test]
fn registry_reconnects_closed_completed_marker_with_retained_metadata() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");
    let first_session_id = first.metadata.session_id.clone();
    let first_thread_id = first.metadata.thread_id.clone();
    registry.release(first.clone(), true, Some("resp_fixed_1"));
    first.upstream_ws.lock().expect("lock socket").mark_closed();

    let reconnected = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect("closed completed marker should reconnect");
    assert!(!reconnected.created);
    assert!(!Arc::ptr_eq(
        &first.upstream_ws.0,
        &reconnected.upstream_ws.0
    ));
    assert_eq!(reconnected.metadata.session_id, first_session_id);
    assert_eq!(reconnected.metadata.thread_id, first_thread_id);
    assert_eq!(reconnected.metadata.window_generation, 1);
    assert_eq!(
        reconnected.metadata.last_response_id.as_deref(),
        Some("resp_fixed_1")
    );
    assert_eq!(
        first.upstream_ws.lock().expect("lock socket").close_calls,
        1
    );
    registry.release(reconnected, true, Some("resp_fixed_2"));
}

#[tokio::test]
async fn registry_reconnect_failure_removes_closed_completed_marker() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");
    registry.release(first.clone(), true, Some("resp_fixed_1"));
    first.upstream_ws.lock().expect("lock socket").mark_closed();

    let error = registry
        .acquire_async_with_metadata(Some("resp_fixed_1"), |_metadata| async {
            Err(std::io::Error::other("reconnect failed").into())
        })
        .await
        .expect_err("failed reconnect should become missing marker");
    let not_found = error
        .downcast_ref::<ResponsesWebsocketSessionNotFoundError>()
        .expect("not found error");
    assert_eq!(not_found.response_id, "resp_fixed_1");

    let error = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect_err("failed reconnect should remove marker");
    assert!(error
        .downcast_ref::<ResponsesWebsocketSessionNotFoundError>()
        .is_some());
}

#[tokio::test]
async fn responses_route_uses_websocket_bridge_when_enabled() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_nonstream", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_nonstream", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                connect_calls.fetch_add(1, Ordering::SeqCst);
                Ok(socket)
            })
        }),
    )
    .await
    .expect("server should start");

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let body: Value = response.json().await.expect("json body");
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["id"], "resp_ws_nonstream");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 1);
    let outbound: Value = serde_json::from_str(&sent_messages[0]).expect("outbound json");
    assert_eq!(outbound["type"], "response.create");
    assert_eq!(outbound["model"], "gpt-5.4");
    assert_eq!(outbound["stream"], true);
    assert!(outbound.get("previous_response_id").is_none());
}

#[tokio::test]
async fn responses_route_intercepts_chatmock_job_tool_calls() {
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_tool_turn", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "chatmock_poll_job",
                "call_id": "call_chatmock_poll",
                "arguments": "{\"job_id\":\"job_missing\",\"max_wait_ms\":60000}"
            }
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_tool_turn", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_after_tool", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_after_tool", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            enable_chatmock_jobs: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "start a long job"}))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let body: Value = response.json().await.expect("json body");
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["id"], "resp_after_tool");

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
    let first_outbound: Value = serde_json::from_str(&sent_messages[0]).expect("first outbound");
    assert!(first_outbound["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .any(|tool| tool["name"] == "chatmock_start_job"));
    assert!(first_outbound["instructions"]
        .as_str()
        .expect("instructions")
        .contains("chatmock_start_job"));

    let follow_up: Value = serde_json::from_str(&sent_messages[1]).expect("follow-up outbound");
    assert_eq!(follow_up["type"], "response.create");
    assert_eq!(follow_up["previous_response_id"], "resp_tool_turn");
    assert_eq!(follow_up["input"][0]["type"], "function_call_output");
    assert_eq!(follow_up["input"][0]["call_id"], "call_chatmock_poll");
    assert!(follow_up["input"][0]["output"]
        .as_str()
        .expect("tool output")
        .contains("Job not found"));
}

#[tokio::test]
async fn responses_route_batches_multiple_chatmock_job_tool_calls_after_completed() {
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_tool_batch", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "chatmock_poll_job",
                "call_id": "call_chatmock_poll",
                "arguments": "{\"job_id\":\"job_missing\",\"max_wait_ms\":60000}"
            }
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "chatmock_get_job_result",
                "call_id": "call_chatmock_result",
                "arguments": "{\"job_id\":\"job_missing\"}"
            }
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_tool_batch", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_after_batch", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_after_batch", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            enable_chatmock_jobs: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "check jobs"}))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let body: Value = response.json().await.expect("json body");
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["id"], "resp_after_batch");

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
    let follow_up: Value = serde_json::from_str(&sent_messages[1]).expect("follow-up outbound");
    assert_eq!(follow_up["type"], "response.create");
    assert_eq!(follow_up["previous_response_id"], "resp_tool_batch");
    assert_eq!(follow_up["input"].as_array().expect("input").len(), 2);
    assert_eq!(follow_up["input"][0]["call_id"], "call_chatmock_poll");
    assert_eq!(follow_up["input"][1]["call_id"], "call_chatmock_result");
}

#[tokio::test]
async fn responses_websocket_route_rejects_invalid_json_frame() {
    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            Box::pin(
                async move { Err(std::io::Error::other("connector should not be used").into()) },
            )
        }),
    )
    .await
    .expect("server should start");

    let (mut websocket, _) = connect_async(format!(
        "{}/v1/responses",
        websocket_url(&server.base_url())
    ))
    .await
    .expect("websocket should connect");

    websocket
        .send(Message::Text("not-json".into()))
        .await
        .expect("send invalid json");

    let event: Value =
        serde_json::from_str(&next_text_message(&mut websocket).await).expect("error event json");
    assert_eq!(
        event,
        json!({
            "type": "error",
            "status_code": 400,
            "error": {"message": "Websocket frames must be valid JSON objects."}
        })
    );
    assert_websocket_stops(&mut websocket).await;
}

#[tokio::test]
async fn responses_websocket_route_rejects_non_object_frame() {
    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            Box::pin(
                async move { Err(std::io::Error::other("connector should not be used").into()) },
            )
        }),
    )
    .await
    .expect("server should start");

    let (mut websocket, _) = connect_async(format!(
        "{}/v1/responses",
        websocket_url(&server.base_url())
    ))
    .await
    .expect("websocket should connect");

    websocket
        .send(Message::Text("\"hello\"".into()))
        .await
        .expect("send string json");

    let event: Value =
        serde_json::from_str(&next_text_message(&mut websocket).await).expect("error event json");
    assert_eq!(
        event,
        json!({
            "type": "error",
            "status_code": 400,
            "error": {"message": "Websocket frames must be JSON objects."}
        })
    );
    assert_websocket_stops(&mut websocket).await;
}

#[tokio::test]
async fn responses_websocket_route_requires_response_create_first() {
    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            Box::pin(
                async move { Err(std::io::Error::other("connector should not be used").into()) },
            )
        }),
    )
    .await
    .expect("server should start");

    let (mut websocket, _) = connect_async(format!(
        "{}/v1/responses",
        websocket_url(&server.base_url())
    ))
    .await
    .expect("websocket should connect");

    websocket
        .send(Message::Text(
            json!({"type": "response.cancel", "response_id": "resp_1"})
                .to_string()
                .into(),
        ))
        .await
        .expect("send wrong first frame");

    let event: Value =
        serde_json::from_str(&next_text_message(&mut websocket).await).expect("error event json");
    assert_eq!(
        event,
        json!({
            "type": "error",
            "status_code": 400,
            "error": {"message": "The first websocket message must be a response.create request."}
        })
    );
    assert_websocket_stops(&mut websocket).await;
}

#[tokio::test]
async fn responses_websocket_route_relays_upstream_messages() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_route", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_route", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                connect_calls.fetch_add(1, Ordering::SeqCst);
                Ok(socket)
            })
        }),
    )
    .await
    .expect("server should start");

    let (mut websocket, _) = connect_async(format!(
        "{}/v1/responses",
        websocket_url(&server.base_url())
    ))
    .await
    .expect("websocket should connect");
    websocket
        .send(Message::Text(
            json!({"type": "response.create", "model": "gpt-5.4", "input": "hello"})
                .to_string()
                .into(),
        ))
        .await
        .expect("send create frame");

    let first_event: Value =
        serde_json::from_str(&next_text_message(&mut websocket).await).expect("first event json");
    let second_event: Value =
        serde_json::from_str(&next_text_message(&mut websocket).await).expect("second event json");

    assert_eq!(first_event["type"], "response.created");
    assert_eq!(second_event["type"], "response.completed");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 1);
    let outbound: Value = serde_json::from_str(&sent_messages[0]).expect("outbound json");
    assert_eq!(outbound["type"], "response.create");
    assert_eq!(outbound["model"], "gpt-5.4");
}

#[tokio::test]
async fn responses_route_reuses_retained_websocket_for_previous_response_id() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let connect_contexts = Arc::new(Mutex::new(Vec::<ResponsesWebsocketConnectContext>::new()));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_turn_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_turn_1", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_turn_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_turn_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);
    let connect_contexts_for_connector = Arc::clone(&connect_contexts);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |context| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            let connect_contexts = Arc::clone(&connect_contexts_for_connector);
            Box::pin(async move {
                connect_contexts
                    .lock()
                    .expect("lock contexts")
                    .push(context);
                let call_index = connect_calls.fetch_add(1, Ordering::SeqCst);
                if call_index == 0 {
                    Ok(socket)
                } else {
                    Err(std::io::Error::other("unexpected second websocket connect").into())
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");

    let first_status = first_response.status();
    let first_body: Value = first_response.json().await.expect("first json body");
    assert_eq!(first_status, reqwest::StatusCode::OK);
    assert_eq!(first_body["id"], "resp_ws_turn_1");

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_ws_turn_1"
        }))
        .send()
        .await
        .expect("second request should succeed");

    let second_status = second_response.status();
    let second_body: Value = second_response.json().await.expect("second json body");
    assert_eq!(second_status, reqwest::StatusCode::OK);
    assert_eq!(second_body["id"], "resp_ws_turn_2");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    {
        let contexts = connect_contexts.lock().expect("lock contexts");
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].session_id, contexts[0].thread_id);
        assert_eq!(contexts[0].window_generation, 1);
        assert_eq!(contexts[0].turn_state, None);
    }

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
    let first_outbound: Value = serde_json::from_str(&sent_messages[0]).expect("first outbound");
    let second_outbound: Value = serde_json::from_str(&sent_messages[1]).expect("second outbound");
    assert!(first_outbound.get("previous_response_id").is_none());
    assert_eq!(second_outbound["previous_response_id"], "resp_ws_turn_1");
}

#[tokio::test]
async fn responses_route_reconnects_closed_completed_marker_for_previous_response_id() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let connect_contexts = Arc::new(Mutex::new(Vec::<ResponsesWebsocketConnectContext>::new()));
    let first_socket = SharedUpstreamWebsocket::scripted_with_turn_state(
        vec![
            ScriptedUpstreamReceive::text(json!({
                "type": "response.created",
                "response": {"id": "resp_ws_closed_1", "object": "response", "status": "in_progress"}
            })),
            ScriptedUpstreamReceive::text(json!({
                "type": "response.completed",
                "response": {"id": "resp_ws_closed_1", "object": "response", "status": "completed", "output": []}
            })),
        ],
        "turn-state-closed-1",
    );
    let second_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_closed_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_closed_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let second_socket_for_connector = second_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);
    let connect_contexts_for_connector = Arc::clone(&connect_contexts);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |context| {
            let first_socket = first_socket_for_connector.clone();
            let second_socket = second_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            let connect_contexts = Arc::clone(&connect_contexts_for_connector);
            Box::pin(async move {
                connect_contexts
                    .lock()
                    .expect("lock contexts")
                    .push(context);
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    1 => Ok(second_socket),
                    _ => Err(std::io::Error::other("unexpected websocket connect").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: Value = first_response.json().await.expect("first json body");
    assert_eq!(first_body["id"], "resp_ws_closed_1");

    first_socket.close_socket();

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_ws_closed_1"
        }))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: Value = second_response.json().await.expect("second json body");
    assert_eq!(second_body["id"], "resp_ws_closed_2");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);

    {
        let contexts = connect_contexts.lock().expect("lock contexts");
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1].session_id, contexts[0].session_id);
        assert_eq!(contexts[1].thread_id, contexts[0].thread_id);
        assert_eq!(contexts[1].window_generation, 1);
        assert_eq!(
            contexts[1].turn_state.as_deref(),
            Some("turn-state-closed-1")
        );
    }

    let first_sent = first_socket.scripted_sent_messages().await;
    let second_sent = second_socket.scripted_sent_messages().await;
    assert_eq!(first_sent.len(), 1);
    assert_eq!(second_sent.len(), 1);
    let outbound: Value = serde_json::from_str(&second_sent[0]).expect("second outbound");
    assert_eq!(outbound["previous_response_id"], "resp_ws_closed_1");
}

#[tokio::test]
async fn responses_route_reconnects_when_retained_websocket_fails_before_first_event() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let connect_contexts = Arc::new(Mutex::new(Vec::<ResponsesWebsocketConnectContext>::new()));
    let first_socket = SharedUpstreamWebsocket::scripted_with_turn_state(
        vec![
            ScriptedUpstreamReceive::text(json!({
                "type": "response.created",
                "response": {"id": "resp_ws_idle_1", "object": "response", "status": "in_progress"}
            })),
            ScriptedUpstreamReceive::text(json!({
                "type": "response.completed",
                "response": {"id": "resp_ws_idle_1", "object": "response", "status": "completed", "output": []}
            })),
            ScriptedUpstreamReceive::error("idle websocket reset"),
        ],
        "turn-state-idle-1",
    );
    let second_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_idle_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_idle_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let second_socket_for_connector = second_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);
    let connect_contexts_for_connector = Arc::clone(&connect_contexts);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |context| {
            let first_socket = first_socket_for_connector.clone();
            let second_socket = second_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            let connect_contexts = Arc::clone(&connect_contexts_for_connector);
            Box::pin(async move {
                connect_contexts
                    .lock()
                    .expect("lock contexts")
                    .push(context);
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    1 => Ok(second_socket),
                    _ => Err(std::io::Error::other("unexpected websocket connect").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_ws_idle_1"
        }))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: Value = second_response.json().await.expect("second json body");
    assert_eq!(second_body["id"], "resp_ws_idle_2");
    assert!(first_socket.is_socket_closed());
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);

    {
        let contexts = connect_contexts.lock().expect("lock contexts");
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1].session_id, contexts[0].session_id);
        assert_eq!(contexts[1].thread_id, contexts[0].thread_id);
        assert_eq!(contexts[1].window_generation, 1);
        assert_eq!(contexts[1].turn_state.as_deref(), Some("turn-state-idle-1"));
    }

    let first_sent = first_socket.scripted_sent_messages().await;
    let second_sent = second_socket.scripted_sent_messages().await;
    assert_eq!(first_sent.len(), 2);
    assert_eq!(second_sent.len(), 1);
    let failed_outbound: Value = serde_json::from_str(&first_sent[1]).expect("failed outbound");
    let resent_outbound: Value = serde_json::from_str(&second_sent[0]).expect("resent outbound");
    assert_eq!(failed_outbound, resent_outbound);
    assert_eq!(resent_outbound["previous_response_id"], "resp_ws_idle_1");
}

#[tokio::test]
async fn responses_route_does_not_retry_after_receiving_an_event() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_partial_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_partial_1", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_partial_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::error("mid response reset"),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                if connect_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(socket)
                } else {
                    Err(std::io::Error::other("unexpected reconnect").into())
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_ws_partial_1"
        }))
        .send()
        .await
        .expect("second request should complete");
    assert_eq!(second_response.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
}

#[tokio::test]
async fn responses_route_returns_previous_response_not_found_when_reconnect_fails() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let first_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_reconnect_fail_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_reconnect_fail_1", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let first_socket = first_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    _ => Err(std::io::Error::other("reconnect failed").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);

    first_socket.close_socket();

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_ws_reconnect_fail_1"
        }))
        .send()
        .await
        .expect("second request should complete");
    let status = second_response.status();
    let body: Value = second_response.json().await.expect("second json body");
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "previous_response_not_found");
    assert_eq!(
        body["error"]["message"],
        "No response found for previous_response_id resp_ws_reconnect_fail_1."
    );
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn responses_route_stateful_internal_tool_retains_final_response_marker() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_tool_turn", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "chatmock_poll_job",
                "call_id": "call_chatmock_poll",
                "arguments": "{\"job_id\":\"job_missing\",\"max_wait_ms\":60000}"
            }
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_tool_turn", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_after_tool", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_after_tool", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_followup", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_followup", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            enable_chatmock_jobs: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                connect_calls.fetch_add(1, Ordering::SeqCst);
                Ok(socket)
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({"model": "gpt-5.4", "input": "check job"}))
        .send()
        .await
        .expect("first request should succeed");

    let first_status = first_response.status();
    let first_body: Value = first_response.json().await.expect("first json body");
    assert_eq!(first_status, reqwest::StatusCode::OK);
    assert_eq!(first_body["id"], "resp_after_tool");

    let rejected_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "wrong marker",
            "previous_response_id": "resp_tool_turn"
        }))
        .send()
        .await
        .expect("rejected request should complete");
    assert_eq!(rejected_response.status(), reqwest::StatusCode::BAD_REQUEST);

    let follow_up_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "correct marker",
            "previous_response_id": "resp_after_tool"
        }))
        .send()
        .await
        .expect("follow-up should succeed");

    let follow_up_status = follow_up_response.status();
    let follow_up_body: Value = follow_up_response
        .json()
        .await
        .expect("follow-up json body");
    assert_eq!(follow_up_status, reqwest::StatusCode::OK);
    assert_eq!(follow_up_body["id"], "resp_followup");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 3);
    let internal_follow_up: Value =
        serde_json::from_str(&sent_messages[1]).expect("internal follow-up");
    let user_follow_up: Value = serde_json::from_str(&sent_messages[2]).expect("user follow-up");
    assert_eq!(internal_follow_up["previous_response_id"], "resp_tool_turn");
    assert_eq!(user_follow_up["previous_response_id"], "resp_after_tool");
}

#[tokio::test]
async fn responses_route_rejects_missing_retained_marker_in_stateful_mode() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![ScriptedUpstreamReceive::text(
        json!({
            "type": "response.completed",
            "response": {"id": "resp_unused", "object": "response", "status": "completed", "output": []}
        }),
    )]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                connect_calls.fetch_add(1, Ordering::SeqCst);
                Ok(socket)
            })
        }),
    )
    .await
    .expect("server should start");

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_missing"
        }))
        .send()
        .await
        .expect("request should complete");

    let status = response.status();
    let body: Value = response.json().await.expect("json body");
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "previous_response_not_found");
    assert_eq!(
        body["error"]["message"],
        "No response found for previous_response_id resp_missing."
    );
    assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn responses_route_streams_previous_response_not_found_for_missing_retained_marker() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![ScriptedUpstreamReceive::text(
        json!({
            "type": "response.completed",
            "response": {"id": "resp_unused", "object": "response", "status": "completed", "output": []}
        }),
    )]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                connect_calls.fetch_add(1, Ordering::SeqCst);
                Ok(socket)
            })
        }),
    )
    .await
    .expect("server should start");

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_missing",
            "stream": true
        }))
        .send()
        .await
        .expect("request should complete");

    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .expect("content-type header")
        .to_string();
    let body = response.text().await.expect("stream body");
    let event = first_sse_event(&body);

    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(content_type.contains("text/event-stream"));
    assert_eq!(event["type"], "error");
    assert_eq!(event["status_code"], 400);
    assert_eq!(event["error"]["code"], "previous_response_not_found");
    assert_eq!(
        event["error"]["message"],
        "No response found for previous_response_id resp_missing."
    );
    assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn responses_route_stream_reconnects_closed_completed_marker_for_previous_response_id() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let connect_contexts = Arc::new(Mutex::new(Vec::<ResponsesWebsocketConnectContext>::new()));
    let first_socket = SharedUpstreamWebsocket::scripted_with_turn_state(
        vec![
            ScriptedUpstreamReceive::text(json!({
                "type": "response.created",
                "response": {"id": "resp_stream_closed_1", "object": "response", "status": "in_progress"}
            })),
            ScriptedUpstreamReceive::text(json!({
                "type": "response.completed",
                "response": {"id": "resp_stream_closed_1", "object": "response", "status": "completed", "output": []}
            })),
        ],
        "turn-state-stream-closed-1",
    );
    let second_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_stream_closed_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_stream_closed_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let second_socket_for_connector = second_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);
    let connect_contexts_for_connector = Arc::clone(&connect_contexts);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |context| {
            let first_socket = first_socket_for_connector.clone();
            let second_socket = second_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            let connect_contexts = Arc::clone(&connect_contexts_for_connector);
            Box::pin(async move {
                connect_contexts
                    .lock()
                    .expect("lock contexts")
                    .push(context);
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    1 => Ok(second_socket),
                    _ => Err(std::io::Error::other("unexpected websocket connect").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body = first_response.text().await.expect("first stream body");
    assert!(first_body.contains("resp_stream_closed_1"));

    first_socket.close_socket();

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_stream_closed_1",
            "stream": true
        }))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body = second_response.text().await.expect("second stream body");
    assert!(second_body.contains("resp_stream_closed_2"));
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);

    {
        let contexts = connect_contexts.lock().expect("lock contexts");
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1].session_id, contexts[0].session_id);
        assert_eq!(contexts[1].thread_id, contexts[0].thread_id);
        assert_eq!(contexts[1].window_generation, 1);
        assert_eq!(
            contexts[1].turn_state.as_deref(),
            Some("turn-state-stream-closed-1")
        );
    }

    let second_sent = second_socket.scripted_sent_messages().await;
    assert_eq!(second_sent.len(), 1);
    let outbound: Value = serde_json::from_str(&second_sent[0]).expect("second outbound");
    assert_eq!(outbound["previous_response_id"], "resp_stream_closed_1");
}

#[tokio::test]
async fn responses_route_stream_reconnects_when_retained_websocket_fails_before_first_event() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let connect_contexts = Arc::new(Mutex::new(Vec::<ResponsesWebsocketConnectContext>::new()));
    let first_socket = SharedUpstreamWebsocket::scripted_with_turn_state(
        vec![
            ScriptedUpstreamReceive::text(json!({
                "type": "response.created",
                "response": {"id": "resp_stream_idle_1", "object": "response", "status": "in_progress"}
            })),
            ScriptedUpstreamReceive::text(json!({
                "type": "response.completed",
                "response": {"id": "resp_stream_idle_1", "object": "response", "status": "completed", "output": []}
            })),
            ScriptedUpstreamReceive::error("idle websocket reset"),
        ],
        "turn-state-stream-idle-1",
    );
    let second_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_stream_idle_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_stream_idle_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let second_socket_for_connector = second_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);
    let connect_contexts_for_connector = Arc::clone(&connect_contexts);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |context| {
            let first_socket = first_socket_for_connector.clone();
            let second_socket = second_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            let connect_contexts = Arc::clone(&connect_contexts_for_connector);
            Box::pin(async move {
                connect_contexts
                    .lock()
                    .expect("lock contexts")
                    .push(context);
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    1 => Ok(second_socket),
                    _ => Err(std::io::Error::other("unexpected websocket connect").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body = first_response.text().await.expect("first stream body");
    assert!(first_body.contains("resp_stream_idle_1"));

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_stream_idle_1",
            "stream": true
        }))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body = second_response.text().await.expect("second stream body");
    assert!(second_body.contains("resp_stream_idle_2"));
    assert!(first_socket.is_socket_closed());
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);

    {
        let contexts = connect_contexts.lock().expect("lock contexts");
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1].session_id, contexts[0].session_id);
        assert_eq!(contexts[1].thread_id, contexts[0].thread_id);
        assert_eq!(contexts[1].window_generation, 1);
        assert_eq!(
            contexts[1].turn_state.as_deref(),
            Some("turn-state-stream-idle-1")
        );
    }

    let first_sent = first_socket.scripted_sent_messages().await;
    let second_sent = second_socket.scripted_sent_messages().await;
    assert_eq!(first_sent.len(), 2);
    assert_eq!(second_sent.len(), 1);
    let failed_outbound: Value = serde_json::from_str(&first_sent[1]).expect("failed outbound");
    let resent_outbound: Value = serde_json::from_str(&second_sent[0]).expect("resent outbound");
    assert_eq!(failed_outbound, resent_outbound);
    assert_eq!(
        resent_outbound["previous_response_id"],
        "resp_stream_idle_1"
    );
}

#[tokio::test]
async fn responses_route_streams_previous_response_not_found_when_reconnect_fails() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let first_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_stream_reconnect_fail_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_stream_reconnect_fail_1", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let first_socket_for_connector = first_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let first_socket = first_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                match connect_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(first_socket),
                    _ => Err(std::io::Error::other("reconnect failed").into()),
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body = first_response.text().await.expect("first stream body");
    assert!(first_body.contains("resp_stream_reconnect_fail_1"));

    first_socket.close_socket();

    let second_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_stream_reconnect_fail_1",
            "stream": true
        }))
        .send()
        .await
        .expect("second request should complete");
    let status = second_response.status();
    let body = second_response.text().await.expect("second stream body");
    let event = first_sse_event(&body);
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(event["type"], "error");
    assert_eq!(event["status_code"], 400);
    assert_eq!(event["error"]["code"], "previous_response_not_found");
    assert_eq!(
        event["error"]["message"],
        "No response found for previous_response_id resp_stream_reconnect_fail_1."
    );
    assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn responses_route_rejects_same_retained_marker_while_follow_up_is_in_progress() {
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let release_second_turn = Arc::new(Notify::new());
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_turn_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_turn_1", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::wait(Arc::clone(&release_second_turn)),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_ws_turn_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_ws_turn_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();
    let connect_calls_for_connector = Arc::clone(&connect_calls);

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_session_id| {
            let socket = scripted_socket_for_connector.clone();
            let connect_calls = Arc::clone(&connect_calls_for_connector);
            Box::pin(async move {
                let call_index = connect_calls.fetch_add(1, Ordering::SeqCst);
                if call_index == 0 {
                    Ok(socket)
                } else {
                    Err(std::io::Error::other("unexpected second websocket connect").into())
                }
            })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("first request should succeed");

    assert_eq!(first_response.status(), reqwest::StatusCode::OK);

    let base_url = server.base_url().to_string();
    let first_follow_up_client = client.clone();
    let first_follow_up = tokio::spawn(async move {
        first_follow_up_client
            .post(format!("{}/v1/responses", base_url))
            .header("X-Session-Id", "stateful-session")
            .json(&json!({
                "model": "gpt-5.4",
                "input": "follow up",
                "previous_response_id": "resp_ws_turn_1"
            }))
            .send()
            .await
    });

    wait_for_sent_messages(&scripted_socket, 2).await;

    let second_follow_up = client
        .post(format!("{}/v1/responses", server.base_url()))
        .header("X-Session-Id", "stateful-session")
        .json(&json!({
            "model": "gpt-5.4",
            "input": "another follow up",
            "previous_response_id": "resp_ws_turn_1"
        }))
        .send()
        .await
        .expect("second follow-up should complete");

    let second_follow_up_status = second_follow_up.status();
    let second_follow_up_body: Value = second_follow_up
        .json()
        .await
        .expect("second follow-up json body");
    assert_eq!(second_follow_up_status, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        second_follow_up_body["error"]["message"],
        "A stateful HTTP websocket bridge request for response 'resp_ws_turn_1' is already in progress."
    );

    release_second_turn.notify_waiters();

    let first_follow_up_response = first_follow_up
        .await
        .expect("first follow-up task should join")
        .expect("first follow-up request should succeed");
    let first_follow_up_status = first_follow_up_response.status();
    let first_follow_up_body: Value = first_follow_up_response
        .json()
        .await
        .expect("first follow-up json body");
    assert_eq!(first_follow_up_status, reqwest::StatusCode::OK);
    assert_eq!(first_follow_up_body["id"], "resp_ws_turn_2");
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn responses_route_stateful_stream_yields_first_chunk_before_completion() {
    let release_completion = Arc::new(Notify::new());
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_streaming", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::wait(Arc::clone(&release_completion)),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_streaming", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let mut response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("request should complete");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let first_chunk = timeout(Duration::from_secs(1), response.chunk())
        .await
        .expect("first chunk before completion")
        .expect("first chunk result")
        .expect("first chunk bytes");
    let first_chunk = String::from_utf8(first_chunk.to_vec()).expect("utf8 chunk");
    assert!(first_chunk.contains("\"type\":\"response.created\""));

    release_completion.notify_waiters();
    let rest = timeout(Duration::from_secs(1), response.chunk())
        .await
        .expect("completed chunk")
        .expect("completed chunk result")
        .expect("completed chunk bytes");
    assert!(String::from_utf8(rest.to_vec())
        .expect("utf8 completed")
        .contains("\"type\":\"response.completed\""));
}

#[tokio::test]
async fn responses_route_stateful_stream_continues_after_internal_tool_follow_up() {
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_tool_stream", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "chatmock_poll_job",
                "call_id": "call_chatmock_poll",
                "arguments": "{\"job_id\":\"job_missing\",\"max_wait_ms\":60000}"
            }
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_tool_stream", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_after_stream_tool", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_after_stream_tool", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            enable_chatmock_jobs: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let mut response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "check job",
            "stream": true
        }))
        .send()
        .await
        .expect("request should complete");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let mut body = String::new();
    for _ in 0..4 {
        let Some(chunk) = timeout(Duration::from_secs(1), response.chunk())
            .await
            .expect("stream chunk")
            .expect("stream chunk result")
        else {
            break;
        };
        body.push_str(core::str::from_utf8(&chunk).expect("utf8 chunk"));
        if body.contains("resp_after_stream_tool")
            && body.contains("\"type\":\"response.completed\"")
        {
            break;
        }
    }

    assert!(body.contains("resp_after_stream_tool"));
    assert!(!body.contains("call_chatmock_poll"));
    assert!(
        !body.contains("\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tool_stream\"")
    );

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
    let follow_up: Value = serde_json::from_str(&sent_messages[1]).expect("follow-up outbound");
    assert_eq!(follow_up["previous_response_id"], "resp_tool_stream");
    assert_eq!(follow_up["input"][0]["call_id"], "call_chatmock_poll");
}

#[tokio::test]
async fn responses_route_stateful_stream_sends_comment_keep_alive() {
    let release_first_event = Arc::new(Notify::new());
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::wait(Arc::clone(&release_first_event)),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_keepalive", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let mut response = reqwest::Client::new()
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("request should complete");

    let keep_alive = timeout(Duration::from_secs(2), response.chunk())
        .await
        .expect("keep-alive chunk")
        .expect("keep-alive result")
        .expect("keep-alive bytes");
    assert_eq!(keep_alive.as_ref(), b": keep-alive\n\n");

    release_first_event.notify_waiters();
}

#[tokio::test]
async fn responses_route_stateful_stream_drains_after_client_disconnect_to_completed() {
    let release_completion = Arc::new(Notify::new());
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_drain_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::wait(Arc::clone(&release_completion)),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_drain_1", "object": "response", "status": "completed", "output": []}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_drain_2", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::text(json!({
            "type": "response.completed",
            "response": {"id": "resp_drain_2", "object": "response", "status": "completed", "output": []}
        })),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            responses_websocket_disconnect_drain_timeout_ms: 1_000,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let mut first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("first request should complete");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);

    let first_chunk = timeout(Duration::from_secs(1), first_response.chunk())
        .await
        .expect("first chunk")
        .expect("first chunk result")
        .expect("first chunk bytes");
    assert!(String::from_utf8(first_chunk.to_vec())
        .expect("utf8 first")
        .contains("\"type\":\"response.created\""));
    drop(first_response);

    release_completion.notify_waiters();

    let second_response = timeout(Duration::from_secs(2), async {
        loop {
            let response = client
                .post(format!("{}/v1/responses", server.base_url()))
                .json(&json!({
                    "model": "gpt-5.4",
                    "input": "follow up",
                    "previous_response_id": "resp_drain_1"
                }))
                .send()
                .await
                .expect("follow-up request should complete");
            if response.status() == reqwest::StatusCode::OK {
                return response;
            }
            yield_now().await;
        }
    })
    .await
    .expect("drain should retain completed response");

    let second_body: Value = second_response.json().await.expect("second body");
    assert_eq!(second_body["id"], "resp_drain_2");
}

#[tokio::test]
async fn responses_route_stateful_stream_disconnect_drain_timeout_does_not_retain() {
    let never_complete = Arc::new(Notify::new());
    let scripted_socket = SharedUpstreamWebsocket::scripted(vec![
        ScriptedUpstreamReceive::text(json!({
            "type": "response.created",
            "response": {"id": "resp_timeout_1", "object": "response", "status": "in_progress"}
        })),
        ScriptedUpstreamReceive::wait(Arc::clone(&never_complete)),
    ]);
    let scripted_socket_for_connector = scripted_socket.clone();

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
            responses_websocket_disconnect_drain_timeout_ms: 50,
            ..RuntimeConfig::default()
        },
        Arc::new(move |_context| {
            let socket = scripted_socket_for_connector.clone();
            Box::pin(async move { Ok(socket) })
        }),
    )
    .await
    .expect("server should start");

    let client = reqwest::Client::new();
    let mut first_response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        }))
        .send()
        .await
        .expect("first request should complete");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let _ = timeout(Duration::from_secs(1), first_response.chunk())
        .await
        .expect("first chunk")
        .expect("first chunk result")
        .expect("first chunk bytes");
    drop(first_response);

    tokio::time::sleep(Duration::from_millis(150)).await;

    let response = client
        .post(format!("{}/v1/responses", server.base_url()))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "follow up",
            "previous_response_id": "resp_timeout_1"
        }))
        .send()
        .await
        .expect("follow-up should complete");
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}
