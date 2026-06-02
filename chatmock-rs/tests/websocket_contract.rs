use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chatmock_rs::server;
use chatmock_rs::websocket::registry::{
    ResponsesWebsocketSessionCapacityError, ResponsesWebsocketSessionConflictError,
    ResponsesWebsocketSessionNotFoundError, RetainedUpstreamWebsocket,
    RetainedUpstreamWebsocketRegistry,
};
use chatmock_rs::websocket::upstream::{ScriptedUpstreamReceive, SharedUpstreamWebsocket};
use chatmock_rs::RuntimeConfig;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

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
fn registry_treats_closed_retained_websocket_as_missing_marker() {
    let registry = RetainedUpstreamWebsocketRegistry::new(2);
    let first = registry
        .acquire(None, || Ok(make_socket()))
        .expect("first lease");
    registry.release(first.clone(), true, Some("resp_fixed_1"));
    first.upstream_ws.lock().expect("lock socket").mark_closed();

    let error = registry
        .acquire(Some("resp_fixed_1"), || Ok(make_socket()))
        .expect_err("closed socket should be treated as missing");
    let not_found = error
        .downcast_ref::<ResponsesWebsocketSessionNotFoundError>()
        .expect("not found error");
    assert_eq!(not_found.response_id, "resp_fixed_1");
    assert_eq!(
        first.upstream_ws.lock().expect("lock socket").close_calls,
        1
    );
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

    let server = server::spawn_server_with_websocket_connector(
        RuntimeConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            responses_websocket_upstream: true,
            responses_websocket_upstream_stateful: true,
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

    let sent_messages = scripted_socket.scripted_sent_messages().await;
    assert_eq!(sent_messages.len(), 2);
    let first_outbound: Value = serde_json::from_str(&sent_messages[0]).expect("first outbound");
    let second_outbound: Value = serde_json::from_str(&sent_messages[1]).expect("second outbound");
    assert!(first_outbound.get("previous_response_id").is_none());
    assert_eq!(second_outbound["previous_response_id"], "resp_ws_turn_1");
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
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        body["error"]["message"],
        "No retained upstream websocket exists for response 'resp_missing'."
    );
    assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
}
