use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    auth::load_effective_chatgpt_auth_from_env_with_refresher,
    routes::json_response,
    upstream::{
        build_upstream_headers, refresh_chatgpt_tokens, upstream_websocket_url, UpstreamResponse,
    },
    upstream_errors::{build_upstream_error, UpstreamErrorContext},
    websocket::registry::{
        ResponsesWebsocketSessionCapacityError, ResponsesWebsocketSessionConflictError,
        ResponsesWebsocketSessionNotFoundError, RetainedUpstreamWebsocket,
        RetainedUpstreamWebsocketRegistry,
    },
};

pub type BoxedUpstreamWebsocketError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponsesWebsocketConnectFuture = Pin<
    Box<dyn Future<Output = Result<SharedUpstreamWebsocket, BoxedUpstreamWebsocketError>> + Send>,
>;
pub type ResponsesWebsocketConnectorFn =
    dyn Fn(String) -> ResponsesWebsocketConnectFuture + Send + Sync;

#[derive(Clone)]
pub struct ResponsesWebsocketConnector(Arc<ResponsesWebsocketConnectorFn>);

impl ResponsesWebsocketConnector {
    pub fn new(connector: Arc<ResponsesWebsocketConnectorFn>) -> Self {
        Self(connector)
    }

    pub async fn connect(
        &self,
        session_id: String,
    ) -> Result<SharedUpstreamWebsocket, BoxedUpstreamWebsocketError> {
        (self.0)(session_id).await
    }
}

pub fn live_responses_websocket_connector(
    http_client: reqwest::Client,
) -> ResponsesWebsocketConnector {
    ResponsesWebsocketConnector::new(Arc::new(move |session_id| {
        let http_client = http_client.clone();
        Box::pin(async move {
            let refresh_client = http_client.clone();
            let effective_auth =
                load_effective_chatgpt_auth_from_env_with_refresher(move |refresh_token| {
                    let http_client = refresh_client.clone();
                    async move { refresh_chatgpt_tokens(http_client, refresh_token).await }
                })
                .await
                .ok_or_else(|| {
                    std::io::Error::other(
                        "Missing ChatGPT credentials. Run 'chatmock-rs login' first.",
                    )
                })?;

            let mut request = upstream_websocket_url()
                .map_err(std::io::Error::other)?
                .into_client_request()?;
            let headers = build_upstream_headers(
                &effective_auth.access_token,
                &effective_auth.account_id,
                &session_id,
                "application/json",
            );
            for (name, value) in &headers {
                request.headers_mut().insert(name, value.clone());
            }

            let (stream, _) = connect_async(request).await?;
            Ok(SharedUpstreamWebsocket::live(stream))
        })
    }))
}

impl std::fmt::Debug for ResponsesWebsocketConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResponsesWebsocketConnector(..)")
    }
}

#[derive(Debug, Clone)]
pub struct ScriptedUpstreamReceive {
    step: ScriptedUpstreamReceiveStep,
}

#[derive(Debug, Clone)]
enum ScriptedUpstreamReceiveStep {
    Text(String),
    Wait(Arc<Notify>),
}

impl ScriptedUpstreamReceive {
    pub fn text(payload: Value) -> Self {
        Self {
            step: ScriptedUpstreamReceiveStep::Text(
                serde_json::to_string(&payload).expect("scripted payload json"),
            ),
        }
    }

    pub fn wait(notify: Arc<Notify>) -> Self {
        Self {
            step: ScriptedUpstreamReceiveStep::Wait(notify),
        }
    }
}

#[derive(Debug)]
struct ScriptedUpstreamWebsocketState {
    scripted_receives: VecDeque<ScriptedUpstreamReceiveStep>,
    sent_messages: Vec<String>,
    closed: bool,
}

struct LiveUpstreamWebsocketState {
    stream: tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    closed: AtomicBool,
}

#[derive(Clone)]
pub struct SharedUpstreamWebsocket {
    backend: SharedUpstreamWebsocketBackend,
}

#[derive(Clone)]
enum SharedUpstreamWebsocketBackend {
    Scripted(Arc<Mutex<ScriptedUpstreamWebsocketState>>),
    Live(Arc<LiveUpstreamWebsocketState>),
}

impl std::fmt::Debug for SharedUpstreamWebsocket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SharedUpstreamWebsocket(..)")
    }
}

impl SharedUpstreamWebsocket {
    pub fn scripted(scripted_receives: Vec<ScriptedUpstreamReceive>) -> Self {
        Self {
            backend: SharedUpstreamWebsocketBackend::Scripted(Arc::new(Mutex::new(
                ScriptedUpstreamWebsocketState {
                    scripted_receives: scripted_receives
                        .into_iter()
                        .map(|message| message.step)
                        .collect(),
                    sent_messages: Vec::new(),
                    closed: false,
                },
            ))),
        }
    }

    pub fn live(stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Self {
        Self {
            backend: SharedUpstreamWebsocketBackend::Live(Arc::new(LiveUpstreamWebsocketState {
                stream: tokio::sync::Mutex::new(Some(stream)),
                closed: AtomicBool::new(false),
            })),
        }
    }

    pub async fn send_text(&self, message: String) -> Result<(), BoxedUpstreamWebsocketError> {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => {
                state
                    .lock()
                    .expect("scripted websocket lock")
                    .sent_messages
                    .push(message);
                Ok(())
            }
            SharedUpstreamWebsocketBackend::Live(state) => {
                let mut guard = state.stream.lock().await;
                let Some(stream) = guard.as_mut() else {
                    state.closed.store(true, Ordering::SeqCst);
                    return Err(std::io::Error::other("Upstream websocket is closed").into());
                };
                if let Err(error) = stream.send(Message::Text(message.into())).await {
                    state.closed.store(true, Ordering::SeqCst);
                    *guard = None;
                    return Err(error.into());
                }
                Ok(())
            }
        }
    }

    pub async fn recv_text(&self) -> Result<Option<String>, BoxedUpstreamWebsocketError> {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => loop {
                let next = state
                    .lock()
                    .expect("scripted websocket lock")
                    .scripted_receives
                    .pop_front();
                match next {
                    Some(ScriptedUpstreamReceiveStep::Text(message)) => return Ok(Some(message)),
                    Some(ScriptedUpstreamReceiveStep::Wait(notify)) => notify.notified().await,
                    None => return Ok(None),
                }
            },
            SharedUpstreamWebsocketBackend::Live(state) => loop {
                let mut guard = state.stream.lock().await;
                let Some(stream) = guard.as_mut() else {
                    state.closed.store(true, Ordering::SeqCst);
                    return Ok(None);
                };
                let frame = stream.next().await.transpose()?;
                match frame {
                    Some(Message::Text(text)) => return Ok(Some(text.to_string())),
                    Some(Message::Binary(bytes)) => {
                        return Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
                    }
                    Some(Message::Ping(payload)) => {
                        stream.send(Message::Pong(payload)).await?;
                    }
                    Some(Message::Pong(_)) | Some(Message::Frame(_)) => {}
                    Some(Message::Close(_)) | None => {
                        state.closed.store(true, Ordering::SeqCst);
                        *guard = None;
                        return Ok(None);
                    }
                }
            },
        }
    }

    pub async fn scripted_sent_messages(&self) -> Vec<String> {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => state
                .lock()
                .expect("scripted websocket lock")
                .sent_messages
                .clone(),
            SharedUpstreamWebsocketBackend::Live(_) => Vec::new(),
        }
    }
}

impl RetainedUpstreamWebsocket for SharedUpstreamWebsocket {
    fn close_socket(&self) {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => {
                state.lock().expect("scripted websocket lock").closed = true;
            }
            SharedUpstreamWebsocketBackend::Live(state) => {
                state.closed.store(true, Ordering::SeqCst);
                if let Ok(mut guard) = state.stream.try_lock() {
                    *guard = None;
                }
            }
        }
    }

    fn is_socket_closed(&self) -> bool {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => {
                state.lock().expect("scripted websocket lock").closed
            }
            SharedUpstreamWebsocketBackend::Live(state) => state.closed.load(Ordering::SeqCst),
        }
    }
}

pub async fn send_responses_create_request(
    connector: &ResponsesWebsocketConnector,
    session_id: &str,
    payload: Value,
) -> Result<UpstreamResponse, Response> {
    let socket = connector
        .connect(session_id.to_string())
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket connection failed", error))?;

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    socket
        .send_text(outbound)
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket request failed", error))?;

    let mut body = String::new();
    while let Some(message) = socket
        .recv_text()
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket receive failed", error))?
    {
        body.push_str("data: ");
        body.push_str(&message);
        body.push_str("\n\n");

        if websocket_terminal_event(&message) {
            break;
        }
    }

    Ok(UpstreamResponse {
        status_code: StatusCode::OK,
        content_type: Some("text/event-stream".to_string()),
        body: body.into_bytes(),
    })
}

pub async fn send_stateful_responses_create_request(
    connector: &ResponsesWebsocketConnector,
    registry: &RetainedUpstreamWebsocketRegistry<SharedUpstreamWebsocket>,
    session_id: &str,
    payload: Value,
    previous_response_id: Option<&str>,
) -> Result<UpstreamResponse, Response> {
    let lease = registry
        .acquire_async(previous_response_id, || {
            connector.connect(session_id.to_string())
        })
        .await
        .map_err(websocket_registry_error)?;

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    if let Err(error) = lease.upstream_ws.send_text(outbound).await {
        registry.release(lease, false, None);
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    let mut body = String::new();
    let mut retained_response_id = None;
    let mut retain = false;
    loop {
        let message = match lease.upstream_ws.recv_text().await {
            Ok(message) => message,
            Err(error) => {
                registry.release(lease, false, None);
                return Err(websocket_gateway_error(
                    "Upstream websocket receive failed",
                    error,
                ));
            }
        };

        let Some(message) = message else {
            break;
        };

        body.push_str("data: ");
        body.push_str(&message);
        body.push_str("\n\n");

        if let Some(metadata) = websocket_event_metadata(&message) {
            if metadata.response_id.is_some() {
                retained_response_id = metadata.response_id;
            }
            if metadata.completed {
                retain = retained_response_id.is_some();
                break;
            }
            if metadata.failed || metadata.errored {
                break;
            }
        }
    }

    registry.release(lease, retain, retained_response_id.as_deref());

    Ok(UpstreamResponse {
        status_code: StatusCode::OK,
        content_type: Some("text/event-stream".to_string()),
        body: body.into_bytes(),
    })
}

fn websocket_gateway_error(message: &str, error: BoxedUpstreamWebsocketError) -> Response {
    json_response(
        StatusCode::BAD_GATEWAY,
        serde_json::to_value(build_upstream_error(
            Some(message),
            UpstreamErrorContext {
                exception: Some(error.to_string()),
                ..UpstreamErrorContext::default()
            },
        ))
        .expect("upstream websocket error payload"),
    )
}

fn websocket_registry_error(error: BoxedUpstreamWebsocketError) -> Response {
    if error
        .downcast_ref::<ResponsesWebsocketSessionNotFoundError>()
        .is_some()
    {
        let response_id = error
            .downcast_ref::<ResponsesWebsocketSessionNotFoundError>()
            .map(|not_found| not_found.response_id.as_str());
        return json_response(
            StatusCode::BAD_REQUEST,
            previous_response_not_found_payload(response_id),
        );
    }
    if error
        .downcast_ref::<ResponsesWebsocketSessionConflictError>()
        .is_some()
    {
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({"error": {"message": error.to_string()}}),
        );
    }
    if error
        .downcast_ref::<ResponsesWebsocketSessionCapacityError>()
        .is_some()
    {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": {"message": error.to_string()}}),
        );
    }
    websocket_gateway_error("Upstream websocket connection failed", error)
}

#[derive(Debug)]
struct WebsocketEventMetadata {
    response_id: Option<String>,
    completed: bool,
    failed: bool,
    errored: bool,
}

fn websocket_event_metadata(message: &str) -> Option<WebsocketEventMetadata> {
    let value = serde_json::from_str::<Value>(message).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;
    let response_id = value
        .get("response")
        .and_then(Value::as_object)
        .and_then(|response| response.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Some(WebsocketEventMetadata {
        response_id,
        completed: event_type == "response.completed",
        failed: event_type == "response.failed",
        errored: event_type == "error",
    })
}

fn websocket_terminal_event(message: &str) -> bool {
    serde_json::from_str::<Value>(message)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .map(|event_type| {
            matches!(
                event_type.as_str(),
                "response.completed" | "response.failed" | "error"
            )
        })
        .unwrap_or(false)
}

fn previous_response_not_found_payload(response_id: Option<&str>) -> Value {
    let message = response_id
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("No response found for previous_response_id {value}."))
        .unwrap_or_else(|| "No response found for previous_response_id.".to_string());
    json!({
        "code": "previous_response_not_found",
        "message": message,
        "error": {
            "message": message,
            "code": "previous_response_not_found",
        }
    })
}

fn websocket_request_payload(payload: Value) -> Value {
    match payload {
        Value::Object(mut object) => {
            object.insert(
                "type".to_string(),
                Value::String("response.create".to_string()),
            );
            Value::Object(object)
        }
        other => serde_json::json!({
            "type": "response.create",
            "payload": other,
        }),
    }
}
