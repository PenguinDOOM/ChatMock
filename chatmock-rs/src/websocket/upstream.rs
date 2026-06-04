use std::{
    collections::VecDeque,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use axum::{body::Body, response::Response};
use futures_util::{stream::BoxStream, SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};
use tokio::time::{interval, timeout, Duration, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    auth::load_effective_chatgpt_auth_from_env_with_refresher,
    jobs::JobManager,
    routes::json_response,
    upstream::{
        build_upstream_websocket_headers, refresh_chatgpt_tokens, upstream_websocket_url,
        UpstreamResponse,
    },
    upstream_errors::{build_upstream_error, UpstreamErrorContext},
    websocket::registry::{
        ResponsesWebsocketSessionCapacityError, ResponsesWebsocketSessionConflictError,
        ResponsesWebsocketSessionNotFoundError, RetainedUpstreamWebsocket,
        RetainedUpstreamWebsocketLeaseGuard, RetainedUpstreamWebsocketRegistry,
    },
};

pub type BoxedUpstreamWebsocketError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponsesWebsocketConnectFuture = Pin<
    Box<dyn Future<Output = Result<SharedUpstreamWebsocket, BoxedUpstreamWebsocketError>> + Send>,
>;
pub type ResponsesWebsocketConnectorFn =
    dyn Fn(ResponsesWebsocketConnectContext) -> ResponsesWebsocketConnectFuture + Send + Sync;

const STATEFUL_SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(1);
const STATEFUL_SSE_DISCONNECT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct StatefulResponsesWebsocketStreamConfig {
    pub keep_alive_interval: Duration,
    pub disconnect_drain_timeout: Duration,
}

impl Default for StatefulResponsesWebsocketStreamConfig {
    fn default() -> Self {
        Self {
            keep_alive_interval: STATEFUL_SSE_KEEP_ALIVE_INTERVAL,
            disconnect_drain_timeout: STATEFUL_SSE_DISCONNECT_DRAIN_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesWebsocketConnectContext {
    pub session_id: String,
    pub thread_id: String,
    pub window_generation: u64,
    pub turn_state: Option<String>,
}

impl ResponsesWebsocketConnectContext {
    pub fn compatibility(session_id: impl Into<String>) -> Self {
        let session_id = session_id.into();
        Self {
            thread_id: session_id.clone(),
            session_id,
            window_generation: 1,
            turn_state: None,
        }
    }
}

#[derive(Clone)]
pub struct ResponsesWebsocketConnector(Arc<ResponsesWebsocketConnectorFn>);

impl ResponsesWebsocketConnector {
    pub fn new(connector: Arc<ResponsesWebsocketConnectorFn>) -> Self {
        Self(connector)
    }

    pub async fn connect(
        &self,
        context: ResponsesWebsocketConnectContext,
    ) -> Result<SharedUpstreamWebsocket, BoxedUpstreamWebsocketError> {
        (self.0)(context).await
    }
}

pub fn live_responses_websocket_connector(
    http_client: reqwest::Client,
) -> ResponsesWebsocketConnector {
    ResponsesWebsocketConnector::new(Arc::new(move |context| {
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
            let headers = build_upstream_websocket_headers(
                &effective_auth.access_token,
                &effective_auth.account_id,
                &context.session_id,
                &context.thread_id,
                context.window_generation,
                context.turn_state.as_deref(),
            );
            for (name, value) in &headers {
                request.headers_mut().insert(name, value.clone());
            }

            let (stream, response) = connect_async(request).await?;
            let turn_state = response
                .headers()
                .get("x-codex-turn-state")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty());
            Ok(SharedUpstreamWebsocket::live(stream, turn_state))
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
    turn_state: Option<String>,
    closed: bool,
}

struct LiveUpstreamWebsocketState {
    stream: tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    turn_state: Option<String>,
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
                    turn_state: None,
                    closed: false,
                },
            ))),
        }
    }

    pub fn scripted_with_turn_state(
        scripted_receives: Vec<ScriptedUpstreamReceive>,
        turn_state: impl Into<String>,
    ) -> Self {
        let socket = Self::scripted(scripted_receives);
        if let SharedUpstreamWebsocketBackend::Scripted(state) = &socket.backend {
            state.lock().expect("scripted websocket lock").turn_state = Some(turn_state.into());
        }
        socket
    }

    pub fn live(
        stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        turn_state: Option<String>,
    ) -> Self {
        Self {
            backend: SharedUpstreamWebsocketBackend::Live(Arc::new(LiveUpstreamWebsocketState {
                stream: tokio::sync::Mutex::new(Some(stream)),
                turn_state,
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

    pub fn upgrade_turn_state(&self) -> Option<String> {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => state
                .lock()
                .expect("scripted websocket lock")
                .turn_state
                .clone(),
            SharedUpstreamWebsocketBackend::Live(state) => state.turn_state.clone(),
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
    job_manager: Option<Arc<JobManager>>,
) -> Result<UpstreamResponse, Response> {
    let socket = connector
        .connect(ResponsesWebsocketConnectContext::compatibility(session_id))
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket connection failed", error))?;

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    socket
        .send_text(outbound)
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket request failed", error))?;

    let mut body = String::new();
    let mut current_response_id = None;
    while let Some(message) = socket
        .recv_text()
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket receive failed", error))?
    {
        if let Some(response_id) = websocket_message_response_id(&message) {
            current_response_id = Some(response_id);
        }
        if intercept_chatmock_job_tool_call(
            &socket,
            job_manager.as_ref(),
            &message,
            current_response_id.as_deref(),
        )
        .await?
        {
            continue;
        }
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
    registry: Arc<RetainedUpstreamWebsocketRegistry<SharedUpstreamWebsocket>>,
    _session_id: &str,
    payload: Value,
    previous_response_id: Option<&str>,
    job_manager: Option<Arc<JobManager>>,
) -> Result<UpstreamResponse, Response> {
    let lease = registry
        .acquire_async_with_metadata(previous_response_id, |metadata| async move {
            let context = ResponsesWebsocketConnectContext {
                session_id: metadata.session_id,
                thread_id: metadata.thread_id,
                window_generation: metadata.window_generation,
                turn_state: metadata.turn_state,
            };
            connector.connect(context).await
        })
        .await
        .map_err(websocket_registry_error)?;
    let mut guard = RetainedUpstreamWebsocketLeaseGuard::new(Arc::clone(&registry), lease);
    guard.set_turn_state(guard.lease().upstream_ws.upgrade_turn_state());

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    if let Err(error) = guard.lease().upstream_ws.send_text(outbound).await {
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    let mut body = String::new();
    let mut retained_response_id = None;
    let mut retain = false;
    loop {
        let message = match guard.lease().upstream_ws.recv_text().await {
            Ok(message) => message,
            Err(error) => {
                return Err(websocket_gateway_error(
                    "Upstream websocket receive failed",
                    error,
                ));
            }
        };

        let Some(message) = message else {
            break;
        };

        if let Some(response_id) = websocket_message_response_id(&message) {
            retained_response_id = Some(response_id);
        }
        if intercept_chatmock_job_tool_call(
            &guard.lease().upstream_ws,
            job_manager.as_ref(),
            &message,
            retained_response_id.as_deref(),
        )
        .await?
        {
            continue;
        }

        body.push_str("data: ");
        body.push_str(&message);
        body.push_str("\n\n");

        if let Some(metadata) = websocket_event_metadata(&message) {
            if metadata.response_id.is_some() {
                retained_response_id = metadata.response_id;
            }
            if metadata.completed {
                retain = retained_response_id.is_some();
                if let Some(response_id) = retained_response_id.clone() {
                    guard.mark_completed(response_id);
                }
                break;
            }
            if metadata.failed || metadata.errored {
                break;
            }
        }
    }

    if !retain {
        drop(retained_response_id);
    }
    guard.release();

    Ok(UpstreamResponse {
        status_code: StatusCode::OK,
        content_type: Some("text/event-stream".to_string()),
        body: body.into_bytes(),
    })
}

pub async fn send_stateful_responses_create_stream(
    connector: &ResponsesWebsocketConnector,
    registry: Arc<RetainedUpstreamWebsocketRegistry<SharedUpstreamWebsocket>>,
    _session_id: &str,
    payload: Value,
    previous_response_id: Option<&str>,
    stream_config: StatefulResponsesWebsocketStreamConfig,
    job_manager: Option<Arc<JobManager>>,
) -> Result<Response<Body>, Response> {
    let lease = registry
        .acquire_async_with_metadata(previous_response_id, |metadata| async move {
            let context = ResponsesWebsocketConnectContext {
                session_id: metadata.session_id,
                thread_id: metadata.thread_id,
                window_generation: metadata.window_generation,
                turn_state: metadata.turn_state,
            };
            connector.connect(context).await
        })
        .await
        .map_err(websocket_registry_error)?;
    let mut guard = RetainedUpstreamWebsocketLeaseGuard::new(Arc::clone(&registry), lease);
    guard.set_turn_state(guard.lease().upstream_ws.upgrade_turn_state());

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    if let Err(error) = guard.lease().upstream_ws.send_text(outbound).await {
        drop(guard);
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    Ok(crate::routes::event_streaming_response(
        StatusCode::OK,
        stateful_sse_stream(guard, stream_config, job_manager),
    ))
}

fn stateful_sse_stream(
    mut guard: RetainedUpstreamWebsocketLeaseGuard<SharedUpstreamWebsocket>,
    config: StatefulResponsesWebsocketStreamConfig,
    job_manager: Option<Arc<JobManager>>,
) -> BoxStream<'static, Result<Vec<u8>, Infallible>> {
    let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, Infallible>>(8);
    tokio::spawn(async move {
        let mut retained_response_id = None;
        let mut downstream_open = true;
        let mut disconnect_deadline = None;
        let mut keep_alive = interval(config.keep_alive_interval);
        keep_alive.tick().await;
        loop {
            let message = if downstream_open {
                tokio::select! {
                    message = guard.lease().upstream_ws.recv_text() => message,
                    _ = keep_alive.tick() => {
                        if sender.send(Ok(b": keep-alive\n\n".to_vec())).await.is_err() {
                            downstream_open = false;
                            disconnect_deadline =
                                Some(Instant::now() + config.disconnect_drain_timeout);
                        }
                        continue;
                    }
                }
            } else {
                let Some(deadline) = disconnect_deadline else {
                    break;
                };
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                match timeout(deadline - now, guard.lease().upstream_ws.recv_text()).await {
                    Ok(message) => message,
                    Err(_) => break,
                }
            };
            let message = match message {
                Ok(Some(message)) => message,
                Ok(None) | Err(_) => break,
            };

            if let Some(response_id) = websocket_message_response_id(&message) {
                retained_response_id = Some(response_id);
            }
            match intercept_chatmock_job_tool_call(
                &guard.lease().upstream_ws,
                job_manager.as_ref(),
                &message,
                retained_response_id.as_deref(),
            )
            .await
            {
                Ok(true) => continue,
                Ok(false) => {}
                Err(_) => break,
            }

            if downstream_open {
                let mut frame = Vec::with_capacity(message.len() + 8);
                frame.extend_from_slice(b"data: ");
                frame.extend_from_slice(message.as_bytes());
                frame.extend_from_slice(b"\n\n");
                if sender.send(Ok(frame)).await.is_err() {
                    downstream_open = false;
                    disconnect_deadline = Some(Instant::now() + config.disconnect_drain_timeout);
                }
            }

            if let Some(metadata) = websocket_event_metadata(&message) {
                if metadata.response_id.is_some() {
                    retained_response_id = metadata.response_id;
                }
                if metadata.completed {
                    if let Some(response_id) = retained_response_id {
                        guard.mark_completed(response_id);
                    }
                    break;
                }
                if metadata.failed || metadata.errored {
                    break;
                }
            }
        }
    });
    Box::pin(tokio_stream_from_receiver(receiver))
}

fn tokio_stream_from_receiver<T: Send + 'static>(
    receiver: mpsc::Receiver<T>,
) -> impl futures_util::Stream<Item = T> + Send + 'static {
    futures_util::stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|item| (item, receiver))
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

fn websocket_message_response_id(message: &str) -> Option<String> {
    serde_json::from_str::<Value>(message)
        .ok()
        .and_then(|value| event_response_id(&value))
}

fn event_response_id(value: &Value) -> Option<String> {
    value
        .get("response")
        .and_then(Value::as_object)
        .and_then(|response| response.get("id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("response_id").and_then(Value::as_str))
        .map(str::to_string)
}

async fn intercept_chatmock_job_tool_call(
    socket: &SharedUpstreamWebsocket,
    job_manager: Option<&Arc<JobManager>>,
    message: &str,
    current_response_id: Option<&str>,
) -> Result<bool, Response> {
    let Some(event) = serde_json::from_str::<Value>(message).ok() else {
        return Ok(false);
    };
    if event.get("type").and_then(Value::as_str) != Some("response.output_item.done") {
        return Ok(false);
    }
    let Some(item) = event.get("item").and_then(Value::as_object) else {
        return Ok(false);
    };
    if item.get("type").and_then(Value::as_str) != Some("function_call") {
        return Ok(false);
    }
    let Some(name) = item.get("name").and_then(Value::as_str) else {
        return Ok(false);
    };
    if !name.starts_with("chatmock_") {
        return Ok(false);
    }

    let call_id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = item
        .get("arguments")
        .and_then(Value::as_str)
        .unwrap_or("{}");
    let output = match job_manager {
        Some(job_manager) => job_manager.execute_tool_call(name, arguments).await,
        None => serde_json::json!({
            "status": "failed",
            "error": {
                "message": "ChatMock jobs are disabled."
            }
        }),
    };
    let previous_response_id = current_response_id
        .map(str::to_string)
        .or_else(|| event_response_id(&event))
        .ok_or_else(|| {
            json_response(
                StatusCode::BAD_GATEWAY,
                serde_json::to_value(build_upstream_error(
                    Some("ChatMock internal tool call did not include a response id"),
                    UpstreamErrorContext::default(),
                ))
                .expect("internal tool error payload"),
            )
        })?;
    let follow_up = serde_json::json!({
        "type": "response.create",
        "previous_response_id": previous_response_id,
        "input": [{
            "type": "function_call_output",
            "call_id": call_id,
            "output": output.to_string(),
        }]
    });
    socket
        .send_text(follow_up.to_string())
        .await
        .map_err(|error| {
            websocket_gateway_error("Upstream websocket internal tool follow-up failed", error)
        })?;
    Ok(true)
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
