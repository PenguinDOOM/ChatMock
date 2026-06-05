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
    tungstenite::{client::IntoClientRequest, protocol::frame::coding::CloseCode, Message},
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
    Close(UpstreamCloseMetadata),
    Error(String),
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

    pub fn close(code: Option<CloseCode>, reason: Option<String>) -> Self {
        Self {
            step: ScriptedUpstreamReceiveStep::Close(UpstreamCloseMetadata { code, reason }),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            step: ScriptedUpstreamReceiveStep::Error(message.into()),
        }
    }
}

#[derive(Debug)]
struct ScriptedUpstreamWebsocketState {
    scripted_receives: VecDeque<ScriptedUpstreamReceiveStep>,
    sent_messages: Vec<String>,
    turn_state: Option<String>,
    close_metadata: Option<UpstreamCloseMetadata>,
    closed: bool,
}

struct LiveUpstreamWebsocketState {
    outbound_tx: mpsc::Sender<OutboundWsMessage>,
    inbound_rx: tokio::sync::Mutex<mpsc::Receiver<Result<String, BoxedUpstreamWebsocketError>>>,
    turn_state: Option<String>,
    closed: AtomicBool,
    close_metadata: Mutex<Option<UpstreamCloseMetadata>>,
}

enum OutboundWsMessage {
    Text(String),
    #[allow(dead_code)]
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

#[derive(Clone)]
pub struct SharedUpstreamWebsocket {
    backend: SharedUpstreamWebsocketBackend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamCloseMetadata {
    pub code: Option<CloseCode>,
    pub reason: Option<String>,
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
                    close_metadata: None,
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
        let (outbound_tx, outbound_rx) = mpsc::channel(32);
        let (inbound_tx, inbound_rx) = mpsc::channel(32);
        let state = Arc::new(LiveUpstreamWebsocketState {
            outbound_tx,
            inbound_rx: tokio::sync::Mutex::new(inbound_rx),
            turn_state,
            closed: AtomicBool::new(false),
            close_metadata: Mutex::new(None),
        });
        tokio::spawn(run_live_upstream_websocket_pump(
            stream,
            outbound_rx,
            inbound_tx,
            Arc::clone(&state),
        ));
        Self {
            backend: SharedUpstreamWebsocketBackend::Live(state),
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
                if state.closed.load(Ordering::SeqCst) {
                    state.closed.store(true, Ordering::SeqCst);
                    return Err(std::io::Error::other("Upstream websocket is closed").into());
                }
                state
                    .outbound_tx
                    .send(OutboundWsMessage::Text(message))
                    .await
                    .map_err(|_| {
                        state.closed.store(true, Ordering::SeqCst);
                        std::io::Error::other("Upstream websocket pump is closed")
                    })?;
                tracing::trace!("send_text_enqueued");
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
                    Some(ScriptedUpstreamReceiveStep::Close(metadata)) => {
                        let mut state = state.lock().expect("scripted websocket lock");
                        state.closed = true;
                        state.close_metadata = Some(metadata);
                        return Ok(None);
                    }
                    Some(ScriptedUpstreamReceiveStep::Error(message)) => {
                        let mut state = state.lock().expect("scripted websocket lock");
                        state.closed = true;
                        return Err(std::io::Error::other(message).into());
                    }
                    None => return Ok(None),
                }
            },
            SharedUpstreamWebsocketBackend::Live(state) => {
                let mut receiver = state.inbound_rx.lock().await;
                match receiver.recv().await {
                    Some(Ok(message)) => {
                        tracing::trace!("recv_text_delivered");
                        Ok(Some(message))
                    }
                    Some(Err(error)) => Err(error),
                    None => Ok(None),
                }
            }
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

    pub fn close_metadata(&self) -> Option<UpstreamCloseMetadata> {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => state
                .lock()
                .expect("scripted websocket lock")
                .close_metadata
                .clone(),
            SharedUpstreamWebsocketBackend::Live(state) => state
                .close_metadata
                .lock()
                .expect("live close metadata lock")
                .clone(),
        }
    }
}

async fn run_live_upstream_websocket_pump(
    mut stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    mut outbound_rx: mpsc::Receiver<OutboundWsMessage>,
    inbound_tx: mpsc::Sender<Result<String, BoxedUpstreamWebsocketError>>,
    state: Arc<LiveUpstreamWebsocketState>,
) {
    tracing::debug!("ws_pump_started");
    loop {
        tokio::select! {
            outbound = outbound_rx.recv() => {
                let Some(outbound) = outbound else {
                    mark_live_upstream_closed(&state, None, None);
                    break;
                };
                let frame = match outbound {
                    OutboundWsMessage::Text(text) => outbound_ws_message_to_frame(OutboundWsMessage::Text(text)),
                    OutboundWsMessage::Ping(payload) => outbound_ws_message_to_frame(OutboundWsMessage::Ping(payload)),
                    OutboundWsMessage::Pong(payload) => outbound_ws_message_to_frame(OutboundWsMessage::Pong(payload)),
                    OutboundWsMessage::Close => {
                        mark_live_upstream_closed(&state, None, None);
                        let _ = stream.close(None).await;
                        break;
                    }
                };
                if let Err(error) = stream.send(frame).await {
                    let message = error.to_string();
                    mark_live_upstream_error(&state, &message);
                    let _ = inbound_tx
                        .send(Err(std::io::Error::other(message).into()))
                        .await;
                    break;
                }
            }
            frame = stream.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        tracing::trace!("ws_pump_text_received");
                        if inbound_tx.send(Ok(text.to_string())).await.is_err() {
                            mark_live_upstream_closed(&state, None, None);
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        tracing::trace!("ws_pump_text_received");
                        let message = String::from_utf8_lossy(&bytes).into_owned();
                        if inbound_tx.send(Ok(message)).await.is_err() {
                            mark_live_upstream_closed(&state, None, None);
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        tracing::trace!("ws_pump_ping_received");
                        let pong = outbound_ws_message_to_frame(OutboundWsMessage::Pong(payload.to_vec()));
                        if let Err(error) = stream.send(pong).await {
                            let message = error.to_string();
                            mark_live_upstream_error(&state, &message);
                            let _ = inbound_tx
                                .send(Err(std::io::Error::other(message).into()))
                                .await;
                            break;
                        }
                        tracing::trace!("ws_pump_pong_sent");
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        mark_live_upstream_closed(
                            &state,
                            frame.as_ref().map(|frame| frame.code),
                            frame.as_ref().map(|frame| frame.reason.to_string()),
                        );
                        break;
                    }
                    Some(Err(error)) => {
                        let message = error.to_string();
                        mark_live_upstream_error(&state, &message);
                        let _ = inbound_tx
                            .send(Err(std::io::Error::other(message).into()))
                            .await;
                        break;
                    }
                    None => {
                        mark_live_upstream_closed(&state, None, None);
                        break;
                    }
                }
            }
        }
    }
}

fn outbound_ws_message_to_frame(message: OutboundWsMessage) -> Message {
    match message {
        OutboundWsMessage::Text(text) => Message::Text(text.into()),
        OutboundWsMessage::Ping(payload) => Message::Ping(payload.into()),
        OutboundWsMessage::Pong(payload) => Message::Pong(payload.into()),
        OutboundWsMessage::Close => Message::Close(None),
    }
}

fn mark_live_upstream_closed(
    state: &LiveUpstreamWebsocketState,
    code: Option<CloseCode>,
    reason: Option<String>,
) {
    state.closed.store(true, Ordering::SeqCst);
    let metadata = UpstreamCloseMetadata { code, reason };
    tracing::warn!(
        code = ?metadata.code,
        reason = ?metadata.reason.as_deref(),
        "ws_pump_closed"
    );
    *state
        .close_metadata
        .lock()
        .expect("live close metadata lock") = Some(metadata);
}

fn mark_live_upstream_error(state: &LiveUpstreamWebsocketState, reason: &str) {
    state.closed.store(true, Ordering::SeqCst);
    *state
        .close_metadata
        .lock()
        .expect("live close metadata lock") = Some(UpstreamCloseMetadata {
        code: None,
        reason: Some(reason.to_string()),
    });
    tracing::warn!(error = %reason, "ws_pump_error");
}

impl RetainedUpstreamWebsocket for SharedUpstreamWebsocket {
    fn close_socket(&self) {
        match &self.backend {
            SharedUpstreamWebsocketBackend::Scripted(state) => {
                state.lock().expect("scripted websocket lock").closed = true;
            }
            SharedUpstreamWebsocketBackend::Live(state) => {
                state.closed.store(true, Ordering::SeqCst);
                let _ = state.outbound_tx.try_send(OutboundWsMessage::Close);
                *state
                    .close_metadata
                    .lock()
                    .expect("live close metadata lock") = Some(UpstreamCloseMetadata {
                    code: None,
                    reason: None,
                });
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
    const ROUTE: &str = "http_nonstream";
    let socket = connector
        .connect(ResponsesWebsocketConnectContext::compatibility(session_id))
        .await
        .map_err(|error| websocket_gateway_error("Upstream websocket connection failed", error))?;

    let outbound = serde_json::to_string(&websocket_request_payload(payload))
        .expect("responses websocket payload json");
    if let Err(error) = socket.send_text(outbound).await {
        tracing::warn!(route = ROUTE, error = %error, "upstream_send_failed");
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    let mut body = String::new();
    let mut response_buffer = String::new();
    let mut current_response_id = None;
    let mut pending_internal_outputs = Vec::new();
    loop {
        let message = match socket.recv_text().await {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(route = ROUTE, error = %error, "upstream_receive_failed");
                return Err(websocket_gateway_error(
                    "Upstream websocket receive failed",
                    error,
                ));
            }
        };

        let Some(message) = message else {
            log_upstream_socket_closed(ROUTE, &socket);
            tracing::debug!(
                route = ROUTE,
                reason = "upstream_socket_closed",
                "loop_break"
            );
            break;
        };

        trace_upstream_event(ROUTE, &message);
        if let Some(response_id) = websocket_message_response_id(&message) {
            current_response_id = Some(response_id);
        }
        if let Some(output) =
            maybe_intercept_chatmock_job_tool_call(ROUTE, job_manager.as_ref(), &message).await?
        {
            pending_internal_outputs.push(output);
            tracing::debug!(
                route = ROUTE,
                pending_count = pending_internal_outputs.len(),
                "internal_tool_output_pending"
            );
            continue;
        }

        if let Some(metadata) = websocket_event_metadata(&message) {
            if metadata.completed && !pending_internal_outputs.is_empty() {
                tracing::debug!(
                    route = ROUTE,
                    response_id = metadata.response_id.as_deref(),
                    pending_count = pending_internal_outputs.len(),
                    "intermediate_response_completed"
                );
                send_pending_internal_tool_follow_up(
                    ROUTE,
                    &socket,
                    metadata
                        .response_id
                        .as_deref()
                        .or(current_response_id.as_deref()),
                    &mut pending_internal_outputs,
                )
                .await?;
                current_response_id = None;
                response_buffer.clear();
                continue;
            }
        }

        push_sse_message(&mut response_buffer, &message);

        if websocket_terminal_event(&message) {
            body.push_str(&response_buffer);
            if let Some(metadata) = websocket_event_metadata(&message) {
                if metadata.completed {
                    tracing::debug!(
                        route = ROUTE,
                        response_id = metadata.response_id.as_deref(),
                        "final_response_completed"
                    );
                }
                tracing::debug!(
                    route = ROUTE,
                    completed = metadata.completed,
                    failed = metadata.failed,
                    errored = metadata.errored,
                    "loop_break"
                );
            } else {
                tracing::debug!(route = ROUTE, reason = "terminal_event", "loop_break");
            }
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
    const ROUTE: &str = "http_stateful";
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
    if let Err(error) = guard.lease().upstream_ws.send_text(outbound.clone()).await {
        tracing::warn!(route = ROUTE, error = %error, "upstream_send_failed");
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    let mut body = String::new();
    let mut response_buffer = String::new();
    let mut retained_response_id = None;
    let mut pending_internal_outputs = Vec::new();
    let mut retain = false;
    let mut received_any_event = false;
    let mut retry_used = false;
    loop {
        let message = match guard.lease().upstream_ws.recv_text().await {
            Ok(Some(message)) => {
                if retry_used && !received_any_event {
                    trace_upstream_event_after_retry(ROUTE, &message);
                }
                received_any_event = true;
                message
            }
            Ok(None) => {
                if should_retry_initial_receive(&guard, received_any_event, retry_used) {
                    log_upstream_socket_closed(ROUTE, &guard.lease().upstream_ws);
                    retry_used = true;
                    guard = reconnect_and_resend_stateful_request(
                        ROUTE,
                        connector,
                        Arc::clone(&registry),
                        guard,
                        &outbound,
                    )
                    .await?;
                    received_any_event = false;
                    continue;
                }
                log_upstream_socket_closed(ROUTE, &guard.lease().upstream_ws);
                tracing::debug!(
                    route = ROUTE,
                    reason = "upstream_socket_closed",
                    "loop_break"
                );
                break;
            }
            Err(error) => {
                let before_first_event = !received_any_event;
                tracing::warn!(
                    route = ROUTE,
                    before_first_event,
                    retry_used,
                    error = %error,
                    "upstream_receive_failed"
                );
                if should_retry_initial_receive(&guard, received_any_event, retry_used) {
                    retry_used = true;
                    guard = reconnect_and_resend_stateful_request(
                        ROUTE,
                        connector,
                        Arc::clone(&registry),
                        guard,
                        &outbound,
                    )
                    .await?;
                    received_any_event = false;
                    continue;
                }
                return Err(websocket_gateway_error(
                    "Upstream websocket receive failed",
                    error,
                ));
            }
        };

        trace_upstream_event(ROUTE, &message);
        if let Some(response_id) = websocket_message_response_id(&message) {
            retained_response_id = Some(response_id);
        }
        if let Some(output) =
            maybe_intercept_chatmock_job_tool_call(ROUTE, job_manager.as_ref(), &message).await?
        {
            pending_internal_outputs.push(output);
            tracing::debug!(
                route = ROUTE,
                pending_count = pending_internal_outputs.len(),
                "internal_tool_output_pending"
            );
            continue;
        }

        let metadata = websocket_event_metadata(&message);
        if let Some(metadata) = metadata.as_ref() {
            if metadata.response_id.is_some() {
                retained_response_id = metadata.response_id.clone();
            }
            if metadata.completed && !pending_internal_outputs.is_empty() {
                tracing::debug!(
                    route = ROUTE,
                    response_id = retained_response_id.as_deref(),
                    pending_count = pending_internal_outputs.len(),
                    "intermediate_response_completed"
                );
                send_pending_internal_tool_follow_up(
                    ROUTE,
                    &guard.lease().upstream_ws,
                    retained_response_id.as_deref(),
                    &mut pending_internal_outputs,
                )
                .await?;
                retained_response_id = None;
                response_buffer.clear();
                continue;
            }
        }

        push_sse_message(&mut response_buffer, &message);

        if let Some(metadata) = metadata {
            if metadata.completed {
                body.push_str(&response_buffer);
                retain = retained_response_id.is_some();
                tracing::debug!(
                    route = ROUTE,
                    response_id = retained_response_id.as_deref(),
                    "final_response_completed"
                );
                if let Some(response_id) = retained_response_id.clone() {
                    tracing::debug!(
                        route = ROUTE,
                        response_id = %response_id,
                        "registry_mark_completed"
                    );
                    guard.mark_completed(response_id);
                }
                tracing::debug!(
                    route = ROUTE,
                    reason = "final_response_completed",
                    "loop_break"
                );
                break;
            }
            if metadata.failed || metadata.errored {
                body.push_str(&response_buffer);
                tracing::debug!(
                    route = ROUTE,
                    failed = metadata.failed,
                    errored = metadata.errored,
                    "loop_break"
                );
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
    const ROUTE: &str = "stateful_sse";
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
    if let Err(error) = guard.lease().upstream_ws.send_text(outbound.clone()).await {
        tracing::warn!(route = ROUTE, error = %error, "upstream_send_failed");
        drop(guard);
        return Err(websocket_gateway_error(
            "Upstream websocket request failed",
            error,
        ));
    }

    Ok(crate::routes::event_streaming_response(
        StatusCode::OK,
        stateful_sse_stream(
            connector.clone(),
            registry,
            guard,
            outbound,
            stream_config,
            job_manager,
        ),
    ))
}

fn stateful_sse_stream(
    connector: ResponsesWebsocketConnector,
    registry: Arc<RetainedUpstreamWebsocketRegistry<SharedUpstreamWebsocket>>,
    mut guard: RetainedUpstreamWebsocketLeaseGuard<SharedUpstreamWebsocket>,
    outbound: String,
    config: StatefulResponsesWebsocketStreamConfig,
    job_manager: Option<Arc<JobManager>>,
) -> BoxStream<'static, Result<Vec<u8>, Infallible>> {
    let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, Infallible>>(8);
    tokio::spawn(async move {
        let mut retained_response_id = None;
        let mut pending_internal_outputs = Vec::new();
        let mut downstream_open = true;
        let mut disconnect_deadline = None;
        let mut received_any_event = false;
        let mut retry_used = false;
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
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "downstream_closed_without_deadline",
                        "loop_break"
                    );
                    break;
                };
                let now = Instant::now();
                if now >= deadline {
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "disconnect_drain_timeout",
                        "loop_break"
                    );
                    break;
                }
                match timeout(deadline - now, guard.lease().upstream_ws.recv_text()).await {
                    Ok(message) => message,
                    Err(_) => {
                        tracing::debug!(
                            route = "stateful_sse",
                            reason = "disconnect_drain_timeout",
                            "loop_break"
                        );
                        break;
                    }
                }
            };
            let message = match message {
                Ok(Some(message)) => {
                    if retry_used && !received_any_event {
                        trace_upstream_event_after_retry("stateful_sse", &message);
                    }
                    received_any_event = true;
                    message
                }
                Ok(None) => {
                    if should_retry_initial_receive(&guard, received_any_event, retry_used) {
                        log_upstream_socket_closed("stateful_sse", &guard.lease().upstream_ws);
                        retry_used = true;
                        match reconnect_and_resend_stateful_request(
                            "stateful_sse",
                            &connector,
                            Arc::clone(&registry),
                            guard,
                            &outbound,
                        )
                        .await
                        {
                            Ok(next_guard) => {
                                guard = next_guard;
                                received_any_event = false;
                                continue;
                            }
                            Err(error_response) => {
                                send_sse_error_response(&sender, error_response).await;
                                break;
                            }
                        }
                    }
                    log_upstream_socket_closed("stateful_sse", &guard.lease().upstream_ws);
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "upstream_socket_closed",
                        "loop_break"
                    );
                    break;
                }
                Err(error) => {
                    let before_first_event = !received_any_event;
                    tracing::warn!(
                        route = "stateful_sse",
                        before_first_event,
                        retry_used,
                        error = %error,
                        "upstream_receive_failed"
                    );
                    if should_retry_initial_receive(&guard, received_any_event, retry_used) {
                        retry_used = true;
                        match reconnect_and_resend_stateful_request(
                            "stateful_sse",
                            &connector,
                            Arc::clone(&registry),
                            guard,
                            &outbound,
                        )
                        .await
                        {
                            Ok(next_guard) => {
                                guard = next_guard;
                                received_any_event = false;
                                continue;
                            }
                            Err(error_response) => {
                                send_sse_error_response(&sender, error_response).await;
                                break;
                            }
                        }
                    }
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "upstream_receive_failed",
                        "loop_break"
                    );
                    break;
                }
            };

            trace_upstream_event("stateful_sse", &message);
            if let Some(response_id) = websocket_message_response_id(&message) {
                retained_response_id = Some(response_id);
            }
            match maybe_intercept_chatmock_job_tool_call(
                "stateful_sse",
                job_manager.as_ref(),
                &message,
            )
            .await
            {
                Ok(Some(output)) => {
                    pending_internal_outputs.push(output);
                    tracing::debug!(
                        route = "stateful_sse",
                        pending_count = pending_internal_outputs.len(),
                        "internal_tool_output_pending"
                    );
                    continue;
                }
                Ok(None) => {}
                Err(_) => {
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "internal_tool_error",
                        "loop_break"
                    );
                    break;
                }
            }

            let metadata = websocket_event_metadata(&message);
            if let Some(metadata) = metadata.as_ref() {
                if metadata.response_id.is_some() {
                    retained_response_id = metadata.response_id.clone();
                }
                if metadata.completed && !pending_internal_outputs.is_empty() {
                    tracing::debug!(
                        route = "stateful_sse",
                        response_id = retained_response_id.as_deref(),
                        pending_count = pending_internal_outputs.len(),
                        "intermediate_response_completed"
                    );
                    if send_pending_internal_tool_follow_up(
                        "stateful_sse",
                        &guard.lease().upstream_ws,
                        retained_response_id.as_deref(),
                        &mut pending_internal_outputs,
                    )
                    .await
                    .is_err()
                    {
                        tracing::debug!(
                            route = "stateful_sse",
                            reason = "internal_tool_followup_failed",
                            "loop_break"
                        );
                        break;
                    }
                    retained_response_id = None;
                    continue;
                }
            }

            if downstream_open {
                let frame = sse_frame(&message);
                if sender.send(Ok(frame)).await.is_err() {
                    downstream_open = false;
                    disconnect_deadline = Some(Instant::now() + config.disconnect_drain_timeout);
                }
            }

            if let Some(metadata) = metadata {
                if metadata.completed {
                    tracing::debug!(
                        route = "stateful_sse",
                        response_id = retained_response_id.as_deref(),
                        "final_response_completed"
                    );
                    if let Some(response_id) = retained_response_id {
                        tracing::debug!(
                            route = "stateful_sse",
                            response_id = %response_id,
                            "registry_mark_completed"
                        );
                        guard.mark_completed(response_id);
                    }
                    tracing::debug!(
                        route = "stateful_sse",
                        reason = "final_response_completed",
                        "loop_break"
                    );
                    break;
                }
                if metadata.failed || metadata.errored {
                    tracing::debug!(
                        route = "stateful_sse",
                        failed = metadata.failed,
                        errored = metadata.errored,
                        "loop_break"
                    );
                    break;
                }
            }
        }
    });
    Box::pin(tokio_stream_from_receiver(receiver))
}

fn should_retry_initial_receive(
    guard: &RetainedUpstreamWebsocketLeaseGuard<SharedUpstreamWebsocket>,
    received_any_event: bool,
    retry_used: bool,
) -> bool {
    !retry_used
        && !received_any_event
        && !guard.lease().created
        && guard.lease().response_id.is_some()
}

async fn reconnect_and_resend_stateful_request(
    route: &str,
    connector: &ResponsesWebsocketConnector,
    registry: Arc<RetainedUpstreamWebsocketRegistry<SharedUpstreamWebsocket>>,
    guard: RetainedUpstreamWebsocketLeaseGuard<SharedUpstreamWebsocket>,
    outbound: &str,
) -> Result<RetainedUpstreamWebsocketLeaseGuard<SharedUpstreamWebsocket>, Response> {
    let response_id = guard
        .lease()
        .response_id
        .clone()
        .expect("retry requires retained response id");
    tracing::debug!(
        route,
        response_id = response_id.as_str(),
        "retain_original_for_reconnect"
    );
    guard.retain_original_response_id();

    let lease = registry
        .acquire_async_with_metadata(Some(response_id.as_str()), |metadata| async move {
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
    guard
        .lease()
        .upstream_ws
        .send_text(outbound.to_string())
        .await
        .map_err(|error| {
            tracing::warn!(route, error = %error, "upstream_send_failed");
            websocket_gateway_error("Upstream websocket request failed", error)
        })?;
    tracing::debug!(
        route,
        previous_response_id = response_id.as_str(),
        "upstream_request_resent"
    );
    Ok(guard)
}

async fn send_sse_error_response(
    sender: &mpsc::Sender<Result<Vec<u8>, Infallible>>,
    response: Response,
) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let error = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|payload| {
            payload
                .get("error")
                .filter(|value| value.is_object())
                .cloned()
        })
        .unwrap_or_else(|| {
            json!({
                "message": status
                    .canonical_reason()
                    .unwrap_or("Upstream websocket request failed")
            })
        });
    let event = json!({
        "type": "error",
        "status_code": status.as_u16(),
        "error": error,
    });
    let _ = sender.send(Ok(sse_frame(&event.to_string()))).await;
}

fn push_sse_message(body: &mut String, message: &str) {
    body.push_str("data: ");
    body.push_str(message);
    body.push_str("\n\n");
}

fn sse_frame(message: &str) -> Vec<u8> {
    let mut frame = Vec::with_capacity(message.len() + 8);
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(message.as_bytes());
    frame.extend_from_slice(b"\n\n");
    frame
}

fn tokio_stream_from_receiver<T: Send + 'static>(
    receiver: mpsc::Receiver<T>,
) -> impl futures_util::Stream<Item = T> + Send + 'static {
    futures_util::stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|item| (item, receiver))
    })
}

fn trace_upstream_event(route: &str, message: &str) {
    let event_type = websocket_message_event_type(message);
    let response_id = websocket_message_response_id(message);
    tracing::trace!(
        route,
        event_type = event_type.as_deref(),
        response_id = response_id.as_deref(),
        "upstream_event_received"
    );
}

fn trace_upstream_event_after_retry(route: &str, message: &str) {
    let event_type = websocket_message_event_type(message);
    let response_id = websocket_message_response_id(message);
    tracing::trace!(
        route,
        event_type = event_type.as_deref(),
        response_id = response_id.as_deref(),
        "upstream_event_received_after_retry"
    );
    if should_log_upstream_error_event(message) {
        tracing::warn!(route, error_event = %message, "upstream_error_event");
    }
}

fn should_log_upstream_error_event(message: &str) -> bool {
    websocket_message_event_type(message).as_deref() == Some("error")
}

fn log_upstream_socket_closed(route: &str, socket: &SharedUpstreamWebsocket) {
    let close = socket.close_metadata();
    tracing::warn!(
        route,
        code = ?close.as_ref().and_then(|metadata| metadata.code),
        reason = ?close.as_ref().and_then(|metadata| metadata.reason.as_deref()),
        "upstream_socket_closed"
    );
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

fn websocket_message_event_type(message: &str) -> Option<String> {
    serde_json::from_str::<Value>(message)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
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

#[derive(Debug)]
struct PendingInternalToolOutput {
    call_id: String,
    output: Value,
}

async fn maybe_intercept_chatmock_job_tool_call(
    route: &str,
    job_manager: Option<&Arc<JobManager>>,
    message: &str,
) -> Result<Option<PendingInternalToolOutput>, Response> {
    let Some(event) = serde_json::from_str::<Value>(message).ok() else {
        return Ok(None);
    };
    if event.get("type").and_then(Value::as_str) != Some("response.output_item.done") {
        return Ok(None);
    }
    let Some(item) = event.get("item").and_then(Value::as_object) else {
        return Ok(None);
    };
    if item.get("type").and_then(Value::as_str) != Some("function_call") {
        return Ok(None);
    }
    let Some(name) = item.get("name").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !name.starts_with("chatmock_") {
        return Ok(None);
    }

    let call_id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    tracing::debug!(route, name, call_id, "internal_tool_detected");
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
    Ok(Some(PendingInternalToolOutput {
        call_id: call_id.to_string(),
        output,
    }))
}

async fn send_pending_internal_tool_follow_up(
    route: &str,
    socket: &SharedUpstreamWebsocket,
    previous_response_id: Option<&str>,
    pending_outputs: &mut Vec<PendingInternalToolOutput>,
) -> Result<(), Response> {
    let previous_response_id = previous_response_id.ok_or_else(|| {
        json_response(
            StatusCode::BAD_GATEWAY,
            serde_json::to_value(build_upstream_error(
                Some("ChatMock internal tool call did not include a response id"),
                UpstreamErrorContext::default(),
            ))
            .expect("internal tool error payload"),
        )
    })?;
    let output_count = pending_outputs.len();
    let input = pending_outputs
        .drain(..)
        .map(|tool| {
            serde_json::json!({
                "type": "function_call_output",
                "call_id": tool.call_id,
                "output": tool.output.to_string(),
            })
        })
        .collect::<Vec<_>>();
    let follow_up = serde_json::json!({
        "type": "response.create",
        "previous_response_id": previous_response_id,
        "input": input
    });
    socket
        .send_text(follow_up.to_string())
        .await
        .map_err(|error| {
            tracing::warn!(
                route,
                previous_response_id,
                output_count,
                error = %error,
                "upstream_send_failed"
            );
            websocket_gateway_error("Upstream websocket internal tool follow-up failed", error)
        })?;
    tracing::debug!(
        route,
        previous_response_id,
        output_count,
        "internal_tool_followup_sent"
    );
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::should_log_upstream_error_event;

    #[test]
    fn upstream_error_event_body_is_loggable_after_retry() {
        assert!(should_log_upstream_error_event(
            r#"{"type":"error","error":{"message":"cannot continue"}}"#
        ));
        assert!(!should_log_upstream_error_event(
            r#"{"type":"response.created","response":{"id":"resp_1"}}"#
        ));
    }
}
