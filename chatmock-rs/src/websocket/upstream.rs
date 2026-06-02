use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::response::Response;
use reqwest::StatusCode;
use serde_json::Value;

use crate::{
    routes::json_response,
    upstream::UpstreamResponse,
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

impl std::fmt::Debug for ResponsesWebsocketConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResponsesWebsocketConnector(..)")
    }
}

#[derive(Debug, Clone)]
pub struct ScriptedUpstreamReceive {
    text: String,
}

impl ScriptedUpstreamReceive {
    pub fn text(payload: Value) -> Self {
        Self {
            text: serde_json::to_string(&payload).expect("scripted payload json"),
        }
    }
}

#[derive(Debug)]
struct SharedUpstreamWebsocketState {
    scripted_receives: VecDeque<String>,
    sent_messages: Vec<String>,
    closed: bool,
}

#[derive(Debug, Clone)]
pub struct SharedUpstreamWebsocket {
    state: Arc<Mutex<SharedUpstreamWebsocketState>>,
}

impl SharedUpstreamWebsocket {
    pub fn scripted(scripted_receives: Vec<ScriptedUpstreamReceive>) -> Self {
        Self {
            state: Arc::new(Mutex::new(SharedUpstreamWebsocketState {
                scripted_receives: scripted_receives
                    .into_iter()
                    .map(|message| message.text)
                    .collect(),
                sent_messages: Vec::new(),
                closed: false,
            })),
        }
    }

    pub async fn send_text(&self, message: String) -> Result<(), BoxedUpstreamWebsocketError> {
        self.state
            .lock()
            .expect("scripted websocket lock")
            .sent_messages
            .push(message);
        Ok(())
    }

    pub async fn recv_text(&self) -> Result<Option<String>, BoxedUpstreamWebsocketError> {
        Ok(self
            .state
            .lock()
            .expect("scripted websocket lock")
            .scripted_receives
            .pop_front())
    }

    pub async fn scripted_sent_messages(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("scripted websocket lock")
            .sent_messages
            .clone()
    }
}

impl RetainedUpstreamWebsocket for SharedUpstreamWebsocket {
    fn close_socket(&self) {
        self.state.lock().expect("scripted websocket lock").closed = true;
    }

    fn is_socket_closed(&self) -> bool {
        self.state.lock().expect("scripted websocket lock").closed
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
            if metadata.failed {
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
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"error": {"message": error.to_string()}}),
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
                "response.completed" | "response.failed"
            )
        })
        .unwrap_or(false)
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
