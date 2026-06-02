use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use futures_util::StreamExt;
use serde_json::Value;

use crate::upstream_errors::{build_upstream_error, UpstreamErrorContext};

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new().route("/v1/responses", get(responses_websocket))
}

async fn responses_websocket(
    ws: WebSocketUpgrade,
    State(state): State<crate::server::AppState>,
    headers: HeaderMap,
) -> Response {
    ws.on_upgrade(move |socket| handle_responses_websocket(socket, state, headers))
}

async fn handle_responses_websocket(
    mut client_socket: WebSocket,
    state: crate::server::AppState,
    headers: HeaderMap,
) {
    let Some(connector) = state.responses_websocket_connector.as_ref() else {
        let _ = send_websocket_error_event(
            &mut client_socket,
            "Responses websocket upstream is not enabled.",
            StatusCode::NOT_IMPLEMENTED,
        )
        .await;
        return;
    };

    let mut upstream_socket = None;
    let mut upstream_session_id = None;

    while let Some(result) = client_socket.next().await {
        let message = match result {
            Ok(message) => message,
            Err(_) => break,
        };

        let Some(incoming_text) = websocket_message_text(message) else {
            continue;
        };

        let payload = match serde_json::from_str::<Value>(&incoming_text) {
            Ok(Value::Object(payload)) => payload,
            Ok(_) => {
                let _ = send_websocket_terminal_error_event(
                    &mut client_socket,
                    "Websocket frames must be JSON objects.",
                    StatusCode::BAD_REQUEST,
                )
                .await;
                return;
            }
            Err(_) => {
                let _ = send_websocket_terminal_error_event(
                    &mut client_socket,
                    "Websocket frames must be valid JSON objects.",
                    StatusCode::BAD_REQUEST,
                )
                .await;
                return;
            }
        };

        let mut outbound_text = incoming_text;
        let mut session_id = upstream_session_id.clone();

        if payload.get("type").and_then(Value::as_str) == Some("response.create") {
            let normalized = match crate::responses::normalize_responses_payload(
                &payload,
                &state.responses_config,
                crate::routes::client_session_id(&headers),
            ) {
                Ok(normalized) => normalized,
                Err(error) => {
                    let _ = send_websocket_error_event(
                        &mut client_socket,
                        &error.message,
                        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::BAD_REQUEST),
                    )
                    .await;
                    continue;
                }
            };

            session_id = Some(normalized.session_id.clone());
            outbound_text = serde_json::to_string(&websocket_response_create_payload(
                Value::Object(normalized.payload),
            ))
            .expect("responses websocket payload json");
        } else if upstream_socket.is_none() {
            let _ = send_websocket_terminal_error_event(
                &mut client_socket,
                "The first websocket message must be a response.create request.",
                StatusCode::BAD_REQUEST,
            )
            .await;
            return;
        }

        if upstream_socket.is_none() || session_id != upstream_session_id {
            if let Some(existing_socket) = upstream_socket.take() {
                crate::websocket::registry::RetainedUpstreamWebsocket::close_socket(
                    &existing_socket,
                );
            }

            let Some(next_session_id) = session_id.clone() else {
                let _ = send_websocket_terminal_error_event(
                    &mut client_socket,
                    "The first websocket message must be a response.create request.",
                    StatusCode::BAD_REQUEST,
                )
                .await;
                return;
            };

            let connected_socket = match connector.connect(next_session_id.clone()).await {
                Ok(socket) => socket,
                Err(error) => {
                    let error_message = build_upstream_error(
                        Some("Upstream websocket connection failed"),
                        UpstreamErrorContext {
                            exception: Some(error.to_string()),
                            ..UpstreamErrorContext::default()
                        },
                    )
                    .error
                    .message;
                    let _ = send_websocket_error_event(
                        &mut client_socket,
                        &error_message,
                        StatusCode::BAD_GATEWAY,
                    )
                    .await;
                    break;
                }
            };

            upstream_session_id = Some(next_session_id);
            upstream_socket = Some(connected_socket);
        }

        let Some(active_upstream_socket) = upstream_socket.as_ref() else {
            break;
        };

        if let Err(error) = active_upstream_socket.send_text(outbound_text).await {
            let error_message = build_upstream_error(
                Some("Upstream websocket request failed"),
                UpstreamErrorContext {
                    exception: Some(error.to_string()),
                    ..UpstreamErrorContext::default()
                },
            )
            .error
            .message;
            let _ = send_websocket_error_event(
                &mut client_socket,
                &error_message,
                StatusCode::BAD_GATEWAY,
            )
            .await;
            break;
        }

        loop {
            let upstream_message = match active_upstream_socket.recv_text().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    let error_message = build_upstream_error(
                        Some("Upstream websocket closed unexpectedly"),
                        UpstreamErrorContext::default(),
                    )
                    .error
                    .message;
                    let _ = send_websocket_error_event(
                        &mut client_socket,
                        &error_message,
                        StatusCode::BAD_GATEWAY,
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    let error_message = build_upstream_error(
                        Some("Upstream websocket receive failed"),
                        UpstreamErrorContext {
                            exception: Some(error.to_string()),
                            ..UpstreamErrorContext::default()
                        },
                    )
                    .error
                    .message;
                    let _ = send_websocket_error_event(
                        &mut client_socket,
                        &error_message,
                        StatusCode::BAD_GATEWAY,
                    )
                    .await;
                    return;
                }
            };

            if client_socket
                .send(Message::Text(upstream_message.clone().into()))
                .await
                .is_err()
            {
                return;
            }

            if websocket_terminal_event(&upstream_message) {
                break;
            }
        }
    }

    if let Some(socket) = upstream_socket {
        crate::websocket::registry::RetainedUpstreamWebsocket::close_socket(&socket);
    }
}

fn websocket_message_text(message: Message) -> Option<String> {
    match message {
        Message::Text(text) => Some(text.to_string()),
        Message::Binary(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        Message::Close(_) => None,
        Message::Ping(_) | Message::Pong(_) => None,
    }
}

async fn send_websocket_error_event(
    client_socket: &mut WebSocket,
    message: &str,
    status_code: StatusCode,
) -> Result<(), axum::Error> {
    client_socket
        .send(Message::Text(
            serde_json::json!({
                "type": "error",
                "status_code": status_code.as_u16(),
                "error": {
                    "message": message,
                },
            })
            .to_string()
            .into(),
        ))
        .await
}

async fn send_websocket_terminal_error_event(
    client_socket: &mut WebSocket,
    message: &str,
    status_code: StatusCode,
) -> Result<(), axum::Error> {
    send_websocket_error_event(client_socket, message, status_code).await?;
    client_socket.send(Message::Close(None)).await
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

fn websocket_response_create_payload(payload: Value) -> Value {
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
