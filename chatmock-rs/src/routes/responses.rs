use axum::{
    body,
    extract::{Request, State},
    http::StatusCode,
    response::Response,
    routing::post,
    Router,
};
use serde_json::Value;

use crate::{
    routes::{error_response, event_stream_response, json_response},
    sse::{self, SseInspection},
    upstream,
    upstream_errors::{build_upstream_error, UpstreamErrorContext},
};

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new().route("/v1/responses", post(responses_create))
}

fn stream_error_event_response(status: StatusCode, error: Value) -> Response {
    let event = serde_json::json!({
        "type": "error",
        "status_code": status.as_u16(),
        "error": error,
    });
    event_stream_response(StatusCode::OK, format!("data: {event}\n\n").into_bytes())
}

async fn into_stream_error_event_response(response: Response) -> Response {
    let status = response.status();
    let body = body::to_bytes(response.into_body(), usize::MAX)
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
            serde_json::json!({
                "message": status
                    .canonical_reason()
                    .unwrap_or("Upstream websocket request failed")
            })
        });
    stream_error_event_response(status, error)
}

fn sse_error_event(body: &[u8]) -> Option<(StatusCode, Value)> {
    let mut error = None;
    sse::for_each_sse_event(body, None, |event| {
        if event.get("type").and_then(Value::as_str) != Some("error") {
            return;
        }

        let status = event
            .get("status_code")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .and_then(|value| StatusCode::from_u16(value).ok())
            .unwrap_or(StatusCode::BAD_GATEWAY);
        let body = event
            .get("error")
            .filter(|value| value.is_object())
            .cloned()
            .map(|payload| serde_json::json!({"error": payload}))
            .unwrap_or_else(
                || serde_json::json!({"error": {"message": "Upstream websocket error"}}),
            );
        error = Some((status, body));
    });
    error
}

async fn responses_create(
    State(state): State<crate::server::AppState>,
    request: Request,
) -> Response {
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(body) => body,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "Invalid JSON body"),
    };
    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(payload)) => payload,
        Ok(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "Request body must be a JSON object",
            )
        }
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "Invalid JSON body"),
    };

    let normalized = match crate::responses::normalize_responses_payload(
        &payload,
        &state.responses_config,
        crate::routes::client_session_id(&headers),
    ) {
        Ok(normalized) => normalized,
        Err(error) => {
            let mut payload = serde_json::json!({
                "error": {
                    "message": error.message,
                }
            });
            if let Some(code) = error.code {
                payload["error"]["code"] = Value::String(code);
            }
            return json_response(
                StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::BAD_REQUEST),
                payload,
            );
        }
    };

    let stream = normalized
        .payload
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let previous_response_id = normalized
        .payload
        .get("previous_response_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut upstream_payload = normalized.payload.clone();
    if state.responses_websocket_registry.is_none() {
        upstream_payload.remove("previous_response_id");
    }
    upstream_payload.insert("stream".to_string(), Value::Bool(true));
    let upstream_payload = Value::Object(upstream_payload);

    let upstream = if let (Some(connector), Some(registry)) = (
        state.responses_websocket_connector.as_ref(),
        state.responses_websocket_registry.as_ref(),
    ) {
        match crate::websocket::upstream::send_stateful_responses_create_request(
            connector,
            registry,
            &normalized.session_id,
            upstream_payload.clone(),
            previous_response_id.as_deref(),
        )
        .await
        {
            Ok(upstream) => upstream,
            Err(response) => {
                if stream {
                    return into_stream_error_event_response(response).await;
                }
                return response;
            }
        }
    } else if let Some(connector) = state.responses_websocket_connector.as_ref() {
        match crate::websocket::upstream::send_responses_create_request(
            connector,
            &normalized.session_id,
            upstream_payload.clone(),
        )
        .await
        {
            Ok(upstream) => upstream,
            Err(response) => {
                if stream {
                    return into_stream_error_event_response(response).await;
                }
                return response;
            }
        }
    } else {
        match upstream::start_upstream_raw_request(
            &state,
            upstream_payload,
            Some(&normalized.session_id),
            true,
        )
        .await
        {
            Ok(upstream) => upstream,
            Err(response) => return response,
        }
    };

    if upstream.status_code.is_client_error() || upstream.status_code.is_server_error() {
        if let Ok(parsed_error) = serde_json::from_slice::<Value>(&upstream.body) {
            if parsed_error.is_object() {
                return json_response(upstream.status_code, parsed_error);
            }
        }
        return json_response(
            upstream.status_code,
            serde_json::to_value(build_upstream_error(
                Some("Upstream error"),
                UpstreamErrorContext {
                    status_code: Some(upstream.status_code.as_u16()),
                    body: Some(upstream.body.clone()),
                    content_type: upstream.content_type.clone(),
                    ..UpstreamErrorContext::default()
                },
            ))
            .expect("upstream error payload"),
        );
    }

    if stream {
        return event_stream_response(upstream.status_code, upstream.body);
    }

    if upstream
        .content_type
        .as_deref()
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(false)
    {
        if let Ok(body) = serde_json::from_slice::<Value>(&upstream.body) {
            if body.is_object() {
                return json_response(upstream.status_code, body);
            }
        }
    }

    let mut inspection = SseInspection::default();
    let (response_obj, error_obj) =
        sse::aggregate_response_from_sse(&upstream.body, Some(&mut inspection));
    if let Some((status, error_event)) = sse_error_event(&upstream.body) {
        return json_response(status, error_event);
    }
    if let Some(error_obj) = error_obj {
        let is_detailed_error = error_obj
            .get("error")
            .and_then(Value::as_object)
            .map(|error| {
                error.get("message").and_then(Value::as_str) != Some("response.failed")
                    || error.len() > 1
            })
            .unwrap_or(false);
        if is_detailed_error {
            return json_response(StatusCode::BAD_GATEWAY, error_obj);
        }
        return json_response(
            StatusCode::BAD_GATEWAY,
            serde_json::to_value(build_upstream_error(
                Some("Upstream response failed"),
                UpstreamErrorContext {
                    phase: Some("response.failed".to_string()),
                    content_type: upstream.content_type.clone(),
                    body: inspection
                        .invalid_event_data
                        .or(inspection.last_event_data)
                        .map(|body| body.into_bytes()),
                    ..UpstreamErrorContext::default()
                },
            ))
            .expect("upstream error payload"),
        );
    }

    if response_obj.is_none() || !inspection.completed_seen {
        return json_response(
            StatusCode::BAD_GATEWAY,
            serde_json::to_value(build_upstream_error(
                Some("Upstream response stream did not contain a completed response object"),
                UpstreamErrorContext {
                    phase: Some("response.completed".to_string()),
                    content_type: upstream.content_type.clone(),
                    body: inspection
                        .invalid_event_data
                        .or(inspection.last_event_data)
                        .map(|body| body.into_bytes()),
                    ..UpstreamErrorContext::default()
                },
            ))
            .expect("upstream error payload"),
        );
    }

    json_response(
        upstream.status_code,
        response_obj.expect("checked response object"),
    )
}
