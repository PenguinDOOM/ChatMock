use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use serde_json::{Map, Value};

use crate::protocol::ResponsesConfig;

pub mod chat_completions;
pub mod completions;
pub mod models;
pub mod responses;

pub(crate) fn openai_router() -> Router<crate::server::AppState> {
    Router::new()
        .merge(models::router())
        .merge(chat_completions::router())
        .merge(completions::router())
        .merge(crate::websocket::client_route::router())
        .merge(responses::router())
}

pub(crate) fn json_response(status: StatusCode, payload: Value) -> Response<Body> {
    let mut response = (status, Json(payload)).into_response();
    apply_cors(response.headers_mut());
    response
}

pub(crate) fn error_response(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    json_response(
        status,
        serde_json::json!({
            "error": {
                "message": message.into(),
            }
        }),
    )
}

pub(crate) fn event_stream_response(status: StatusCode, body: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    apply_cors(headers);
    response
}

pub(crate) fn apply_cors(headers: &mut HeaderMap) {
    headers.insert(HeaderNameExt::ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(
        HeaderNameExt::ALLOW_METHODS,
        HeaderValue::from_static("POST, GET, OPTIONS"),
    );
    headers.insert(
        HeaderNameExt::ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type, Accept"),
    );
    headers.insert(HeaderNameExt::MAX_AGE, HeaderValue::from_static("86400"));
}

pub(crate) fn client_session_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("X-Session-Id")
        .or_else(|| headers.get("session_id"))
        .and_then(|value| value.to_str().ok())
}

pub(crate) fn route_should_inject_base_instructions(
    config: &ResponsesConfig,
    route_name: &str,
    payload: &Map<String, Value>,
) -> bool {
    if route_name == "/v1/responses" && payload.contains_key("instructions") {
        return false;
    }

    match config
        .base_instructions_mode
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" => false,
        "always" => true,
        _ => match route_name {
            "/v1/responses" => !payload.contains_key("instructions"),
            "/v1/completions" => true,
            "/v1/chat/completions" | "/api/chat" => payload
                .get("messages")
                .and_then(Value::as_array)
                .map(|messages| {
                    !messages.iter().any(|message| {
                        message
                            .as_object()
                            .and_then(|message| message.get("role"))
                            .and_then(Value::as_str)
                            .map(|role| role.trim().eq_ignore_ascii_case("system"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(true),
            _ => false,
        },
    }
}

pub(crate) fn resolve_builtin_instructions(
    config: &ResponsesConfig,
    model: &str,
) -> Option<String> {
    let base = config
        .base_instructions
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if crate::models::uses_codex_instructions(Some(model)) {
        if let Some(codex) = config
            .gpt5_codex_instructions
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
        {
            return Some(codex);
        }
    }
    base
}

pub(crate) fn convert_chat_messages_to_responses_input(
    messages: &[Value],
    preserve_system_messages: bool,
) -> Vec<Value> {
    let mut input_items = Vec::new();

    for message in messages {
        let Some(message) = message.as_object() else {
            continue;
        };
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if role == "system" && !preserve_system_messages {
            continue;
        }

        if role == "tool" {
            let call_id = message
                .get("tool_call_id")
                .or_else(|| message.get("id"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty());
            if let Some(call_id) = call_id {
                let content = match message.get("content") {
                    Some(Value::String(text)) => Some(text.clone()),
                    Some(Value::Array(parts)) => {
                        let texts = parts
                            .iter()
                            .filter_map(|part| {
                                part.as_object()
                                    .and_then(|part| {
                                        part.get("text").or_else(|| part.get("content"))
                                    })
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            })
                            .collect::<Vec<_>>();
                        Some(texts.join("\n"))
                    }
                    _ => None,
                };
                if let Some(output) = content {
                    input_items.push(serde_json::json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    }));
                }
            }
            continue;
        }

        if role == "assistant" {
            if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                for tool_call in tool_calls {
                    let Some(tool_call) = tool_call.as_object() else {
                        continue;
                    };
                    if tool_call
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("function")
                        != "function"
                    {
                        continue;
                    }
                    let call_id = tool_call
                        .get("id")
                        .or_else(|| tool_call.get("call_id"))
                        .and_then(Value::as_str);
                    let function = tool_call.get("function").and_then(Value::as_object);
                    let name = function
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str);
                    let arguments = function
                        .and_then(|function| function.get("arguments"))
                        .and_then(Value::as_str);
                    if let (Some(call_id), Some(name), Some(arguments)) = (call_id, name, arguments)
                    {
                        input_items.push(serde_json::json!({
                            "type": "function_call",
                            "name": name,
                            "arguments": arguments,
                            "call_id": call_id,
                        }));
                    }
                }
            }
        }

        let mut content_items = Vec::new();
        match message.get("content") {
            Some(Value::Array(parts)) => {
                for part in parts {
                    let Some(part) = part.as_object() else {
                        continue;
                    };
                    if part.get("type").and_then(Value::as_str) == Some("text") {
                        if let Some(text) = part
                            .get("text")
                            .or_else(|| part.get("content"))
                            .and_then(Value::as_str)
                            .filter(|text| !text.is_empty())
                        {
                            let kind = if role == "assistant" {
                                "output_text"
                            } else {
                                "input_text"
                            };
                            content_items.push(serde_json::json!({
                                "type": kind,
                                "text": text,
                            }));
                        }
                    }
                }
            }
            Some(Value::String(text)) if !text.is_empty() => {
                let kind = if role == "assistant" {
                    "output_text"
                } else {
                    "input_text"
                };
                content_items.push(serde_json::json!({
                    "type": kind,
                    "text": text,
                }));
            }
            _ => {}
        }

        if content_items.is_empty() {
            continue;
        }
        let normalized_role = match role {
            "assistant" => "assistant",
            "system" => "system",
            _ => "user",
        };
        input_items.push(serde_json::json!({
            "type": "message",
            "role": normalized_role,
            "content": content_items,
        }));
    }

    input_items
}

pub(crate) fn convert_tools_chat_to_responses(tools: Option<&Value>) -> Vec<Value> {
    let Some(tools) = tools.and_then(Value::as_array) else {
        return Vec::new();
    };

    tools
        .iter()
        .filter_map(|tool| {
            let tool = tool.as_object()?;
            if tool.get("type").and_then(Value::as_str) != Some("function") {
                return None;
            }
            let function = tool.get("function").and_then(Value::as_object)?;
            let name = function.get("name").and_then(Value::as_str)?;
            let description = function
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let parameters = function
                .get("parameters")
                .and_then(Value::as_object)
                .cloned()
                .map(Value::Object)
                .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
            Some(serde_json::json!({
                "type": "function",
                "name": name,
                "description": description,
                "strict": false,
                "parameters": parameters,
            }))
        })
        .collect()
}

struct HeaderNameExt;

impl HeaderNameExt {
    const ALLOW_ORIGIN: header::HeaderName = header::ACCESS_CONTROL_ALLOW_ORIGIN;
    const ALLOW_METHODS: header::HeaderName = header::ACCESS_CONTROL_ALLOW_METHODS;
    const ALLOW_HEADERS: header::HeaderName = header::ACCESS_CONTROL_ALLOW_HEADERS;
    const MAX_AGE: header::HeaderName = header::ACCESS_CONTROL_MAX_AGE;
}
