use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use serde_json::{Map, Value};

use crate::{
    models::{allowed_efforts_for_model, extract_reasoning_from_model_name, normalize_model_name},
    reasoning::{apply_reasoning_to_message, build_reasoning_param},
    routes::{
        client_session_id, convert_chat_messages_to_responses_input,
        convert_tools_chat_to_responses, error_response, event_stream_response, json_response,
        resolve_builtin_instructions, route_should_inject_base_instructions,
    },
    sse,
    upstream::{self, UpstreamRequestPayload},
    upstream_errors::{build_upstream_error, UpstreamErrorContext},
};

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new().route("/v1/chat/completions", post(chat_completions))
}

async fn chat_completions(
    State(state): State<crate::server::AppState>,
    headers: HeaderMap,
    request: Request,
) -> Response {
    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(body) => body,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "Invalid JSON body"),
    };
    let payload = match parse_json_object(&body) {
        Ok(payload) => payload,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let requested_model = payload
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let model = normalize_model_name(
        requested_model.as_deref(),
        state.responses_config.debug_model.as_deref(),
    );

    let mut messages = normalized_messages(&payload);
    let request_instructions = if route_should_inject_base_instructions(
        &state.responses_config,
        "/v1/chat/completions",
        &payload,
    ) {
        resolve_builtin_instructions(&state.responses_config, &model)
    } else {
        None
    };

    if request_instructions.is_none() {
        if let Some(system_index) = messages.iter().position(|message| {
            message
                .as_object()
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str)
                == Some("system")
        }) {
            if let Some(system_message) = messages.get(system_index).cloned() {
                messages.remove(system_index);
                let content = system_message
                    .as_object()
                    .and_then(|message| message.get("content"))
                    .cloned()
                    .unwrap_or(Value::String(String::new()));
                messages.insert(
                    0,
                    serde_json::json!({
                        "role": "user",
                        "content": content,
                    }),
                );
            }
        }
    }

    let is_stream = payload
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let include_usage = payload
        .get("stream_options")
        .and_then(Value::as_object)
        .and_then(|options| options.get("include_usage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut input_items =
        convert_chat_messages_to_responses_input(&messages, request_instructions.is_some());
    if input_items.is_empty() {
        if let Some(prompt) = payload
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            input_items.push(serde_json::json!({
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }));
        }
    }

    let reasoning_overrides = payload
        .get("reasoning")
        .and_then(Value::as_object)
        .cloned()
        .or_else(|| {
            extract_reasoning_from_model_name(requested_model.as_deref()).map(|reasoning| {
                reasoning
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), Value::String(value)))
                    .collect::<Map<String, Value>>()
            })
        });
    let reasoning_param = build_reasoning_param(
        &state.responses_config.reasoning_effort,
        &state.responses_config.reasoning_summary,
        reasoning_overrides.as_ref(),
        Some(&allowed_efforts_for_model(&model)),
    );

    let service_tier_resolution = crate::fast_mode::resolve_service_tier(
        Some(&model),
        payload.get("fast_mode").cloned(),
        payload.get("service_tier").and_then(Value::as_str),
        state.responses_config.fast_mode,
    );
    if let Some(message) = service_tier_resolution.error_message {
        return error_response(StatusCode::BAD_REQUEST, message);
    }

    let session_id = crate::responses::ensure_session_id(
        request_instructions.as_deref().unwrap_or_default(),
        &input_items,
        client_session_id(&headers),
    );
    let upstream_payload = UpstreamRequestPayload {
        model: model.clone(),
        instructions: request_instructions,
        input: input_items,
        tools: convert_tools_chat_to_responses(payload.get("tools")),
        tool_choice: payload
            .get("tool_choice")
            .cloned()
            .filter(|value| matches!(value, Value::String(_) | Value::Object(_)))
            .unwrap_or_else(|| Value::String("auto".to_string())),
        parallel_tool_calls: payload
            .get("parallel_tool_calls")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        reasoning: Some(reasoning_param),
        service_tier: service_tier_resolution.service_tier,
        include_reasoning_encrypted_content: true,
        prompt_cache_key: Some(session_id.clone()),
        stream: true,
        store: false,
    };

    let upstream = match upstream::start_upstream_raw_request(
        &state,
        upstream_payload.to_value(),
        Some(&session_id),
        true,
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(response) => return response,
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

    let created = unix_timestamp_now();
    if is_stream {
        let body = sse::translate_chat_completion_stream(
            &upstream.body,
            requested_model.as_deref().unwrap_or(&model),
            created,
            &state.reasoning_compat,
            include_usage,
        );
        return event_stream_response(upstream.status_code, body);
    }

    let mut full_text = String::new();
    let mut reasoning_summary_text = String::new();
    let mut reasoning_full_text = String::new();
    let mut response_id = String::from("chatcmpl");
    let mut tool_calls = Vec::new();
    let mut error_message = None;
    let mut usage = None;

    sse::for_each_sse_event(&upstream.body, None, |event| {
        if let Some(response) = event.get("response").and_then(Value::as_object) {
            if let Some(id) = response.get("id").and_then(Value::as_str) {
                response_id = id.to_string();
            }
            if let Some(event_usage) = sse::usage_from_response(response) {
                usage = Some(event_usage);
            }
        }

        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                full_text.push_str(
                    event
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                );
            }
            Some("response.reasoning_summary_text.delta") => {
                reasoning_summary_text.push_str(
                    event
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                );
            }
            Some("response.reasoning_text.delta") => {
                reasoning_full_text.push_str(
                    event
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                );
            }
            Some("response.output_item.done") => {
                if let Some(item) = event.get("item").and_then(Value::as_object) {
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let call_id = item
                            .get("call_id")
                            .or_else(|| item.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
                        let arguments = item
                            .get("arguments")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if !call_id.is_empty() && !name.is_empty() {
                            tool_calls.push(serde_json::json!({
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": arguments,
                                }
                            }));
                        }
                    }
                }
            }
            Some("response.failed") => {
                error_message = event
                    .get("response")
                    .and_then(Value::as_object)
                    .and_then(|response| response.get("error"))
                    .and_then(Value::as_object)
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| Some("response.failed".to_string()));
            }
            _ => {}
        }
    });

    if let Some(error_message) = error_message {
        return error_response(StatusCode::BAD_GATEWAY, error_message);
    }

    let mut message = Map::new();
    message.insert("role".to_string(), Value::String("assistant".to_string()));
    message.insert(
        "content".to_string(),
        if full_text.is_empty() {
            Value::Null
        } else {
            Value::String(full_text)
        },
    );
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }
    apply_reasoning_to_message(
        &mut message,
        &reasoning_summary_text,
        &reasoning_full_text,
        &state.reasoning_compat,
    );

    let mut response = serde_json::json!({
        "id": response_id,
        "object": "chat.completion",
        "created": created,
        "model": requested_model.unwrap_or(model),
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": "stop",
        }],
    });
    if let Some(usage) = usage {
        response["usage"] = Value::Object(usage);
    }
    json_response(upstream.status_code, response)
}

fn parse_json_object(body: &[u8]) -> Result<Map<String, Value>, &'static str> {
    let parsed = serde_json::from_slice::<Value>(body).map_err(|_| "Invalid JSON body")?;
    parsed
        .as_object()
        .cloned()
        .ok_or("Request body must be a JSON object")
}

fn normalized_messages(payload: &Map<String, Value>) -> Vec<Value> {
    if let Some(messages) = payload.get("messages") {
        if let Some(messages) = messages.as_array() {
            return messages.clone();
        }
        return Vec::new();
    }
    if let Some(prompt) = payload.get("prompt").and_then(Value::as_str) {
        return vec![serde_json::json!({"role": "user", "content": prompt})];
    }
    if let Some(input) = payload.get("input").and_then(Value::as_str) {
        return vec![serde_json::json!({"role": "user", "content": input})];
    }
    Vec::new()
}

fn unix_timestamp_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
