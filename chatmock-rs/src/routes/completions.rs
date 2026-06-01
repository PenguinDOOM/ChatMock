use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::Response,
    routing::post,
    Router,
};
use serde_json::{Map, Value};

use crate::{
    models::{allowed_efforts_for_model, extract_reasoning_from_model_name, normalize_model_name},
    reasoning::build_reasoning_param,
    routes::{
        convert_chat_messages_to_responses_input, error_response, event_stream_response,
        json_response, resolve_builtin_instructions, route_should_inject_base_instructions,
    },
    sse,
    upstream::{self, UpstreamRequestPayload},
};

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new().route("/v1/completions", post(completions))
}

async fn completions(State(state): State<crate::server::AppState>, request: Request) -> Response {
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

    let requested_model = payload
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let model = normalize_model_name(
        requested_model.as_deref(),
        state.responses_config.debug_model.as_deref(),
    );

    let prompt = match payload.get("prompt") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(""),
        Some(Value::String(prompt)) => prompt.clone(),
        _ => payload
            .get("suffix")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    };
    let stream = payload
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let include_usage = payload
        .get("stream_options")
        .and_then(Value::as_object)
        .and_then(|options| options.get("include_usage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let input_items = convert_chat_messages_to_responses_input(
        &[serde_json::json!({"role": "user", "content": prompt})],
        false,
    );
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

    let instructions = if route_should_inject_base_instructions(
        &state.responses_config,
        "/v1/completions",
        &payload,
    ) {
        resolve_builtin_instructions(&state.responses_config, &model)
    } else {
        None
    };

    let upstream_payload = UpstreamRequestPayload {
        model: model.clone(),
        instructions,
        input: input_items,
        tools: Vec::new(),
        tool_choice: Value::String("auto".to_string()),
        parallel_tool_calls: false,
        reasoning: Some(reasoning_param),
        service_tier: service_tier_resolution.service_tier,
        include_reasoning_encrypted_content: true,
        prompt_cache_key: None,
        stream: true,
        store: false,
    };
    let upstream =
        match upstream::start_upstream_raw_request(&state, upstream_payload.to_value(), None, true)
            .await
        {
            Ok(upstream) => upstream,
            Err(response) => return response,
        };

    if upstream.status_code.is_client_error() || upstream.status_code.is_server_error() {
        let error_body = serde_json::from_slice::<Value>(&upstream.body)
            .ok()
            .and_then(|body| {
                body.get("error")
                    .and_then(Value::as_object)
                    .and_then(|error| error.get("message"))
                    .cloned()
            })
            .unwrap_or_else(|| Value::String("Upstream error".to_string()));
        return json_response(
            upstream.status_code,
            serde_json::json!({"error": {"message": error_body}}),
        );
    }

    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    if stream {
        return event_stream_response(
            upstream.status_code,
            sse::translate_text_completion_stream(
                &upstream.body,
                requested_model.as_deref().unwrap_or(&model),
                created,
                include_usage,
            ),
        );
    }

    let mut full_text = String::new();
    let mut response_id = String::from("cmpl");
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
        if event.get("type").and_then(Value::as_str) == Some("response.output_text.delta") {
            full_text.push_str(
                event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
        }
    });

    let mut response = serde_json::json!({
        "id": response_id,
        "object": "text_completion",
        "created": created,
        "model": requested_model.unwrap_or(model),
        "choices": [{
            "index": 0,
            "text": full_text,
            "finish_reason": "stop",
            "logprobs": Value::Null,
        }],
    });
    if let Some(usage) = usage {
        response["usage"] = Value::Object(usage);
    }
    json_response(upstream.status_code, response)
}
