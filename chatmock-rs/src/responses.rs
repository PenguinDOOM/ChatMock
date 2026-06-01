use serde_json::{Map, Value};
use thiserror::Error;

use crate::fast_mode::ServiceTierResolution;
use crate::models::{
    allowed_efforts_for_model, extract_reasoning_from_model_name, normalize_model_name,
    uses_codex_instructions,
};
use crate::protocol::ResponsesConfig;
use crate::reasoning::build_reasoning_param;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ResponsesRequestError {
    pub message: String,
    pub status_code: u16,
    pub code: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedResponsesRequest {
    pub payload: Map<String, Value>,
    pub requested_model: Option<String>,
    pub normalized_model: String,
    pub session_id: String,
    pub service_tier_resolution: ServiceTierResolution,
}

pub fn normalize_responses_payload(
    payload: &Map<String, Value>,
    config: &ResponsesConfig,
    client_session_id: Option<&str>,
) -> Result<NormalizedResponsesRequest, ResponsesRequestError> {
    let requested_model = payload
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    let normalized_model =
        normalize_model_name(requested_model.as_deref(), config.debug_model.as_deref());

    let mut normalized = payload.clone();
    normalized.insert("model".to_string(), Value::String(normalized_model.clone()));
    normalized.remove("max_output_tokens");
    normalized.remove("truncation");

    if let Some(input) = normalized.get("input").cloned() {
        normalized.insert("input".to_string(), canonicalize_responses_input(&input));
    }

    normalized.insert("store".to_string(), Value::Bool(false));

    if should_inject_base_instructions(&normalized, config) {
        if let Some(instructions) = resolve_builtin_instructions(config, &normalized_model) {
            normalized.insert("instructions".to_string(), Value::String(instructions));
        }
    }
    if !normalized.contains_key("instructions") {
        normalized.insert("instructions".to_string(), Value::String(String::new()));
    }

    let reasoning_overrides = normalized
        .get("reasoning")
        .and_then(Value::as_object)
        .cloned();
    let reasoning_overrides = reasoning_overrides.or_else(|| {
        extract_reasoning_from_model_name(requested_model.as_deref()).map(|values| {
            values
                .into_iter()
                .map(|(key, value)| (key.to_string(), Value::String(value)))
                .collect()
        })
    });
    let reasoning = build_reasoning_param(
        &config.reasoning_effort,
        &config.reasoning_summary,
        reasoning_overrides.as_ref(),
        Some(&allowed_efforts_for_model(&normalized_model)),
    );
    normalized.insert("reasoning".to_string(), Value::Object(reasoning));

    let mut include_list = normalized
        .get("include")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|value| Value::String(value.to_string())))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !include_list
        .iter()
        .any(|item| item.as_str() == Some("reasoning.encrypted_content"))
    {
        include_list.push(Value::String("reasoning.encrypted_content".to_string()));
    }
    normalized.insert("include".to_string(), Value::Array(include_list));

    let tools_missing = normalized
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| tools.is_empty())
        .unwrap_or(true);
    let tool_choice_none = normalized
        .get("tool_choice")
        .and_then(Value::as_str)
        .map(|value| value.trim().eq_ignore_ascii_case("none"))
        .unwrap_or(false);
    if tools_missing && config.default_web_search && !tool_choice_none {
        normalized.insert(
            "tools".to_string(),
            Value::Array(vec![serde_json::json!({"type": "web_search"})]),
        );
    }

    let service_tier_resolution = crate::fast_mode::resolve_service_tier(
        Some(&normalized_model),
        normalized.get("fast_mode").cloned(),
        normalized.get("service_tier").and_then(Value::as_str),
        config.fast_mode,
    );
    if let Some(error_message) = &service_tier_resolution.error_message {
        return Err(ResponsesRequestError {
            message: error_message.clone(),
            status_code: 400,
            code: None,
        });
    }
    match &service_tier_resolution.service_tier {
        Some(service_tier) => {
            normalized.insert(
                "service_tier".to_string(),
                Value::String(service_tier.clone()),
            );
        }
        None => {
            normalized.remove("service_tier");
        }
    }
    normalized.remove("fast_mode");

    let input_items = input_items_for_session(normalized.get("input"));
    let instructions = normalized
        .get("instructions")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let session_id = ensure_session_id(instructions, &input_items, client_session_id);
    let has_prompt_cache_key = normalized
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if !has_prompt_cache_key {
        normalized.insert(
            "prompt_cache_key".to_string(),
            Value::String(session_id.clone()),
        );
    }

    Ok(NormalizedResponsesRequest {
        payload: normalized,
        requested_model,
        normalized_model,
        session_id,
        service_tier_resolution,
    })
}

fn should_inject_base_instructions(payload: &Map<String, Value>, config: &ResponsesConfig) -> bool {
    if payload.contains_key("instructions") {
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
        _ => true,
    }
}

fn resolve_builtin_instructions(config: &ResponsesConfig, model: &str) -> Option<String> {
    let base_instructions = config
        .base_instructions
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if uses_codex_instructions(Some(model)) {
        if let Some(codex_instructions) = config
            .gpt5_codex_instructions
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
        {
            return Some(codex_instructions);
        }
    }
    base_instructions
}

fn input_items_for_session(input: Option<&Value>) -> Vec<Value> {
    match input {
        Some(Value::Array(items)) => items
            .iter()
            .filter(|item| item.is_object())
            .cloned()
            .collect(),
        Some(Value::Object(item)) => vec![Value::Object(item.clone())],
        Some(Value::String(text)) if !text.trim().is_empty() => vec![serde_json::json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}],
        })],
        _ => Vec::new(),
    }
}

pub fn canonicalize_responses_input(raw_input: &Value) -> Value {
    match raw_input {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .filter(|item| item.is_object())
                .cloned()
                .collect(),
        ),
        Value::Object(item) => Value::Array(vec![Value::Object(item.clone())]),
        Value::String(text) => {
            Value::Array(input_items_for_session(Some(&Value::String(text.clone()))))
        }
        _ => raw_input.clone(),
    }
}

pub fn ensure_session_id(
    instructions: &str,
    input_items: &[Value],
    client_session_id: Option<&str>,
) -> String {
    if let Some(client_session_id) = client_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return client_session_id.to_string();
    }

    let canonical = serde_json::json!({
        "instructions": instructions,
        "input": input_items,
    })
    .to_string();
    format!("session-{:016x}", fnv1a64(canonical.as_bytes()))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
