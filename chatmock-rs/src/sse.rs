use serde_json::{Map, Value};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SseInspection {
    pub completed_seen: bool,
    pub invalid_event_data: Option<String>,
    pub last_event_data: Option<String>,
}

pub fn for_each_sse_event<F>(
    body: &[u8],
    mut inspection: Option<&mut SseInspection>,
    mut on_event: F,
) where
    F: FnMut(&Value),
{
    let text = String::from_utf8_lossy(body);
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if !line.starts_with("data: ") {
            continue;
        }
        let data = line[6..].trim();
        if data.is_empty() || data == "[DONE]" {
            if data == "[DONE]" {
                break;
            }
            continue;
        }
        if let Some(inspection) = inspection.as_deref_mut() {
            inspection.last_event_data = Some(data.to_string());
        }
        match serde_json::from_str::<Value>(data) {
            Ok(event) if event.is_object() => {
                if let Some(inspection) = inspection.as_deref_mut() {
                    if event.get("type").and_then(Value::as_str) == Some("response.completed") {
                        inspection.completed_seen = true;
                    }
                }
                on_event(&event);
            }
            Ok(_) => {}
            Err(_) => {
                if let Some(inspection) = inspection.as_deref_mut() {
                    if inspection.invalid_event_data.is_none() {
                        inspection.invalid_event_data = Some(data.to_string());
                    }
                }
            }
        }
    }
}

pub fn usage_from_response(response: &Map<String, Value>) -> Option<Map<String, Value>> {
    let usage = response.get("usage")?.as_object()?;
    let prompt_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let completion_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(prompt_tokens + completion_tokens);
    Some(Map::from_iter([
        ("prompt_tokens".to_string(), Value::from(prompt_tokens)),
        (
            "completion_tokens".to_string(),
            Value::from(completion_tokens),
        ),
        ("total_tokens".to_string(), Value::from(total_tokens)),
    ]))
}

pub fn aggregate_response_from_sse(
    body: &[u8],
    inspection: Option<&mut SseInspection>,
) -> (Option<Value>, Option<Value>) {
    let mut response_obj = None;
    let mut error_obj = None;
    let mut completed_output_items = Vec::new();

    for_each_sse_event(body, inspection, |event| {
        if let Some(response) = event.get("response").and_then(Value::as_object) {
            response_obj = Some(Value::Object(response.clone()));
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_item.done") => {
                if let Some(item) = event.get("item").and_then(Value::as_object) {
                    completed_output_items.push(Value::Object(item.clone()));
                }
            }
            Some("response.failed") => {
                error_obj = Some(
                    event
                        .get("response")
                        .and_then(Value::as_object)
                        .and_then(|response| response.get("error"))
                        .filter(|error| error.is_object())
                        .cloned()
                        .map(|error| serde_json::json!({"error": error}))
                        .unwrap_or_else(
                            || serde_json::json!({"error": {"message": "response.failed"}}),
                        ),
                );
            }
            Some("response.completed") => {
                if let Some(Value::Object(response)) = response_obj.as_mut() {
                    let should_backfill = response
                        .get("output")
                        .and_then(Value::as_array)
                        .map(|output| output.is_empty())
                        .unwrap_or(true);
                    if should_backfill && !completed_output_items.is_empty() {
                        response.insert(
                            "output".to_string(),
                            Value::Array(completed_output_items.clone()),
                        );
                    }
                }
            }
            _ => {}
        }
    });

    (response_obj, error_obj)
}

pub fn translate_chat_completion_stream(
    body: &[u8],
    model: &str,
    created: i64,
    reasoning_compat: &str,
    include_usage: bool,
) -> Vec<u8> {
    let compat = reasoning_compat.trim().to_ascii_lowercase();
    let mut response_id = String::from("chatcmpl-stream");
    let mut chunks = Vec::new();
    let mut think_open = false;
    let mut think_closed = false;
    let mut sent_stop_chunk = false;
    let mut usage = None;

    for_each_sse_event(body, None, |event| {
        if let Some(response) = event.get("response").and_then(Value::as_object) {
            if let Some(id) = response.get("id").and_then(Value::as_str) {
                response_id = id.to_string();
            }
            if let Some(event_usage) = usage_from_response(response) {
                usage = Some(event_usage);
            }
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if compat == "think-tags" && think_open && !think_closed {
                    push_sse_chunk(
                        &mut chunks,
                        &serde_json::json!({
                            "id": response_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{"index": 0, "delta": {"content": "</think>"}, "finish_reason": Value::Null}],
                        }),
                    );
                    think_open = false;
                    think_closed = true;
                }
                push_sse_chunk(
                    &mut chunks,
                    &serde_json::json!({
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model,
                        "choices": [{"index": 0, "delta": {"content": event.get("delta").and_then(Value::as_str).unwrap_or_default()}, "finish_reason": Value::Null}],
                    }),
                );
            }
            Some("response.reasoning_summary_text.delta")
            | Some("response.reasoning_text.delta") => {
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if compat == "think-tags" {
                    if !think_open && !think_closed {
                        push_sse_chunk(
                            &mut chunks,
                            &serde_json::json!({
                                "id": response_id,
                                "object": "chat.completion.chunk",
                                "created": created,
                                "model": model,
                                "choices": [{"index": 0, "delta": {"content": "<think>"}, "finish_reason": Value::Null}],
                            }),
                        );
                        think_open = true;
                    }
                    push_sse_chunk(
                        &mut chunks,
                        &serde_json::json!({
                            "id": response_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": Value::Null}],
                        }),
                    );
                }
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
                            push_sse_chunk(
                                &mut chunks,
                                &serde_json::json!({
                                    "id": response_id,
                                    "object": "chat.completion.chunk",
                                    "created": created,
                                    "model": model,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {
                                            "tool_calls": [{
                                                "index": 0,
                                                "id": call_id,
                                                "type": "function",
                                                "function": {"name": name, "arguments": arguments},
                                            }]
                                        },
                                        "finish_reason": Value::Null,
                                    }],
                                }),
                            );
                            push_sse_chunk(
                                &mut chunks,
                                &serde_json::json!({
                                    "id": response_id,
                                    "object": "chat.completion.chunk",
                                    "created": created,
                                    "model": model,
                                    "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
                                }),
                            );
                        }
                    }
                }
            }
            Some("response.output_text.done") => {
                push_sse_chunk(
                    &mut chunks,
                    &serde_json::json!({
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    }),
                );
                sent_stop_chunk = true;
            }
            Some("response.failed") => {
                push_sse_chunk(
                    &mut chunks,
                    &serde_json::json!({
                        "error": {
                            "message": event
                                .get("response")
                                .and_then(Value::as_object)
                                .and_then(|response| response.get("error"))
                                .and_then(Value::as_object)
                                .and_then(|error| error.get("message"))
                                .and_then(Value::as_str)
                                .unwrap_or("response.failed")
                        }
                    }),
                );
            }
            Some("response.completed") => {
                if compat == "think-tags" && think_open && !think_closed {
                    push_sse_chunk(
                        &mut chunks,
                        &serde_json::json!({
                            "id": response_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{"index": 0, "delta": {"content": "</think>"}, "finish_reason": Value::Null}],
                        }),
                    );
                    think_open = false;
                    think_closed = true;
                }
                if !sent_stop_chunk {
                    push_sse_chunk(
                        &mut chunks,
                        &serde_json::json!({
                            "id": response_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        }),
                    );
                }
                if include_usage {
                    if let Some(usage) = &usage {
                        push_sse_chunk(
                            &mut chunks,
                            &Value::Object(Map::from_iter([
                                ("id".to_string(), Value::String(response_id.clone())),
                                (
                                    "object".to_string(),
                                    Value::String("chat.completion.chunk".to_string()),
                                ),
                                ("created".to_string(), Value::from(created)),
                                ("model".to_string(), Value::String(model.to_string())),
                                (
                                    "choices".to_string(),
                                    serde_json::json!([{"index": 0, "delta": {}, "finish_reason": Value::Null}]),
                                ),
                                ("usage".to_string(), Value::Object(usage.clone())),
                            ])),
                        );
                    }
                }
            }
            _ => {}
        }
    });

    chunks.extend_from_slice(b"data: [DONE]\n\n");
    chunks
}

pub fn translate_text_completion_stream(
    body: &[u8],
    model: &str,
    created: i64,
    include_usage: bool,
) -> Vec<u8> {
    let mut response_id = String::from("cmpl-stream");
    let mut chunks = Vec::new();
    let mut usage = None;
    for_each_sse_event(body, None, |event| {
        if let Some(response) = event.get("response").and_then(Value::as_object) {
            if let Some(id) = response.get("id").and_then(Value::as_str) {
                response_id = id.to_string();
            }
            if let Some(event_usage) = usage_from_response(response) {
                usage = Some(event_usage);
            }
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => push_sse_chunk(
                &mut chunks,
                &serde_json::json!({
                    "id": response_id,
                    "object": "text_completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "text": event.get("delta").and_then(Value::as_str).unwrap_or_default(), "finish_reason": Value::Null}],
                }),
            ),
            Some("response.output_text.done") => push_sse_chunk(
                &mut chunks,
                &serde_json::json!({
                    "id": response_id,
                    "object": "text_completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "text": "", "finish_reason": "stop"}],
                }),
            ),
            Some("response.completed") if include_usage => {
                if let Some(usage) = &usage {
                    push_sse_chunk(
                        &mut chunks,
                        &Value::Object(Map::from_iter([
                            ("id".to_string(), Value::String(response_id.clone())),
                            (
                                "object".to_string(),
                                Value::String("text_completion.chunk".to_string()),
                            ),
                            ("created".to_string(), Value::from(created)),
                            ("model".to_string(), Value::String(model.to_string())),
                            (
                                "choices".to_string(),
                                serde_json::json!([{"index": 0, "text": "", "finish_reason": Value::Null}]),
                            ),
                            ("usage".to_string(), Value::Object(usage.clone())),
                        ])),
                    );
                }
            }
            _ => {}
        }
    });
    chunks.extend_from_slice(b"data: [DONE]\n\n");
    chunks
}

fn push_sse_chunk(chunks: &mut Vec<u8>, payload: &Value) {
    chunks.extend_from_slice(b"data: ");
    chunks.extend_from_slice(payload.to_string().as_bytes());
    chunks.extend_from_slice(b"\n\n");
}

#[cfg(test)]
mod tests {
    use super::{aggregate_response_from_sse, translate_chat_completion_stream, SseInspection};

    #[test]
    fn aggregate_backfills_completed_output_items() {
        let body = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"id\":\"msg_1\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut inspection = SseInspection::default();
        let (response, error) = aggregate_response_from_sse(body.as_bytes(), Some(&mut inspection));

        assert!(error.is_none());
        assert!(inspection.completed_seen);
        let output = response
            .expect("response")
            .get("output")
            .and_then(|output| output.as_array().cloned())
            .expect("output array");
        assert_eq!(output[0]["id"], "msg_1");
    }

    #[test]
    fn chat_stream_translation_closes_think_block_before_output() {
        let body = concat!(
            "data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"thought\"}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
            "data: [DONE]\n\n"
        );

        let translated = String::from_utf8(translate_chat_completion_stream(
            body.as_bytes(),
            "gpt-5.4",
            1,
            "think-tags",
            false,
        ))
        .expect("utf8 stream");

        assert!(translated.contains("<think>"));
        assert!(translated.contains("</think>"));
        assert!(translated.contains("answer"));
        assert!(translated.contains("[DONE]"));
    }
}
