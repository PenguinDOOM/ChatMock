use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::models::ALL_REASONING_EFFORTS;

pub fn build_reasoning_param(
    base_effort: &str,
    base_summary: &str,
    overrides: Option<&Map<String, Value>>,
    allowed_efforts: Option<&HashSet<&'static str>>,
) -> Map<String, Value> {
    let mut effort = base_effort.trim().to_ascii_lowercase();
    let mut summary = base_summary.trim().to_ascii_lowercase();

    let valid_efforts = allowed_efforts
        .cloned()
        .unwrap_or_else(|| ALL_REASONING_EFFORTS.into_iter().collect());
    let valid_summaries = ["auto", "concise", "detailed", "none"];

    if let Some(overrides) = overrides {
        let override_effort = overrides
            .get("effort")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let override_summary = overrides
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !override_effort.is_empty() && valid_efforts.contains(override_effort.as_str()) {
            effort = override_effort;
        }
        if !override_summary.is_empty() && valid_summaries.contains(&override_summary.as_str()) {
            summary = override_summary;
        }
    }

    if !valid_efforts.contains(effort.as_str()) {
        effort = "medium".to_string();
    }
    if !valid_summaries.contains(&summary.as_str()) {
        summary = "auto".to_string();
    }

    let mut reasoning = Map::new();
    reasoning.insert("effort".to_string(), Value::String(effort));
    if summary != "none" {
        reasoning.insert("summary".to_string(), Value::String(summary));
    }
    reasoning
}

pub fn apply_reasoning_to_message(
    message: &mut Map<String, Value>,
    reasoning_summary_text: &str,
    reasoning_full_text: &str,
    compat: &str,
) {
    let compat = compat.trim().to_ascii_lowercase();
    let combined = [reasoning_summary_text.trim(), reasoning_full_text.trim()]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    match compat.as_str() {
        "o3" => {
            if !combined.is_empty() {
                message.insert(
                    "reasoning".to_string(),
                    serde_json::json!({
                        "content": [{"type": "text", "text": combined}],
                    }),
                );
            }
        }
        "legacy" | "current" => {
            if !reasoning_summary_text.is_empty() {
                message.insert(
                    "reasoning_summary".to_string(),
                    Value::String(reasoning_summary_text.to_string()),
                );
            }
            if !reasoning_full_text.is_empty() {
                message.insert(
                    "reasoning".to_string(),
                    Value::String(reasoning_full_text.to_string()),
                );
            }
        }
        _ => {
            if combined.is_empty() {
                return;
            }
            let existing_content = message
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            message.insert(
                "content".to_string(),
                Value::String(format!("<think>{combined}</think>{existing_content}")),
            );
        }
    }
}
