use serde_json::Value;

use crate::models::normalize_model_name;

const PRIORITY_SUPPORTED_MODELS: &[&str] = &[
    "gpt-5.4",
    "gpt-5.2",
    "gpt-5.1",
    "gpt-5",
    "gpt-5.1-codex",
    "gpt-5-codex",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTierResolution {
    pub service_tier: Option<String>,
    pub error_message: Option<String>,
    pub warning_message: Option<String>,
    pub used_server_default: bool,
}

pub fn parse_optional_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub fn supports_priority_service_tier(model: Option<&str>) -> bool {
    let normalized = normalize_model_name(model, None);
    PRIORITY_SUPPORTED_MODELS.contains(&normalized.as_str())
}

pub fn resolve_service_tier(
    model: Option<&str>,
    request_fast_mode: Option<Value>,
    request_service_tier: Option<&str>,
    server_fast_mode: bool,
) -> ServiceTierResolution {
    let explicit_fast_mode = request_fast_mode.as_ref().and_then(parse_optional_bool);

    let (service_tier, explicit_request, used_server_default) =
        if let Some(explicit_fast_mode) = explicit_fast_mode {
            (
                explicit_fast_mode.then(|| "priority".to_string()),
                true,
                false,
            )
        } else if let Some(request_service_tier) =
            request_service_tier.filter(|value| !value.trim().is_empty())
        {
            (
                Some(request_service_tier.trim().to_ascii_lowercase()),
                true,
                false,
            )
        } else if server_fast_mode {
            (Some("priority".to_string()), false, true)
        } else {
            (None, false, false)
        };

    if service_tier.as_deref() == Some("priority") && !supports_priority_service_tier(model) {
        let normalized = normalize_model_name(model, None);
        let message = format!(
            "Fast mode is not supported for model '{normalized}'. Use a supported GPT-5 priority-processing model or disable fast mode for this request."
        );
        if explicit_request {
            return ServiceTierResolution {
                service_tier: None,
                error_message: Some(message),
                warning_message: None,
                used_server_default,
            };
        }
        return ServiceTierResolution {
            service_tier: None,
            error_message: None,
            warning_message: Some(message),
            used_server_default,
        };
    }

    ServiceTierResolution {
        service_tier,
        error_message: None,
        warning_message: None,
        used_server_default,
    }
}
