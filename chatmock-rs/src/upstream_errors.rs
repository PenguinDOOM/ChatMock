use std::sync::OnceLock;

use regex::Regex;

use crate::protocol::{ErrorPayload, OpenAiError};

const TRUNCATION_MARKER: &str = "...(truncated)";
const MAX_SNIPPET_CHARS: usize = 160;
const MAX_MESSAGE_CHARS: usize = 500;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpstreamErrorContext {
    pub status_code: Option<u16>,
    pub body: Option<Vec<u8>>,
    pub content_type: Option<String>,
    pub exception: Option<String>,
    pub phase: Option<String>,
}

pub fn build_upstream_error(message: Option<&str>, context: UpstreamErrorContext) -> ErrorPayload {
    let base_message = normalize_text(message).unwrap_or_else(|| "Upstream error".to_string());
    let mut details = Vec::new();

    if let Some(phase) = normalize_owned(context.phase) {
        details.push(format!("phase={phase}"));
    }
    if let Some(status_code) = context.status_code {
        details.push(format!("upstream_status={status_code}"));
    }
    if let Some(content_type) = normalize_owned(context.content_type) {
        details.push(format!("content_type={content_type}"));
    }
    if let Some(exception) =
        normalize_owned(context.exception).map(|text| truncate(&text, MAX_SNIPPET_CHARS))
    {
        details.push(format!("exception={exception}"));
    }
    if let Some(body) = context
        .body
        .map(|body| String::from_utf8_lossy(&body).into_owned())
        .and_then(|body| normalize_owned(Some(body)))
        .map(|text| truncate(&text, MAX_SNIPPET_CHARS))
    {
        details.push(format!("body={body}"));
    }

    let mut full_message = base_message;
    if !details.is_empty() {
        full_message.push_str(" (");
        full_message.push_str(&details.join("; "));
        full_message.push(')');
    }

    ErrorPayload {
        error: OpenAiError {
            message: truncate(&full_message, MAX_MESSAGE_CHARS),
        },
    }
}

fn normalize_text(text: Option<&str>) -> Option<String> {
    normalize_owned(text.map(str::to_string))
}

fn normalize_owned(text: Option<String>) -> Option<String> {
    let text = text?;
    let redacted = redact(&text).trim().to_string();
    if redacted.is_empty() {
        return None;
    }
    Some(redacted)
}

fn redact(text: &str) -> String {
    let mut redacted = text.to_string();
    for redaction in redaction_patterns() {
        redacted = match redaction {
            RedactionPattern::Static(pattern, replacement) => {
                pattern.replace_all(&redacted, *replacement).into_owned()
            }
            RedactionPattern::Bearer(pattern) => pattern
                .replace_all(&redacted, |captures: &regex::Captures<'_>| {
                    let token = captures
                        .get(1)
                        .map(|capture| capture.as_str())
                        .unwrap_or_default();
                    if token.eq_ignore_ascii_case("[redacted]") {
                        captures
                            .get(0)
                            .map(|capture| capture.as_str())
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        "Bearer [redacted]".to_string()
                    }
                })
                .into_owned(),
        };
    }
    redacted
}

fn truncate(value: &str, limit: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= limit {
        return value.to_string();
    }
    let keep = limit.saturating_sub(TRUNCATION_MARKER.chars().count());
    value.chars().take(keep).collect::<String>() + TRUNCATION_MARKER
}

enum RedactionPattern {
    Static(Regex, &'static str),
    Bearer(Regex),
}

fn redaction_patterns() -> &'static [RedactionPattern] {
    static PATTERNS: OnceLock<Vec<RedactionPattern>> = OnceLock::new();
    PATTERNS
        .get_or_init(|| {
            vec![
                RedactionPattern::Static(
                    Regex::new(r"Authorization:\s*Bearer\s+[^\s,;]+").expect("authorization regex"),
                    "Authorization: Bearer [redacted]",
                ),
                RedactionPattern::Bearer(
                    Regex::new(r"\bBearer\s+([^\s,;]+)").expect("bearer regex"),
                ),
                RedactionPattern::Static(
                    Regex::new(r"\bsk-[A-Za-z0-9_-]+\b").expect("sk token regex"),
                    "[redacted]",
                ),
                RedactionPattern::Static(
                    Regex::new(r"\b(session_id|access_token|token)=([^&\s,;]+)")
                        .expect("key value token regex"),
                    "$1=[redacted]",
                ),
            ]
        })
        .as_slice()
}
