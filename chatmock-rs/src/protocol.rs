use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesConfig {
    pub debug_model: Option<String>,
    pub base_instructions_mode: String,
    pub base_instructions: Option<String>,
    pub gpt5_codex_instructions: Option<String>,
    pub reasoning_effort: String,
    pub reasoning_summary: String,
    pub default_web_search: bool,
    pub fast_mode: bool,
}

impl Default for ResponsesConfig {
    fn default() -> Self {
        Self {
            debug_model: None,
            base_instructions_mode: "fallback".to_string(),
            base_instructions: None,
            gpt5_codex_instructions: None,
            reasoning_effort: "medium".to_string(),
            reasoning_summary: "auto".to_string(),
            default_web_search: false,
            fast_mode: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenAiError {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorPayload {
    pub error: OpenAiError,
}
