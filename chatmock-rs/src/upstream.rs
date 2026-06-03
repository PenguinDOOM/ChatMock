use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    http::{header, HeaderMap, HeaderValue},
    response::Response,
};
use reqwest::StatusCode;
use reqwest::Url;
use serde_json::{Map, Value};

use crate::{
    auth::{load_effective_chatgpt_auth_from_env_with_refresher, RefreshedAuthTokens},
    routes::error_response,
    upstream_errors::{build_upstream_error, UpstreamErrorContext},
};

const DEFAULT_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const DEFAULT_CHATGPT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEFAULT_OAUTH_ISSUER: &str = "https://auth.openai.com";

#[derive(Debug, Clone)]
pub struct UpstreamResponse {
    pub status_code: StatusCode,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct UpstreamRequestPayload {
    pub model: String,
    pub instructions: Option<String>,
    pub input: Vec<Value>,
    pub tools: Vec<Value>,
    pub tool_choice: Value,
    pub parallel_tool_calls: bool,
    pub reasoning: Option<Map<String, Value>>,
    pub service_tier: Option<String>,
    pub include_reasoning_encrypted_content: bool,
    pub prompt_cache_key: Option<String>,
    pub stream: bool,
    pub store: bool,
}

impl UpstreamRequestPayload {
    pub fn to_value(&self) -> Value {
        let mut payload = Map::new();
        payload.insert("model".to_string(), Value::String(self.model.clone()));
        payload.insert(
            "instructions".to_string(),
            self.instructions
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        payload.insert("input".to_string(), Value::Array(self.input.clone()));
        payload.insert("tools".to_string(), Value::Array(self.tools.clone()));
        payload.insert("tool_choice".to_string(), self.tool_choice.clone());
        payload.insert(
            "parallel_tool_calls".to_string(),
            Value::Bool(self.parallel_tool_calls),
        );
        payload.insert("store".to_string(), Value::Bool(self.store));
        payload.insert("stream".to_string(), Value::Bool(self.stream));
        if let Some(prompt_cache_key) = &self.prompt_cache_key {
            payload.insert(
                "prompt_cache_key".to_string(),
                Value::String(prompt_cache_key.clone()),
            );
        }
        if self.include_reasoning_encrypted_content {
            payload.insert(
                "include".to_string(),
                Value::Array(vec![Value::String(
                    "reasoning.encrypted_content".to_string(),
                )]),
            );
        }
        if let Some(reasoning) = &self.reasoning {
            payload.insert("reasoning".to_string(), Value::Object(reasoning.clone()));
        }
        if let Some(service_tier) = &self.service_tier {
            payload.insert(
                "service_tier".to_string(),
                Value::String(service_tier.clone()),
            );
        }
        Value::Object(payload)
    }
}

pub(crate) async fn start_upstream_raw_request(
    state: &crate::server::AppState,
    responses_payload: Value,
    session_id: Option<&str>,
    stream: bool,
) -> Result<UpstreamResponse, Response> {
    let http_client = state.http_client.clone();
    let effective_auth =
        load_effective_chatgpt_auth_from_env_with_refresher(move |refresh_token| {
            let http_client = http_client.clone();
            async move { refresh_chatgpt_tokens(http_client, refresh_token).await }
        })
        .await;
    let effective_auth = match effective_auth {
        Some(credentials) => credentials,
        None => {
            return Err(error_response(
                StatusCode::UNAUTHORIZED,
                "Missing ChatGPT credentials. Run 'chatmock-rs login' first.",
            ))
        }
    };

    let effective_session_id = effective_session_id(session_id, &responses_payload);
    let response = state
        .http_client
        .post(upstream_url())
        .headers(build_upstream_headers(
            &effective_auth.access_token,
            &effective_auth.account_id,
            &effective_session_id,
            if stream {
                "text/event-stream"
            } else {
                "application/json"
            },
        ))
        .json(&responses_payload)
        .send()
        .await
        .map_err(|error| {
            let payload = serde_json::to_value(build_upstream_error(
                Some("Upstream ChatGPT request failed"),
                UpstreamErrorContext {
                    exception: Some(error.to_string()),
                    ..UpstreamErrorContext::default()
                },
            ))
            .expect("upstream error payload");
            crate::routes::json_response(StatusCode::BAD_GATEWAY, payload)
        })?;

    let status_code = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.bytes().await.map_err(|error| {
        let payload = serde_json::to_value(build_upstream_error(
            Some("Upstream ChatGPT request failed"),
            UpstreamErrorContext {
                exception: Some(error.to_string()),
                ..UpstreamErrorContext::default()
            },
        ))
        .expect("upstream error payload");
        crate::routes::json_response(StatusCode::BAD_GATEWAY, payload)
    })?;

    Ok(UpstreamResponse {
        status_code,
        content_type,
        body: body.to_vec(),
    })
}

pub(crate) fn upstream_url() -> String {
    env::var("CHATGPT_RESPONSES_URL").unwrap_or_else(|_| DEFAULT_RESPONSES_URL.to_string())
}

pub(crate) fn upstream_websocket_url() -> Result<String, String> {
    let mut url = Url::parse(&upstream_url()).map_err(|error| error.to_string())?;
    match url.scheme() {
        "https" => {
            let _ = url.set_scheme("wss");
        }
        "http" => {
            let _ = url.set_scheme("ws");
        }
        _ => {}
    }
    Ok(url.to_string())
}

pub(crate) fn build_upstream_headers(
    access_token: &str,
    account_id: &str,
    session_id: &str,
    accept: &str,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {access_token}")).expect("authorization header"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::ACCEPT,
        HeaderValue::from_str(accept).expect("accept header"),
    );
    headers.insert(
        "chatgpt-account-id",
        HeaderValue::from_str(account_id).expect("account header"),
    );
    headers.insert(
        "OpenAI-Beta",
        HeaderValue::from_static("responses=experimental"),
    );
    headers.insert(
        "session_id",
        HeaderValue::from_str(session_id).expect("session header"),
    );
    headers
}

fn effective_session_id(session_id: Option<&str>, responses_payload: &Value) -> String {
    if let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) {
        return session_id.to_string();
    }
    if let Some(prompt_cache_key) = responses_payload
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return prompt_cache_key.to_string();
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

pub(crate) async fn refresh_chatgpt_tokens(
    http_client: reqwest::Client,
    refresh_token: String,
) -> Option<RefreshedAuthTokens> {
    let client_id = env::var("CHATGPT_LOCAL_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CHATGPT_CLIENT_ID.to_string());
    let issuer = env::var("CHATGPT_LOCAL_ISSUER")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_OAUTH_ISSUER.to_string());
    let token_url = format!("{}/oauth/token", issuer.trim_end_matches('/'));

    let response = http_client
        .post(token_url)
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "scope": "openid profile email offline_access",
        }))
        .send()
        .await
        .ok()?;

    if response.status().is_client_error() || response.status().is_server_error() {
        return None;
    }

    let payload: Value = response.json().await.ok()?;
    let access_token = payload
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())?;
    let id_token = payload
        .get("id_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())?;
    let refresh_token = payload
        .get("refresh_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
        .unwrap_or(refresh_token);

    Some(RefreshedAuthTokens {
        access_token: Some(access_token),
        account_id: None,
        id_token: Some(id_token),
        refresh_token: Some(refresh_token),
    })
}
