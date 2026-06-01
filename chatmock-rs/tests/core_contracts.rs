use chatmock_rs::fast_mode::{
    parse_optional_bool, resolve_service_tier, supports_priority_service_tier,
};
use chatmock_rs::models::{allowed_efforts_for_model, list_public_models, normalize_model_name};
use chatmock_rs::prompts::{read_prompt_text, PromptLookup};
use chatmock_rs::protocol::ResponsesConfig;
use chatmock_rs::reasoning::{apply_reasoning_to_message, build_reasoning_param};
use chatmock_rs::responses::normalize_responses_payload;
use chatmock_rs::upstream_errors::{build_upstream_error, UpstreamErrorContext};
use chatmock_rs::{auth, auth::AuthFile};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use tempfile::TempDir;

#[test]
fn model_registry_normalizes_aliases_and_reasoning_suffixes() {
    assert_eq!(normalize_model_name(Some("gpt5"), None), "gpt-5");
    assert_eq!(normalize_model_name(Some("gpt5.4"), None), "gpt-5.4");
    assert_eq!(
        normalize_model_name(Some("gpt5.4-mini"), None),
        "gpt-5.4-mini"
    );
    assert_eq!(
        normalize_model_name(Some("codex"), None),
        "codex-mini-latest"
    );
    assert_eq!(normalize_model_name(Some("gpt-5.4-high"), None), "gpt-5.4");
    assert_eq!(
        normalize_model_name(Some("gpt-5.1-codex:high"), None),
        "gpt-5.1-codex"
    );
}

#[test]
fn model_registry_allowed_efforts_follow_public_contract() {
    assert_eq!(
        allowed_efforts_for_model("gpt-5.5"),
        ["none", "low", "medium", "high", "xhigh"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        allowed_efforts_for_model("gpt-5.4-mini"),
        ["low", "medium", "high", "xhigh"].into_iter().collect()
    );
    assert_eq!(
        allowed_efforts_for_model("gpt-5.1-codex"),
        ["low", "medium", "high"].into_iter().collect()
    );
}

#[test]
fn model_registry_lists_public_models_and_reasoning_variants() {
    let model_ids = list_public_models(true);

    assert!(model_ids.contains(&"gpt-5.5".to_string()));
    assert!(model_ids.contains(&"gpt-5.4".to_string()));
    assert!(model_ids.contains(&"gpt-5.4-mini".to_string()));
    assert!(model_ids.contains(&"gpt-5.3-codex-spark".to_string()));
    assert!(model_ids.contains(&"gpt-5.5-none".to_string()));
    assert!(model_ids.contains(&"gpt-5.4-none".to_string()));
    assert!(model_ids.contains(&"gpt-5.4-mini-xhigh".to_string()));
    assert!(!model_ids.contains(&"gpt-5.4-mini-none".to_string()));
    assert!(model_ids.contains(&"gpt-5.1-codex-max-xhigh".to_string()));
    assert!(!model_ids.contains(&"codex-mini-high".to_string()));
}

#[test]
fn fast_mode_parses_optional_bools_and_normalizes_model_allowlist() {
    assert_eq!(parse_optional_bool(&true.into()), Some(true));
    assert_eq!(parse_optional_bool(&"true".into()), Some(true));
    assert_eq!(parse_optional_bool(&false.into()), Some(false));
    assert_eq!(parse_optional_bool(&"off".into()), Some(false));
    assert_eq!(parse_optional_bool(&"maybe".into()), None);

    let supported_models = list_public_models(false)
        .into_iter()
        .filter(|model| supports_priority_service_tier(Some(model)))
        .collect::<BTreeSet<_>>();
    let expected_supported_models = [
        "gpt-5",
        "gpt-5.1",
        "gpt-5.1-codex",
        "gpt-5.2",
        "gpt-5.4",
        "gpt-5-codex",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

    assert_eq!(supported_models, expected_supported_models);
    assert!(supports_priority_service_tier(Some("gpt5.4")));
    assert!(!supports_priority_service_tier(Some("gpt-5.3-codex")));
}

#[test]
fn fast_mode_rejects_unsupported_explicit_priority_and_warns_on_server_default() {
    let explicit = resolve_service_tier(Some("gpt-5.3-codex"), Some(true.into()), None, false);
    assert_eq!(explicit.service_tier.as_deref(), None);
    assert!(explicit.error_message.is_some());
    assert!(explicit.warning_message.is_none());

    let server_default = resolve_service_tier(Some("gpt-5.3-codex"), None, None, true);
    assert_eq!(server_default.service_tier.as_deref(), None);
    assert!(server_default.error_message.is_none());
    assert!(server_default.warning_message.is_some());

    let request_false = resolve_service_tier(Some("gpt-5.4"), Some(false.into()), None, true);
    assert_eq!(request_false.service_tier.as_deref(), None);
    assert!(request_false.error_message.is_none());
}

#[test]
fn reasoning_builds_params_and_applies_o3_compat_shape() {
    let overrides = json!({"effort": "high", "summary": "none"});
    let reasoning = build_reasoning_param(
        "medium",
        "auto",
        overrides.as_object(),
        Some(&allowed_efforts_for_model("gpt-5.4")),
    );
    assert_eq!(reasoning.get("effort"), Some(&json!("high")));
    assert!(!reasoning.contains_key("summary"));

    let mut message =
        serde_json::Map::from_iter([(String::from("content"), json!("assistant output"))]);
    apply_reasoning_to_message(&mut message, "summary", "full", "o3");
    assert_eq!(
        message.get("reasoning"),
        Some(&json!({"content": [{"type": "text", "text": "summary\n\nfull"}]}))
    );
}

#[test]
fn responses_normalization_enforces_defaults_and_supported_fields() {
    let config = ResponsesConfig {
        base_instructions_mode: "fallback".to_string(),
        base_instructions: Some("server base instructions".to_string()),
        gpt5_codex_instructions: Some("server codex instructions".to_string()),
        reasoning_effort: "medium".to_string(),
        reasoning_summary: "auto".to_string(),
        default_web_search: true,
        fast_mode: false,
        ..ResponsesConfig::default()
    };

    let payload = json!({
        "model": "gpt5.4-mini",
        "input": "hello",
        "store": true,
        "max_output_tokens": 20,
        "truncation": "auto"
    });

    let normalized =
        normalize_responses_payload(payload.as_object().expect("object"), &config, None)
            .expect("normalized payload");

    assert_eq!(normalized.normalized_model, "gpt-5.4-mini");
    assert_eq!(normalized.payload.get("store"), Some(&json!(false)));
    assert_eq!(
        normalized.payload.get("instructions"),
        Some(&json!("server base instructions"))
    );
    assert_eq!(
        normalized.payload.get("input"),
        Some(&json!([
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}]
            }
        ]))
    );
    assert_eq!(
        normalized.payload.get("reasoning"),
        Some(&json!({"effort": "medium", "summary": "auto"}))
    );
    assert_eq!(
        normalized.payload.get("include"),
        Some(&json!(["reasoning.encrypted_content"]))
    );
    assert_eq!(
        normalized.payload.get("tools"),
        Some(&json!([{"type": "web_search"}]))
    );
    assert!(!normalized.payload.contains_key("max_output_tokens"));
    assert!(!normalized.payload.contains_key("truncation"));
    assert!(matches!(
        normalized.payload.get("prompt_cache_key"),
        Some(Value::String(_))
    ));
}

#[test]
fn responses_normalization_preserves_explicit_instructions_and_rejects_unsupported_priority() {
    let fallback = ResponsesConfig {
        base_instructions_mode: "off".to_string(),
        base_instructions: Some("server base instructions".to_string()),
        gpt5_codex_instructions: Some("server codex instructions".to_string()),
        ..ResponsesConfig::default()
    };
    let preserved = normalize_responses_payload(
        json!({
            "model": "gpt-5.4",
            "input": "hello",
            "instructions": "",
            "include": ["reasoning.encrypted_content"],
            "tool_choice": "none"
        })
        .as_object()
        .expect("object"),
        &fallback,
        Some("session-fixed"),
    )
    .expect("normalized payload");
    assert_eq!(preserved.payload.get("instructions"), Some(&json!("")));
    assert!(!preserved.payload.contains_key("tools"));
    assert_eq!(preserved.session_id, "session-fixed");

    let unsupported = normalize_responses_payload(
        json!({
            "model": "gpt-5.3-codex",
            "input": "hello",
            "service_tier": "priority"
        })
        .as_object()
        .expect("object"),
        &ResponsesConfig::default(),
        None,
    )
    .expect_err("unsupported explicit priority should fail");
    assert!(unsupported.message.contains("Fast mode is not supported"));
}

#[test]
fn upstream_errors_redact_and_truncate_sensitive_context() {
    let payload = build_upstream_error(
        Some("Upstream failure"),
        UpstreamErrorContext {
            status_code: Some(503),
            body: Some(
                b"Authorization: Bearer secret-auth Bearer session-secret sk-1234567890abcdef session_id=session-secret access_token=access-secret token=token-secret ".repeat(4),
            ),
            content_type: Some("text/plain; charset=utf-8".to_string()),
            exception: Some("Bearer exception-secret".to_string()),
            phase: Some("sse_parse".to_string()),
        },
    );

    let message = payload.error.message;
    assert!(message.contains("Upstream failure"));
    assert!(message.contains("phase=sse_parse"));
    assert!(message.contains("upstream_status=503"));
    assert!(message.contains("content_type=text/plain; charset=utf-8"));
    assert!(message.contains("Authorization: Bearer [redacted]"));
    assert!(message.contains("Bearer [redacted]"));
    assert!(message.contains("session_id=[redacted]"));
    assert!(message.contains("access_token=[redacted]"));
    assert!(message.contains("token=[redacted]"));
    assert!(!message.contains("secret-auth"));
    assert!(!message.contains("session-secret"));
    assert!(!message.contains("access-secret"));
    assert!(!message.contains("token-secret"));
    assert!(!message.contains("sk-1234567890abcdef"));
    assert!(message.contains("...(truncated)"));
    assert!(message.len() <= 500);
}

#[test]
fn auth_file_layout_respects_precedence_and_atomic_write() {
    let temp = TempDir::new().expect("tempdir");
    let home_root = temp.path().join("home");
    let chatgpt_home = temp.path().join("chatgpt-home");
    let codex_home = temp.path().join("codex-home");
    fs::create_dir_all(&chatgpt_home).expect("chatgpt home");
    fs::create_dir_all(&codex_home).expect("codex home");

    let codex_auth = json!({"OPENAI_API_KEY": "codex-key"});
    fs::write(
        codex_home.join("auth.json"),
        serde_json::to_vec_pretty(&codex_auth).expect("json"),
    )
    .expect("codex auth");

    let read = auth::read_auth_file(Some(&chatgpt_home), Some(&codex_home), Some(&home_root))
        .expect("fallback to codex auth");
    assert_eq!(read.openai_api_key.as_deref(), Some("codex-key"));

    let auth_file = AuthFile {
        openai_api_key: Some("contract-api-key".to_string()),
        tokens: Some(auth::AuthTokens {
            access_token: Some("contract-access-token".to_string()),
            account_id: Some("contract-account-id".to_string()),
            id_token: Some("contract-id-token".to_string()),
            refresh_token: None,
            extra: Default::default(),
        }),
        last_refresh: Some(json!(12345)),
        extra: Default::default(),
    };

    let written_path = auth::write_auth_file(&chatgpt_home, &auth_file).expect("write auth");
    let written = serde_json::from_slice::<Value>(&fs::read(&written_path).expect("read auth"))
        .expect("parse auth");
    assert_eq!(written["OPENAI_API_KEY"], "contract-api-key");
    assert_eq!(written["tokens"]["access_token"], "contract-access-token");
    assert!(!chatgpt_home.join("auth.json.tmp").exists());
}

#[test]
fn prompt_lookup_distinguishes_dev_packaged_and_container_layouts() {
    let dev = TempDir::new().expect("tempdir");
    let dev_root = dev.path().join("repo");
    let dev_module = dev_root.join("chatmock");
    let dev_cwd = dev.path().join("cwd");
    fs::create_dir_all(&dev_module).expect("dev module");
    fs::create_dir_all(&dev_cwd).expect("dev cwd");
    fs::write(dev_root.join("prompt.md"), "root prompt\n").expect("root prompt");
    fs::write(dev_module.join("prompt.md"), "package prompt\n").expect("module prompt");
    let dev_lookup = PromptLookup {
        repo_root: dev_root.clone(),
        module_dir: dev_module,
        meipass_dir: None,
        cwd: dev_cwd,
    };
    assert_eq!(
        read_prompt_text("prompt.md", &dev_lookup).as_deref(),
        Some("root prompt")
    );

    let packaged = TempDir::new().expect("tempdir");
    let packaged_lookup = PromptLookup {
        repo_root: packaged.path().join("missing-root"),
        module_dir: packaged.path().join("missing-module"),
        meipass_dir: Some(packaged.path().join("bundle")),
        cwd: packaged.path().join("missing-cwd"),
    };
    fs::create_dir_all(packaged_lookup.meipass_dir.as_ref().expect("bundle")).expect("bundle dir");
    fs::write(
        packaged_lookup
            .meipass_dir
            .as_ref()
            .expect("bundle")
            .join("prompt.md"),
        "bundle prompt\n",
    )
    .expect("bundle prompt");
    assert_eq!(
        read_prompt_text("prompt.md", &packaged_lookup).as_deref(),
        Some("bundle prompt")
    );

    let container = TempDir::new().expect("tempdir");
    let container_lookup = PromptLookup {
        repo_root: container.path().join("missing-root"),
        module_dir: container.path().join("missing-module"),
        meipass_dir: None,
        cwd: container.path().join("workdir"),
    };
    fs::create_dir_all(&container_lookup.cwd).expect("container cwd");
    fs::write(container_lookup.cwd.join("prompt.md"), "cwd prompt\n").expect("cwd prompt");
    assert_eq!(
        read_prompt_text("prompt.md", &container_lookup).as_deref(),
        Some("cwd prompt")
    );
}
