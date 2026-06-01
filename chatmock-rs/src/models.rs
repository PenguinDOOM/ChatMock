use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

pub const ALL_REASONING_EFFORTS: [&str; 6] = ["none", "minimal", "low", "medium", "high", "xhigh"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSpec {
    pub public_id: &'static str,
    pub upstream_id: &'static str,
    pub aliases: &'static [&'static str],
    pub allowed_efforts: &'static [&'static str],
    pub variant_efforts: &'static [&'static str],
    pub uses_codex_instructions: bool,
}

const MODEL_SPECS: &[ModelSpec] = &[
    ModelSpec {
        public_id: "gpt-5",
        upstream_id: "gpt-5",
        aliases: &["gpt5", "gpt-5-latest"],
        allowed_efforts: &ALL_REASONING_EFFORTS,
        variant_efforts: &["high", "medium", "low", "minimal"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.1",
        upstream_id: "gpt-5.1",
        aliases: &[],
        allowed_efforts: &["low", "medium", "high"],
        variant_efforts: &["high", "medium", "low"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.2",
        upstream_id: "gpt-5.2",
        aliases: &["gpt5.2", "gpt-5.2-latest"],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.4",
        upstream_id: "gpt-5.4",
        aliases: &["gpt5.4", "gpt-5.4-latest"],
        allowed_efforts: &["none", "low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low", "none"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.4-mini",
        upstream_id: "gpt-5.4-mini",
        aliases: &["gpt5.4-mini", "gpt-5.4-mini-latest"],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.5",
        upstream_id: "gpt-5.5",
        aliases: &["gpt5.5", "gpt-5.5-latest"],
        allowed_efforts: &["none", "low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low", "none"],
        uses_codex_instructions: false,
    },
    ModelSpec {
        public_id: "gpt-5.3-codex",
        upstream_id: "gpt-5.3-codex",
        aliases: &["gpt5.3-codex", "gpt-5.3-codex-latest"],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5.3-codex-spark",
        upstream_id: "gpt-5.3-codex-spark",
        aliases: &["gpt5.3-codex-spark", "gpt-5.3-codex-spark-latest"],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5-codex",
        upstream_id: "gpt-5-codex",
        aliases: &["gpt5-codex", "gpt-5-codex-latest"],
        allowed_efforts: &ALL_REASONING_EFFORTS,
        variant_efforts: &["high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5.2-codex",
        upstream_id: "gpt-5.2-codex",
        aliases: &["gpt5.2-codex", "gpt-5.2-codex-latest"],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5.1-codex",
        upstream_id: "gpt-5.1-codex",
        aliases: &[],
        allowed_efforts: &["low", "medium", "high"],
        variant_efforts: &["high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5.1-codex-max",
        upstream_id: "gpt-5.1-codex-max",
        aliases: &[],
        allowed_efforts: &["low", "medium", "high", "xhigh"],
        variant_efforts: &["xhigh", "high", "medium", "low"],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "gpt-5.1-codex-mini",
        upstream_id: "gpt-5.1-codex-mini",
        aliases: &[],
        allowed_efforts: &["low", "medium", "high"],
        variant_efforts: &[],
        uses_codex_instructions: true,
    },
    ModelSpec {
        public_id: "codex-mini",
        upstream_id: "codex-mini-latest",
        aliases: &["codex", "codex-mini-latest"],
        allowed_efforts: &ALL_REASONING_EFFORTS,
        variant_efforts: &[],
        uses_codex_instructions: true,
    },
];

fn alias_map() -> &'static HashMap<&'static str, &'static str> {
    static ALIASES: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
    ALIASES.get_or_init(|| {
        let mut aliases = HashMap::new();
        for spec in MODEL_SPECS {
            aliases.insert(spec.public_id, spec.upstream_id);
            for alias in spec.aliases {
                aliases.insert(*alias, spec.upstream_id);
            }
        }
        aliases
    })
}

fn strip_model_name(model: Option<&str>) -> (String, Option<String>) {
    let Some(model) = model else {
        return (String::new(), None);
    };
    let value = model.trim().to_ascii_lowercase();
    if value.is_empty() {
        return (String::new(), None);
    }
    if let Some((base, maybe_effort)) = value.rsplit_once(':') {
        if ALL_REASONING_EFFORTS.contains(&maybe_effort) {
            return (base.to_string(), Some(maybe_effort.to_string()));
        }
    }
    for separator in ['-', '_'] {
        for effort in ALL_REASONING_EFFORTS {
            let suffix = format!("{separator}{effort}");
            if value.ends_with(&suffix) {
                return (
                    value[..value.len() - suffix.len()].to_string(),
                    Some(effort.to_string()),
                );
            }
        }
    }
    (value, None)
}

pub fn model_spec_for_name(model: Option<&str>) -> Option<&'static ModelSpec> {
    let (base, _) = strip_model_name(model);
    let upstream_id = alias_map().get(base.as_str())?;
    MODEL_SPECS
        .iter()
        .find(|spec| spec.upstream_id == *upstream_id)
}

pub fn normalize_model_name(model: Option<&str>, debug_model: Option<&str>) -> String {
    if let Some(debug_model) = debug_model.filter(|value| !value.trim().is_empty()) {
        return debug_model.trim().to_string();
    }
    if let Some(spec) = model_spec_for_name(model) {
        return spec.upstream_id.to_string();
    }
    let (base, _) = strip_model_name(model);
    if base.is_empty() {
        "gpt-5.4".to_string()
    } else {
        base
    }
}

pub fn uses_codex_instructions(model: Option<&str>) -> bool {
    if let Some(spec) = model_spec_for_name(model) {
        return spec.uses_codex_instructions;
    }
    model
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .contains("codex")
}

pub fn allowed_efforts_for_model(model: &str) -> HashSet<&'static str> {
    if let Some(spec) = model_spec_for_name(Some(model)) {
        return spec.allowed_efforts.iter().copied().collect();
    }
    ALL_REASONING_EFFORTS.into_iter().collect()
}

pub fn extract_reasoning_from_model_name(
    model: Option<&str>,
) -> Option<HashMap<&'static str, String>> {
    let (_, effort) = strip_model_name(model);
    effort.map(|effort| HashMap::from([("effort", effort)]))
}

pub fn list_public_models(expose_reasoning_models: bool) -> Vec<String> {
    let mut model_ids = Vec::new();
    for spec in MODEL_SPECS {
        model_ids.push(spec.public_id.to_string());
        if expose_reasoning_models {
            model_ids.extend(
                spec.variant_efforts
                    .iter()
                    .map(|effort| format!("{}-{effort}", spec.public_id)),
            );
        }
    }
    model_ids
}

pub fn iter_public_models() -> impl Iterator<Item = &'static ModelSpec> {
    MODEL_SPECS.iter()
}
