use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptLookup {
    pub repo_root: PathBuf,
    pub module_dir: PathBuf,
    pub meipass_dir: Option<PathBuf>,
    pub cwd: PathBuf,
}

pub fn read_prompt_text(filename: &str, lookup: &PromptLookup) -> Option<String> {
    for candidate in prompt_candidates(filename, lookup) {
        let Ok(content) = fs::read_to_string(&candidate) else {
            continue;
        };
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

pub fn prompt_candidates(filename: &str, lookup: &PromptLookup) -> Vec<PathBuf> {
    let mut candidates = vec![
        lookup.repo_root.join(filename),
        lookup.module_dir.join(filename),
    ];
    if let Some(meipass_dir) = &lookup.meipass_dir {
        candidates.push(meipass_dir.join(filename));
    }
    candidates.push(lookup.cwd.join(filename));
    candidates
}
