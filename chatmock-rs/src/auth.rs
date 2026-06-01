use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthTokens {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthFile {
    #[serde(rename = "OPENAI_API_KEY", skip_serializing_if = "Option::is_none")]
    pub openai_api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<AuthTokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refresh: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

pub fn get_home_dir(
    chatgpt_local_home: Option<&Path>,
    codex_home: Option<&Path>,
    user_home: Option<&Path>,
) -> PathBuf {
    if let Some(path) = chatgpt_local_home.filter(|path| !path.as_os_str().is_empty()) {
        return path.to_path_buf();
    }
    if let Some(path) = codex_home.filter(|path| !path.as_os_str().is_empty()) {
        return path.to_path_buf();
    }
    user_home
        .unwrap_or_else(|| Path::new("."))
        .join(".chatgpt-local")
}

pub fn auth_search_roots(
    chatgpt_local_home: Option<&Path>,
    codex_home: Option<&Path>,
    user_home: Option<&Path>,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(path) = chatgpt_local_home.filter(|path| !path.as_os_str().is_empty()) {
        roots.push(path.to_path_buf());
    }
    if let Some(path) = codex_home.filter(|path| !path.as_os_str().is_empty()) {
        roots.push(path.to_path_buf());
    }
    if let Some(user_home) = user_home {
        roots.push(user_home.join(".chatgpt-local"));
        roots.push(user_home.join(".codex"));
    }
    roots
}

pub fn read_auth_file(
    chatgpt_local_home: Option<&Path>,
    codex_home: Option<&Path>,
    user_home: Option<&Path>,
) -> Option<AuthFile> {
    for root in auth_search_roots(chatgpt_local_home, codex_home, user_home) {
        let path = root.join("auth.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if let Ok(auth_file) = serde_json::from_slice::<AuthFile>(&bytes) {
            return Some(auth_file);
        }
    }
    None
}

fn should_remove_temp_auth_file(existing_file_removed: bool, rename_failed: bool) -> bool {
    #[cfg(windows)]
    if existing_file_removed && rename_failed {
        return false;
    }

    true
}

fn cleanup_temp_auth_file(temp_path: &Path, existing_file_removed: bool, rename_failed: bool) {
    if should_remove_temp_auth_file(existing_file_removed, rename_failed) && temp_path.exists() {
        let _ = fs::remove_file(temp_path);
    }
}

pub fn write_auth_file(home_dir: &Path, auth: &AuthFile) -> io::Result<PathBuf> {
    fs::create_dir_all(home_dir)?;
    let path = home_dir.join("auth.json");
    let temp_path = home_dir.join("auth.json.tmp");
    let mut existing_file_removed = false;
    let mut rename_failed = false;

    let write_result = (|| -> io::Result<()> {
        let mut temp_file = fs::File::create(&temp_path)?;
        let bytes = serde_json::to_vec_pretty(auth)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        temp_file.write_all(&bytes)?;
        temp_file.flush()?;
        drop(temp_file);

        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(&path)?;
            existing_file_removed = true;
        }

        if let Err(error) = fs::rename(&temp_path, &path) {
            rename_failed = true;
            return Err(error);
        }

        Ok(())
    })();

    if write_result.is_err() {
        cleanup_temp_auth_file(&temp_path, existing_file_removed, rename_failed);
    }

    write_result.map(|_| path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn cleanup_removes_temp_file_for_generic_write_failures() {
        let temp = TempDir::new().expect("tempdir");
        let temp_path = temp.path().join("auth.json.tmp");
        fs::write(&temp_path, b"new auth").expect("write temp auth");

        cleanup_temp_auth_file(&temp_path, false, true);

        assert!(!temp_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn cleanup_preserves_temp_file_after_replace_failure() {
        let temp = TempDir::new().expect("tempdir");
        let temp_path = temp.path().join("auth.json.tmp");
        fs::write(&temp_path, b"new auth").expect("write temp auth");

        cleanup_temp_auth_file(&temp_path, true, true);

        assert!(temp_path.exists());
    }
}
