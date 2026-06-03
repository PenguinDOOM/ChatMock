use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::future::Future;
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveChatgptAuth {
    pub access_token: String,
    pub account_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefreshedAuthTokens {
    pub access_token: Option<String>,
    pub account_id: Option<String>,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
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

pub async fn load_effective_chatgpt_auth_from_env() -> Option<EffectiveChatgptAuth> {
    load_effective_chatgpt_auth_from_env_with_refresher(|_refresh_token| async { None }).await
}

pub async fn load_effective_chatgpt_auth_from_env_with_refresher<F, Fut>(
    refresher: F,
) -> Option<EffectiveChatgptAuth>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RefreshedAuthTokens>>,
{
    let chatgpt_local_home = env_path("CHATGPT_LOCAL_HOME");
    let codex_home = env_path("CODEX_HOME");
    let user_home = env_path("USERPROFILE").or_else(|| env_path("HOME"));
    load_effective_chatgpt_auth_with_refresher(
        chatgpt_local_home.as_deref(),
        codex_home.as_deref(),
        user_home.as_deref(),
        refresher,
    )
    .await
}

pub async fn load_effective_chatgpt_auth_with_refresher<F, Fut>(
    chatgpt_local_home: Option<&Path>,
    codex_home: Option<&Path>,
    user_home: Option<&Path>,
    refresher: F,
) -> Option<EffectiveChatgptAuth>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RefreshedAuthTokens>>,
{
    let (home_dir, mut auth_file) =
        read_auth_file_with_home_dir(chatgpt_local_home, codex_home, user_home)?;
    let mut tokens = auth_file.tokens.clone()?;

    if let Some(refresh_token) = non_empty_str(tokens.refresh_token.as_deref()) {
        if should_refresh_access_token(
            tokens.access_token.as_deref(),
            auth_file.last_refresh.as_ref(),
            SystemTime::now(),
        ) {
            if let Some(refreshed) = refresher(refresh_token.to_string()).await {
                apply_refreshed_tokens(&mut tokens, refreshed);
                if tokens.account_id.is_none() {
                    tokens.account_id = derive_account_id(tokens.id_token.as_deref());
                }

                auth_file.tokens = Some(tokens.clone());
                auth_file.last_refresh =
                    Some(Value::String(system_time_to_iso8601(SystemTime::now())));
                let _ = write_auth_file(&home_dir, &auth_file);
            }
        }
    }

    let access_token = non_empty_str(tokens.access_token.as_deref())?.to_string();
    let account_id = tokens
        .account_id
        .clone()
        .or_else(|| derive_account_id(tokens.id_token.as_deref()))?;

    Some(EffectiveChatgptAuth {
        access_token,
        account_id,
    })
}

fn read_auth_file_with_home_dir(
    chatgpt_local_home: Option<&Path>,
    codex_home: Option<&Path>,
    user_home: Option<&Path>,
) -> Option<(PathBuf, AuthFile)> {
    for root in auth_search_roots(chatgpt_local_home, codex_home, user_home) {
        let path = root.join("auth.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if let Ok(auth_file) = serde_json::from_slice::<AuthFile>(&bytes) {
            return Some((root, auth_file));
        }
    }
    None
}

fn non_empty_str(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn apply_refreshed_tokens(tokens: &mut AuthTokens, refreshed: RefreshedAuthTokens) {
    if let Some(access_token) = refreshed.access_token.filter(|value| !value.is_empty()) {
        tokens.access_token = Some(access_token);
    }
    if let Some(account_id) = refreshed.account_id.filter(|value| !value.is_empty()) {
        tokens.account_id = Some(account_id);
    }
    if let Some(id_token) = refreshed.id_token.filter(|value| !value.is_empty()) {
        tokens.id_token = Some(id_token);
    }
    if let Some(refresh_token) = refreshed.refresh_token.filter(|value| !value.is_empty()) {
        tokens.refresh_token = Some(refresh_token);
    }
}

fn should_refresh_access_token(
    access_token: Option<&str>,
    last_refresh: Option<&Value>,
    now: SystemTime,
) -> bool {
    let Some(access_token) = non_empty_str(access_token) else {
        return true;
    };

    if let Some(exp) = parse_jwt_claims(access_token)
        .and_then(|claims| claims.get("exp").cloned())
        .and_then(|value| value.as_f64())
    {
        if let Some(expiry) = unix_seconds_to_system_time(exp) {
            let refresh_deadline = now.checked_add(Duration::from_secs(5 * 60)).unwrap_or(now);
            return expiry <= refresh_deadline;
        }
    }

    if let Some(refreshed_at) = last_refresh
        .and_then(Value::as_str)
        .and_then(parse_iso8601_system_time)
    {
        let refresh_cutoff = now
            .checked_sub(Duration::from_secs(55 * 60))
            .unwrap_or(UNIX_EPOCH);
        return refreshed_at <= refresh_cutoff;
    }

    false
}

fn derive_account_id(id_token: Option<&str>) -> Option<String> {
    let claims = parse_jwt_claims(non_empty_str(id_token)?)?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object)
        .and_then(|auth_claims| auth_claims.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

pub fn derive_account_id_from_id_token(id_token: &str) -> Option<String> {
    derive_account_id(Some(id_token))
}

fn parse_jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = decode_base64url(payload)?;
    serde_json::from_slice(&decoded).ok()
}

fn decode_base64url(value: &str) -> Option<Vec<u8>> {
    let mut bytes = Vec::with_capacity((value.len() * 3) / 4 + 3);
    let mut buffer = 0u32;
    let mut bits = 0u8;

    for byte in value.bytes() {
        let sextet = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => continue,
            _ => return None,
        } as u32;

        buffer = (buffer << 6) | sextet;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push(((buffer >> bits) & 0xff) as u8);
        }
    }

    Some(bytes)
}

fn unix_seconds_to_system_time(seconds: f64) -> Option<SystemTime> {
    if !seconds.is_finite() {
        return None;
    }

    if seconds >= 0.0 {
        UNIX_EPOCH.checked_add(Duration::from_secs_f64(seconds))
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs_f64(-seconds))
    }
}

fn parse_iso8601_system_time(value: &str) -> Option<SystemTime> {
    let (date_part, time_part) = value.split_once('T')?;
    let mut date_iter = date_part.split('-');
    let year: i64 = date_iter.next()?.parse().ok()?;
    let month: u32 = date_iter.next()?.parse().ok()?;
    let day: u32 = date_iter.next()?.parse().ok()?;
    if date_iter.next().is_some() {
        return None;
    }

    let (time_part, offset_seconds) = if let Some(time_part) = time_part.strip_suffix('Z') {
        (time_part, 0i64)
    } else if let Some(index) = time_part.rfind(['+', '-']) {
        let sign = if time_part.as_bytes()[index] == b'+' {
            1i64
        } else {
            -1i64
        };
        let offset = &time_part[index + 1..];
        let mut offset_iter = offset.split(':');
        let offset_hours: i64 = offset_iter.next()?.parse().ok()?;
        let offset_minutes: i64 = offset_iter.next()?.parse().ok()?;
        if offset_iter.next().is_some() {
            return None;
        }
        (
            &time_part[..index],
            sign * (offset_hours * 60 * 60 + offset_minutes * 60),
        )
    } else {
        return None;
    };

    let mut time_iter = time_part.split(':');
    let hour: i64 = time_iter.next()?.parse().ok()?;
    let minute: i64 = time_iter.next()?.parse().ok()?;
    let second_part = time_iter.next()?;
    if time_iter.next().is_some() {
        return None;
    }
    let second_text = second_part
        .split_once('.')
        .map_or(second_part, |(seconds, _)| seconds);
    let second: i64 = second_text.parse().ok()?;

    let days = days_from_civil(year, month, day)?;
    let unix_seconds = days
        .checked_mul(24 * 60 * 60)?
        .checked_add(hour.checked_mul(60 * 60)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(offset_seconds)?;

    if unix_seconds >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(unix_seconds as u64))
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs((-unix_seconds) as u64))
    }
}

fn system_time_to_iso8601(time: SystemTime) -> String {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    let unix_seconds = duration.as_secs() as i64;
    let days = unix_seconds.div_euclid(24 * 60 * 60);
    let seconds_of_day = unix_seconds.rem_euclid(24 * 60 * 60);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / (60 * 60);
    let minute = (seconds_of_day % (60 * 60)) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

pub fn format_system_time_as_iso8601(time: SystemTime) -> String {
    system_time_to_iso8601(time)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year / 400
    } else {
        (adjusted_year - 399) / 400
    };
    let year_of_era = adjusted_year - era * 400;
    let month_prime = month as i64 + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days / 146_097
    } else {
        (shifted_days - 146_096) / 146_097
    };
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month, day)
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
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

fn create_temp_auth_file(temp_path: &Path) -> io::Result<fs::File> {
    #[cfg(unix)]
    {
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(temp_path)
    }

    #[cfg(not(unix))]
    {
        fs::File::create(temp_path)
    }
}

pub fn write_auth_file(home_dir: &Path, auth: &AuthFile) -> io::Result<PathBuf> {
    fs::create_dir_all(home_dir)?;
    let path = home_dir.join("auth.json");
    let temp_path = home_dir.join("auth.json.tmp");
    let mut existing_file_removed = false;
    let mut rename_failed = false;

    let write_result = (|| -> io::Result<()> {
        let mut temp_file = create_temp_auth_file(&temp_path)?;
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

    #[test]
    fn derive_account_id_reads_openai_auth_claim() {
        let id_token = format!(
            "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.{}.signature",
            "eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjb3VudC0xMjMifX0"
        );

        assert_eq!(
            derive_account_id(Some(&id_token)),
            Some("account-123".to_string())
        );
    }

    #[test]
    fn system_time_iso8601_round_trips_second_precision() {
        let timestamp = parse_iso8601_system_time("2026-06-03T12:34:56Z").expect("parse time");

        assert_eq!(system_time_to_iso8601(timestamp), "2026-06-03T12:34:56Z");
    }

    #[cfg(unix)]
    #[test]
    fn create_temp_auth_file_keeps_permissions_restrictive_on_unix() {
        let temp = TempDir::new().expect("tempdir");
        let temp_path = temp.path().join("auth.json.tmp");

        let temp_file = create_temp_auth_file(&temp_path).expect("create temp auth file");
        drop(temp_file);

        let mode = fs::metadata(&temp_path)
            .expect("auth metadata")
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(mode, 0o600);
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
