use std::fs;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use chatmock_rs::auth::{load_effective_chatgpt_auth_with_refresher, RefreshedAuthTokens};
use serde_json::{json, Value};
use tempfile::TempDir;

#[tokio::test]
async fn derives_account_id_from_id_token_when_auth_json_omits_it() {
    let auth_home = TempDir::new().expect("tempdir");
    fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": "contract-access-token",
                "id_token": jwt_with_claims(json!({
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "derived-account-id"
                    }
                }))
            }
        })
        .to_string(),
    )
    .expect("write auth file");

    let auth = load_effective_chatgpt_auth_with_refresher(
        Some(auth_home.path()),
        None,
        None,
        |_refresh_token| async move { None },
    )
    .await
    .expect("effective auth");

    assert_eq!(auth.access_token, "contract-access-token");
    assert_eq!(auth.account_id, "derived-account-id");
}

#[tokio::test]
async fn refreshes_missing_access_token_and_persists_updated_auth_json() {
    let auth_home = TempDir::new().expect("tempdir");
    fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "refresh_token": "refresh-token-123",
                "id_token": jwt_with_claims(json!({
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "stale-account-id"
                    }
                }))
            },
            "last_refresh": "2000-01-01T00:00:00Z"
        })
        .to_string(),
    )
    .expect("write auth file");

    let refresh_calls = Arc::new(AtomicUsize::new(0));
    let refresh_calls_clone = Arc::clone(&refresh_calls);
    let refreshed_id_token = jwt_with_claims(json!({
        "https://api.openai.com/auth": {
            "chatgpt_account_id": "refreshed-account-id"
        }
    }));

    let auth = load_effective_chatgpt_auth_with_refresher(
        Some(auth_home.path()),
        None,
        None,
        move |refresh_token| {
            let refresh_calls = Arc::clone(&refresh_calls_clone);
            let refreshed_id_token = refreshed_id_token.clone();
            async move {
                refresh_calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(refresh_token, "refresh-token-123");
                Some(RefreshedAuthTokens {
                    access_token: Some("refreshed-access-token".to_string()),
                    account_id: None,
                    id_token: Some(refreshed_id_token),
                    refresh_token: Some("refreshed-refresh-token".to_string()),
                })
            }
        },
    )
    .await
    .expect("effective auth");

    assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
    assert_eq!(auth.access_token, "refreshed-access-token");
    assert_eq!(auth.account_id, "refreshed-account-id");

    let persisted: Value = serde_json::from_slice(
        &fs::read(auth_home.path().join("auth.json")).expect("read auth file"),
    )
    .expect("persisted auth json");
    assert_eq!(
        persisted["tokens"]["access_token"],
        json!("refreshed-access-token")
    );
    assert_eq!(
        persisted["tokens"]["refresh_token"],
        json!("refreshed-refresh-token")
    );
    assert_eq!(
        persisted["tokens"]["account_id"],
        json!("refreshed-account-id")
    );
    assert!(persisted["last_refresh"].as_str().is_some());
}

#[tokio::test]
async fn skips_refresh_when_access_token_is_still_fresh() {
    let auth_home = TempDir::new().expect("tempdir");
    fs::write(
        auth_home.path().join("auth.json"),
        json!({
            "tokens": {
                "access_token": jwt_with_claims(json!({
                    "exp": future_unix_timestamp(60 * 60)
                })),
                "refresh_token": "refresh-token-123",
                "id_token": jwt_with_claims(json!({
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "derived-account-id"
                    }
                }))
            },
            "last_refresh": "2000-01-01T00:00:00Z"
        })
        .to_string(),
    )
    .expect("write auth file");

    let refresh_calls = Arc::new(AtomicUsize::new(0));
    let refresh_calls_clone = Arc::clone(&refresh_calls);

    let auth = load_effective_chatgpt_auth_with_refresher(
        Some(auth_home.path()),
        None,
        None,
        move |_refresh_token| {
            let refresh_calls = Arc::clone(&refresh_calls_clone);
            async move {
                refresh_calls.fetch_add(1, Ordering::SeqCst);
                None
            }
        },
    )
    .await
    .expect("effective auth");

    assert_eq!(refresh_calls.load(Ordering::SeqCst), 0);
    assert_eq!(auth.account_id, "derived-account-id");
}

fn jwt_with_claims(claims: Value) -> String {
    format!(
        "{}.{}.",
        base64url_encode(br#"{"alg":"none","typ":"JWT"}"#),
        base64url_encode(claims.to_string().as_bytes())
    )
}

fn base64url_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::new();
    let mut index = 0;

    while index + 3 <= bytes.len() {
        let chunk = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | (bytes[index + 2] as u32);
        encoded.push(TABLE[((chunk >> 18) & 0x3f) as usize] as char);
        encoded.push(TABLE[((chunk >> 12) & 0x3f) as usize] as char);
        encoded.push(TABLE[((chunk >> 6) & 0x3f) as usize] as char);
        encoded.push(TABLE[(chunk & 0x3f) as usize] as char);
        index += 3;
    }

    let remainder = bytes.len() - index;
    if remainder == 1 {
        let chunk = (bytes[index] as u32) << 16;
        encoded.push(TABLE[((chunk >> 18) & 0x3f) as usize] as char);
        encoded.push(TABLE[((chunk >> 12) & 0x3f) as usize] as char);
    } else if remainder == 2 {
        let chunk = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
        encoded.push(TABLE[((chunk >> 18) & 0x3f) as usize] as char);
        encoded.push(TABLE[((chunk >> 12) & 0x3f) as usize] as char);
        encoded.push(TABLE[((chunk >> 6) & 0x3f) as usize] as char);
    }

    encoded
}

fn future_unix_timestamp(offset_seconds: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix time")
        .as_secs()
        + offset_seconds
}
