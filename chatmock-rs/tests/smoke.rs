use chatmock_rs::{server, RuntimeConfig};

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let server = server::spawn_server(RuntimeConfig {
        port: 0,
        ..RuntimeConfig::default()
    })
    .await
    .expect("server should start");

    let response = reqwest::get(format!("{}/health", server.base_url()))
        .await
        .expect("request should succeed");

    assert!(
        response.status().is_success(),
        "unexpected status: {}",
        response.status()
    );

    let body: serde_json::Value = response.json().await.expect("json body");
    assert_eq!(body["status"], "ok");

    let bind_address = body["bind_address"]
        .as_str()
        .expect("bind_address should be a string");
    assert_eq!(bind_address, server.address.to_string());
}
