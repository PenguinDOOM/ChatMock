use axum::{http::StatusCode, response::Response, routing::get, Router};
use serde::Serialize;

use crate::models::list_public_models;
use crate::routes::json_response;

#[derive(Debug, Serialize)]
struct ModelsResponse {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Debug, Serialize)]
struct ModelEntry {
    id: String,
    object: &'static str,
    owned_by: &'static str,
}

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new().route("/v1/models", get(list_models))
}

async fn list_models(
    axum::extract::State(state): axum::extract::State<crate::server::AppState>,
) -> Response {
    let data = list_public_models(state.expose_reasoning_models)
        .into_iter()
        .map(|id| ModelEntry {
            id,
            object: "model",
            owned_by: "owner",
        })
        .collect();
    json_response(
        StatusCode::OK,
        serde_json::to_value(ModelsResponse {
            object: "list",
            data,
        })
        .expect("models response payload"),
    )
}
