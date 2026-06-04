use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::{
    jobs::{JobIdArgs, JobManagerError, ReadJobOutputArgs, StartJobArgs},
    routes::json_response,
};

pub(crate) fn router() -> Router<crate::server::AppState> {
    Router::new()
        .route("/_chatmock/jobs", post(start_job))
        .route("/_chatmock/jobs/{job_id}", get(get_job))
        .route("/_chatmock/jobs/{job_id}/output", get(read_job_output))
        .route("/_chatmock/jobs/{job_id}/cancel", post(cancel_job))
}

async fn start_job(
    State(state): State<crate::server::AppState>,
    Json(args): Json<StartJobArgs>,
) -> Response {
    if !state.chatmock_jobs_enabled {
        return jobs_disabled();
    }
    match state.job_manager.start_job(args).await {
        Ok(result) => json_response(StatusCode::OK, serde_json::to_value(result).expect("json")),
        Err(error) => job_error_response(error),
    }
}

async fn get_job(
    State(state): State<crate::server::AppState>,
    Path(job_id): Path<String>,
) -> Response {
    if !state.chatmock_jobs_enabled {
        return jobs_disabled();
    }
    match state.job_manager.get_job_result(JobIdArgs { job_id }).await {
        Ok(result) => json_response(StatusCode::OK, serde_json::to_value(result).expect("json")),
        Err(error) => job_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct OutputQuery {
    since_offset: Option<u64>,
    max_bytes: Option<usize>,
}

async fn read_job_output(
    State(state): State<crate::server::AppState>,
    Path(job_id): Path<String>,
    Query(query): Query<OutputQuery>,
) -> Response {
    if !state.chatmock_jobs_enabled {
        return jobs_disabled();
    }
    match state
        .job_manager
        .read_job_output(ReadJobOutputArgs {
            job_id,
            since_offset: query.since_offset,
            max_bytes: query.max_bytes,
        })
        .await
    {
        Ok(result) => json_response(StatusCode::OK, serde_json::to_value(result).expect("json")),
        Err(error) => job_error_response(error),
    }
}

async fn cancel_job(
    State(state): State<crate::server::AppState>,
    Path(job_id): Path<String>,
) -> Response {
    if !state.chatmock_jobs_enabled {
        return jobs_disabled();
    }
    match state.job_manager.cancel_job(JobIdArgs { job_id }).await {
        Ok(result) => json_response(StatusCode::OK, serde_json::to_value(result).expect("json")),
        Err(error) => job_error_response(error),
    }
}

pub(crate) fn job_error_response(error: JobManagerError) -> Response {
    let status = match error {
        JobManagerError::NotFound(_) => StatusCode::NOT_FOUND,
        JobManagerError::Capacity => StatusCode::SERVICE_UNAVAILABLE,
        JobManagerError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
    };
    json_response(
        status,
        serde_json::json!({
            "error": {
                "message": error.to_string(),
            }
        }),
    )
}

fn jobs_disabled() -> Response {
    json_response(
        StatusCode::FORBIDDEN,
        serde_json::json!({
            "error": {
                "message": "ChatMock jobs are disabled. Start with --enable-chatmock-jobs to use this endpoint.",
            }
        }),
    )
}
