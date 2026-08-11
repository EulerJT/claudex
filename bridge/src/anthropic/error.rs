use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ErrorEnvelope {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub error: ErrorDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorDetail {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: String,
}

pub fn json_error(
    status: StatusCode,
    kind: impl Into<String>,
    message: impl Into<String>,
) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            kind: "error",
            error: ErrorDetail {
                kind: kind.into(),
                message: message.into(),
            },
        }),
    )
        .into_response()
}
