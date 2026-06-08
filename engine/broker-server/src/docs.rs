//! OpenAPI spec + Scalar UI (embedded, served at `/docs` and `/api-reference`).

use axum::{
    http::header,
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Router,
};

#[cfg(feature = "cloud")]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.cloud.openapi.json");
#[cfg(not(feature = "cloud"))]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.openapi.json");
const SCALAR_HTML: &str = include_str!("../embedded/scalar.html");

pub fn router() -> Router {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(scalar))
        .route("/api-reference", get(scalar))
        .route(
            "/api-reference/",
            get(|| async { Redirect::permanent("/api-reference") }),
        )
        .route("/docs/", get(|| async { Redirect::permanent("/docs") }))
}

async fn openapi_json() -> Response {
    (
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        OPENAPI_JSON,
    )
        .into_response()
}

async fn scalar() -> Html<&'static str> {
    Html(SCALAR_HTML)
}
