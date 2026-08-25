//! OpenAPI spec + Scalar UI (embedded, served at `/docs` and `/api-reference`).

use axum::{
    http::{header, HeaderValue},
    middleware,
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Router,
};

#[cfg(feature = "cloud")]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.cloud.openapi.json");
#[cfg(not(feature = "cloud"))]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.openapi.json");
const SCALAR_HTML: &str = include_str!("../embedded/scalar.html");
const SCALAR_JS: &[u8] = include_bytes!("../embedded/scalar-standalone.js");

pub fn router() -> Router {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(scalar))
        .route("/docs/scalar.js", get(scalar_js))
        .route("/api-reference", get(scalar))
        .route(
            "/api-reference/",
            get(|| async { Redirect::permanent("/api-reference") }),
        )
        .route("/docs/", get(|| async { Redirect::permanent("/docs") }))
        .layer(middleware::from_fn(crate::security::docs_security_headers))
}

async fn openapi_json() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        OPENAPI_JSON,
    )
        .into_response()
}

async fn scalar() -> Html<&'static str> {
    Html(SCALAR_HTML)
}

async fn scalar_js() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/javascript; charset=utf-8"),
        )],
        SCALAR_JS,
    )
        .into_response()
}
