//! Control panel static assets (embedded at `cargo build` time).

use axum::{
    body::Body,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;
use std::path::PathBuf;

#[derive(RustEmbed)]
#[folder = "../control-panel/"]
struct PanelAssets;

pub fn embedded_router() -> Router {
    Router::new()
        .route("/panel-config.js", get(panel_config_js))
        .route("/", get(|| serve("index.html".into())))
        .route(
            "/{*path}",
            get(|axum::extract::Path(p): axum::extract::Path<String>| serve(p)),
        )
}

pub fn resolve_router() -> Router {
    if let Ok(dir) = std::env::var("BETTERMQ_PANEL_DIR") {
        let path = PathBuf::from(&dir);
        if path.join("index.html").is_file() {
            tracing::info!(panel = %path.display(), "serving control panel from disk");
            return filesystem_router(path);
        }
    }
    tracing::info!("serving embedded control panel");
    embedded_router()
}

pub fn filesystem_router(dir: PathBuf) -> Router {
    let service = tower_http::services::ServeDir::new(dir.clone())
        .append_index_html_on_directories(true)
        .not_found_service(tower_http::services::ServeFile::new(dir.join("index.html")));
    Router::new()
        .route("/panel-config.js", get(panel_config_js))
        .fallback_service(service)
}

async fn panel_config_js() -> Response {
    let body = if cfg!(feature = "cloud") {
        "window.__BETTERMQ_EXTERNAL_AUTH__=true;"
    } else {
        "window.__BETTERMQ_EXTERNAL_AUTH__=false;"
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap()
}

async fn serve(path: String) -> Response {
    let path = path.trim_start_matches('/');
    let key = if path.is_empty() { "index.html" } else { path };

    if let Some(file) = PanelAssets::get(key) {
        return asset_response(key, file);
    }

    if let Some(file) = PanelAssets::get("index.html") {
        return asset_response("index.html", file);
    }

    StatusCode::NOT_FOUND.into_response()
}

fn asset_response(path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(file.data.into_owned()))
        .unwrap()
}
