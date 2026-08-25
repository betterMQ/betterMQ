//! Security headers for the control panel and API docs.

use axum::{
    extract::Request,
    http::{header, HeaderValue},
    middleware::Next,
    response::Response,
};

const PANEL_CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'";
const DOCS_CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'; base-uri 'self'";

fn apply_common(res: &mut Response) {
    res.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    res.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

pub async fn panel_security_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    apply_common(&mut res);
    if let Ok(v) = HeaderValue::from_str(PANEL_CSP) {
        res.headers_mut().insert(header::CONTENT_SECURITY_POLICY, v);
    }
    res.headers_mut()
        .insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    res
}

pub async fn docs_security_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    apply_common(&mut res);
    if let Ok(v) = HeaderValue::from_str(DOCS_CSP) {
        res.headers_mut().insert(header::CONTENT_SECURITY_POLICY, v);
    }
    res.headers_mut().insert(
        header::X_FRAME_OPTIONS,
        HeaderValue::from_static("SAMEORIGIN"),
    );
    res
}

pub async fn api_security_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    apply_common(&mut res);
    res
}
