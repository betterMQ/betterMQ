//! Shared secret for inter-broker `/internal/v1/*` calls (CP6b.3).
//!
//! Fail-closed: when `BETTERMQ_CLUSTER_SECRET` is unset/empty, all internal
//! routes return 401. Single-node deployments do not need the secret for normal
//! operation (peers never call `/internal/v1/*`); multi-node clusters must set
//! the same secret on every node.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

pub const CLUSTER_SECRET_HEADER: &str = "x-bettermq-cluster-secret";

pub fn cluster_secret() -> Option<String> {
    std::env::var("BETTERMQ_CLUSTER_SECRET")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn apply_cluster_secret(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(secret) = cluster_secret() {
        builder.header(CLUSTER_SECRET_HEADER, secret)
    } else {
        builder
    }
}

pub async fn require_cluster_secret(
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected) = cluster_secret() else {
        // Fail closed: never expose internal admin/replication APIs without a secret.
        return Err(StatusCode::UNAUTHORIZED);
    };
    let authorized = req
        .headers()
        .get(CLUSTER_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|got| constant_time_eq(got, &expected));
    if !authorized {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_rejects_length_mismatch() {
        assert!(!constant_time_eq("ab", "abc"));
        assert!(constant_time_eq("same", "same"));
    }
}
