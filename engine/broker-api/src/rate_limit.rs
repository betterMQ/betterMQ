//! Simple in-process rate limiter for auth-sensitive endpoints.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<State>>,
    max: u32,
    window: Duration,
}

struct State {
    hits: HashMap<String, Vec<Instant>>,
}

impl RateLimiter {
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                hits: HashMap::new(),
            })),
            max,
            window,
        }
    }

    /// Returns true if the request is allowed.
    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut state = self.inner.lock();
        let entries = state.hits.entry(key.to_string()).or_default();
        entries.retain(|t| now.duration_since(*t) < self.window);
        if entries.len() as u32 >= self.max {
            return false;
        }
        entries.push(now);
        // Opportunistic cleanup to bound map size.
        if state.hits.len() > 10_000 {
            state.hits.retain(|_, v| {
                v.retain(|t| now.duration_since(*t) < self.window);
                !v.is_empty()
            });
        }
        true
    }
}

fn trust_proxy() -> bool {
    matches!(
        std::env::var("BETTERMQ_TRUST_PROXY")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

pub fn client_ip_key(headers: &axum::http::HeaderMap, fallback: Option<IpAddr>) -> String {
    if trust_proxy() {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(first) = xff.split(',').next() {
                let ip = first.trim();
                if !ip.is_empty() {
                    return ip.to_string();
                }
            }
        }
    }
    fallback
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_burst() {
        let lim = RateLimiter::new(2, Duration::from_secs(60));
        assert!(lim.check("a"));
        assert!(lim.check("a"));
        assert!(!lim.check("a"));
        assert!(lim.check("b"));
    }
}
