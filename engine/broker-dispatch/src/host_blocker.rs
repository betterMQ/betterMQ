//! Per-destination host circuit breaker (CP6a / CP6b).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use url::Url;

#[derive(Debug, Clone)]
pub struct HostBlockerConfig {
    pub failures_before_block: u32,
    pub initial_cooldown_ms: u64,
    pub max_cooldown_ms: u64,
    pub multiplier: f64,
}

impl Default for HostBlockerConfig {
    fn default() -> Self {
        Self {
            failures_before_block: 3,
            initial_cooldown_ms: 30_000,
            max_cooldown_ms: 900_000,
            multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
struct HostState {
    failures: u32,
    blocked_until: Option<Instant>,
    cooldown_ms: u64,
}

#[derive(Default)]
pub struct HostBlocker {
    cfg: HostBlockerConfig,
    hosts: Mutex<HashMap<String, HostState>>,
}

impl HostBlocker {
    pub fn new(cfg: HostBlockerConfig) -> Self {
        Self {
            cfg,
            hosts: Mutex::new(HashMap::new()),
        }
    }

    pub fn host_key(url: &str) -> Option<String> {
        let parsed = Url::parse(url).ok()?;
        let host = parsed.host_str()?;
        let port = parsed
            .port()
            .unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
        Some(format!("{}://{}:{}", parsed.scheme(), host, port))
    }

    pub fn is_blocked(&self, url: &str) -> bool {
        let Some(key) = Self::host_key(url) else {
            return false;
        };
        let hosts = self.hosts.lock().expect("host blocker lock");
        let Some(state) = hosts.get(&key) else {
            return false;
        };
        state
            .blocked_until
            .is_some_and(|until| Instant::now() < until)
    }

    pub fn record_success(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        self.hosts.lock().expect("host blocker lock").remove(&key);
    }

    pub fn record_transport_failure(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        let mut hosts = self.hosts.lock().expect("host blocker lock");
        let state = hosts.entry(key).or_insert_with(|| HostState {
            failures: 0,
            blocked_until: None,
            cooldown_ms: self.cfg.initial_cooldown_ms,
        });
        state.failures += 1;
        if state.failures >= self.cfg.failures_before_block {
            let wait = Duration::from_millis(state.cooldown_ms);
            state.blocked_until = Some(Instant::now() + wait);
            state.cooldown_ms = ((state.cooldown_ms as f64) * self.cfg.multiplier)
                .min(self.cfg.max_cooldown_ms as f64) as u64;
            tracing::warn!(
                destination = %url,
                cooldown_ms = wait.as_millis(),
                "host blocked after transport failures"
            );
        }
    }

    pub fn unblock(&self, host: &str) -> bool {
        let key = if host.contains("://") {
            Self::host_key(host).unwrap_or_else(|| host.to_string())
        } else {
            host.to_string()
        };
        self.hosts
            .lock()
            .expect("host blocker lock")
            .remove(&key)
            .is_some()
    }

    pub fn blocked_hosts(&self) -> Vec<(String, u64)> {
        let hosts = self.hosts.lock().expect("host blocker lock");
        let now = Instant::now();
        hosts
            .iter()
            .filter_map(|(k, s)| {
                s.blocked_until.and_then(|until| {
                    if until > now {
                        Some((
                            k.clone(),
                            until.saturating_duration_since(now).as_millis() as u64,
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
}
