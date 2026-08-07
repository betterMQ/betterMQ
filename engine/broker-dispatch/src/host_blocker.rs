//! Per-destination host circuit breaker (CP6a / CP6b).
//! When `BETTERMQ_SHARED_META_DIR` is set, block state is shared across brokers (Phase E).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
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
    /// Wall-clock ms since epoch when block ends (shared-durable).
    blocked_until_ms: Option<i64>,
    cooldown_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SharedHostFile {
    hosts: HashMap<String, SharedHostEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedHostEntry {
    failures: u32,
    blocked_until_ms: Option<i64>,
    cooldown_ms: u64,
}

#[derive(Default)]
pub struct HostBlocker {
    cfg: HostBlockerConfig,
    hosts: Mutex<HashMap<String, HostState>>,
    shared_path: Option<PathBuf>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl HostBlocker {
    pub fn new(cfg: HostBlockerConfig) -> Self {
        let shared_path = std::env::var("BETTERMQ_SHARED_META_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|d| PathBuf::from(d).join("host_breaker.json"));
        let hb = Self {
            cfg,
            hosts: Mutex::new(HashMap::new()),
            shared_path: shared_path.clone(),
        };
        if let Some(ref path) = shared_path {
            hb.load_shared(path);
        }
        hb
    }

    fn load_shared(&self, path: &Path) {
        let Ok(mut f) = File::open(path) else {
            return;
        };
        let mut buf = String::new();
        if f.read_to_string(&mut buf).is_err() {
            return;
        }
        let Ok(file) = serde_json::from_str::<SharedHostFile>(&buf) else {
            return;
        };
        let mut hosts = self.hosts.lock().expect("host blocker lock");
        for (k, e) in file.hosts {
            hosts.insert(
                k,
                HostState {
                    failures: e.failures,
                    blocked_until_ms: e.blocked_until_ms,
                    cooldown_ms: e.cooldown_ms,
                },
            );
        }
    }

    fn persist_shared(&self) {
        let Some(ref path) = self.shared_path else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let hosts = self.hosts.lock().expect("host blocker lock");
        let file = SharedHostFile {
            hosts: hosts
                .iter()
                .map(|(k, s)| {
                    (
                        k.clone(),
                        SharedHostEntry {
                            failures: s.failures,
                            blocked_until_ms: s.blocked_until_ms,
                            cooldown_ms: s.cooldown_ms,
                        },
                    )
                })
                .collect(),
        };
        drop(hosts);
        let Ok(json) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
        {
            let _ = f.write_all(&json);
            let _ = f.sync_all();
            let _ = std::fs::rename(&tmp, path);
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
        // Refresh from shared file periodically for multi-broker.
        if let Some(ref path) = self.shared_path {
            self.load_shared(path);
        }
        let Some(key) = Self::host_key(url) else {
            return false;
        };
        let hosts = self.hosts.lock().expect("host blocker lock");
        let Some(state) = hosts.get(&key) else {
            return false;
        };
        state.blocked_until_ms.is_some_and(|until| now_ms() < until)
    }

    pub fn record_success(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        self.hosts.lock().expect("host blocker lock").remove(&key);
        self.persist_shared();
    }

    pub fn record_transport_failure(&self, url: &str) {
        let Some(key) = Self::host_key(url) else {
            return;
        };
        {
            let mut hosts = self.hosts.lock().expect("host blocker lock");
            let state = hosts.entry(key).or_insert_with(|| HostState {
                failures: 0,
                blocked_until_ms: None,
                cooldown_ms: self.cfg.initial_cooldown_ms,
            });
            state.failures += 1;
            if state.failures >= self.cfg.failures_before_block {
                let wait = state.cooldown_ms;
                state.blocked_until_ms = Some(now_ms() + wait as i64);
                state.cooldown_ms = ((state.cooldown_ms as f64) * self.cfg.multiplier)
                    .min(self.cfg.max_cooldown_ms as f64)
                    as u64;
                tracing::warn!(
                    destination = %url,
                    cooldown_ms = wait,
                    "host blocked after transport failures"
                );
            }
        }
        self.persist_shared();
    }

    /// Operator override — block a host immediately (URL or `scheme://host:port` key).
    pub fn block_manual(&self, host: &str, duration_ms: u64) -> String {
        let key = if host.contains("://") {
            Self::host_key(host).unwrap_or_else(|| host.trim().to_string())
        } else {
            host.trim().to_string()
        };
        let wait = duration_ms.max(1_000);
        self.hosts.lock().expect("host blocker lock").insert(
            key.clone(),
            HostState {
                failures: self.cfg.failures_before_block,
                blocked_until_ms: Some(now_ms() + wait as i64),
                cooldown_ms: duration_ms,
            },
        );
        tracing::warn!(host = %key, cooldown_ms = wait, "host manually blocked");
        self.persist_shared();
        key
    }

    pub fn unblock(&self, host: &str) -> bool {
        let key = if host.contains("://") {
            Self::host_key(host).unwrap_or_else(|| host.to_string())
        } else {
            host.to_string()
        };
        let removed = self
            .hosts
            .lock()
            .expect("host blocker lock")
            .remove(&key)
            .is_some();
        if removed {
            self.persist_shared();
        }
        removed
    }

    pub fn blocked_hosts(&self) -> Vec<(String, u64)> {
        if let Some(ref path) = self.shared_path {
            self.load_shared(path);
        }
        let hosts = self.hosts.lock().expect("host blocker lock");
        let now = now_ms();
        hosts
            .iter()
            .filter_map(|(k, s)| {
                s.blocked_until_ms.and_then(|until| {
                    if until > now {
                        Some((k.clone(), (until - now) as u64))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_failures() {
        let hb = HostBlocker::new(HostBlockerConfig {
            failures_before_block: 2,
            initial_cooldown_ms: 60_000,
            max_cooldown_ms: 60_000,
            multiplier: 2.0,
        });
        let url = "https://example.com/hook";
        assert!(!hb.is_blocked(url));
        hb.record_transport_failure(url);
        assert!(!hb.is_blocked(url));
        hb.record_transport_failure(url);
        assert!(hb.is_blocked(url));
        hb.record_success(url);
        assert!(!hb.is_blocked(url));
    }
}
