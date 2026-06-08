//! Pause dispatch fetches when process memory is high (CP6a).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MemoryGuardConfig {
    pub fetch_limit_percent: u8,
    pub limit_mb: Option<u64>,
}

impl Default for MemoryGuardConfig {
    fn default() -> Self {
        Self {
            fetch_limit_percent: 75,
            limit_mb: std::env::var("BETTERMQ_MEMORY_LIMIT_MB")
                .ok()
                .and_then(|s| s.parse().ok()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessResourceStats {
    pub rss_mb: Option<u64>,
    pub cpu_percent: Option<f32>,
}

#[derive(Default)]
pub struct MemoryGuard {
    critical: AtomicBool,
    cfg: MemoryGuardConfig,
}

impl MemoryGuard {
    pub fn new(cfg: MemoryGuardConfig) -> Self {
        Self {
            critical: AtomicBool::new(false),
            cfg,
        }
    }

    pub fn spawn_monitor(self: &std::sync::Arc<Self>) {
        let guard = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                guard.sample();
            }
        });
    }

    fn sample(&self) {
        let Some(limit_mb) = self.cfg.limit_mb else {
            return;
        };
        let rss_mb = sample_process_resources().rss_mb.unwrap_or(0);
        let threshold = limit_mb.saturating_mul(self.cfg.fetch_limit_percent as u64) / 100;
        let is_critical = rss_mb >= threshold;
        let was = self.critical.swap(is_critical, Ordering::SeqCst);
        if is_critical && !was {
            tracing::error!(
                rss_mb,
                threshold_mb = threshold,
                limit_mb,
                "memory critical: pausing dispatch fetch"
            );
        } else if !is_critical && was {
            tracing::info!(rss_mb, "memory usage back to normal");
        }
    }

    pub fn limit_mb(&self) -> Option<u64> {
        self.cfg.limit_mb
    }

    pub fn fetch_limit_percent(&self) -> u8 {
        self.cfg.fetch_limit_percent
    }

    /// True when RSS is at or above the fetch pause threshold.
    pub fn is_critical(&self) -> bool {
        self.critical.load(Ordering::SeqCst)
    }

    pub async fn wait_below_limit(&self) {
        if !self.critical.load(Ordering::SeqCst) {
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if !self.critical.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}

/// RSS + CPU for the broker process (best-effort; used by `/metrics` and the panel).
pub fn sample_process_resources() -> ProcessResourceStats {
    #[cfg(unix)]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=,%cpu=", "-p", &pid.to_string()])
            .output()
            .ok();
        if let Some(out) = out {
            if out.status.success() {
                let line = String::from_utf8_lossy(&out.stdout);
                let mut parts = line.split(',');
                let rss_kb: Option<u64> = parts.next().and_then(|s| s.trim().parse().ok());
                let cpu: Option<f32> = parts.next().and_then(|s| s.trim().parse().ok());
                return ProcessResourceStats {
                    rss_mb: rss_kb.map(|kb| kb / 1024),
                    cpu_percent: cpu,
                };
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(rss) = linux_rss_mb() {
            return ProcessResourceStats {
                rss_mb: Some(rss),
                cpu_percent: None,
            };
        }
    }
    ProcessResourceStats::default()
}

#[cfg(target_os = "linux")]
fn linux_rss_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(kb) = line.strip_prefix("VmRSS:") {
            let kb: u64 = kb.split_whitespace().next()?.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}
