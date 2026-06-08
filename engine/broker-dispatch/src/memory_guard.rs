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
        let rss_mb = current_rss_mb().unwrap_or(0);
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

fn current_rss_mb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(kb) = line.strip_prefix("VmRSS:") {
                let kb: u64 = kb.split_whitespace().next()?.parse().ok()?;
                return Some(kb / 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
