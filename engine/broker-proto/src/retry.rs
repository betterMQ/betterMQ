//! Per-message and default retry / backoff policy.

use serde::{Deserialize, Serialize};

fn default_initial_ms() -> u64 {
    500
}

fn default_max_ms() -> u64 {
    30_000
}

fn default_multiplier() -> f64 {
    2.0
}

/// Backoff between delivery attempts.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum RetryBackoffKind {
    /// Same delay before every retry.
    Fixed,
    /// `initial_ms * multiplier^attempt`, capped at `max_ms`.
    #[default]
    Exponential,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RetryBackoff {
    #[serde(default)]
    pub kind: RetryBackoffKind,
    #[serde(default = "default_initial_ms")]
    pub initial_ms: u64,
    #[serde(default = "default_max_ms")]
    pub max_ms: u64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self {
            kind: RetryBackoffKind::Exponential,
            initial_ms: default_initial_ms(),
            max_ms: default_max_ms(),
            multiplier: default_multiplier(),
        }
    }
}

impl RetryBackoff {
    /// Delay before retry `attempt` (1-based: first retry after failure #1).
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        match self.kind {
            RetryBackoffKind::Fixed => self.initial_ms,
            RetryBackoffKind::Exponential => {
                let pow = attempt.min(16);
                let factor = self.multiplier.powi(pow as i32);
                ((self.initial_ms as f64) * factor)
                    .min(self.max_ms as f64)
                    .round() as u64
            }
        }
    }
}

fn default_max_retries() -> u32 {
    0
}

/// Broker-wide defaults (bettermq.json `dispatch` section).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RetryDefaults {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub backoff: RetryBackoff,
}

impl Default for RetryDefaults {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff: RetryBackoff::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_backoff_is_constant() {
        let b = RetryBackoff {
            kind: RetryBackoffKind::Fixed,
            initial_ms: 2_000,
            ..RetryBackoff::default()
        };
        assert_eq!(b.delay_ms(1), 2_000);
        assert_eq!(b.delay_ms(5), 2_000);
    }

    #[test]
    fn exponential_backoff_caps() {
        let b = RetryBackoff::default();
        assert_eq!(b.delay_ms(1), 1_000);
        assert!(b.delay_ms(20) <= b.max_ms);
    }
}
