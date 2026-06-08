//! Message priority (0 = lowest, 9 = highest) and flow-control helpers.

pub const DEFAULT_PRIORITY: u8 = 5;
pub const MIN_PRIORITY: u8 = 0;
pub const MAX_PRIORITY: u8 = 9;

pub fn clamp_priority(p: u8) -> u8 {
    p.min(MAX_PRIORITY)
}

pub fn normalize_priority(p: Option<u8>) -> u8 {
    clamp_priority(p.unwrap_or(DEFAULT_PRIORITY))
}
