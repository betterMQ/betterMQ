//! Shared fail-closed metadata recovery flag.

/// When `BETTERMQ_METADATA_RECOVER=empty`, corrupt JSON may be replaced with defaults.
pub fn allow_empty_metadata_recovery() -> bool {
    std::env::var("BETTERMQ_METADATA_RECOVER").is_ok_and(|v| v.eq_ignore_ascii_case("empty"))
}
