//! Path-segment sanitization so tenant/topic/blob keys cannot escape data dirs.

use std::path::{Component, Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PathSegmentError {
    #[error("path segment is empty")]
    Empty,
    #[error("path segment is reserved (`.` or `..`)")]
    Reserved,
    #[error("path segment contains a separator or NUL")]
    Separator,
    #[error("path segment contains a control character")]
    InvalidChar,
    #[error("path segment is too long")]
    TooLong,
    #[error("path escapes storage root")]
    EscapesRoot,
}

/// Reject empty, `.`, `..`, separators, NUL, and control characters.
pub fn sanitize_path_segment(s: &str) -> Result<&str, PathSegmentError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(PathSegmentError::Empty);
    }
    if t == "." || t == ".." {
        return Err(PathSegmentError::Reserved);
    }
    if t.contains('\0') || t.contains('/') || t.contains('\\') || t.contains(':') {
        return Err(PathSegmentError::Separator);
    }
    if t.chars().any(|c| c.is_control()) {
        return Err(PathSegmentError::InvalidChar);
    }
    Ok(t)
}

const MAX_NAME_LEN: usize = 200;

/// User-created queue names: path-safe, not reserved internal topics.
pub fn validate_queue_name(s: &str) -> Result<&str, PathSegmentError> {
    let t = sanitize_path_segment(s)?;
    if t.len() > MAX_NAME_LEN {
        return Err(PathSegmentError::TooLong);
    }
    if t == "__direct" || t.ends_with(".__dlq") || t.contains(".__dlq.") {
        return Err(PathSegmentError::Reserved);
    }
    Ok(t)
}

/// Flow-control keys: path-safe, bounded length.
pub fn validate_flow_key(s: &str) -> Result<&str, PathSegmentError> {
    let t = sanitize_path_segment(s)?;
    if t.len() > MAX_NAME_LEN {
        return Err(PathSegmentError::TooLong);
    }
    Ok(t)
}

/// Join `segments` under `root`, rejecting any segment that would escape.
pub fn join_under_root(root: &Path, segments: &[&str]) -> Result<PathBuf, PathSegmentError> {
    let mut out = root.to_path_buf();
    for seg in segments {
        let clean = sanitize_path_segment(seg)?;
        out.push(clean);
    }
    if !path_is_under(root, &out) {
        return Err(PathSegmentError::EscapesRoot);
    }
    Ok(out)
}

fn path_is_under(root: &Path, candidate: &Path) -> bool {
    let mut root_comps = root.components();
    let mut cand_comps = candidate.components();
    loop {
        match (root_comps.next(), cand_comps.next()) {
            (None, _) => return true,
            (Some(_), None) => return false,
            (Some(a), Some(b)) if a == b => {}
            (Some(Component::CurDir), Some(b)) => {
                // treat extra `.` on candidate as skip — already sanitized
                let _ = b;
                return false;
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn rejects_traversal() {
        assert!(sanitize_path_segment("..").is_err());
        assert!(sanitize_path_segment("../etc").is_err());
        assert!(sanitize_path_segment("foo/bar").is_err());
        assert!(sanitize_path_segment("foo\\bar").is_err());
        assert!(sanitize_path_segment("").is_err());
        assert!(sanitize_path_segment("ok-topic.__dlq").is_ok());
        assert!(sanitize_path_segment("__direct").is_ok());
        assert!(validate_queue_name("__direct").is_err());
        assert!(validate_queue_name("jobs.__dlq").is_err());
        assert!(validate_queue_name("orders").is_ok());
        assert!(validate_flow_key("user:1").is_err());
        assert!(validate_flow_key("user-1").is_ok());
    }

    #[test]
    fn join_stays_under_root() {
        let root = Path::new("/data/blobs");
        let p = join_under_root(root, &["payloads", "default", "id"]).unwrap();
        assert!(p.starts_with(root));
        assert!(join_under_root(root, &[".."]).is_err());
    }
}
