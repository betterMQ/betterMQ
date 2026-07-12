//! Destination URL egress policy (SSRF controls).
//!
//! Blocks loopback, link-local (incl. cloud metadata), and private RFC1918/ULA
//! addresses unless `BETTERMQ_ALLOW_PRIVATE_DESTINATIONS=1`.

use std::net::IpAddr;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum EgressError {
    #[error("invalid destination URL")]
    InvalidUrl,
    #[error("destination URL must be http or https")]
    BadScheme,
    #[error("destination URL missing host")]
    MissingHost,
    #[error("destination host is not allowed")]
    HostNotAllowed,
}

/// Validate a webhook / queue destination before enqueue or delivery.
pub fn validate_destination_url(raw: &str) -> Result<(), EgressError> {
    let parsed = Url::parse(raw.trim()).map_err(|_| EgressError::InvalidUrl)?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err(EgressError::BadScheme),
    }
    let host = parsed
        .host_str()
        .ok_or(EgressError::MissingHost)?
        .to_ascii_lowercase();

    if is_blocked_hostname(&host) {
        return Err(EgressError::HostNotAllowed);
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_blocked_destination_ip(ip) {
            return Err(EgressError::HostNotAllowed);
        }
    }

    Ok(())
}

fn allow_private_destinations() -> bool {
    matches!(
        std::env::var("BETTERMQ_ALLOW_PRIVATE_DESTINATIONS")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

fn is_blocked_hostname(host: &str) -> bool {
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    if host == "metadata.google.internal"
        || host == "metadata"
        || host.ends_with(".metadata.google.internal")
    {
        return true;
    }
    false
}

fn is_blocked_destination_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            if v4.is_loopback() || v4.is_link_local() || v4.is_unspecified() {
                return true;
            }
            if !allow_private_destinations() && (v4.is_private() || v4.is_broadcast()) {
                return true;
            }
            // Carrier-grade NAT / documentation / benchmarking ranges often used in SSRF.
            let octets = v4.octets();
            if !allow_private_destinations()
                && (octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000 // 100.64/10
                    || octets[0] == 192 && octets[1] == 0 && octets[2] == 0
                    || octets[0] == 192 && octets[1] == 0 && octets[2] == 2
                    || octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)
                    || octets[0] == 198 && octets[1] == 51 && octets[2] == 100
                    || octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
            {
                return true;
            }
            false
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            let segments = v6.segments();
            // link-local fe80::/10
            if (segments[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // unique local fc00::/7
            if !allow_private_destinations() && (segments[0] & 0xfe00) == 0xfc00 {
                return true;
            }
            // IPv4-mapped
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_blocked_destination_ip(IpAddr::V4(v4));
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_public_https() {
        assert!(validate_destination_url("https://example.com/hook").is_ok());
    }

    #[test]
    fn blocks_metadata_and_loopback() {
        assert!(validate_destination_url("http://169.254.169.254/latest").is_err());
        assert!(validate_destination_url("http://127.0.0.1/").is_err());
        assert!(validate_destination_url("http://localhost/hook").is_err());
        assert!(validate_destination_url("http://metadata.google.internal/").is_err());
    }

    #[test]
    fn blocks_private_by_default() {
        assert!(validate_destination_url("http://10.0.0.5/hook").is_err());
        assert!(validate_destination_url("http://192.168.1.1/hook").is_err());
    }
}
