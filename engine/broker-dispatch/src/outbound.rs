//! Build outbound HTTP requests from stored delivery specs.

use broker_partition::HttpDeliverySpec;
use broker_storage::StoredMessage;
use bytes::Bytes;
use chrono::Utc;
use reqwest::Method;
use std::str::FromStr;

use crate::hmac_sig::sign_payload;

#[derive(Clone)]
pub struct OutboundRequest {
    pub method: Method,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

pub fn build_outbound(msg: &StoredMessage) -> OutboundRequest {
    let spec = HttpDeliverySpec::from_stored(
        msg.http_method.as_deref(),
        msg.http_headers_json.as_deref(),
        msg.http_sign,
    );
    build_outbound_with_spec(msg, &spec)
}

pub fn build_outbound_with_spec(msg: &StoredMessage, spec: &HttpDeliverySpec) -> OutboundRequest {
    let method = parse_method(&spec.method);
    let body = Bytes::copy_from_slice(&msg.payload);
    let mut headers: Vec<(String, String)> = spec
        .headers
        .iter()
        .filter(|(k, _)| {
            broker_partition::http_delivery::is_valid_header_name(k)
                && !broker_partition::http_delivery::is_denied_outbound_header(k)
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    if spec.sign {
        if let Some(secret) = msg.destination_secret.as_deref().filter(|s| !s.is_empty()) {
            let timestamp_ms = Utc::now().timestamp_millis();
            let signature = sign_payload(secret, &body, timestamp_ms);
            headers.push(("BetterMQ-Signature".into(), signature));
            headers.push(("BetterMQ-Timestamp".into(), timestamp_ms.to_string()));
        }
    }

    OutboundRequest {
        method,
        headers,
        body,
    }
}

fn parse_method(s: &str) -> Method {
    let m = s.trim().to_ascii_uppercase();
    if !broker_partition::http_delivery::is_allowed_http_method(&m) {
        return Method::POST;
    }
    Method::from_str(&m).unwrap_or(Method::POST)
}

pub fn apply_to_reqwest(
    client: &reqwest::Client,
    url: &str,
    outbound: OutboundRequest,
) -> reqwest::RequestBuilder {
    let mut req = client.request(outbound.method.clone(), url);
    for (name, value) in outbound.headers {
        req = req.header(name, value);
    }
    if outbound.method == Method::GET || outbound.method == Method::HEAD {
        req
    } else {
        req.body(outbound.body)
    }
}
