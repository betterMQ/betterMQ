//! Outbound HTTP delivery spec (curl-style): method, headers, raw body, optional HMAC sign.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HttpDeliverySpec {
    #[serde(default = "default_http_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// When true, add `BetterMQ-Signature` using the queue/destination secret.
    #[serde(default)]
    pub sign: bool,
}

fn default_http_method() -> String {
    "POST".into()
}

impl HttpDeliverySpec {
    pub fn merge(
        method: Option<String>,
        headers: Option<HashMap<String, String>>,
        sign: Option<bool>,
        nested: Option<HttpDeliverySpec>,
    ) -> Self {
        let mut spec = nested.unwrap_or_default();
        if let Some(m) = method {
            if !m.trim().is_empty() {
                spec.method = m;
            }
        }
        if let Some(h) = headers {
            for (k, v) in h {
                spec.headers.insert(k, v);
            }
        }
        if let Some(s) = sign {
            spec.sign = s;
        }
        spec.normalize();
        spec
    }

    pub fn normalize(&mut self) {
        self.method = self.method.trim().to_uppercase();
        if self.method.is_empty() {
            self.method = default_http_method();
        }
    }

    pub fn headers_json(&self) -> Option<String> {
        if self.headers.is_empty() {
            None
        } else {
            serde_json::to_string(&self.headers).ok()
        }
    }

    pub fn from_stored(
        method: Option<&str>,
        headers_json: Option<&str>,
        sign: Option<bool>,
    ) -> Self {
        let headers = headers_json
            .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
            .unwrap_or_default();
        let mut spec = Self {
            method: method
                .map(|s| s.to_string())
                .unwrap_or_else(default_http_method),
            headers,
            sign: sign.unwrap_or(false),
        };
        spec.normalize();
        spec
    }
}

/// Optional curl-style overrides on publish/enqueue (nested `request` object).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HttpDeliveryInput {
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_headers")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
}

/// Accept `headers` as a JSON object or array of `[name, value]` pairs (curl -H style).
pub fn deserialize_optional_headers<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<Value> = Option::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(Some(
        parse_headers_value(&value).map_err(serde::de::Error::custom)?,
    ))
}

pub fn parse_headers_value(value: &Value) -> Result<HashMap<String, String>, String> {
    match value {
        Value::Object(map) => {
            let mut out = HashMap::new();
            for (k, v) in map {
                out.insert(k.clone(), header_value_to_string(v));
            }
            Ok(out)
        }
        Value::Array(items) => {
            let mut out = HashMap::new();
            for item in items {
                match item {
                    Value::Array(pair) if pair.len() >= 2 => {
                        let name = pair[0]
                            .as_str()
                            .ok_or_else(|| "header name must be a string".to_string())?;
                        out.insert(name.to_string(), header_value_to_string(&pair[1]));
                    }
                    Value::String(line) => {
                        let (name, val) = line
                            .split_once(':')
                            .ok_or_else(|| format!("invalid header line: {line}"))?;
                        out.insert(name.trim().to_string(), val.trim().to_string());
                    }
                    _ => {
                        return Err(
                            "headers array entries must be [name, value] or \"Name: value\"".into(),
                        )
                    }
                }
            }
            Ok(out)
        }
        _ => Err("headers must be a JSON object or array".into()),
    }
}

fn header_value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_object_headers() {
        let v: Value = serde_json::json!({"Authorization": "Bearer x"});
        let h = parse_headers_value(&v).unwrap();
        assert_eq!(h.get("Authorization").unwrap(), "Bearer x");
    }

    #[test]
    fn parses_array_headers() {
        let v: Value = serde_json::json!([["X-Custom", "1"], "Accept: application/json"]);
        let h = parse_headers_value(&v).unwrap();
        assert_eq!(h.len(), 2);
    }
}
