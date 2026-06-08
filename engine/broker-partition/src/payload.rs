//! Publish payload: accepts a JSON string or any JSON value (object/array/number).

use serde::de::{self, Deserializer};
use serde::Deserialize;
use serde_json::Value;

/// Deserialize `payload` as either a JSON string or any JSON value (stored as UTF-8 JSON text).
pub fn deserialize_flexible_payload<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(s) => Ok(s),
        other => serde_json::to_string(&other).map_err(de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Req {
        #[serde(deserialize_with = "deserialize_flexible_payload")]
        payload: String,
    }

    #[test]
    fn string_payload() {
        let r: Req = serde_json::from_str(r#"{"payload":"hello"}"#).unwrap();
        assert_eq!(r.payload, "hello");
    }

    #[test]
    fn object_payload() {
        let r: Req = serde_json::from_str(r#"{"payload":{"hi":{"nested":"ok"}}}"#).unwrap();
        assert!(r.payload.contains("nested"));
    }
}
