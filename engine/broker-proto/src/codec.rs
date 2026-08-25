//! Bincode-compatible codec via maintained `bincode-next`, using the legacy
//! config so on-disk frames stay compatible with historical bincode 1 frames.

use serde::{de::DeserializeOwned, Serialize};

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode_next::error::EncodeError> {
    bincode_next::serde::encode_to_vec(value, bincode_next::config::legacy())
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode_next::error::DecodeError> {
    bincode_next::serde::decode_from_slice(bytes, bincode_next::config::legacy())
        .map(|(value, _)| value)
}
