//! Password-protected API token for standalone (non–control-plane) brokers.
//! Token plaintext is returned once at setup/regenerate; only hashes are stored.

use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

const FILE_NAME: &str = "local-auth.json";
const TOKEN_PREFIX: &str = "sk_local_";

#[derive(Debug, Error)]
pub enum LocalAuthError {
    #[error("local auth already configured")]
    AlreadyConfigured,
    #[error("local auth not configured")]
    NotConfigured,
    #[error("invalid password")]
    InvalidPassword,
    #[error("invalid API token")]
    InvalidToken,
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("password hashing failed")]
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stored {
    password_hash: String,
    api_key_hash: String,
}

/// Password + API token hashes for cluster sync (never includes plaintext token).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthCredentials {
    pub password_hash: String,
    pub api_key_hash: String,
}

pub struct LocalAuthStore {
    path: PathBuf,
}

impl LocalAuthStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, LocalAuthError> {
        let path = std::env::var("BETTERMQ_LOCAL_AUTH_FILE")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.as_ref().join(FILE_NAME));
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(Self { path })
    }

    pub fn is_configured(&self) -> bool {
        self.path.is_file()
    }

    pub fn setup(&self, password: &str) -> Result<String, LocalAuthError> {
        if self.is_configured() {
            return Err(LocalAuthError::AlreadyConfigured);
        }
        validate_password(password)?;
        let token = generate_token();
        let stored = Stored {
            password_hash: hash_password(password)?,
            api_key_hash: hash_token(&token),
        };
        write_atomic(&self.path, &stored)?;
        Ok(token)
    }

    pub fn regenerate(&self, password: &str) -> Result<String, LocalAuthError> {
        let stored = self.load()?;
        if !verify_password(password, &stored.password_hash)? {
            return Err(LocalAuthError::InvalidPassword);
        }
        let token = generate_token();
        let updated = Stored {
            password_hash: stored.password_hash,
            api_key_hash: hash_token(&token),
        };
        write_atomic(&self.path, &updated)?;
        Ok(token)
    }

    pub fn verify_token(&self, token: &str) -> Result<bool, LocalAuthError> {
        if !self.is_configured() {
            return Ok(false);
        }
        let stored = self.load()?;
        Ok(constant_time_eq(&hash_token(token), &stored.api_key_hash))
    }

    pub fn export_credentials(&self) -> Result<Option<AuthCredentials>, LocalAuthError> {
        if !self.is_configured() {
            return Ok(None);
        }
        let stored = self.load()?;
        Ok(Some(AuthCredentials {
            password_hash: stored.password_hash,
            api_key_hash: stored.api_key_hash,
        }))
    }

    /// Apply credentials from cluster seed (same API token works on every node).
    pub fn apply_credentials(&self, creds: &AuthCredentials) -> Result<(), LocalAuthError> {
        write_atomic(
            &self.path,
            &Stored {
                password_hash: creds.password_hash.clone(),
                api_key_hash: creds.api_key_hash.clone(),
            },
        )
    }

    fn load(&self) -> Result<Stored, LocalAuthError> {
        if !self.is_configured() {
            return Err(LocalAuthError::NotConfigured);
        }
        let raw = fs::read_to_string(&self.path)?;
        Ok(serde_json::from_str(&raw)?)
    }
}

fn validate_password(password: &str) -> Result<(), LocalAuthError> {
    if password.len() < 8 {
        return Err(LocalAuthError::InvalidPassword);
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String, LocalAuthError> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|_| LocalAuthError::Hash)
}

fn verify_password(password: &str, encoded: &str) -> Result<bool, LocalAuthError> {
    let parsed = PasswordHash::new(encoded).map_err(|_| LocalAuthError::Hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{TOKEN_PREFIX}{}", hex::encode(bytes))
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(digest)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn write_atomic(path: &Path, stored: &Stored) -> Result<(), LocalAuthError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(stored)?;
    fs::write(&tmp, json)?;
    fs::rename(tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn setup_and_verify() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let token = store.setup("password123").unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));
        assert!(store.verify_token(&token).unwrap());
        assert!(!store.verify_token("sk_local_deadbeef").unwrap());
    }

    #[test]
    fn regenerate_rotates_token() {
        let dir = tempdir().unwrap();
        let store = LocalAuthStore::open(dir.path()).unwrap();
        let t1 = store.setup("password123").unwrap();
        let t2 = store.regenerate("password123").unwrap();
        assert_ne!(t1, t2);
        assert!(!store.verify_token(&t1).unwrap());
        assert!(store.verify_token(&t2).unwrap());
    }
}
