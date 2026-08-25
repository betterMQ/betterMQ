//! S3-compatible object store (MinIO dev, R2/S3 prod) for SlateDB.

use object_store::aws::AmazonS3Builder;
pub use object_store::ObjectStore;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3StoreError {
    #[error("object_store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("config: {0}")]
    Config(String),
}

#[derive(Debug, Clone)]
pub struct S3ConnectionConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

pub fn open_object_store_from_config(
    cfg: &S3ConnectionConfig,
) -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    if cfg.endpoint.trim().is_empty() {
        return Err(S3StoreError::Config("endpoint required".into()));
    }
    if cfg.bucket.trim().is_empty() {
        return Err(S3StoreError::Config("bucket required".into()));
    }
    let store = AmazonS3Builder::new()
        .with_endpoint(cfg.endpoint.trim())
        .with_bucket_name(cfg.bucket.trim())
        .with_access_key_id(cfg.access_key.trim())
        .with_secret_access_key(cfg.secret_key.trim())
        .with_region(cfg.region.trim())
        .with_allow_http(cfg.endpoint.trim().starts_with("http://"))
        .build()?;
    Ok(Arc::new(store))
}

pub async fn test_s3_connection(cfg: &S3ConnectionConfig) -> Result<(), S3StoreError> {
    use object_store::path::Path as ObjectPath;
    let store = open_object_store_from_config(cfg)?;
    store
        .head(&ObjectPath::from(".bettermq-health"))
        .await
        .map(|_| ())
        .or_else(|e| match e {
            object_store::Error::NotFound { .. } => Ok(()),
            other => Err(S3StoreError::ObjectStore(other)),
        })
}

pub fn open_object_store_from_env() -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let (endpoint, r2) = endpoint_from_env()?;
    let bucket = std::env::var("S3_BUCKET")
        .or_else(|_| std::env::var("R2_BUCKET"))
        .map_err(|_| S3StoreError::Config("S3_BUCKET or R2_BUCKET required".into()))?;
    open_bucket(endpoint, bucket, r2)
}

fn endpoint_from_env() -> Result<(String, bool), S3StoreError> {
    if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
        return Ok((endpoint, false));
    }
    std::env::var("R2_ENDPOINT")
        .map(|endpoint| (endpoint, true))
        .map_err(|_| S3StoreError::Config("S3_ENDPOINT or R2_ENDPOINT required".into()))
}

fn open_bucket(
    endpoint: String,
    bucket: String,
    r2: bool,
) -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let access_key = std::env::var("S3_ACCESS_KEY")
        .or_else(|_| std::env::var("R2_ACCESS_KEY"))
        .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID"))
        .map_err(|_| S3StoreError::Config("S3/R2 access key required".into()))?;
    let secret_key = std::env::var("S3_SECRET_KEY")
        .or_else(|_| std::env::var("R2_SECRET_KEY"))
        .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY"))
        .map_err(|_| S3StoreError::Config("S3/R2 secret key required".into()))?;
    let region = std::env::var("S3_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .unwrap_or_else(|_| {
            if r2 {
                "auto".into()
            } else {
                "us-east-1".into()
            }
        });
    let allow_http = endpoint.trim().starts_with("http://");

    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_allow_http(allow_http)
        .build()?;

    Ok(Arc::new(store))
}

/// Object store for large message bodies (`S3_PAYLOAD_BUCKET`, e.g. `bettermq-payloads`).
pub fn open_payload_object_store_from_env() -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let (endpoint, r2) = endpoint_from_env()?;
    let bucket = std::env::var("S3_PAYLOAD_BUCKET")
        .or_else(|_| std::env::var("R2_PAYLOAD_BUCKET"))
        .map_err(|_| S3StoreError::Config("S3_PAYLOAD_BUCKET required".into()))?;
    open_bucket(endpoint, bucket, r2)
}

/// Object store used by the asynchronous sealed-segment archive.
pub fn open_archive_object_store_from_env() -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let (endpoint, r2) = endpoint_from_env()?;
    let bucket = std::env::var("BETTERMQ_ARCHIVE_BUCKET")
        .or_else(|_| std::env::var("S3_ARCHIVE_BUCKET"))
        .or_else(|_| std::env::var("R2_ARCHIVE_BUCKET"))
        .map_err(|_| S3StoreError::Config("BETTERMQ_ARCHIVE_BUCKET required".into()))?;
    open_bucket(endpoint, bucket, r2)
}
