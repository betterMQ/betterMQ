//! S3-compatible object store (MinIO dev, R2/S3 prod) for SlateDB.

#[cfg(feature = "slate")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "slate")]
pub use object_store::ObjectStore;
#[cfg(feature = "slate")]
use std::sync::Arc;
#[cfg(feature = "slate")]
use thiserror::Error;

#[cfg(feature = "slate")]
#[derive(Debug, Error)]
pub enum S3StoreError {
    #[error("object_store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("config: {0}")]
    Config(String),
}

#[cfg(feature = "slate")]
#[derive(Debug, Clone)]
pub struct S3ConnectionConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

#[cfg(feature = "slate")]
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
        .with_allow_http(true)
        .build()?;
    Ok(Arc::new(store))
}

#[cfg(feature = "slate")]
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

#[cfg(feature = "slate")]
pub fn open_object_store_from_env() -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let endpoint = std::env::var("S3_ENDPOINT")
        .or_else(|_| std::env::var("R2_ENDPOINT"))
        .map_err(|_| S3StoreError::Config("S3_ENDPOINT or R2_ENDPOINT required".into()))?;
    let bucket = std::env::var("S3_BUCKET")
        .map_err(|_| S3StoreError::Config("S3_BUCKET required".into()))?;
    let access_key = std::env::var("S3_ACCESS_KEY")
        .or_else(|_| std::env::var("R2_ACCESS_KEY"))
        .map_err(|_| S3StoreError::Config("S3_ACCESS_KEY required".into()))?;
    let secret_key = std::env::var("S3_SECRET_KEY")
        .or_else(|_| std::env::var("R2_SECRET_KEY"))
        .map_err(|_| S3StoreError::Config("S3_SECRET_KEY required".into()))?;
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into());

    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_allow_http(true)
        .build()?;

    Ok(Arc::new(store))
}

/// Object store for large message bodies (`S3_PAYLOAD_BUCKET`, e.g. `bettermq-payloads`).
#[cfg(feature = "slate")]
pub fn open_payload_object_store_from_env() -> Result<Arc<dyn ObjectStore>, S3StoreError> {
    let endpoint = std::env::var("S3_ENDPOINT")
        .or_else(|_| std::env::var("R2_ENDPOINT"))
        .map_err(|_| S3StoreError::Config("S3_ENDPOINT or R2_ENDPOINT required".into()))?;
    let bucket = std::env::var("S3_PAYLOAD_BUCKET")
        .map_err(|_| S3StoreError::Config("S3_PAYLOAD_BUCKET required".into()))?;
    let access_key = std::env::var("S3_ACCESS_KEY")
        .or_else(|_| std::env::var("R2_ACCESS_KEY"))
        .map_err(|_| S3StoreError::Config("S3_ACCESS_KEY required".into()))?;
    let secret_key = std::env::var("S3_SECRET_KEY")
        .or_else(|_| std::env::var("R2_SECRET_KEY"))
        .map_err(|_| S3StoreError::Config("S3_SECRET_KEY required".into()))?;
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into());

    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_allow_http(true)
        .build()?;

    Ok(Arc::new(store))
}
