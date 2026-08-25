//! Batch enqueue API — canonical ingest batch (delay_ms preserved).

use crate::ingest::{IngestBatch, MAX_BATCH_BYTES, MAX_BATCH_MESSAGES, MAX_HT_BATCH_MESSAGES};
use crate::routes::ApiError;
use crate::AppState;
use axum::{
    body::Body,
    extract::{Extension, Request, State},
    http::{header::CONTENT_ENCODING, HeaderMap, StatusCode},
    Json,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct BatchEnqueueRequest {
    pub messages: Vec<broker_partition::PublishRequest>,
}

#[derive(Debug, serde::Serialize)]
pub struct BatchEnqueueResponse {
    pub accepted: usize,
    pub failed: usize,
    /// True when independently committed records produced mixed outcomes.
    pub partial: bool,
    pub message_ids: Vec<uuid::Uuid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<crate::ingest::IngestItemResult>,
}

#[cfg(feature = "cloud")]
const CLOUD_MAX_BATCH_MESSAGES: usize = MAX_BATCH_MESSAGES;

async fn enqueue_batch_inner(
    state: Arc<AppState>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    body: BatchEnqueueRequest,
    max_messages: usize,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    if body.messages.len() > max_messages {
        return Err(ApiError::BadRequest(format!(
            "batch exceeds max size of {max_messages}"
        )));
    }
    if state.uses_cloud_auth() {
        #[cfg(feature = "cloud")]
        {
            if body.messages.len() > CLOUD_MAX_BATCH_MESSAGES && max_messages <= MAX_BATCH_MESSAGES
            {
                return Err(ApiError::BadRequest(format!(
                    "batch exceeds max size of {CLOUD_MAX_BATCH_MESSAGES}"
                )));
            }
            if let (Some(auth), Some(plan)) =
                (ingest.as_ref().map(|e| e.0), plan.as_ref().map(|e| &e.0))
            {
                crate::metering::check_cloud_batch_messages_cap(
                    &state,
                    auth.tenant_id,
                    plan,
                    body.messages.len(),
                )
                .await?;
                for req in &body.messages {
                    if req.payload.len() as u64 > plan.max_message_bytes {
                        return Err(ApiError::BadRequest(format!(
                            "message exceeds plan limit of {} bytes",
                            plan.max_message_bytes
                        )));
                    }
                }
            }
        }
    }

    let batch = IngestBatch::from_requests(body.messages)?;
    let responses = crate::ingest::submit_batch(&state, ingest.as_ref().map(|e| e.0), batch).await;
    let mut ids = Vec::new();
    let mut results = Vec::new();
    let mut failed = 0usize;
    for outcome in responses {
        match outcome.result {
            Ok(resp) => {
                if let Some(message_id) = resp.message_id {
                    ids.push(message_id);
                }
                if resp.scheduled.is_none() && !resp.duplicate {
                    crate::ingest::notify_dispatch(&state, &resp);
                    crate::metrics::record_accepted();
                } else if resp.duplicate {
                    crate::metrics::record_duplicate();
                }
                results.push(crate::ingest::IngestItemResult {
                    index: outcome.index,
                    accepted: true,
                    message_id: resp.message_id,
                    topic: resp.topic.clone(),
                    partition: resp.partition,
                    offset: resp.offset,
                    duplicate: resp.duplicate,
                    commit_epoch: resp.commit_epoch,
                    scheduled: resp.scheduled.clone(),
                    error: None,
                });
            }
            Err(error) => {
                failed += 1;
                results.push(crate::ingest::IngestItemResult {
                    index: outcome.index,
                    accepted: false,
                    message_id: None,
                    topic: outcome.topic,
                    partition: None,
                    offset: None,
                    duplicate: false,
                    commit_epoch: None,
                    scheduled: None,
                    error: Some(format!("{error:?}")),
                });
            }
        }
    }
    results.sort_by_key(|r| r.index);
    let accepted = results.len().saturating_sub(failed);
    let partial = accepted > 0 && failed > 0;
    let status = if failed > 0 {
        StatusCode::MULTI_STATUS
    } else {
        StatusCode::ACCEPTED
    };
    Ok((
        status,
        Json(BatchEnqueueResponse {
            accepted,
            failed,
            partial,
            message_ids: ids,
            results,
        }),
    ))
}

pub async fn batch_enqueue(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(body): Json<BatchEnqueueRequest>,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    #[cfg(feature = "cloud")]
    return enqueue_batch_inner(state, ingest, plan, body, MAX_BATCH_MESSAGES).await;
    #[cfg(not(feature = "cloud"))]
    enqueue_batch_inner(state, ingest, body, MAX_BATCH_MESSAGES).await
}

/// High-throughput batch (up to 1000 records) after the compatibility 100-record cap.
pub async fn batch_enqueue_ht(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    Json(body): Json<BatchEnqueueRequest>,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    #[cfg(feature = "cloud")]
    return enqueue_batch_inner(state, ingest, plan, body, MAX_HT_BATCH_MESSAGES).await;
    #[cfg(not(feature = "cloud"))]
    enqueue_batch_inner(state, ingest, body, MAX_HT_BATCH_MESSAGES).await
}

pub(crate) fn parse_ndjson_record(
    line: &[u8],
    line_number: usize,
) -> Result<broker_partition::PublishRequest, ApiError> {
    if let Ok(request) = serde_json::from_slice(line) {
        return Ok(request);
    }
    let mut value = serde_json::from_slice::<serde_json::Value>(line).map_err(|error| {
        ApiError::BadRequest(format!(
            "invalid NDJSON record at line {line_number}: {error}"
        ))
    })?;
    if let Some(object) = value.as_object_mut() {
        if !object.contains_key("payload") {
            if let Some(body) = object.remove("body") {
                object.insert("payload".into(), body);
            }
        }
    }
    serde_json::from_value(value).map_err(|error| {
        ApiError::BadRequest(format!(
            "invalid NDJSON record at line {line_number}: {error}"
        ))
    })
}

#[cfg(test)]
fn parse_ndjson(body: &[u8]) -> Result<BatchEnqueueRequest, ApiError> {
    if body.len() > MAX_BATCH_BYTES {
        return Err(ApiError::BadRequest(format!(
            "NDJSON batch exceeds max bytes of {MAX_BATCH_BYTES}"
        )));
    }
    let mut messages = Vec::new();
    for (index, line) in body.split(|b| *b == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if messages.len() >= MAX_HT_BATCH_MESSAGES {
            return Err(ApiError::BadRequest(format!(
                "NDJSON batch exceeds max size of {MAX_HT_BATCH_MESSAGES}"
            )));
        }
        messages.push(parse_ndjson_record(line, index + 1)?);
    }
    Ok(BatchEnqueueRequest { messages })
}

pub(crate) async fn parse_ndjson_stream(body: Body) -> Result<BatchEnqueueRequest, ApiError> {
    let mut stream = body.into_data_stream();
    let mut pending = Vec::new();
    let mut messages = Vec::new();
    let mut total = 0usize;
    let mut line_number = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| ApiError::BadRequest(format!("read NDJSON body: {error}")))?;
        total = total.saturating_add(chunk.len());
        if total > MAX_BATCH_BYTES {
            return Err(ApiError::BadRequest(format!(
                "NDJSON batch exceeds max bytes of {MAX_BATCH_BYTES}"
            )));
        }
        pending.extend_from_slice(&chunk);
        let mut consumed = 0usize;
        while let Some(relative) = pending[consumed..].iter().position(|byte| *byte == b'\n') {
            let end = consumed + relative;
            line_number += 1;
            let line = pending[consumed..end]
                .strip_suffix(b"\r")
                .unwrap_or(&pending[consumed..end]);
            if !line.iter().all(u8::is_ascii_whitespace) {
                if messages.len() >= MAX_HT_BATCH_MESSAGES {
                    return Err(ApiError::BadRequest(format!(
                        "NDJSON batch exceeds max size of {MAX_HT_BATCH_MESSAGES}"
                    )));
                }
                messages.push(parse_ndjson_record(line, line_number)?);
            }
            consumed = end + 1;
        }
        if consumed > 0 {
            pending.drain(..consumed);
        }
    }
    if !pending.iter().all(u8::is_ascii_whitespace) {
        line_number += 1;
        if messages.len() >= MAX_HT_BATCH_MESSAGES {
            return Err(ApiError::BadRequest(format!(
                "NDJSON batch exceeds max size of {MAX_HT_BATCH_MESSAGES}"
            )));
        }
        messages.push(parse_ndjson_record(
            pending.strip_suffix(b"\r").unwrap_or(&pending),
            line_number,
        )?);
    }
    if messages.is_empty() {
        return Err(ApiError::BadRequest(
            "batch must contain at least one message".into(),
        ));
    }
    Ok(BatchEnqueueRequest { messages })
}

fn validate_ndjson_content_encoding(headers: &HeaderMap) -> Result<(), ApiError> {
    let encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .unwrap_or("identity");
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(());
    }
    Err(ApiError::BadRequest(
        "compressed NDJSON is not accepted; send identity encoding so the 8 MiB limit applies to the decoded body".into(),
    ))
}

/// Compatibility-safe high-throughput ingest: one PublishRequest JSON object per line.
pub async fn batch_enqueue_ndjson(
    State(state): State<Arc<AppState>>,
    ingest: Option<Extension<crate::metering::IngestAuth>>,
    #[cfg(feature = "cloud")] plan: Option<Extension<broker_control_plane::PlanLimits>>,
    request: Request,
) -> Result<(StatusCode, Json<BatchEnqueueResponse>), ApiError> {
    validate_ndjson_content_encoding(request.headers())?;
    let body = parse_ndjson_stream(request.into_body()).await?;
    #[cfg(feature = "cloud")]
    return enqueue_batch_inner(state, ingest, plan, body, MAX_HT_BATCH_MESSAGES).await;
    #[cfg(not(feature = "cloud"))]
    enqueue_batch_inner(state, ingest, body, MAX_HT_BATCH_MESSAGES).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_accepts_crlf_and_blank_lines() {
        let body = br#"
{"routing_key":"a","payload":"one","url":"https://example.com/a","secret":"s"}

{"routing_key":"b","payload":"two","url":"https://example.com/b","secret":"s"}
"#;
        let parsed = parse_ndjson(body).unwrap();
        assert_eq!(parsed.messages.len(), 2);
        assert_eq!(parsed.messages[0].routing_key, "a");
    }

    #[test]
    fn ndjson_reports_the_bad_line() {
        let error =
            parse_ndjson(b"{\"payload\":\"ok\"}\nnot-json\n{\"payload\":\"ok\"}").unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(message) if message.contains("line 2")));
    }

    #[test]
    fn ndjson_rejects_compression_before_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        assert!(matches!(
            validate_ndjson_content_encoding(&headers),
            Err(ApiError::BadRequest(message)) if message.contains("not accepted")
        ));
    }

    #[tokio::test]
    async fn ndjson_stream_enforces_decoded_size_limit() {
        let body = Body::from(vec![b'x'; MAX_BATCH_BYTES + 1]);
        assert!(matches!(
            parse_ndjson_stream(body).await,
            Err(ApiError::BadRequest(message)) if message.contains("max bytes")
        ));
    }
}
