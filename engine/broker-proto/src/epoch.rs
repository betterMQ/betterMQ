//! Versioned commit-epoch framing for the Core V2 WAL.

use std::io::{self, Read, Write};
use thiserror::Error;

pub const EPOCH_MAGIC: [u8; 4] = *b"BQE2";
pub const EPOCH_FORMAT_V2: u16 = 2;
pub const EPOCH_HEADER_BYTES: usize = 52;
pub const MAX_EPOCH_BODY_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochHeader {
    pub format_version: u16,
    pub flags: u16,
    pub shard_id: u32,
    pub leader_epoch: u64,
    pub first_offset: u64,
    pub record_count: u32,
    pub byte_len: u64,
    /// Exclusive offset covered by this complete epoch.
    pub committed_hwm: u64,
    pub body_crc: u32,
}

impl EpochHeader {
    pub fn v2(
        shard_id: u32,
        leader_epoch: u64,
        first_offset: u64,
        record_count: u32,
        body: &[u8],
    ) -> Self {
        Self {
            format_version: EPOCH_FORMAT_V2,
            flags: 0,
            shard_id,
            leader_epoch,
            first_offset,
            record_count,
            byte_len: body.len() as u64,
            committed_hwm: first_offset.saturating_add(record_count as u64),
            body_crc: crc32fast::hash(body),
        }
    }
}

#[derive(Debug, Error)]
pub enum EpochError {
    #[error("invalid epoch magic")]
    InvalidMagic,
    #[error("unsupported epoch format version {0}")]
    UnsupportedVersion(u16),
    #[error("epoch body too large: {0} bytes")]
    BodyTooLarge(u64),
    #[error("epoch checksum mismatch")]
    ChecksumMismatch,
    #[error("invalid epoch offsets")]
    InvalidOffsets,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub fn encode_epoch(
    header: &EpochHeader,
    body: &[u8],
    writer: &mut impl Write,
) -> Result<(), EpochError> {
    if header.format_version != EPOCH_FORMAT_V2 {
        return Err(EpochError::UnsupportedVersion(header.format_version));
    }
    if header.byte_len != body.len() as u64 || header.byte_len > MAX_EPOCH_BODY_BYTES as u64 {
        return Err(EpochError::BodyTooLarge(header.byte_len));
    }
    if header.committed_hwm
        != header
            .first_offset
            .saturating_add(header.record_count as u64)
    {
        return Err(EpochError::InvalidOffsets);
    }
    writer.write_all(&EPOCH_MAGIC)?;
    writer.write_all(&header.format_version.to_be_bytes())?;
    writer.write_all(&header.flags.to_be_bytes())?;
    writer.write_all(&header.shard_id.to_be_bytes())?;
    writer.write_all(&header.leader_epoch.to_be_bytes())?;
    writer.write_all(&header.first_offset.to_be_bytes())?;
    writer.write_all(&header.record_count.to_be_bytes())?;
    writer.write_all(&header.byte_len.to_be_bytes())?;
    writer.write_all(&header.committed_hwm.to_be_bytes())?;
    writer.write_all(&header.body_crc.to_be_bytes())?;
    writer.write_all(body)?;
    Ok(())
}

pub fn decode_epoch(mut reader: impl Read) -> Result<(EpochHeader, Vec<u8>), EpochError> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != EPOCH_MAGIC {
        return Err(EpochError::InvalidMagic);
    }

    let format_version = read_u16(&mut reader)?;
    if format_version != EPOCH_FORMAT_V2 {
        return Err(EpochError::UnsupportedVersion(format_version));
    }
    let header = EpochHeader {
        format_version,
        flags: read_u16(&mut reader)?,
        shard_id: read_u32(&mut reader)?,
        leader_epoch: read_u64(&mut reader)?,
        first_offset: read_u64(&mut reader)?,
        record_count: read_u32(&mut reader)?,
        byte_len: read_u64(&mut reader)?,
        committed_hwm: read_u64(&mut reader)?,
        body_crc: read_u32(&mut reader)?,
    };
    if header.byte_len > MAX_EPOCH_BODY_BYTES as u64 {
        return Err(EpochError::BodyTooLarge(header.byte_len));
    }
    if header.committed_hwm
        != header
            .first_offset
            .saturating_add(header.record_count as u64)
    {
        return Err(EpochError::InvalidOffsets);
    }
    let mut body = vec![0u8; header.byte_len as usize];
    reader.read_exact(&mut body)?;
    if crc32fast::hash(&body) != header.body_crc {
        return Err(EpochError::ChecksumMismatch);
    }
    Ok((header, body))
}

fn read_u16(reader: &mut impl Read) -> Result<u16, io::Error> {
    let mut bytes = [0; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32, io::Error> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, io::Error> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_roundtrip_and_checksum() {
        let body = b"two encoded records";
        let header = EpochHeader::v2(7, 11, 42, 2, body);
        let mut bytes = Vec::new();
        encode_epoch(&header, body, &mut bytes).unwrap();
        assert_eq!(bytes.len(), EPOCH_HEADER_BYTES + body.len());

        let (decoded, decoded_body) = decode_epoch(bytes.as_slice()).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn epoch_rejects_corrupt_body() {
        let body = b"payload";
        let header = EpochHeader::v2(1, 0, 0, 1, body);
        let mut bytes = Vec::new();
        encode_epoch(&header, body, &mut bytes).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            decode_epoch(bytes.as_slice()),
            Err(EpochError::ChecksumMismatch)
        ));
    }
}
