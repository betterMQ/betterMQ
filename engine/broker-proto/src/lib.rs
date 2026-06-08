//! Wire format and shared domain types for BetterMQ.

pub mod record;
pub mod retry;

pub const PROTOCOL_VERSION: u32 = 1;
pub const RECORD_MAGIC: [u8; 4] = *b"SBK1";

pub use record::{LogRecord, StoredMessage};
pub use retry::{RetryBackoff, RetryBackoffKind, RetryDefaults};
