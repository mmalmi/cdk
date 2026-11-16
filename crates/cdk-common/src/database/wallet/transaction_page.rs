//! Bounds and continuation for indexed transaction reads.

use serde::{Deserialize, Serialize};

use crate::wallet::TransactionId;

/// Maximum number of stored transactions selected by a page.
pub const MAX_TRANSACTION_PAGE_SIZE: u16 = 128;

/// Maximum aggregate stored payload bytes returned by a transaction page.
///
/// Conservatively counts byte lengths of every SQL field (including the text
/// representation of numbers), plus 24 bytes for the three decoded integers.
/// This bounds data passed to row decoding, not database-engine working memory
/// or the heap overhead of decoded Rust values.
pub const MAX_TRANSACTION_PAGE_BYTES: u64 = 4 * 1024 * 1024;

/// Exclusive continuation in ascending timestamp, then binary transaction-ID order.
///
/// The cursor row need not still exist. Use the same optional filters on
/// every page; a cursor does not bind that scope or authorize history deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCursor {
    /// Stored Unix timestamp; values above `i64::MAX` are invalid.
    pub timestamp: u64,
    /// Canonical transaction identifier used as the binary ordering key.
    pub id: TransactionId,
}
