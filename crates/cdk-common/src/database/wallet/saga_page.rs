//! Bounded identity enumeration with one original recovery record at a time.
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Database;
use crate::database::Error;
use crate::wallet::WalletSaga;

/// Maximum number of operation identities returned by a page.
pub const MAX_SAGA_PAGE_SIZE: u16 = 128;

/// Exclusive continuation ordered by creation time, then canonical UUID.
///
/// The cursor row need not still exist. Pages are not a cross-query snapshot;
/// serialize writers when a complete, stable inventory is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SagaCursor {
    /// Stored Unix timestamp; values above `i64::MAX` are invalid.
    pub created_at: u64,
    /// Original operation identifier.
    #[serde(deserialize_with = "canonical_id")]
    pub id: Uuid,
}

impl From<&WalletSaga> for SagaCursor {
    fn from(saga: &WalletSaga) -> Self {
        Self {
            created_at: saga.created_at,
            id: saga.id,
        }
    }
}

/// Validate the shared page bounds before reading storage.
pub fn validate_saga_page_request(after: Option<SagaCursor>, limit: u16) -> Result<(), Error> {
    if limit == 0 || limit > MAX_SAGA_PAGE_SIZE {
        return Err(Error::Internal("Invalid saga page limit".to_owned()));
    }
    if after.is_some_and(|cursor| cursor.created_at > i64::MAX as u64) {
        return Err(Error::Internal("Invalid saga page timestamp".to_owned()));
    }
    Ok(())
}

/// Read unfinished operations without retaining a whole history of payloads.
///
/// At most one page of identities and one full saved operation are loaded.
/// The default loader does not truncate or limit individual admitted records.
/// Deletions between reads are allowed; changed identity or corrupt data fails
/// the scan. Earlier operations may already have been processed on error.
#[derive(Debug)]
pub struct IncompleteSagaReader<'a> {
    store: &'a (dyn Database<Error> + Send + Sync),
    after: Option<SagaCursor>,
    pending: VecDeque<SagaCursor>,
    finished: bool,
}

impl<'a> IncompleteSagaReader<'a> {
    /// Start at the oldest stored operation, across all mints and units.
    pub fn new(store: &'a (dyn Database<Error> + Send + Sync)) -> Self {
        Self {
            store,
            after: None,
            pending: VecDeque::new(),
            finished: false,
        }
    }

    /// Load the next original operation, skipping identities deleted meanwhile.
    pub async fn next(&mut self) -> Result<Option<WalletSaga>, Error> {
        let store = self.store;
        self.next_with(|id| async move { store.get_saga(&id).await })
            .await
    }

    /// Load the next original using a caller-selected record reader.
    ///
    /// The loader must read the requested ID from this reader's store. It may
    /// impose a byte limit; a read error retains the cursor for retry. Returning
    /// `None` means the original was deleted and allows the scan to advance.
    pub async fn next_with<F, R>(&mut self, mut load: F) -> Result<Option<WalletSaga>, Error>
    where
        F: FnMut(Uuid) -> R,
        R: Future<Output = Result<Option<WalletSaga>, Error>>,
    {
        loop {
            if let Some(cursor) = self.pending.front().copied() {
                if let Some(saga) = load(cursor.id).await? {
                    if SagaCursor::from(&saga) != cursor {
                        return Err(Error::Internal(
                            "Saga identity changed during scan".to_owned(),
                        ));
                    }
                    self.pending.pop_front();
                    return Ok(Some(saga));
                }
                self.pending.pop_front();
            } else if self.finished {
                return Ok(None);
            } else {
                let page = self
                    .store
                    .get_incomplete_saga_ids(self.after, MAX_SAGA_PAGE_SIZE)
                    .await?;
                if page.len() > usize::from(MAX_SAGA_PAGE_SIZE) {
                    return Err(Error::Internal("Saga page exceeds row limit".to_owned()));
                }
                let mut previous = self.after;
                for cursor in &page {
                    validate_saga_page_request(Some(*cursor), MAX_SAGA_PAGE_SIZE)?;
                    if previous.is_some_and(|last| *cursor <= last) {
                        return Err(Error::Internal(
                            "Saga page is not strictly ordered".to_owned(),
                        ));
                    }
                    previous = Some(*cursor);
                }
                self.finished = page.is_empty();
                self.after = previous;
                self.pending = page.into();
            }
        }
    }
}

fn canonical_id<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CanonicalId;
    impl serde::de::Visitor<'_> for CanonicalId {
        type Value = Uuid;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a canonical lowercase hyphenated UUID")
        }
        fn visit_str<E>(self, value: &str) -> Result<Uuid, E>
        where
            E: serde::de::Error,
        {
            if value.len() != 36 {
                return Err(E::custom("Invalid saga ID length"));
            }
            let id = Uuid::parse_str(value).map_err(E::custom)?;
            if id.to_string() != value {
                return Err(E::custom("Noncanonical saga ID"));
            }
            Ok(id)
        }
    }
    deserializer.deserialize_str(CanonicalId)
}
