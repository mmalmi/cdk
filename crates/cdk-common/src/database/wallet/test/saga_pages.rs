//! Storage-only operations carry no mint-signed money.
use super::*;
use crate::database::wallet::{IncompleteSagaReader, SagaCursor, MAX_SAGA_PAGE_SIZE};

/// Ties, deleted cursors, and bounded scans across mint/unit scopes.
pub async fn incomplete_saga_pages<DB>(db: DB)
where
    DB: Database<crate::database::Error> + Send + Sync,
{
    let mut original = Vec::new();
    for i in 0..259u128 {
        let mut saga = test_wallet_saga(test_mint_url());
        saga.id = uuid::Uuid::from_u128(i + 1);
        saga.created_at = 1_000 + (i / 4) as u64;
        if i % 2 == 0 {
            saga.unit = CurrencyUnit::Msat;
        }
        if i % 3 == 0 {
            saga.mint_url = "https://foreign.example".parse().unwrap();
        }
        original.push(saga);
    }
    for saga in original.iter().rev() {
        db.add_saga(saga.clone()).await.unwrap();
    }
    for limit in [0, MAX_SAGA_PAGE_SIZE + 1] {
        assert!(db.get_incomplete_saga_ids(None, limit).await.is_err());
    }
    assert!(db
        .get_incomplete_saga_ids(
            Some(SagaCursor {
                created_at: u64::MAX,
                id: original[0].id
            }),
            1
        )
        .await
        .is_err());
    let first = db
        .get_incomplete_saga_ids(None, MAX_SAGA_PAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(
        first,
        original[..128]
            .iter()
            .map(SagaCursor::from)
            .collect::<Vec<_>>()
    );
    db.delete_saga(&first[127].id).await.unwrap();
    let next = db
        .get_incomplete_saga_ids(first.last().copied(), MAX_SAGA_PAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(
        next,
        original[128..256]
            .iter()
            .map(SagaCursor::from)
            .collect::<Vec<_>>()
    );
    let mut reader = IncompleteSagaReader::new(&db);
    assert!(reader
        .next_with(|id| async move {
            assert_eq!(id, uuid::Uuid::from_u128(1));
            Err(crate::database::Error::Internal("Read rejected".to_owned()))
        })
        .await
        .is_err());
    let store = &db;
    assert!(reader
        .next_with(|id| async move {
            let mut saga = store.get_saga(&id).await?.unwrap();
            saga.created_at += 1;
            Ok(Some(saga))
        })
        .await
        .is_err());
    for (i, expected) in original.iter().enumerate() {
        if i == 127 || i == 1 {
            continue;
        }
        assert_eq!(reader.next().await.unwrap().as_ref(), Some(expected));
        db.delete_saga(&expected.id).await.unwrap();
        // Delete a prefetched identity before its exact read.
        if i == 0 {
            db.delete_saga(&original[1].id).await.unwrap();
        }
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert!(db
        .get_incomplete_saga_ids(None, 1)
        .await
        .unwrap()
        .is_empty());
}
