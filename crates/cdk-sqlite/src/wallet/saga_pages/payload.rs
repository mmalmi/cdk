//! Payload pages preserve originals and distinguish retryable aggregate limits.
use cdk_common::database::wallet::SagaCursor;
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::wallet::OperationData;

use super::{original, Fixture};

#[tokio::test]
async fn payload_pages_seek_past_deleted_cursors_and_tied_timestamps() {
    let f = Fixture::new().await;
    let mut saga = original();
    if let OperationData::Send(data) = &mut saga.data {
        data.memo = None;
    }
    let mut originals = Vec::new();
    for id in 1..=130 {
        let mut saved = saga.clone();
        saved.id = uuid::Uuid::from_u128(id);
        saved.created_at = if id < 130 { 7 } else { 8 };
        f.db.add_saga(saved.clone()).await.unwrap();
        originals.push(saved);
    }
    let first =
        f.db.get_incomplete_sagas_page(None, 128, 1024 * 1024)
            .await
            .unwrap();
    assert_eq!(first, originals[..128]);
    let after = SagaCursor::from(first.last().unwrap());
    f.db.delete_saga(&after.id).await.unwrap();
    let last =
        f.db.get_incomplete_sagas_page(Some(after), 128, 1024 * 1024)
            .await
            .unwrap();
    assert_eq!(last, originals[128..]);
    assert!(f
        .db
        .get_incomplete_sagas_page(Some(SagaCursor::from(last.last().unwrap())), 128, 0)
        .await
        .unwrap()
        .is_empty());
    for limit in [0, 129, u16::MAX] {
        assert!(f
            .db
            .get_incomplete_sagas_page(None, limit, u32::MAX)
            .await
            .is_err());
    }
    assert!(f
        .db
        .get_incomplete_sagas_page(
            Some(SagaCursor {
                created_at: u64::MAX,
                id: after.id
            }),
            128,
            u32::MAX
        )
        .await
        .is_err());
}

#[tokio::test]
async fn payload_pages_enforce_aggregate_and_individual_limits_without_mutation() {
    let f = Fixture::new().await;
    let mut first = original();
    if let OperationData::Send(data) = &mut first.data {
        data.memo = Some("é".repeat(32 * 1024));
    }
    let mut second = first.clone();
    second.id = uuid::Uuid::from_u128(first.id.as_u128() + 1);
    f.db.add_saga(first.clone()).await.unwrap();
    f.db.add_saga(second.clone()).await.unwrap();
    let bytes: u32 = f.raw.query_row("SELECT SUM(length(CAST(id AS BLOB)) + length(CAST(kind AS BLOB)) + length(CAST(state AS BLOB)) + length(CAST(amount AS BLOB)) + length(CAST(mint_url AS BLOB)) + length(CAST(unit AS BLOB)) + COALESCE(length(CAST(quote_id AS BLOB)),0) + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB)) + length(CAST(data AS BLOB)) + length(CAST(version AS BLOB))) FROM wallet_sagas", [], |row| row.get(0)).unwrap();
    let originals = vec![first.clone(), second.clone()];
    assert_eq!(
        f.db.get_incomplete_sagas_page(None, 128, bytes)
            .await
            .unwrap(),
        originals
    );
    assert!(
        matches!(f.db.get_incomplete_sagas_page(None, 128, bytes - 1).await,
        Err(Error::SagaPageTooLarge { bytes: actual, limit }) if actual == u64::from(bytes) && limit == bytes - 1)
    );
    assert_eq!(
        f.db.get_incomplete_sagas_page(None, 1, bytes / 2)
            .await
            .unwrap(),
        vec![first.clone()]
    );
    assert_eq!(
        f.db.get_incomplete_sagas_page(Some(SagaCursor::from(&first)), 1, bytes / 2)
            .await
            .unwrap(),
        vec![second]
    );
    let limited = super::super::test_db::open(&f.path)
        .await
        .with_saga_read_limit(bytes / 2 - 1);
    for page_budget in [0, bytes, u32::MAX] {
        assert!(
            matches!(limited.get_incomplete_sagas_page(None, 128, page_budget).await,
            Err(Error::SagaTooLarge { bytes: actual, limit }) if actual == u64::from(bytes / 2) && limit == bytes / 2 - 1)
        );
    }
    let exact = super::super::test_db::open(&f.path)
        .await
        .with_saga_read_limit(bytes / 2);
    assert_eq!(
        exact
            .get_incomplete_sagas_page(None, 128, bytes)
            .await
            .unwrap(),
        originals
    );
    assert_eq!(f.db.get_incomplete_sagas().await.unwrap(), originals);
}
