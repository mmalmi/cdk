//! The header query must not decode recovery data or hide corrupt identities.
use cdk_common::database::wallet::{IncompleteSagaReader, SagaCursor};
use cdk_common::database::WalletDatabase;
use cdk_common::wallet::{
    OperationData, SendOperationData, SendSagaState, WalletSaga, WalletSagaState,
};
use cdk_common::CurrencyUnit;

use super::test_db::Fixture;

mod fences;
mod payload;
mod read_policy;

fn original() -> WalletSaga {
    WalletSaga::new(
        uuid::Uuid::from_u128(0xabcdef),
        WalletSagaState::Send(SendSagaState::TokenCreated),
        1.into(),
        "https://saga.example".parse().unwrap(),
        CurrencyUnit::Sat,
        OperationData::Send(SendOperationData {
            preparation: None,
            reclaim: None,
            confirmation: None,
            amount: 1.into(),
            memo: Some("x".repeat(5 * 1024 * 1024)),
            counter_start: None,
            counter_end: None,
            token: None,
            proofs: None,
        }),
    )
}

#[tokio::test]
async fn saga_pages_keep_large_original_and_retry_corrupt_payload_without_skipping() {
    let f = Fixture::new().await;
    let saga = original();
    f.db.add_saga(saga.clone()).await.unwrap();
    f.raw
        .execute("UPDATE wallet_sagas SET data = '{broken'", [])
        .unwrap();
    assert_eq!(
        f.db.get_incomplete_saga_ids(None, 1).await.unwrap(),
        vec![SagaCursor::from(&saga)]
    );
    let mut reader = IncompleteSagaReader::new(&f.db);
    assert!(reader.next().await.is_err());
    assert!(f
        .db
        .get_incomplete_sagas_page(None, 128, u32::MAX)
        .await
        .is_err());
    assert!(reader.next().await.is_err());
    assert!(f
        .db
        .get_incomplete_sagas_page(None, 128, u32::MAX)
        .await
        .is_err());
    f.raw
        .execute(
            "UPDATE wallet_sagas SET data = ?1",
            [serde_json::to_string(&saga.data).unwrap()],
        )
        .unwrap();
    assert_eq!(
        f.db.get_incomplete_sagas_page(None, 128, u32::MAX)
            .await
            .unwrap(),
        vec![saga.clone()]
    );
    assert_eq!(reader.next().await.unwrap(), Some(saga));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn saga_pages_reject_corrupt_headers_and_seek_the_cursor_index() {
    let f = Fixture::new().await;
    let saga = original();
    f.db.add_saga(saga.clone()).await.unwrap();
    for id in [
        "x".repeat(5 * 1024 * 1024),
        saga.id.to_string().to_uppercase(),
        "bad-id".to_owned(),
    ] {
        f.raw
            .execute("UPDATE wallet_sagas SET id = ?1", [id])
            .unwrap();
        assert!(f.db.get_incomplete_saga_ids(None, 1).await.is_err());
        assert!(f
            .db
            .get_incomplete_sagas_page(None, 128, u32::MAX)
            .await
            .is_err());
    }
    f.raw
        .execute(
            "UPDATE wallet_sagas SET id = ?1, created_at = -1",
            [saga.id.to_string()],
        )
        .unwrap();
    assert!(f.db.get_incomplete_saga_ids(None, 1).await.is_err());
    assert!(f
        .db
        .get_incomplete_sagas_page(None, 128, u32::MAX)
        .await
        .is_err());
    f.raw
        .execute(
            "UPDATE wallet_sagas SET created_at = ?1",
            ["x".repeat(5 * 1024 * 1024)],
        )
        .unwrap();
    assert!(f.db.get_incomplete_saga_ids(None, 1).await.is_err());
    assert!(f
        .db
        .get_incomplete_sagas_page(None, 128, u32::MAX)
        .await
        .is_err());
    f.raw
        .execute("UPDATE wallet_sagas SET created_at = 0.5", [])
        .unwrap();
    assert!(f.db.get_incomplete_saga_ids(None, 1).await.is_err());
    assert!(f
        .db
        .get_incomplete_sagas_page(None, 128, u32::MAX)
        .await
        .is_err());
    let mut statement = f.raw.prepare("EXPLAIN QUERY PLAN SELECT id, created_at FROM wallet_sagas WHERE (created_at, id) > (?1, ?2) ORDER BY created_at, id LIMIT 128").unwrap();
    let plan: Vec<String> = statement
        .query_map((0i64, saga.id.to_string()), |row| row.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter()
            .any(|line| line.contains("SEARCH") && line.contains("wallet_sagas_cursor_index")),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("TEMP B-TREE")),
        "{plan:?}"
    );
}

#[tokio::test]
async fn bounded_saga_inspection_gates_every_column_and_preserves_recovery() {
    use cdk_common::database::Error;
    let f = Fixture::new().await;
    f.raw
        .execute_batch("PRAGMA ignore_check_constraints = ON;")
        .unwrap();
    let mut saga = original();
    if let OperationData::Send(data) = &mut saga.data {
        data.memo = None;
    }
    f.db.add_saga(saga.clone()).await.unwrap();
    let length: u32 = f.raw.query_row("SELECT length(CAST(id AS BLOB)) + length(CAST(kind AS BLOB)) + length(CAST(state AS BLOB)) + length(CAST(amount AS BLOB)) + length(CAST(mint_url AS BLOB)) + length(CAST(unit AS BLOB)) + COALESCE(length(CAST(quote_id AS BLOB)),0) + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB)) + length(CAST(data AS BLOB)) + length(CAST(version AS BLOB)) FROM wallet_sagas", [], |row| row.get(0)).unwrap();
    assert_eq!(
        f.db.get_saga_bounded(&saga.id, length).await.unwrap(),
        Some(saga.clone())
    );
    assert!(matches!(f.db.get_saga_bounded(&saga.id, length - 1).await,
        Err(Error::SagaTooLarge { bytes, limit }) if bytes == u64::from(length) && limit == length - 1));
    let limited = super::test_db::open(&f.path)
        .await
        .with_saga_read_limit(length - 1);
    assert!(matches!(limited.get_saga(&saga.id).await,
        Err(Error::SagaTooLarge { bytes, limit }) if bytes == u64::from(length) && limit == length - 1));
    assert!(matches!(limited.get_saga_bounded(&saga.id, u32::MAX).await,
        Err(Error::SagaTooLarge { bytes, limit }) if bytes == u64::from(length) && limit == length - 1));
    let exact = super::test_db::open(&f.path)
        .await
        .with_saga_read_limit(length);
    assert_eq!(exact.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    assert!(matches!(exact.get_saga_bounded(&saga.id, 0).await,
        Err(Error::SagaTooLarge { bytes, limit: 0 }) if bytes == u64::from(length)));
    assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    assert!(f
        .db
        .get_saga_bounded(&uuid::Uuid::new_v4(), 0)
        .await
        .unwrap()
        .is_none());
    for column in [
        "kind",
        "state",
        "amount",
        "mint_url",
        "unit",
        "quote_id",
        "created_at",
        "updated_at",
        "data",
        "version",
    ] {
        f.raw
            .execute(
                &format!("UPDATE wallet_sagas SET {column} = ?1"),
                ["\u{00e9}".repeat(4096)],
            )
            .unwrap();
        assert!(
            matches!(
                f.db.get_saga_bounded(&saga.id, 4096).await,
                Err(Error::SagaTooLarge { .. })
            ),
            "{column} crossed the bounded read"
        );
        assert!(
            matches!(
                f.db.get_incomplete_sagas_page(None, 128, 4096).await,
                Err(Error::SagaPageTooLarge { .. })
            ),
            "{column} crossed the page budget"
        );
        f.db.delete_saga(&saga.id).await.unwrap();
        f.db.add_saga(saga.clone()).await.unwrap();
    }
    let large = original();
    f.db.delete_saga(&saga.id).await.unwrap();
    f.db.add_saga(large.clone()).await.unwrap();
    assert!(matches!(
        f.db.get_saga_bounded(&large.id, 4 * 1024 * 1024).await,
        Err(Error::SagaTooLarge { .. })
    ));
    assert_eq!(f.db.get_saga(&large.id).await.unwrap(), Some(large));
}
