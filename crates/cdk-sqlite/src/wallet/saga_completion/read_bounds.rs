use cdk_common::database::Error;

use super::*;
use crate::wallet::test_db::Fixture;

#[tokio::test]
async fn receipt_comparison_bounds_growth_and_keeps_large_originals() {
    for large in [false, true] {
        let f = Fixture::new().await;
        let (saga, next, mut proposed) = completion();
        if large {
            proposed.memo = Some("é\0".repeat(2 * 1024 * 1024));
        }
        let mut completed = proposed.clone();
        completed.status = TransactionStatus::Completed;
        f.db.add_saga(saga.clone()).await.unwrap();
        f.db.add_transaction(completed.clone()).await.unwrap();
        if !large {
            let oversized = "x".repeat(4 * 1024 * 1024);
            f.raw
                .execute(
                    "UPDATE transactions SET metadata=?1 WHERE id=?2",
                    rusqlite::params![oversized, proposed.id().as_slice()],
                )
                .unwrap();
            let error =
                f.db.complete_saga(&saga, next.clone(), proposed.clone())
                    .await
                    .unwrap_err();
            assert!(
                matches!(error, Error::TransactionPageTooLarge { bytes, limit }
                if bytes > limit && limit == 4 * 1024 * 1024),
                "receipt comparison must bound stored growth before decoding: {error}"
            );
            assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
            let retained: String = f
                .raw
                .query_row(
                    "SELECT metadata FROM transactions WHERE id=?1",
                    [proposed.id().as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, oversized);
            // Equivalent encoding must still compare by value after repair.
            f.raw
                .execute(
                    "UPDATE transactions SET metadata=' { } ' WHERE id=?1",
                    [proposed.id().as_slice()],
                )
                .unwrap();
        }
        let mut conflicting = proposed.clone();
        conflicting.fee = 1.into();
        assert!(!f
            .db
            .complete_saga(&saga, next.clone(), conflicting)
            .await
            .unwrap());
        assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(
            f.db.get_transaction(proposed.id()).await.unwrap(),
            Some(completed.clone())
        );
        assert!(f
            .db
            .complete_saga(&saga, next.clone(), proposed.clone())
            .await
            .unwrap());
        assert_eq!(
            f.db.get_transaction(proposed.id()).await.unwrap(),
            Some(completed)
        );
        assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(next));
    }
}
