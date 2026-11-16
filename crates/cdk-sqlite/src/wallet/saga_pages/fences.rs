use super::*;
use cdk_common::database::wallet::ProofChange;
use cdk_common::database::Error;

#[tokio::test]
async fn atomic_saga_guard_bounds_growth_before_decoding() {
    let f = Fixture::new().await;
    let mut saga = original();
    let OperationData::Send(data) = &mut saga.data else {
        unreachable!()
    };
    data.memo = None;
    f.db.add_saga(saga.clone()).await.unwrap();
    let coin = super::super::proof_changes::proof();
    f.db.update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    let change = ProofChange {
        y: coin.y,
        before: Some(coin.clone()),
        after: None,
    };
    let oversized = "x".repeat(4 * 1024 * 1024);
    f.raw
        .execute(
            "UPDATE wallet_sagas SET data=?1 WHERE id=?2",
            rusqlite::params![oversized, saga.id.to_string()],
        )
        .unwrap();
    for changes in [vec![], vec![change.clone()]] {
        let error =
            f.db.compare_and_swap_proofs(changes, Some(&saga))
                .await
                .unwrap_err();
        assert!(
            matches!(error, Error::SagaTooLarge { bytes, limit }
            if bytes > u64::from(limit) && limit == 4 * 1024 * 1024),
            "atomic saga guard must bound changed evidence before decoding: {error}"
        );
        assert_eq!(
            f.db.get_proofs_by_ys(vec![coin.y]).await.unwrap(),
            vec![coin.clone()]
        );
        let retained: String = f
            .raw
            .query_row(
                "SELECT data FROM wallet_sagas WHERE id=?1",
                [saga.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, oversized);
    }
    f.raw
        .execute(
            "UPDATE wallet_sagas SET data=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&saga.data).unwrap(),
                saga.id.to_string()
            ],
        )
        .unwrap();
    assert!(f
        .db
        .compare_and_swap_proofs(vec![change], Some(&saga))
        .await
        .unwrap());
    assert!(f
        .db
        .get_proofs_by_ys(vec![coin.y])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(saga));
}

#[tokio::test]
async fn atomic_saga_guard_keeps_large_originals_and_typed_comparison() {
    for large in [false, true] {
        let f = Fixture::new().await;
        let mut saga = original();
        if !large {
            let OperationData::Send(data) = &mut saga.data else {
                unreachable!()
            };
            data.memo = None;
        }
        f.db.add_saga(saga.clone()).await.unwrap();
        if !large {
            // Equivalent encodings within the read budget must still match.
            f.raw
                .execute(
                    "UPDATE wallet_sagas SET data=?1 WHERE id=?2",
                    rusqlite::params![
                        format!(" {} ", serde_json::to_string(&saga.data).unwrap()),
                        saga.id.to_string()
                    ],
                )
                .unwrap();
        }
        let mut stale = saga.clone();
        stale.version += 1;
        assert!(!f
            .db
            .compare_and_swap_proofs(vec![], Some(&stale))
            .await
            .unwrap());
        assert!(f
            .db
            .compare_and_swap_proofs(vec![], Some(&saga))
            .await
            .unwrap());
        assert_eq!(f.db.get_saga(&saga.id).await.unwrap(), Some(saga));
    }
}
