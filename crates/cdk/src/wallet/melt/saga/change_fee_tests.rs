//! A paid melt must retain recovery until change fits its original fee budget.
use cdk_common::wallet::TransactionStatus;

use super::paid_change_fixture::Fixture;
use crate::Amount;

#[tokio::test]
async fn test_paid_change_bound_rejects_missing_or_excessive_change_before_writes() {
    for amounts in [vec![], vec![16], vec![32]] {
        let f = Fixture::with_reserve(1).await;
        let before = f
            .wallet
            .localstore
            .get_reserved_proofs(&f.saga.id)
            .await
            .unwrap();
        assert!(
            f.restore(&amounts).await.is_err(),
            "response must fit the original 1-sat fee reserve: {amounts:?}"
        );
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            f.wallet
                .localstore
                .get_reserved_proofs(&f.saga.id)
                .await
                .unwrap(),
            before
        );
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::ZERO);
        assert!(f
            .wallet
            .localstore
            .list_transactions(None, None, None)
            .await
            .unwrap()
            .iter()
            .all(|tx| tx.status != TransactionStatus::Completed));

        let result = f.restore(&[16, 8]).await.unwrap().unwrap();
        assert_eq!(result.fee_paid(), Amount::ZERO);
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(24));
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_none());
        let txs = f
            .wallet
            .localstore
            .list_transactions(None, None, None)
            .await
            .unwrap();
        assert_eq!(txs.len(), 1);
        assert_eq!(
            txs[0].metadata.get("purpose").map(String::as_str),
            Some("recovery test")
        );
    }
}

#[tokio::test]
async fn test_paid_change_bound_allows_full_fee_reserve() {
    let f = Fixture::with_reserve(1).await;
    let result = f.restore(&[16, 4, 2, 1]).await.unwrap().unwrap();
    assert_eq!(result.fee_paid(), Amount::from(1));
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(23));
    assert!(f
        .wallet
        .localstore
        .get_saga(&f.saga.id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_paid_change_bound_allows_no_change_when_reserve_is_all_consumed() {
    let f = Fixture::with_reserve(24).await;
    let result = f.restore(&[]).await.unwrap().unwrap();
    assert_eq!(result.fee_paid(), Amount::from(24));
    assert!(result.change().is_none());
    assert!(f
        .wallet
        .localstore
        .get_saga(&f.saga.id)
        .await
        .unwrap()
        .is_none());
}
