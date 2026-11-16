use cdk_common::database::wallet::test::melt_completion_fixture;
use cdk_common::database::wallet::validate_melt_completion;
use cdk_common::database::WalletDatabase;
use cdk_common::wallet::OperationData;

use super::WalletSqliteDatabase;

#[tokio::test]
async fn complete_melt_partial_or_ignored_writes_roll_back() {
    let path = std::env::temp_dir().join(format!("cdk-paid-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let (saga, next, quote, pending) = melt_completion_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    for failure in ["RAISE(ABORT, 'injected write failure')", "RAISE(IGNORE)"] {
        for table in ["transactions", "melt_quote", "wallet_sagas"] {
            let event = if table == "transactions" {
                "INSERT"
            } else {
                "UPDATE"
            };
            raw.execute_batch(&format!("CREATE TRIGGER fail_completion BEFORE {event} ON {table} BEGIN SELECT {failure}; END;")).unwrap();
            assert!(
                db.complete_melt(&saga, next.clone(), &quote).await.is_err(),
                "{table}: {failure}"
            );
            assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
            assert_eq!(
                db.get_melt_quote(&quote.id).await.unwrap(),
                Some(quote.clone())
            );
            assert_eq!(
                db.get_transaction(pending.id()).await.unwrap(),
                Some(pending.clone())
            );
            raw.execute_batch("DROP TRIGGER fail_completion;").unwrap();
        }
    }
    assert!(db.complete_melt(&saga, next, &quote).await.unwrap());
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn complete_melt_rejects_corrupt_pending_metadata() {
    let path =
        std::env::temp_dir().join(format!("cdk-paid-metadata-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let (saga, next, quote, pending) = melt_completion_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    for metadata in ["invalid JSON".to_owned(), "x".repeat(4 * 1024 * 1024)] {
        raw.execute(
            "UPDATE transactions SET metadata=?1 WHERE id=?2",
            rusqlite::params![metadata, pending.id().as_slice()],
        )
        .unwrap();
        let error = db
            .complete_melt(&saga, next.clone(), &quote)
            .await
            .unwrap_err();
        if metadata.len() >= 4 * 1024 * 1024 {
            assert!(
                matches!(error, cdk_common::database::Error::TransactionPageTooLarge { bytes, limit }
                if bytes > limit && limit == 4 * 1024 * 1024),
                "melt receipt comparison must bound stored growth before decoding: {error}"
            );
        }
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
    }
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn complete_melt_rejects_unrepresentable_fee_without_publication() {
    let db = super::memory::empty().await.unwrap();
    let (mut saga, mut next, quote, _) = melt_completion_fixture();
    let OperationData::Melt(data) = &mut saga.data else {
        unreachable!()
    };
    data.change_amount = Some(quote.fee_reserve);
    let payment = data.payment.as_mut().unwrap();
    payment.inputs[0].proof.amount = (1_u64 << 63).into();
    payment.change = Some(vec![]);
    payment.receipt.fee = (1_u64 << 63).into();
    next.data = saga.data.clone();
    let result = validate_melt_completion(&saga, &next, &quote).unwrap();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(result.pending.clone()).await.unwrap();
    assert!(db.complete_melt(&saga, next, &quote).await.is_err());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga));
    assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(quote));
    assert_eq!(
        db.get_transaction(result.pending.id()).await.unwrap(),
        Some(result.pending)
    );
}
