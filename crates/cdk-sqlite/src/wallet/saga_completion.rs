use cdk_common::database::WalletDatabase;
use cdk_common::wallet::{
    OperationData, SwapOperationData, SwapSagaState, Transaction, TransactionDirection,
    TransactionStatus, WalletSaga, WalletSagaState,
};
use cdk_common::CurrencyUnit;

use super::WalletSqliteDatabase;

mod read_bounds;

fn completion() -> (WalletSaga, WalletSaga, Transaction) {
    let saga = WalletSaga::new(
        uuid::Uuid::new_v4(),
        WalletSagaState::Swap(SwapSagaState::ProofsReserved),
        8.into(),
        "https://mint.example".parse().unwrap(),
        CurrencyUnit::Sat,
        OperationData::Swap(SwapOperationData {
            checkpoint: None,
            input_amount: 8.into(),
            output_amount: 8.into(),
        }),
    );
    let mut next = saga.clone();
    next.update_state(next.state);
    let receipt = Transaction {
        role: cdk_common::wallet::TransactionRole::Primary,
        mint_url: saga.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: 0.into(),
        unit: saga.unit.clone(),
        ys: vec![],
        timestamp: saga.created_at,
        memo: None,
        metadata: Default::default(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(saga.id),
        status: TransactionStatus::Pending,
    };
    (saga, next, receipt)
}

#[tokio::test]
async fn failed_saga_write_rolls_back_receipt_publication() {
    let path = std::env::temp_dir().join(format!("cdk-completion-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let (saga, next, receipt) = completion();
    db.add_saga(saga.clone()).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("CREATE TRIGGER fail_completion BEFORE UPDATE ON wallet_sagas BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;").unwrap();
    assert!(db
        .complete_saga(&saga, next.clone(), receipt.clone())
        .await
        .is_err());
    assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    raw.execute_batch("DROP TRIGGER fail_completion;").unwrap();
    assert!(db
        .complete_saga(&saga, next.clone(), receipt.clone())
        .await
        .unwrap());
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(receipt)
    );
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(next));
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
