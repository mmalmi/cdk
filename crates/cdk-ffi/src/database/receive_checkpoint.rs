//! Receive checkpoint fields survive both directions of the callback bridge.
use std::collections::HashMap;
use std::str::FromStr;

use cdk::amount::{FeeAndAmounts, SplitTarget};
use cdk::mint_url::MintUrl;
use cdk::nuts::{CurrencyUnit, Id, PreMintSecrets, PreSwap, SwapRequest};
use cdk_common::wallet::{
    OperationData, ReceiveCheckpoint, ReceiveOperationData, ReceiveSagaState, Transaction,
    TransactionDirection, TransactionStatus, WalletSaga, WalletSagaState,
};
use cdk_common::Amount;

use super::{CdkWalletDatabase, FfiWalletDatabaseWrapper, WalletDatabaseBridge};

#[tokio::test]
async fn receive_request_and_receipt_roundtrip_through_callbacks() {
    let db = WalletDatabaseBridge::new(FfiWalletDatabaseWrapper::new(
        cdk_sqlite::wallet::memory::empty().await.unwrap(),
    ));
    let id = uuid::Uuid::new_v4();
    let mint_url = MintUrl::from_str("https://example.com").unwrap();
    let secrets = PreMintSecrets::random(
        Id::from_str("0094d5a774c40a32").unwrap(),
        Amount::from(8),
        &SplitTarget::None,
        &FeeAndAmounts::from((0, vec![1, 2, 4, 8])),
    )
    .unwrap();
    let transaction = Transaction {
        role: Default::default(),
        mint_url: mint_url.clone(),
        direction: TransactionDirection::Incoming,
        amount: 8.into(),
        fee: Amount::ZERO,
        unit: CurrencyUnit::Sat,
        ys: vec![],
        timestamp: 1,
        memo: Some("receive memo".to_owned()),
        metadata: HashMap::from([("source".to_owned(), "test".to_owned())]),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(id),
        status: TransactionStatus::Completed,
    };
    let saga = WalletSaga::new(
        id,
        WalletSagaState::Receive(ReceiveSagaState::OutputsReady),
        8.into(),
        mint_url,
        CurrencyUnit::Sat,
        OperationData::Receive(ReceiveOperationData {
            checkpoint: Some(Box::new(ReceiveCheckpoint {
                inputs: vec![],
                pre_swap: Some(PreSwap {
                    swap_request: SwapRequest::new(vec![], secrets.blinded_messages()),
                    pre_mint_secrets: secrets,
                    derived_secret_count: 1,
                    fee: Amount::ZERO,
                    p2bk_secret_keys: None,
                }),
                transaction: transaction.clone(),
                outputs: Some(vec![]),
            })),
        }),
    );
    db.add_saga(saga.clone()).await.unwrap();
    assert_eq!(db.get_saga(&id).await.unwrap(), Some(saga.clone()));
    let mut completed = saga.clone();
    completed.update_state(WalletSagaState::Receive(ReceiveSagaState::Completed));
    assert!(db
        .complete_saga(&saga, completed.clone(), transaction.clone())
        .await
        .unwrap());
    assert_eq!(db.get_saga(&id).await.unwrap(), Some(completed));
    assert_eq!(
        db.get_transaction(transaction.id()).await.unwrap(),
        Some(transaction)
    );
}
