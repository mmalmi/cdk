use super::*;

fn fixture() -> (WalletSaga, WalletSaga, Transaction) {
    let saga = test_wallet_saga(test_mint_url());
    let mut next = saga.clone();
    next.update_state(next.state);
    let mut receipt = test_transaction(saga.mint_url.clone(), TransactionDirection::Outgoing);
    receipt.saga_id = Some(saga.id);
    receipt.amount = saga.amount;
    receipt.unit = saga.unit.clone();
    receipt.timestamp = saga.created_at;
    receipt.status = TransactionStatus::Pending;
    (saga, next, receipt)
}

/// Exact parent and immutable receipt checks happen before either write.
pub async fn complete_saga_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    assert!(db.supports_atomic_saga_completion());
    let (saga, next, receipt) = fixture();
    assert!(!db
        .complete_saga(&saga, next.clone(), receipt.clone())
        .await
        .unwrap());
    assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
    db.add_saga(saga.clone()).await.unwrap();
    for fault in 0..8 {
        let mut wrong = next.clone();
        match fault {
            0 => wrong.id = uuid::Uuid::new_v4(),
            1 => wrong.mint_url = test_mint_url_2(),
            2 => wrong.unit = CurrencyUnit::Msat,
            3 => wrong.amount += Amount::from(1),
            4 => wrong.created_at += 1,
            5 => wrong.version += 1,
            6 => wrong.quote_id = Some("changed".to_owned()),
            _ => wrong.version = saga.version,
        }
        assert!(db
            .complete_saga(&saga, wrong, receipt.clone())
            .await
            .is_err());
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
    }
    let mut changed_parent = saga.clone();
    if let OperationData::Swap(data) = &mut changed_parent.data {
        data.output_amount = Amount::from(12345);
    } // same version must still compare the full parent
    assert!(!db
        .complete_saga(&changed_parent, next.clone(), receipt.clone())
        .await
        .unwrap());
    for fault in 0..6 {
        let mut changed = receipt.clone();
        match fault {
            0 => changed.fee += Amount::from(1),
            1 => changed.memo = Some("changed".to_owned()),
            2 => changed.ys.clear(),
            3 => {
                changed
                    .metadata
                    .insert("changed".to_owned(), "yes".to_owned());
            }
            4 => changed.timestamp += 1,
            _ => changed.payment_proof = Some("changed".to_owned()),
        }
        db.add_transaction(changed.clone()).await.unwrap();
        assert!(!db
            .complete_saga(&saga, next.clone(), receipt.clone())
            .await
            .unwrap());
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(changed)
        );
        db.remove_transaction(receipt.id()).await.unwrap();
    }
    for status in [TransactionStatus::Completed, TransactionStatus::Failed] {
        let mut terminal = receipt.clone();
        terminal.status = status;
        db.add_transaction(terminal.clone()).await.unwrap();
        let mut conflict = receipt.clone();
        conflict.status = if status == TransactionStatus::Completed {
            TransactionStatus::Failed
        } else {
            TransactionStatus::Completed
        };
        assert!(!db
            .complete_saga(&saga, next.clone(), conflict)
            .await
            .unwrap());
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(terminal.clone())
        );
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert!(db
            .complete_saga(&saga, next.clone(), receipt.clone())
            .await
            .unwrap());
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(next.clone()));
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(terminal.clone())
        );
        assert!(!db
            .complete_saga(&saga, next.clone(), receipt.clone())
            .await
            .unwrap());
        // Model acknowledged retirement: the real parent is gone, then history is removed.
        db.delete_saga(&saga.id).await.unwrap();
        db.remove_transaction(receipt.id()).await.unwrap();
        assert!(!db
            .complete_saga(&saga, next.clone(), receipt.clone())
            .await
            .unwrap());
        assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
        db.add_saga(saga.clone()).await.unwrap();
    }
    use crate::wallet::{SendOperationData, SendSagaState, TransactionRole};
    let parent = WalletSaga::new(
        uuid::Uuid::new_v4(),
        WalletSagaState::Send(SendSagaState::Confirming),
        saga.amount,
        saga.mint_url.clone(),
        saga.unit.clone(),
        OperationData::Send(SendOperationData {
            preparation: None,
            amount: saga.amount,
            memo: None,
            counter_start: None,
            counter_end: None,
            token: None,
            proofs: None,
            confirmation: None,
            reclaim: None,
        }),
    );
    let mut next_send = parent.clone();
    next_send.update_state(WalletSagaState::Send(SendSagaState::TokenCreated));
    let mut refund = receipt.clone();
    refund.saga_id = Some(parent.id);
    refund.timestamp = parent.created_at;
    refund.role = TransactionRole::SendRefund;
    refund.direction = TransactionDirection::Incoming;
    refund.status = TransactionStatus::Completed;
    db.add_saga(parent.clone()).await.unwrap();
    assert!(db
        .complete_saga(&parent, next_send.clone(), refund.clone())
        .await
        .is_err());
    assert_eq!(db.get_saga(&parent.id).await.unwrap(), Some(parent.clone()));
    assert!(db.get_transaction(refund.id()).await.unwrap().is_none());
    let mut primary = refund.clone();
    primary.role = TransactionRole::Primary;
    primary.direction = TransactionDirection::Outgoing;
    primary.status = TransactionStatus::Pending;
    let mut wrong_namespace = primary.clone();
    wrong_namespace
        .metadata
        .insert("batch_quote_id".to_owned(), "reserved".to_owned());
    assert!(db
        .complete_saga(&parent, next_send.clone(), wrong_namespace.clone())
        .await
        .is_err());
    assert!(db
        .get_transaction(wrong_namespace.id())
        .await
        .unwrap()
        .is_none());
    for change_amount in [false, true] {
        let mut wrong = primary.clone();
        if change_amount {
            wrong.amount += Amount::from(1);
        } else {
            wrong.timestamp += 1;
        }
        assert!(db
            .complete_saga(&parent, next_send.clone(), wrong)
            .await
            .is_err());
        assert_eq!(db.get_saga(&parent.id).await.unwrap(), Some(parent.clone()));
        assert!(db.get_transaction(primary.id()).await.unwrap().is_none());
    }
    // A refund consumer can publish the verified net amount under the real send.
    // The callback preserves the operation's amount while allowing a different receipt amount.
    let mut refund_next = parent.clone();
    refund_next.update_state(WalletSagaState::Send(SendSagaState::RollingBack));
    refund.amount = parent.amount - Amount::from(1);
    assert!(db
        .complete_saga(&parent, refund_next.clone(), refund.clone())
        .await
        .unwrap());
    assert_eq!(db.get_saga(&parent.id).await.unwrap(), Some(refund_next));
    assert_eq!(db.get_transaction(refund.id()).await.unwrap(), Some(refund));
}

/// Two completions cannot both advance one expected parent.
pub async fn complete_saga_concurrent<D>(db: D)
where
    D: Database<Error> + Send + Sync + 'static,
{
    let (saga, next, receipt) = fixture();
    db.add_saga(saga.clone()).await.unwrap();
    let db = std::sync::Arc::new(db);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let start = || {
        let (db, barrier) = (db.clone(), barrier.clone());
        let (saga, next, receipt) = (saga.clone(), next.clone(), receipt.clone());
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            barrier.wait();
            runtime.block_on(db.complete_saga(&saga, next, receipt))
        })
    };
    let a = start();
    let b = start();
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert_eq!(
        usize::from(matches!(a, Ok(true))) + usize::from(matches!(b, Ok(true))),
        1
    );
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(next));
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(receipt)
    );
}
