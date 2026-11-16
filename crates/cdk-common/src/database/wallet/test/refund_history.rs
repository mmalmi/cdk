use super::*;
use crate::wallet::{TransactionId, TransactionRole};

async fn eligible<D>(db: &D) -> (Transaction, Transaction, ProofInfo)
where
    D: Database<Error> + Sync,
{
    let operation = uuid::Uuid::new_v4();
    let mut proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    proof.state = State::Spent;
    proof.used_by_operation = Some(operation);
    let mut outgoing = test_transaction(test_mint_url(), TransactionDirection::Outgoing);
    outgoing.saga_id = Some(operation);
    outgoing.ys = vec![proof.y];
    outgoing.amount = 8.into();
    outgoing.fee = 0.into();
    outgoing.status = TransactionStatus::Failed;
    let mut refund = outgoing.clone();
    refund.role = TransactionRole::SendRefund;
    refund.direction = TransactionDirection::Incoming;
    refund.status = TransactionStatus::Completed;
    refund.amount = 7.into();
    refund.fee = 1.into();
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    db.add_transaction(outgoing.clone()).await.unwrap();
    db.add_transaction(refund.clone()).await.unwrap();
    (outgoing, refund, proof)
}

/// A refund and its original send coexist and can be independently released.
pub async fn refund_receipts_roundtrip_and_retire<D>(db: D)
where
    D: Database<Error> + Sync,
{
    for refund_first in [true, false] {
        let (outgoing, refund, proof) = eligible(&db).await;
        let operation = outgoing.saga_id.unwrap();
        assert_eq!(outgoing.id(), TransactionId::from_saga_id(operation));
        assert_eq!(refund.id(), TransactionId::from_send_refund(operation));
        assert_ne!(outgoing.id(), refund.id());
        assert_eq!(
            db.get_transaction(outgoing.id()).await.unwrap(),
            Some(outgoing.clone())
        );
        assert_eq!(
            db.get_transaction(refund.id()).await.unwrap(),
            Some(refund.clone())
        );
        let listed = db.list_transactions(None, None, None).await.unwrap();
        assert!(listed.contains(&outgoing));
        assert!(listed.contains(&refund));
        if refund_first {
            assert!(db
                .retire_incoming_transactions(vec![refund.clone()])
                .await
                .unwrap());
            assert_eq!(
                db.get_transaction(outgoing.id()).await.unwrap(),
                Some(outgoing.clone())
            );
            assert!(db
                .retire_send_transactions(vec![outgoing.clone()])
                .await
                .unwrap());
        } else {
            assert!(db
                .retire_send_transactions(vec![outgoing.clone()])
                .await
                .unwrap());
            assert_eq!(
                db.get_transaction(refund.id()).await.unwrap(),
                Some(refund.clone())
            );
            assert!(db
                .retire_incoming_transactions(vec![refund.clone()])
                .await
                .unwrap());
        }
        assert!(db.get_transaction(outgoing.id()).await.unwrap().is_none());
        assert!(db.get_transaction(refund.id()).await.unwrap().is_none());
        assert_eq!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap(), [proof]);
    }
}

/// A refund retains the actual parent and cannot release another operation's evidence.
pub async fn refund_receipts_require_parent_and_owner<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (outgoing, refund, proof) = eligible(&db).await;
    let mut saga = test_wallet_saga(test_mint_url());
    saga.id = refund.saga_id.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db
        .retire_incoming_transactions(vec![refund.clone()])
        .await
        .unwrap());
    db.delete_saga(&saga.id).await.unwrap();
    for owner in [None, Some(uuid::Uuid::new_v4())] {
        let mut foreign = proof.clone();
        foreign.used_by_operation = owner;
        db.update_proofs(vec![foreign], vec![]).await.unwrap();
        assert!(!db
            .retire_incoming_transactions(vec![refund.clone()])
            .await
            .unwrap());
    }
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    assert!(db
        .retire_send_transactions(vec![refund.clone()])
        .await
        .is_err());
    let mut changed = refund.clone();
    changed.memo = Some("different snapshot".into());
    assert!(!db
        .retire_incoming_transactions(vec![changed])
        .await
        .unwrap());
    assert_eq!(
        db.get_transaction(refund.id()).await.unwrap(),
        Some(refund.clone())
    );
    assert_eq!(
        db.get_transaction(outgoing.id()).await.unwrap(),
        Some(outgoing)
    );
    assert_eq!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap(), [proof]);
}

/// Metadata cannot select a refund ID; malformed refund rows never enter storage.
pub async fn refund_receipts_validate_role<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (mut outgoing, mut refund, _) = eligible(&db).await;
    let original_id = refund.id();
    outgoing
        .metadata
        .insert("send_refund".into(), "true".into());
    assert_eq!(
        outgoing.id(),
        TransactionId::from_saga_id(outgoing.saga_id.unwrap())
    );
    refund
        .metadata
        .insert("description".into(), "refunded send".into());
    assert_eq!(refund.id(), original_id);
    db.add_transaction(refund.clone()).await.unwrap();
    assert_eq!(
        db.get_transaction(original_id).await.unwrap(),
        Some(refund.clone())
    );
    for malformed in [
        {
            let mut tx = refund.clone();
            tx.saga_id = None;
            tx
        },
        {
            let mut tx = refund.clone();
            tx.direction = TransactionDirection::Outgoing;
            tx
        },
        {
            let mut tx = refund.clone();
            tx.status = TransactionStatus::Pending;
            tx
        },
        {
            let mut tx = refund.clone();
            tx.status = TransactionStatus::Failed;
            tx
        },
        {
            let mut tx = refund.clone();
            tx.quote_id = Some("quote".into());
            tx
        },
        {
            let mut tx = refund.clone();
            tx.payment_request = Some("request".into());
            tx
        },
        {
            let mut tx = refund.clone();
            tx.payment_proof = Some("proof".into());
            tx
        },
        {
            let mut tx = refund.clone();
            tx.payment_method = Some(crate::PaymentMethod::Known(KnownMethod::Bolt11));
            tx
        },
        {
            let mut tx = refund.clone();
            tx.metadata.insert("batch_quote_id".into(), "quote".into());
            tx
        },
    ] {
        assert!(db.add_transaction(malformed.clone()).await.is_err());
        assert!(db
            .retire_incoming_transactions(vec![malformed])
            .await
            .is_err());
    }
    assert_eq!(db.get_transaction(original_id).await.unwrap(), Some(refund));
}
