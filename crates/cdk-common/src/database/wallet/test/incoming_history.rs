use super::*;

async fn eligible<D>(db: &D) -> (Transaction, ProofInfo)
where
    D: Database<Error> + Sync,
{
    let mut proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    proof.state = State::Spent;
    // A self-receive retains the original send owner, not its receive UUID.
    proof.used_by_operation = Some(uuid::Uuid::new_v4());
    let operation = uuid::Uuid::new_v4();
    let mut tx = test_transaction(test_mint_url(), TransactionDirection::Incoming);
    tx.ys = vec![proof.y];
    tx.saga_id = Some(operation);
    tx.amount = 7.into();
    tx.fee = 1.into();
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    db.add_transaction(tx.clone()).await.unwrap();
    (tx, proof)
}

/// Explicit receipt release preserves coins and refuses incomplete or changed batches.
pub async fn retire_incoming_transactions<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (first, proof) = eligible(&db).await;
    let (second, other) = eligible(&db).await;
    let (third, third_proof) = eligible(&db).await;
    let expected = vec![first.clone(), second.clone(), third.clone()];
    let mut changed = second.clone();
    changed.fee = 3.into();
    assert!(!db
        .retire_incoming_transactions(vec![first.clone(), changed])
        .await
        .unwrap());
    for state in [
        State::Unspent,
        State::Pending,
        State::Reserved,
        State::PendingSpent,
    ] {
        db.update_proofs_state(vec![other.y], state).await.unwrap();
        assert!(!db
            .retire_incoming_transactions(expected.clone())
            .await
            .unwrap());
    }
    db.update_proofs(vec![], vec![other.y]).await.unwrap();
    assert!(!db
        .retire_incoming_transactions(expected.clone())
        .await
        .unwrap());
    let mut foreign = other.clone();
    foreign.unit = CurrencyUnit::Usd;
    db.update_proofs(vec![foreign], vec![]).await.unwrap();
    assert!(!db
        .retire_incoming_transactions(expected.clone())
        .await
        .unwrap());
    db.update_proofs(vec![other.clone()], vec![]).await.unwrap();
    let mut saga = test_wallet_saga(test_mint_url());
    saga.id = first.saga_id.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db
        .retire_incoming_transactions(expected.clone())
        .await
        .unwrap());
    db.delete_saga(&saga.id).await.unwrap();
    for malformed in [
        {
            let mut tx = first.clone();
            tx.direction = TransactionDirection::Outgoing;
            tx
        },
        {
            let mut tx = first.clone();
            tx.quote_id = Some("quote".into());
            tx
        },
        {
            let mut tx = first.clone();
            tx.status = crate::wallet::TransactionStatus::Pending;
            tx
        },
        {
            let mut tx = first.clone();
            tx.saga_id = None;
            tx
        },
    ] {
        assert!(db
            .retire_incoming_transactions(vec![malformed])
            .await
            .is_err());
    }
    assert!(db
        .retire_incoming_transactions(vec![first.clone(); 129])
        .await
        .is_err());
    assert!(db
        .retire_incoming_transactions(vec![first.clone(); 2])
        .await
        .is_err());
    let mut wrong_amount = first.clone();
    wrong_amount.amount = 6.into();
    db.add_transaction(wrong_amount.clone()).await.unwrap();
    assert!(!db
        .retire_incoming_transactions(vec![wrong_amount])
        .await
        .unwrap());
    db.add_transaction(first.clone()).await.unwrap();
    let mut outgoing = second.clone();
    outgoing.direction = TransactionDirection::Outgoing;
    outgoing.saga_id = Some(uuid::Uuid::new_v4());
    db.add_transaction(outgoing.clone()).await.unwrap();
    assert!(db.retire_incoming_transactions(expected).await.unwrap());
    for tx in [&first, &second, &third] {
        assert!(db.get_transaction(tx.id()).await.unwrap().is_none());
    }
    assert_eq!(
        db.get_transaction(outgoing.id()).await.unwrap(),
        Some(outgoing)
    );
    for original in [proof, other, third_proof] {
        assert_eq!(
            db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            [original]
        );
    }
    assert!(!db.retire_incoming_transactions(vec![first]).await.unwrap());
    assert!(db.retire_incoming_transactions(vec![]).await.unwrap());
}

/// One native transaction wins when two callers release the same exact receipt.
pub async fn retire_incoming_transactions_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (receipt, _) = eligible(&db).await;
    let (a, b) = tokio::join!(
        db.retire_incoming_transactions(vec![receipt.clone()]),
        db.retire_incoming_transactions(vec![receipt.clone()])
    );
    assert_eq!(
        u8::from(matches!(a, Ok(true))) + u8::from(matches!(b, Ok(true))),
        1
    );
    assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
}
