use super::*;

async fn eligible<D>(db: &D) -> (Transaction, ProofInfo)
where
    D: Database<Error> + Sync,
{
    let mut proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let operation = uuid::Uuid::new_v4();
    proof.state = State::Spent;
    proof.used_by_operation = Some(operation);
    let mut tx = test_transaction(test_mint_url(), TransactionDirection::Outgoing);
    tx.ys = vec![proof.y];
    tx.saga_id = Some(operation);
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    db.add_transaction(tx.clone()).await.unwrap();
    (tx, proof)
}

/// All expected records and completed token inputs must match before removal.
pub async fn retire_send_transactions<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (first, proof) = eligible(&db).await;
    let (second, other) = eligible(&db).await;
    let mut changed = second.clone();
    changed.fee = 100.into();
    assert!(!db
        .retire_send_transactions(vec![first.clone(), changed])
        .await
        .unwrap());
    assert_eq!(
        db.get_transaction(first.id()).await.unwrap(),
        Some(first.clone())
    );
    for state in [State::Unspent, State::Reserved, State::PendingSpent] {
        db.update_proofs_state(vec![other.y], state).await.unwrap();
        assert!(!db
            .retire_send_transactions(vec![first.clone(), second.clone()])
            .await
            .unwrap());
    }
    let mut foreign = other.clone();
    foreign.used_by_operation = Some(uuid::Uuid::new_v4());
    db.update_proofs(vec![foreign], vec![]).await.unwrap();
    assert!(!db
        .retire_send_transactions(vec![second.clone()])
        .await
        .unwrap());
    db.update_proofs(vec![], vec![other.y]).await.unwrap();
    assert!(!db
        .retire_send_transactions(vec![second.clone()])
        .await
        .unwrap());
    db.update_proofs(vec![other.clone()], vec![]).await.unwrap();
    let mut saga = test_wallet_saga(test_mint_url());
    saga.id = first.saga_id.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db
        .retire_send_transactions(vec![first.clone(), second.clone()])
        .await
        .unwrap());
    db.delete_saga(&saga.id).await.unwrap();
    let mut receipt = first.clone();
    receipt.saga_id = Some(uuid::Uuid::new_v4());
    receipt.direction = TransactionDirection::Incoming;
    db.add_transaction(receipt.clone()).await.unwrap();
    let mut pending = first.clone();
    pending.status = crate::wallet::TransactionStatus::Pending;
    assert!(db.retire_send_transactions(vec![pending]).await.is_err());
    assert!(db
        .retire_send_transactions(vec![receipt.clone()])
        .await
        .is_err());
    assert!(db
        .retire_send_transactions(vec![first.clone(), first.clone()])
        .await
        .is_err());
    assert!(db
        .retire_send_transactions(vec![first.clone(), second.clone()])
        .await
        .unwrap());
    assert!(db.get_transaction(first.id()).await.unwrap().is_none());
    assert!(db.get_transaction(second.id()).await.unwrap().is_none());
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(receipt)
    );
    assert_eq!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap(), [proof]);
    assert_eq!(db.get_proofs_by_ys(vec![other.y]).await.unwrap(), [other]);
    assert!(!db.retire_send_transactions(vec![first]).await.unwrap());
    assert!(db.retire_send_transactions(vec![]).await.unwrap());
}

/// Competing removals cannot both certify ownership of the same original row.
pub async fn retire_send_transactions_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (original, _) = eligible(&db).await;
    let (a, b) = tokio::join!(
        db.retire_send_transactions(vec![original.clone()]),
        db.retire_send_transactions(vec![original.clone()]),
    );
    // Serializable native backends may abort one writer rather than return false.
    assert_eq!(
        u8::from(matches!(a, Ok(true))) + u8::from(matches!(b, Ok(true))),
        1
    );
    assert!(db.get_transaction(original.id()).await.unwrap().is_none());
}
