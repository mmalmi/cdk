use super::*;

pub(super) fn keys() -> KeySet {
    let (keys, id) = test_keys_with_id();
    KeySet {
        id,
        unit: CurrencyUnit::Sat,
        active: Some(true),
        keys,
        input_fee_ppk: 0,
        final_expiry: None,
    }
}

/// Keys, counter range and exact parent move together, including conflict retries.
pub async fn output_preparation_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let saga = test_wallet_saga(test_mint_url());
    let mut next = saga.clone();
    next.update_state(next.state);
    let keyset = keys();
    let mut receipt = test_transaction(saga.mint_url.clone(), TransactionDirection::Outgoing);
    receipt.saga_id = Some(saga.id);
    receipt.unit = saga.unit.clone();
    receipt.status = TransactionStatus::Pending;
    db.add_mint(saga.mint_url.clone(), None).await.unwrap();
    assert!(!db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            1,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    let mut changed = saga.clone();
    if let OperationData::Swap(data) = &mut changed.data {
        data.output_amount += Amount::from(1);
    }
    assert!(!db
        .prepare_saga_outputs(
            Some(&changed),
            next.clone(),
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    let mut invalid = next.clone();
    invalid.version += 1;
    assert!(db
        .prepare_saga_outputs(
            Some(&saga),
            invalid,
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .is_err());
    assert!(db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            u32::MAX,
            1,
            Some(receipt.clone())
        )
        .await
        .is_err());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 0);
    assert!(db.get_transaction(receipt.id()).await.unwrap().is_none());
    let mut conflict = receipt.clone();
    conflict.fee += Amount::from(1);
    db.add_transaction(conflict.clone()).await.unwrap();
    assert!(!db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(conflict)
    );
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 0);
    assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
    assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    db.remove_transaction(receipt.id()).await.unwrap();
    assert!(db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(next.clone()));
    assert_eq!(
        db.get_keys(&keyset.id).await.unwrap(),
        Some(keyset.keys.clone())
    );
    assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_some());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 4);
    assert!(!db
        .prepare_saga_outputs(
            Some(&saga),
            next.clone(),
            keyset.clone(),
            0,
            4,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 4);
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(receipt.clone())
    );
    let mut terminal = receipt.clone();
    terminal.status = TransactionStatus::Completed;
    db.add_transaction(terminal.clone()).await.unwrap();
    let mut last = next.clone();
    last.update_state(last.state);
    assert!(db
        .prepare_saga_outputs(
            Some(&next),
            last.clone(),
            keyset.clone(),
            4,
            0,
            Some(receipt.clone())
        )
        .await
        .unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(last));
    assert_eq!(
        db.get_transaction(receipt.id()).await.unwrap(),
        Some(terminal)
    );
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 4);
    // Initial swaps use the same transaction but must not acquire quote owners.
    let initial = test_wallet_saga(test_mint_url());
    let mut invalid = initial.clone();
    invalid.version = 1;
    assert!(db
        .prepare_saga_outputs(None, invalid, keyset.clone(), 4, 2, None)
        .await
        .is_err());
    let (_, _, issue) = issue_acquisition_fixture();
    assert!(db
        .prepare_saga_outputs(None, issue, keyset.clone(), 4, 2, None)
        .await
        .is_err());
    assert!(db
        .prepare_saga_outputs(None, initial.clone(), keyset.clone(), 4, 2, Some(receipt))
        .await
        .is_err());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 4);
    assert!(db
        .prepare_saga_outputs(None, initial.clone(), keyset.clone(), 4, 2, None)
        .await
        .unwrap());
    assert!(!db
        .prepare_saga_outputs(None, initial.clone(), keyset.clone(), 6, 2, None)
        .await
        .unwrap());
    assert_eq!(db.get_saga(&initial.id).await.unwrap(), Some(initial));
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 6);
}

/// Competing parents cannot publish overlapping proposed derivation ranges.
pub async fn output_preparation_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let a = test_wallet_saga(test_mint_url());
    let b = test_wallet_saga(test_mint_url());
    let mut next_a = a.clone();
    next_a.update_state(next_a.state);
    let mut next_b = b.clone();
    next_b.update_state(next_b.state);
    let keyset = keys();
    db.add_mint(a.mint_url.clone(), None).await.unwrap();
    db.add_saga(a.clone()).await.unwrap();
    db.add_saga(b.clone()).await.unwrap();
    let (ra, rb) = tokio::join!(
        db.prepare_saga_outputs(Some(&a), next_a.clone(), keyset.clone(), 0, 4, None),
        db.prepare_saga_outputs(Some(&b), next_b.clone(), keyset.clone(), 0, 4, None),
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_ne!(ra, rb);
    assert_eq!(
        db.get_saga(&a.id).await.unwrap(),
        Some(if ra { next_a } else { a })
    );
    assert_eq!(
        db.get_saga(&b.id).await.unwrap(),
        Some(if rb { next_b } else { b })
    );
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 4);
}
