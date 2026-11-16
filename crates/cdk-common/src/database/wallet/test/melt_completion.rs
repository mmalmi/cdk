use super::*;

/// Missing, changed, already-final or retired receipts never complete or resurrect a payment.
pub async fn complete_melt_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    assert!(db.supports_atomic_melt_completion());
    let (saga, next, quote, pending) = melt_completion_fixture();
    let result = validate_melt_completion(&saga, &next, &quote).unwrap();
    let OperationData::Melt(data) = &saga.data else {
        unreachable!()
    };
    let payment = data.payment.as_ref().unwrap();
    let debug = format!("{saga:?}");
    assert!(!debug.contains(&payment.inputs[0].proof.secret.to_string()));
    assert!(!debug.contains(payment.receipt.payment_proof.as_ref().unwrap()));
    db.add_melt_quote(quote.clone()).await.unwrap();
    assert!(!db.complete_melt(&saga, next.clone(), &quote).await.unwrap());
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db.complete_melt(&saga, next.clone(), &quote).await.unwrap());
    for field in [
        "fee",
        "memo",
        "order",
        "metadata",
        "timestamp",
        "proof",
        "completed",
        "failed",
    ] {
        let mut wrong = pending.clone();
        match field {
            "fee" => wrong.fee += Amount::from(1),
            "memo" => wrong.memo = None,
            "order" => wrong.ys.reverse(),
            "metadata" => {
                wrong
                    .metadata
                    .insert("different".to_owned(), "value".to_owned());
            }
            "timestamp" => wrong.timestamp += 1,
            "proof" => wrong.payment_proof = Some("other-proof".to_owned()),
            "completed" => wrong.status = TransactionStatus::Completed,
            "failed" => wrong.status = TransactionStatus::Failed,
            _ => unreachable!(),
        }
        db.remove_transaction(pending.id()).await.unwrap();
        db.add_transaction(wrong.clone()).await.unwrap();
        assert!(
            !db.complete_melt(&saga, next.clone(), &quote).await.unwrap(),
            "{field}"
        );
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
        assert_eq!(db.get_transaction(pending.id()).await.unwrap(), Some(wrong));
    }
    db.remove_transaction(pending.id()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    assert!(db.complete_melt(&saga, next.clone(), &quote).await.unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(next));
    assert_eq!(
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(result.quote.clone())
    );
    assert_eq!(
        db.get_transaction(pending.id()).await.unwrap(),
        Some(result.receipt.clone())
    );
    let mut completed = saga.clone();
    completed.update_state(crate::wallet::WalletSagaState::Melt(
        crate::wallet::MeltSagaState::Completed,
    ));
    assert!(!db
        .complete_melt(&saga, completed.clone(), &quote)
        .await
        .unwrap());
    db.delete_saga(&saga.id).await.unwrap();
    db.remove_transaction(pending.id()).await.unwrap();
    assert!(!db.complete_melt(&saga, completed, &quote).await.unwrap());
    assert!(db.get_transaction(pending.id()).await.unwrap().is_none());
    assert_eq!(
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(result.quote)
    );
}

/// Full quote and parent snapshots must match, even if their versions are unchanged.
pub async fn complete_melt_exact_snapshots<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (saga, next, quote, pending) = melt_completion_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    let mut stale = saga.clone();
    let OperationData::Melt(data) = &mut stale.data else {
        unreachable!()
    };
    data.counter_end = Some(14);
    let mut stale_next = stale.clone();
    stale_next.update_state(next.state);
    assert!(!db.complete_melt(&stale, stale_next, &quote).await.unwrap());
    for field in ["expiry", "version", "blocks", "index", "mint_url"] {
        let mut stale = quote.clone();
        match field {
            "expiry" => stale.expiry += 1,
            "version" => stale.version += 1,
            "blocks" => stale.estimated_blocks = None,
            "index" => stale.fee_index = None,
            "mint_url" => stale.mint_url = None,
            _ => unreachable!(),
        }
        assert!(
            !db.complete_melt(&saga, next.clone(), &stale).await.unwrap(),
            "{field}"
        );
    }
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga));
    assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(quote));
    assert_eq!(
        db.get_transaction(pending.id()).await.unwrap(),
        Some(pending)
    );
}

/// Concurrent finishers publish exactly one complete three-record result.
pub async fn complete_melt_concurrent<D>(db: D)
where
    D: Database<Error> + Send + Sync + 'static,
{
    let db = std::sync::Arc::new(db);
    let (saga, next, quote, pending) = melt_completion_fixture();
    let result = validate_melt_completion(&saga, &next, &quote).unwrap();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(saga.clone()).await.unwrap();
    db.add_transaction(pending.clone()).await.unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let start = || {
        let db = db.clone();
        let barrier = barrier.clone();
        let saga = saga.clone();
        let next = next.clone();
        let quote = quote.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            barrier.wait();
            runtime.block_on(db.complete_melt(&saga, next, &quote))
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
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(result.quote)
    );
    assert_eq!(
        db.get_transaction(pending.id()).await.unwrap(),
        Some(result.receipt)
    );
}
