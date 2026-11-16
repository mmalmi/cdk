use super::melt_acquisition_fixture::intent;
use super::*;
use crate::wallet::MeltSagaState;

/// A stale input, changed quote or reused operation cannot partially acquire a melt.
pub async fn acquire_melt<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let mut quote = test_melt_quote();
    quote.expiry = 2_000_000_000;
    let proof = test_proof_info(test_keyset_id(), 2048, test_mint_url());
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    let saga = intent(&quote, proof.clone());
    assert!(
        !format!("{saga:?}").contains(&proof.proof.secret.to_string()),
        "melt debug output must redact bearer secrets"
    );
    let mut changed = quote.clone();
    changed.request.push('x');
    assert!(!db.acquire_melt(&saga, &changed).await.unwrap());
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    assert!(db.get_melt_quote(&quote.id).await.unwrap().unwrap() == quote);
    let mut altered = proof.clone();
    altered.created_by_operation = Some(uuid::Uuid::new_v4());
    db.update_proofs(vec![altered.clone()], vec![])
        .await
        .unwrap();
    assert!(!db.acquire_melt(&saga, &quote).await.unwrap());
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    assert!(db.get_melt_quote(&quote.id).await.unwrap().unwrap() == quote);
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    assert!(db.acquire_melt(&saga, &quote).await.unwrap());
    assert!(!db.acquire_melt(&saga, &quote).await.unwrap());
    assert!(db.get_saga(&saga.id).await.unwrap().as_ref() == Some(&saga));
    let owned = db.get_proofs_by_ys(vec![proof.y]).await.unwrap();
    assert_eq!(owned[0].used_by_operation, Some(saga.id));
    let stored = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    assert_eq!(stored.used_by_operation, Some(saga.id.to_string()));
    let other = intent(&quote, proof.clone());
    assert!(!db.acquire_melt(&other, &quote).await.unwrap());
    assert!(db.get_saga(&other.id).await.unwrap().is_none());
    db.release_melt_quote(&saga.id).await.unwrap();
    let quote = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    assert!(
        !db.acquire_melt(&saga, &quote).await.unwrap(),
        "existing saga cannot be overwritten"
    );
    assert!(db.get_proofs_by_ys(vec![proof.y]).await.unwrap() == vec![proof]);
    let mut invalid = other.clone();
    if let OperationData::Melt(data) = &mut invalid.data {
        data.acquisition = Some(vec![]);
    }
    assert!(db.acquire_melt(&invalid, &quote).await.is_err());
}

/// Two preparations racing for one quote and input set have exactly one owner.
pub async fn acquire_melt_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let mut quote = test_melt_quote();
    quote.expiry = 2_000_000_000;
    let proof = test_proof_info(test_keyset_id(), 2048, test_mint_url());
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    let a = intent(&quote, proof.clone());
    let b = intent(&quote, proof.clone());
    let (x, y) = tokio::join!(db.acquire_melt(&a, &quote), db.acquire_melt(&b, &quote));
    assert_eq!(
        usize::from(matches!(x, Ok(true))) + usize::from(matches!(y, Ok(true))),
        1
    );
    let owner = db
        .get_melt_quote(&quote.id)
        .await
        .unwrap()
        .unwrap()
        .used_by_operation
        .unwrap();
    let (winner, loser) = if a.id.to_string() == owner {
        (&a, &b)
    } else {
        (&b, &a)
    };
    assert!(db.get_saga(&winner.id).await.unwrap().as_ref() == Some(winner));
    assert!(db.get_saga(&loser.id).await.unwrap().is_none());
    assert_eq!(
        db.get_proofs_by_ys(vec![proof.y]).await.unwrap()[0].used_by_operation,
        Some(winner.id)
    );
}

/// Mixed input batches and every quote field retain their exact stored evidence.
pub async fn acquire_melt_batch<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (quote, saga, local) = melt_acquisition_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    let changes = super::super::validate_melt_acquisition(&saga, &quote).unwrap();
    let external = changes[1].after.clone().unwrap();
    for field in [
        "request",
        "expiry",
        "blocks",
        "fee_index",
        "version",
        "mint_url",
        "method",
    ] {
        let mut wrong = quote.clone();
        match field {
            "request" => wrong.request.push('x'),
            "expiry" => wrong.expiry += 1,
            "blocks" => wrong.estimated_blocks = None,
            "fee_index" => wrong.fee_index = None,
            "version" => wrong.version += 1,
            "mint_url" => wrong.mint_url = None,
            "method" => wrong.payment_method = cashu::PaymentMethod::Custom("different".to_owned()),
            _ => unreachable!(),
        }
        assert!(!db.acquire_melt(&saga, &wrong).await.unwrap(), "{field}");
    }
    db.update_proofs(vec![external.clone()], vec![])
        .await
        .unwrap();
    assert!(!db.acquire_melt(&saga, &quote).await.unwrap());
    assert_eq!(
        db.get_proofs_by_ys(vec![local.y]).await.unwrap(),
        vec![local.clone()]
    );
    assert_eq!(
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(quote.clone())
    );
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    db.update_proofs(vec![], vec![external.y]).await.unwrap();
    assert!(db.acquire_melt(&saga, &quote).await.unwrap());
    for change in changes {
        assert_eq!(
            db.get_proofs_by_ys(vec![change.y]).await.unwrap(),
            vec![change.after.clone().unwrap()]
        );
    }
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    let mut reserved = quote.clone();
    reserved.used_by_operation = Some(saga.id.to_string());
    assert!(
        db.add_melt_quote(quote.clone()).await.is_err(),
        "a stale quote must not release ownership"
    );
    reserved.version += 1;
    assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(reserved));
}

/// Invalid original ownership, missing journals and altered replacements fail closed.
pub async fn acquire_melt_invalid<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (quote, saga, local) = melt_acquisition_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    for field in [
        "version",
        "state",
        "kind",
        "quote_id",
        "amount",
        "unit",
        "missing",
        "empty",
        "duplicate",
        "owner",
        "local_owner",
        "local_state",
        "replacement",
        "external_origin",
        "external_index",
        "submitted",
    ] {
        let mut invalid = saga.clone();
        let OperationData::Melt(data) = &mut invalid.data else {
            unreachable!()
        };
        let changes = data.acquisition.as_mut().unwrap();
        match field {
            "version" => invalid.version += 1,
            "state" => invalid.state = WalletSagaState::Melt(MeltSagaState::MeltRequested),
            "kind" => invalid.kind = crate::wallet::OperationKind::Swap,
            "quote_id" => invalid.quote_id = None,
            "amount" => data.amount = 1.into(),
            "unit" => invalid.unit = CurrencyUnit::Msat,
            "missing" => data.acquisition = None,
            "empty" => changes.clear(),
            "duplicate" => changes.push(changes[0].clone()),
            "owner" => changes[0].after.as_mut().unwrap().used_by_operation = None,
            "local_owner" => {
                changes[0].before.as_mut().unwrap().used_by_operation = Some(uuid::Uuid::new_v4())
            }
            "local_state" => changes[0].before.as_mut().unwrap().state = State::Pending,
            "replacement" => changes[0].after.as_mut().unwrap().proof.witness = None,
            "external_origin" => {
                changes[1].after.as_mut().unwrap().created_by_operation = Some(uuid::Uuid::new_v4())
            }
            "external_index" => changes[1].after.as_mut().unwrap().derivation_index = Some(4),
            "submitted" => data.final_proof_ys = Some(vec![local.y]),
            _ => unreachable!(),
        }
        assert!(db.acquire_melt(&invalid, &quote).await.is_err(), "{field}");
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
        assert_eq!(
            db.get_proofs_by_ys(vec![local.y]).await.unwrap(),
            vec![local.clone()]
        );
    }
}

/// Independent writers cannot acquire a shared quote or a shared proof through different quotes.
pub async fn acquire_melt_competing<D>(db: D)
where
    D: Database<Error> + Send + Sync + 'static,
{
    let db = std::sync::Arc::new(db);
    for shared_quote in [true, false] {
        let a_quote = test_melt_quote();
        let b_quote = if shared_quote {
            a_quote.clone()
        } else {
            test_melt_quote()
        };
        let a_proof = test_proof_info(test_keyset_id(), 2048, test_mint_url());
        let b_proof = if shared_quote {
            test_proof_info(test_keyset_id(), 2048, test_mint_url())
        } else {
            a_proof.clone()
        };
        db.add_melt_quote(a_quote.clone()).await.unwrap();
        if !shared_quote {
            db.add_melt_quote(b_quote.clone()).await.unwrap();
        }
        db.update_proofs(vec![a_proof.clone(), b_proof.clone()], vec![])
            .await
            .unwrap();
        let a = intent(&a_quote, a_proof.clone());
        let b = intent(&b_quote, b_proof.clone());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let start = |saga: WalletSaga, quote: MeltQuote| {
            let db = db.clone();
            let barrier = barrier.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                runtime.block_on(db.acquire_melt(&saga, &quote))
            })
        };
        let x = start(a.clone(), a_quote.clone());
        let y = start(b.clone(), b_quote.clone());
        let (x, y) = (x.await.unwrap(), y.await.unwrap());
        assert_eq!(
            usize::from(matches!(x, Ok(true))) + usize::from(matches!(y, Ok(true))),
            1
        );
        let (winner, loser, winning_quote, losing_quote, losing_proof) = if matches!(x, Ok(true)) {
            (&a, &b, &a_quote, &b_quote, &b_proof)
        } else {
            (&b, &a, &b_quote, &a_quote, &a_proof)
        };
        assert_eq!(db.get_saga(&winner.id).await.unwrap(), Some(winner.clone()));
        assert!(db.get_saga(&loser.id).await.unwrap().is_none());
        assert_eq!(
            db.get_melt_quote(&winning_quote.id)
                .await
                .unwrap()
                .unwrap()
                .used_by_operation,
            Some(winner.id.to_string())
        );
        if shared_quote {
            assert_eq!(
                db.get_proofs_by_ys(vec![losing_proof.y]).await.unwrap(),
                vec![losing_proof.clone()]
            );
        } else {
            assert_eq!(
                db.get_melt_quote(&losing_quote.id).await.unwrap(),
                Some(losing_quote.clone())
            );
        }
    }
}

/// Exhausted quote versions must fail before any reservation is written.
pub async fn acquire_melt_exhausted_version<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (mut quote, saga, local) = melt_acquisition_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    let stored = quote.clone();
    quote.version = u32::MAX;
    assert!(db.acquire_melt(&saga, &quote).await.is_err());
    assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(stored));
    assert_eq!(
        db.get_proofs(None, None, None, None).await.unwrap(),
        vec![local]
    );
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
}

/// A quote with contradictory payment evidence cannot be acquired for a new payment.
pub async fn acquire_melt_payment_evidence<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (mut quote, saga, local) = melt_acquisition_fixture();
    quote.payment_proof = Some("retained-payment-proof".to_owned());
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    let stored = quote.clone();
    for state in [MeltQuoteState::Unpaid, MeltQuoteState::Failed] {
        quote.state = state;
        assert!(db.acquire_melt(&saga, &quote).await.is_err());
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        assert_eq!(
            db.get_proofs(None, None, None, None).await.unwrap(),
            vec![local.clone()]
        );
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(stored.clone())
        );
    }
}
