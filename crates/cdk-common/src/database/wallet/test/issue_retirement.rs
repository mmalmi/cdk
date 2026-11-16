use super::*;
use crate::wallet::IssueSagaState;

/// Owned two-quote issue and its canonical pending receipts, shared by rollback tests.
pub fn issue_retirement_fixture() -> (Vec<MintQuote>, WalletSaga, Vec<Transaction>) {
    let (_, mut quotes, saga) = issue_acquisition_fixture();
    let receipts = issue_receipts(&saga).unwrap();
    for quote in &mut quotes {
        quote.version += 1;
        quote.used_by_operation = Some(saga.id.to_string());
    }
    (quotes, saga, receipts)
}

/// Original output changes for backend transaction tests; cryptographic verification is native.
pub fn issue_retirement_proofs(saga: &WalletSaga) -> Vec<ProofChange> {
    let OperationData::Mint(data) = &saga.data else {
        unreachable!()
    };
    data.checkpoint
        .as_ref()
        .unwrap()
        .premint_secrets
        .secrets
        .iter()
        .map(|secret| {
            let mut proof = test_proof(secret.blinded_message.keyset_id, secret.amount.into());
            proof.secret = secret.secret.clone();
            let mut info = ProofInfo::new(
                proof,
                saga.mint_url.clone(),
                State::Unspent,
                saga.unit.clone(),
            )
            .unwrap();
            info.created_by_operation = Some(saga.id);
            info.derivation_index = secret.derivation_index;
            ProofChange {
                y: info.y,
                before: None,
                after: Some(info),
            }
        })
        .collect()
}

async fn save<D>(db: &D, quotes: &[MintQuote], saga: &WalletSaga)
where
    D: Database<Error> + Sync,
{
    for quote in quotes {
        db.add_mint_quote(quote.clone()).await.unwrap();
    }
    db.add_saga(saga.clone()).await.unwrap();
}

async fn retained<D>(db: &D, quotes: &[MintQuote], saga: &WalletSaga)
where
    D: Database<Error> + Sync,
{
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    for quote in quotes {
        assert_eq!(
            db.get_mint_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
    }
}

/// Cancellation changes only its pending receipts; completion requires every exact receipt.
pub async fn issue_retirement_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    for submitted in [false, true] {
        let (mut quotes, mut saga, mut receipts) = issue_retirement_fixture();
        if submitted {
            saga.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
            receipts[0].status = TransactionStatus::Completed;
        }
        let mut proofs = submitted.then(|| issue_retirement_proofs(&saga));
        save(&db, &quotes, &saga).await;
        db.add_transaction(receipts[0].clone()).await.unwrap();
        let mut foreign = quotes[0].clone();
        foreign.id = unique_id();
        foreign.mint_url = test_mint_url_2();
        db.add_mint_quote(foreign.clone()).await.unwrap();
        if submitted {
            assert!(!db
                .retire_issue(&saga, &quotes, proofs.clone())
                .await
                .unwrap());
            retained(&db, &quotes, &saga).await;
            receipts[1].status = TransactionStatus::Failed;
            db.add_transaction(receipts[1].clone()).await.unwrap();
            assert!(!db
                .retire_issue(&saga, &quotes, proofs.clone())
                .await
                .unwrap());
            receipts[1].status = TransactionStatus::Pending;
            let mut wrong = receipts[1].clone();
            wrong.amount += Amount::from(1);
            db.add_transaction(wrong.clone()).await.unwrap();
            assert!(!db
                .retire_issue(&saga, &quotes, proofs.clone())
                .await
                .unwrap());
            assert_eq!(db.get_transaction(wrong.id()).await.unwrap(), Some(wrong));
            retained(&db, &quotes, &saga).await;
            db.add_transaction(receipts[1].clone()).await.unwrap();

            let mut reserved = proofs.as_ref().unwrap()[0].after.as_ref().unwrap().clone();
            reserved.state = State::Pending;
            reserved.used_by_operation = Some(uuid::Uuid::new_v4());
            db.update_proofs(vec![reserved.clone()], vec![])
                .await
                .unwrap();
            assert!(!db
                .retire_issue(&saga, &quotes, proofs.clone())
                .await
                .unwrap());
            retained(&db, &quotes, &saga).await;
            let stored = db.get_proofs(None, None, None, None).await.unwrap();
            assert!(stored == vec![reserved.clone()]);
            assert_eq!(
                db.get_transaction(receipts[1].id()).await.unwrap(),
                Some(receipts[1].clone())
            );
            let first = &mut proofs.as_mut().unwrap()[0];
            first.before = Some(reserved.clone());
            first.after = Some(reserved);

            // A status refresh may observe issuance and newer deposits before completion.
            for (quote, receipt) in quotes.iter_mut().zip(&receipts) {
                quote.amount_paid += Amount::from(5);
                quote.amount_issued = receipt.amount;
                quote.updated_at += 1;
                db.add_mint_quote(quote.clone()).await.unwrap();
                *quote = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
            }
        }
        let mut stale = quotes.clone();
        stale[1].version += 1;
        assert!(!db
            .retire_issue(&saga, &stale, proofs.clone())
            .await
            .unwrap());
        retained(&db, &quotes, &saga).await;
        assert!(db
            .retire_issue(&saga, &quotes, proofs.clone())
            .await
            .unwrap());
        assert!(!db
            .retire_issue(&saga, &quotes, proofs.clone())
            .await
            .unwrap());
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        for (mut quote, receipt) in quotes.into_iter().zip(&receipts) {
            quote.used_by_operation = None;
            quote.version += 1;
            if submitted {
                quote.amount_issued = receipt.amount;
                quote.state = MintQuoteState::Issued;
            }
            assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
        }
        assert_eq!(db.get_mint_quote(&foreign.id).await.unwrap(), Some(foreign));
        if !submitted {
            receipts[0].status = TransactionStatus::Failed;
        } else {
            receipts[1].status = TransactionStatus::Completed;
            for proof in proofs.unwrap() {
                let stored = db.get_proofs_by_ys(vec![proof.y]).await.unwrap();
                assert!(stored == vec![proof.after.unwrap()]);
            }
        }
        assert_eq!(
            db.get_transaction(receipts[0].id()).await.unwrap(),
            Some(receipts[0].clone())
        );
        assert_eq!(
            db.get_transaction(receipts[1].id()).await.unwrap(),
            submitted.then(|| receipts[1].clone())
        );
    }
}

/// A submission and cancellation cannot both win the same prepared journal.
pub async fn issue_retirement_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (quotes, saga, receipts) = issue_retirement_fixture();
    save(&db, &quotes, &saga).await;
    for receipt in &receipts {
        db.add_transaction(receipt.clone()).await.unwrap();
    }
    let mut submitted = saga.clone();
    submitted.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
    let (retired, requested) = tokio::join!(
        db.retire_issue(&saga, &quotes, None),
        db.update_saga(submitted.clone())
    );
    assert_eq!(
        usize::from(matches!(retired, Ok(true))) + usize::from(matches!(requested, Ok(true))),
        1
    );
    if matches!(retired, Ok(true)) {
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        for quote in &quotes {
            assert!(db
                .get_mint_quote(&quote.id)
                .await
                .unwrap()
                .unwrap()
                .used_by_operation
                .is_none());
        }
    } else {
        retained(&db, &quotes, &submitted).await;
    }
    for mut receipt in receipts {
        if matches!(retired, Ok(true)) {
            receipt.status = TransactionStatus::Failed;
        }
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(receipt)
        );
    }
}

/// Exhausted quote versions and receipt conflicts cannot erase original authority.
pub async fn issue_retirement_invalid<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (mut quotes, saga, _) = issue_retirement_fixture();
    quotes[1].version = u32::MAX;
    save(&db, &quotes, &saga).await;
    assert!(db.retire_issue(&saga, &quotes, None).await.is_err());
    retained(&db, &quotes, &saga).await;
    let (quotes, saga, _) = issue_retirement_fixture();
    save(&db, &quotes, &saga).await;
    let mut receipts = issue_retirement_fixture().2;
    // An unrelated transaction at the canonical identity must not authorize cleanup.
    receipts[0].saga_id = Some(saga.id);
    receipts[0]
        .metadata
        .insert("batch_quote_id".to_owned(), quotes[0].id.clone());
    db.add_transaction(receipts[0].clone()).await.unwrap();
    assert!(!db.retire_issue(&saga, &quotes, None).await.unwrap());
    retained(&db, &quotes, &saga).await;
}
