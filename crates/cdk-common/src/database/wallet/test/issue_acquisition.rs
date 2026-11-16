use super::*;
use crate::wallet::{IssueSagaState, MintCheckpoint, MintOperationData, MintRequestData};

/// Complete paid quotes and their original partitioned issue intent.
pub fn issue_acquisition_fixture() -> (KeySet, Vec<MintQuote>, WalletSaga) {
    let quotes: Vec<_> = [8, 16]
        .into_iter()
        .map(|amount| {
            let mut quote = test_mint_quote(test_mint_url());
            quote.amount = Some(amount.into());
            quote.amount_paid = amount.into();
            quote.state = MintQuoteState::Paid;
            quote.secret_key = Some(SecretKey::generate());
            quote.estimated_blocks = Some(6);
            quote.version = 3;
            quote
        })
        .collect();
    let data = batch_data(&quotes);
    let saga = WalletSaga::new(
        uuid::Uuid::new_v4(),
        WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
        24.into(),
        test_mint_url(),
        CurrencyUnit::Sat,
        OperationData::Mint(data),
    );
    (super::output_preparation::keys(), quotes, saga)
}

fn batch_data(quotes: &[MintQuote]) -> MintOperationData {
    let amounts: Vec<_> = quotes.iter().map(MintQuote::amount_mintable).collect();
    let mut seed = [0; 64];
    seed[..32].copy_from_slice(&SecretKey::generate().to_secret_bytes());
    let keyset = super::output_preparation::keys();
    let mut premint_secrets = crate::PreMintSecrets::new(keyset.id);
    for (index, amount) in amounts.iter().enumerate() {
        let quote_outputs = crate::PreMintSecrets::from_seed(
            keyset.id,
            index as u32,
            &seed,
            *amount,
            &crate::amount::SplitTarget::None,
            &(0, vec![8, 16]).into(),
        )
        .unwrap();
        premint_secrets.secrets.extend(quote_outputs.secrets);
    }
    let mut data = MintOperationData::new_partitioned_batch(
        quotes.iter().map(|q| q.id.clone()).collect(),
        Amount::try_sum(amounts.iter().copied()).unwrap(),
        vec![1; quotes.len()],
        amounts.clone(),
    );
    data.checkpoint = Some(Box::new(MintCheckpoint {
        quotes: quotes.to_vec(),
        payment_method: quotes[0].payment_method.clone(),
        request: MintRequestData::Batch {
            request: crate::BatchMintRequest {
                quotes: data.quote_ids(),
                quote_amounts: Some(amounts),
                outputs: premint_secrets.blinded_messages(),
                signatures: None,
            },
            legacy_signatures: None,
        },
        premint_secrets,
    }));
    data
}

async fn unchanged<D>(db: &D, quotes: &[MintQuote], saga: &WalletSaga)
where
    D: Database<Error> + Sync,
{
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    for quote in quotes {
        assert_eq!(
            db.get_mint_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
    }
}

/// Conflicts cannot partially reserve a batch or overwrite an existing operation.
pub async fn issue_acquisition_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (keyset, quotes, saga) = issue_acquisition_fixture();
    db.add_mint(saga.mint_url.clone(), None).await.unwrap();
    db.add_mint_quote(quotes[0].clone()).await.unwrap();
    assert!(!db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    unchanged(&db, &quotes[..1], &saga).await;
    db.add_mint_quote(quotes[1].clone()).await.unwrap();
    for field in ["request", "version", "secret", "paid", "issued", "blocks"] {
        let mut stale = quotes.clone();
        match field {
            "request" => stale[1].request.push('x'),
            "version" => stale[1].version += 1,
            "secret" => stale[1].secret_key = None,
            "paid" => stale[1].amount_paid += Amount::from(1),
            "issued" => {
                stale[1].amount_paid += Amount::from(1);
                stale[1].amount_issued += Amount::from(1);
            }
            "blocks" => stale[1].estimated_blocks = None,
            _ => unreachable!(),
        }
        let mut stale_saga = saga.clone();
        let OperationData::Mint(data) = &mut stale_saga.data else {
            unreachable!()
        };
        data.checkpoint.as_mut().unwrap().quotes = stale.clone();
        assert!(
            !db.acquire_issue(&stale_saga, &stale, keyset.clone(), 0, 2)
                .await
                .unwrap(),
            "{field}"
        );
        unchanged(&db, &quotes, &saga).await;
    }
    db.add_saga(saga.clone()).await.unwrap();
    assert!(!db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    db.delete_saga(&saga.id).await.unwrap();
    unchanged(&db, &quotes, &saga).await;
    let receipts = issue_receipts(&saga).unwrap();
    db.add_transaction(receipts[1].clone()).await.unwrap();
    assert!(!db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    unchanged(&db, &quotes, &saga).await;
    assert!(db
        .get_transaction(receipts[0].id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        db.get_transaction(receipts[1].id()).await.unwrap(),
        Some(receipts[1].clone())
    );
    db.remove_transaction(receipts[1].id()).await.unwrap();
    assert!(!db
        .acquire_issue(&saga, &quotes, keyset.clone(), 1, 2)
        .await
        .unwrap());
    unchanged(&db, &quotes, &saga).await;
    assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
    assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 0);
    assert!(db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    assert!(!db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 2);
    assert_eq!(
        db.get_keys(&keyset.id).await.unwrap(),
        Some(keyset.keys.clone())
    );
    for receipt in receipts {
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(receipt)
        );
    }
    for mut quote in quotes {
        quote.version += 1;
        quote.used_by_operation = Some(saga.id.to_string());
        assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(quote));
    }
}

/// Malformed or exhausted authority is rejected before changing any quote.
pub async fn issue_acquisition_invalid<D>(db: D)
where
    D: Database<Error> + Sync,
{
    for case in [
        "duplicate",
        "partition",
        "amount",
        "scope",
        "owner",
        "version",
        "requested",
        "request",
        "secrets",
    ] {
        let (keyset, mut quotes, mut saga) = issue_acquisition_fixture();
        match case {
            "duplicate" => quotes[1] = quotes[0].clone(),
            "partition" => {
                let OperationData::Mint(data) = &mut saga.data else {
                    unreachable!()
                };
                data.batch_output_counts = Some(vec![2, 0]);
            }
            "amount" => saga.amount += Amount::from(1),
            "scope" => quotes[1].unit = CurrencyUnit::Msat,
            "owner" => quotes[1].used_by_operation = Some(uuid::Uuid::new_v4().to_string()),
            "version" => quotes[1].version = u32::MAX,
            "requested" => saga.state = WalletSagaState::Issue(IssueSagaState::MintRequested),
            "request" => {
                let OperationData::Mint(data) = &mut saga.data else {
                    unreachable!()
                };
                let MintRequestData::Batch { request, .. } =
                    &mut data.checkpoint.as_mut().unwrap().request
                else {
                    unreachable!()
                };
                request.quotes.reverse();
            }
            "secrets" => {
                let OperationData::Mint(data) = &mut saga.data else {
                    unreachable!()
                };
                let secrets = &mut data.checkpoint.as_mut().unwrap().premint_secrets.secrets;
                secrets[1].secret = secrets[0].secret.clone();
            }
            _ => unreachable!(),
        }
        let OperationData::Mint(data) = &mut saga.data else {
            unreachable!()
        };
        data.checkpoint.as_mut().unwrap().quotes = quotes.clone();
        let mut inserted = std::collections::HashSet::new();
        for quote in &quotes {
            if inserted.insert(&quote.id) {
                db.add_mint_quote(quote.clone()).await.unwrap();
            }
        }
        assert!(
            db.acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
                .await
                .is_err(),
            "{case}"
        );
        unchanged(&db, &quotes, &saga).await;
    }
}

/// Reversed concurrent batches still have one complete owner and one journal.
pub async fn issue_acquisition_concurrent<D>(db: D)
where
    D: Database<Error> + Sync,
{
    let (keyset, quotes, first) = issue_acquisition_fixture();
    db.add_mint(first.mint_url.clone(), None).await.unwrap();
    for quote in &quotes {
        db.add_mint_quote(quote.clone()).await.unwrap();
    }
    let mut second = first.clone();
    second.id = uuid::Uuid::new_v4();
    let reversed: Vec<_> = quotes.iter().cloned().rev().collect();
    second.quote_id = Some(reversed[0].id.clone());
    second.data = OperationData::Mint(batch_data(&reversed));
    validate_issue_acquisition(&first, &quotes).unwrap();
    validate_issue_acquisition(&second, &reversed).unwrap();
    let (a, b) = tokio::join!(
        db.acquire_issue(&first, &quotes, keyset.clone(), 0, 2),
        db.acquire_issue(&second, &reversed, keyset.clone(), 0, 2)
    );
    assert_eq!(
        usize::from(matches!(a, Ok(true))) + usize::from(matches!(b, Ok(true))),
        1
    );
    let (winner, loser) = if matches!(a, Ok(true)) {
        (&first, &second)
    } else {
        (&second, &first)
    };
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 2);
    assert_eq!(db.get_saga(&winner.id).await.unwrap(), Some(winner.clone()));
    assert!(db.get_saga(&loser.id).await.unwrap().is_none());
    for quote in quotes {
        assert_eq!(
            db.get_mint_quote(&quote.id)
                .await
                .unwrap()
                .unwrap()
                .used_by_operation,
            Some(winner.id.to_string())
        );
    }
}
