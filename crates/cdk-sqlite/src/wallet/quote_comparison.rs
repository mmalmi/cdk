//! Atomic quote comparisons bound unexpected stored growth and retain every owner.
use cdk_common::database::wallet::test::{
    issue_acquisition_fixture, issue_retirement_fixture, melt_acquisition_fixture,
    melt_completion_fixture,
};
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::wallet::OperationData;

use super::test_db::Fixture;

fn grow(f: &Fixture, table: &str, column: &str, id: &str) -> rusqlite::types::Value {
    let original = f
        .raw
        .query_row(
            &format!("SELECT {column} FROM {table} WHERE id=?1"),
            [id],
            |r| r.get(0),
        )
        .unwrap();
    // Character counts and NUL-terminated lengths would miss this byte overflow.
    let oversized = format!("\0{}", "é".repeat(2 * 1024 * 1024));
    f.raw
        .execute(
            &format!("UPDATE {table} SET {column}=?1 WHERE id=?2"),
            rusqlite::params![oversized, id],
        )
        .unwrap();
    original
}

fn retained_and_restore(
    f: &Fixture,
    table: &str,
    column: &str,
    id: &str,
    original: rusqlite::types::Value,
) {
    let bytes: u64 = f
        .raw
        .query_row(
            &format!("SELECT length(CAST({column} AS BLOB)) FROM {table} WHERE id=?1"),
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bytes, 4 * 1024 * 1024 + 1);
    f.raw
        .execute(
            &format!("UPDATE {table} SET {column}=?1 WHERE id=?2"),
            rusqlite::params![original, id],
        )
        .unwrap();
}

fn bounded(result: Result<bool, Error>) {
    assert!(
        matches!(result, Err(Error::QuoteTooLarge { bytes, limit }) if bytes > limit && limit >= 4 * 1024 * 1024),
        "quote comparison must reject stored growth before decoding: {result:?}"
    );
}

#[tokio::test]
async fn mint_quote_comparison_preserves_acquisition_and_retirement() {
    for retiring in [false, true] {
        let f = Fixture::new().await;
        let (keyset, mut quotes, mut saga) = issue_acquisition_fixture();
        let mut receipts = Vec::new();
        if retiring {
            (quotes, saga, receipts) = issue_retirement_fixture();
            f.db.add_saga(saga.clone()).await.unwrap();
            for receipt in &receipts {
                f.db.add_transaction(receipt.clone()).await.unwrap();
            }
        }
        f.db.add_mint(saga.mint_url.clone(), None).await.unwrap();
        for quote in &quotes {
            f.db.add_mint_quote(quote.clone()).await.unwrap();
        }
        for column in ["request", "secret_key", "expiry"] {
            let original = grow(&f, "mint_quote", column, &quotes[1].id);
            bounded(if retiring {
                f.db.retire_issue(&saga, &quotes, None).await
            } else {
                f.db.acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
                    .await
            });
            assert_eq!(
                f.db.get_saga(&saga.id).await.unwrap(),
                retiring.then(|| saga.clone())
            );
            assert!(f.db.get_keys(&keyset.id).await.unwrap().is_none());
            for receipt in &receipts {
                assert_eq!(
                    f.db.get_transaction(receipt.id()).await.unwrap(),
                    Some(receipt.clone())
                );
            }
            retained_and_restore(&f, "mint_quote", column, &quotes[1].id, original);
            for quote in &quotes {
                assert_eq!(
                    f.db.get_mint_quote(&quote.id).await.unwrap(),
                    Some(quote.clone())
                );
            }
        }
        assert!(if retiring {
            f.db.retire_issue(&saga, &quotes, None).await.unwrap()
        } else {
            f.db.acquire_issue(&saga, &quotes, keyset, 0, 2)
                .await
                .unwrap()
        });
    }
}

#[tokio::test]
async fn melt_quote_comparison_preserves_acquisition_and_completion() {
    for completing in [false, true] {
        let f = Fixture::new().await;
        let (mut quote, mut saga, local) = melt_acquisition_fixture();
        let (completed_saga, next, completed_quote, pending) = melt_completion_fixture();
        if completing {
            (quote, saga) = (completed_quote, completed_saga);
            f.db.add_saga(saga.clone()).await.unwrap();
            f.db.add_transaction(pending.clone()).await.unwrap();
        }
        f.db.add_melt_quote(quote.clone()).await.unwrap();
        f.db.update_proofs(vec![local.clone()], vec![])
            .await
            .unwrap();
        for column in ["request", "payment_proof", "fee_index"] {
            let original = grow(&f, "melt_quote", column, &quote.id);
            bounded(if completing {
                f.db.complete_melt(&saga, next.clone(), &quote).await
            } else {
                f.db.acquire_melt(&saga, &quote).await
            });
            assert_eq!(
                f.db.get_saga(&saga.id).await.unwrap(),
                completing.then(|| saga.clone())
            );
            assert_eq!(
                f.db.get_proofs(None, None, None, None).await.unwrap(),
                vec![local.clone()]
            );
            assert_eq!(
                f.db.get_transaction(pending.id()).await.unwrap(),
                completing.then(|| pending.clone())
            );
            retained_and_restore(&f, "melt_quote", column, &quote.id, original);
            assert_eq!(
                f.db.get_melt_quote(&quote.id).await.unwrap(),
                Some(quote.clone())
            );
        }
        assert!(if completing {
            f.db.complete_melt(&saga, next, &quote).await.unwrap()
        } else {
            f.db.acquire_melt(&saga, &quote).await.unwrap()
        });
    }
}

#[tokio::test]
async fn quote_comparison_keeps_large_caller_owned_originals_usable() {
    let f = Fixture::new().await;
    let (keyset, mut quotes, mut saga) = issue_acquisition_fixture();
    quotes[1].request = format!("\0{}", "é".repeat(2 * 1024 * 1024));
    let OperationData::Mint(data) = &mut saga.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().quotes = quotes.clone();
    f.db.add_mint(saga.mint_url.clone(), None).await.unwrap();
    for quote in &quotes {
        f.db.add_mint_quote(quote.clone()).await.unwrap();
    }
    f.raw
        .execute(
            "UPDATE mint_quote SET request=request || 'x' WHERE id=?1",
            [&quotes[1].id],
        )
        .unwrap();
    bounded(
        f.db.acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
            .await,
    );
    f.raw
        .execute(
            "UPDATE mint_quote SET request=?1 WHERE id=?2",
            rusqlite::params![quotes[1].request, quotes[1].id],
        )
        .unwrap();
    assert!(f
        .db
        .acquire_issue(&saga, &quotes, keyset, 0, 2)
        .await
        .unwrap());

    let (mut quote, saga, local) = melt_acquisition_fixture();
    quote.request = quotes[1].request.clone();
    f.db.add_melt_quote(quote.clone()).await.unwrap();
    f.db.update_proofs(vec![local], vec![]).await.unwrap();
    f.raw
        .execute(
            "UPDATE melt_quote SET request=request || 'x' WHERE id=?1",
            [&quote.id],
        )
        .unwrap();
    bounded(f.db.acquire_melt(&saga, &quote).await);
    f.raw
        .execute(
            "UPDATE melt_quote SET request=?1 WHERE id=?2",
            rusqlite::params![quote.request, quote.id],
        )
        .unwrap();
    assert!(f.db.acquire_melt(&saga, &quote).await.unwrap());
}

#[tokio::test]
async fn exact_quote_reads_bound_invalid_payloads_before_decoding() {
    let f = Fixture::new().await;
    let db = super::test_db::open(&f.path)
        .await
        .with_quote_read_limit(4 * 1024 * 1024);
    let (_, quotes, saga) = issue_acquisition_fixture();
    f.db.add_mint(saga.mint_url, None).await.unwrap();
    db.add_mint_quote(quotes[0].clone()).await.unwrap();
    let (quote, _, _) = melt_acquisition_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    for column in ["request", "secret_key", "expiry"] {
        let original = grow(&f, "mint_quote", column, &quotes[0].id);
        bounded(db.get_mint_quote(&quotes[0].id).await.map(|q| q.is_some()));
        retained_and_restore(&f, "mint_quote", column, &quotes[0].id, original);
        assert_eq!(
            db.get_mint_quote(&quotes[0].id).await.unwrap(),
            Some(quotes[0].clone())
        );
    }
    for column in ["request", "payment_proof", "fee_index"] {
        let original = grow(&f, "melt_quote", column, &quote.id);
        bounded(db.get_melt_quote(&quote.id).await.map(|q| q.is_some()));
        retained_and_restore(&f, "melt_quote", column, &quote.id, original);
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
    }
    assert!(db.get_mint_quote("absent").await.unwrap().is_none());
    assert!(db.get_melt_quote("absent").await.unwrap().is_none());
}

#[tokio::test]
async fn quote_admission_reserves_completion_before_changing_owners() {
    let f = Fixture::new().await;
    let db = super::test_db::open(&f.path)
        .await
        .with_quote_read_limit(4 * 1024 * 1024);
    let (mut quote, saga, input) = melt_acquisition_fixture();
    quote.request = "x".repeat(4 * 1024 * 1024 - 32 * 1024);
    bounded(db.add_melt_quote(quote.clone()).await.map(|_| true));
    assert!(f.db.get_melt_quote(&quote.id).await.unwrap().is_none());

    // An older or unconfigured writer can leave a readable quote without room
    // for its payment proof. Refuse acquisition before assigning any owner.
    f.db.add_melt_quote(quote.clone()).await.unwrap();
    f.db.update_proofs(vec![input.clone()], vec![])
        .await
        .unwrap();
    assert_eq!(
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(quote.clone())
    );
    bounded(db.acquire_melt(&saga, &quote).await);
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    assert_eq!(
        db.get_proofs(None, None, None, None).await.unwrap(),
        vec![input]
    );
    assert_eq!(
        db.get_melt_quote(&quote.id).await.unwrap(),
        Some(quote.clone())
    );

    quote.request = "small invoice".to_string();
    db.add_melt_quote(quote.clone()).await.unwrap();
    let quote = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    assert!(db.acquire_melt(&saga, &quote).await.unwrap());
    let owned = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    assert_eq!(owned.used_by_operation, Some(saga.id.to_string()));

    let (mut ready, mut completed, mut quote, mut pending) = melt_completion_fixture();
    let proof_bytes = cdk_common::database::wallet::MAX_MELT_PAYMENT_PROOF_BYTES;
    quote.request = "x".repeat(4 * 1024 * 1024 - proof_bytes - 512);
    pending.payment_request = Some(quote.request.clone());
    for saga in [&mut ready, &mut completed] {
        let OperationData::Melt(data) = &mut saga.data else {
            unreachable!()
        };
        let receipt = &mut data.payment.as_mut().unwrap().receipt;
        receipt.payment_request = Some(quote.request.clone());
        receipt.payment_proof = Some("\0".repeat(proof_bytes));
    }
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.add_saga(ready.clone()).await.unwrap();
    db.add_transaction(pending).await.unwrap();
    assert!(db.complete_melt(&ready, completed, &quote).await.unwrap());
    let paid = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
    assert_eq!(paid.state, cdk_common::MeltQuoteState::Paid);
    assert!(paid.used_by_operation.is_none());
    assert_eq!(paid.payment_proof.unwrap().len(), proof_bytes);

    let (keyset, mut quotes, mut issue) = issue_acquisition_fixture();
    quotes[1].request = "x".repeat(4 * 1024 * 1024);
    let OperationData::Mint(data) = &mut issue.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().quotes = quotes.clone();
    f.db.add_mint(issue.mint_url.clone(), None).await.unwrap();
    for quote in &quotes {
        f.db.add_mint_quote(quote.clone()).await.unwrap();
    }
    bounded(db.add_mint_quote(quotes[1].clone()).await.map(|_| true));
    bounded(
        db.acquire_issue(&issue, &quotes, keyset.clone(), 0, 2)
            .await,
    );
    assert!(db.get_saga(&issue.id).await.unwrap().is_none());
    assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
    for quote in &quotes {
        assert_eq!(
            f.db.get_mint_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
    }
}
