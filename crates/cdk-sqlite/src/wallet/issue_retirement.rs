use cdk_common::database::wallet::issue_receipts;
use cdk_common::database::wallet::test::{issue_retirement_fixture, issue_retirement_proofs};
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::wallet::{
    IssueSagaState, MintQuote, OperationData, Transaction, TransactionStatus, WalletSaga,
    WalletSagaState,
};

use super::test_db::Fixture;

async fn save(
    db: &super::WalletSqliteDatabase,
    quotes: &[MintQuote],
    saga: &WalletSaga,
    receipts: &[Transaction],
) {
    for quote in quotes {
        db.add_mint_quote(quote.clone()).await.unwrap();
    }
    db.add_saga(saga.clone()).await.unwrap();
    for receipt in receipts {
        db.add_transaction(receipt.clone()).await.unwrap();
    }
}

#[tokio::test]
async fn issue_retirement_write_failures_preserve_proofs_receipts_quotes_and_journal() {
    for submitted in [false, true] {
        let fixture = Fixture::new().await;
        let db = &fixture.db;
        let (quotes, mut saga, receipts) = issue_retirement_fixture();
        if submitted {
            saga.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
        }
        let proofs = submitted.then(|| issue_retirement_proofs(&saga));
        save(db, &quotes, &saga, &receipts).await;
        let mut faults = vec![
            ("transactions", "INSERT", String::new()),
            (
                "mint_quote",
                "UPDATE",
                format!("WHEN OLD.id='{}'", quotes[1].id),
            ),
            ("wallet_sagas", "DELETE", String::new()),
        ];
        if submitted {
            faults.push(("proof", "INSERT", String::new()));
        }
        for (table, event, condition) in faults {
            fixture.raw.execute_batch(&format!("CREATE TRIGGER fail_issue_retirement BEFORE {event} ON {table} {condition} BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;")).unwrap();
            assert!(db
                .retire_issue(&saga, &quotes, proofs.clone())
                .await
                .is_err());
            assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
            assert!(db
                .get_proofs(None, None, None, None)
                .await
                .unwrap()
                .is_empty());
            for quote in &quotes {
                assert_eq!(
                    db.get_mint_quote(&quote.id).await.unwrap(),
                    Some(quote.clone())
                );
            }
            for receipt in &receipts {
                assert_eq!(
                    db.get_transaction(receipt.id()).await.unwrap(),
                    Some(receipt.clone())
                );
            }
            fixture
                .raw
                .execute_batch("DROP TRIGGER fail_issue_retirement;")
                .unwrap();
        }
        assert!(db.retire_issue(&saga, &quotes, proofs).await.unwrap());
    }
}

#[tokio::test]
async fn receipt_reads_reject_unexpected_growth_without_releasing_issue_ownership() {
    for submitted in [false, true] {
        let fixture = Fixture::new().await;
        let db = &fixture.db;
        let (quotes, mut saga, receipts) = issue_retirement_fixture();
        if submitted {
            saga.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
        }
        let proofs = submitted.then(|| issue_retirement_proofs(&saga));
        save(db, &quotes, &saga, &receipts).await;
        let id = receipts[1].id();
        // Byte sizing must include data after NUL and multibyte characters,
        // even when a numeric or binary column contains malformed text.
        let oversized = format!("\0{}", "é".repeat(2 * 1024 * 1024));
        for column in ["memo", "metadata", "payment_request", "ys", "fee"] {
            let original: rusqlite::types::Value = fixture
                .raw
                .query_row(
                    &format!("SELECT {column} FROM transactions WHERE id=?1"),
                    [id.as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            fixture
                .raw
                .execute(
                    &format!("UPDATE transactions SET {column}=?1 WHERE id=?2"),
                    rusqlite::params![oversized, id.as_slice()],
                )
                .unwrap();
            let result = db.retire_issue(&saga, &quotes, proofs.clone()).await;
            assert!(
                matches!(result, Err(Error::TransactionPageTooLarge { bytes, limit })
                    if bytes > limit && limit == 4 * 1024 * 1024),
                "issue receipt read must reject stored growth before decoding: {result:?}"
            );
            assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
            assert!(db
                .get_proofs(None, None, None, None)
                .await
                .unwrap()
                .is_empty());
            for quote in &quotes {
                assert_eq!(
                    db.get_mint_quote(&quote.id).await.unwrap(),
                    Some(quote.clone())
                );
            }
            let retained: String = fixture
                .raw
                .query_row(
                    &format!("SELECT {column} FROM transactions WHERE id=?1"),
                    [id.as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, oversized);
            fixture
                .raw
                .execute(
                    &format!("UPDATE transactions SET {column}=?1 WHERE id=?2"),
                    rusqlite::params![original, id.as_slice()],
                )
                .unwrap();
            for receipt in &receipts {
                assert_eq!(
                    db.get_transaction(receipt.id()).await.unwrap(),
                    Some(receipt.clone())
                );
            }
        }
        assert!(db.retire_issue(&saga, &quotes, proofs).await.unwrap());
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn receipt_reads_preserve_large_canonical_originals_and_completed_receipts() {
    let fixture = Fixture::new().await;
    let db = &fixture.db;
    let (mut quotes, mut saga, _) = issue_retirement_fixture();
    let request = "é\0".repeat(2 * 1024 * 1024);
    quotes[0].request = request.clone();
    let OperationData::Mint(data) = &mut saga.data else {
        unreachable!()
    };
    data.checkpoint.as_mut().unwrap().quotes[0].request = request;
    saga.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
    let mut receipts = issue_receipts(&saga).unwrap();
    receipts[0].status = TransactionStatus::Completed;
    save(db, &quotes, &saga, &receipts).await;
    let proofs = Some(issue_retirement_proofs(&saga));
    assert!(db.retire_issue(&saga, &quotes, proofs).await.unwrap());
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    for mut receipt in receipts {
        receipt.status = TransactionStatus::Completed;
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(receipt)
        );
    }
}
