use cdk_common::database::wallet::{issue_receipts, test::issue_acquisition_fixture};
use cdk_common::database::WalletDatabase;

use super::test_db::Fixture;

#[tokio::test]
async fn issue_acquisition_write_failures_roll_back_every_quote() {
    let fixture = Fixture::new().await;
    let db = &fixture.db;
    let (keyset, quotes, saga) = issue_acquisition_fixture();
    fixture
        .db
        .add_mint(saga.mint_url.clone(), None)
        .await
        .unwrap();
    for quote in &quotes {
        db.add_mint_quote(quote.clone()).await.unwrap();
    }
    for (table, event, condition) in [
        (
            "mint_quote",
            "UPDATE",
            format!("WHEN OLD.id='{}'", quotes[1].id),
        ),
        ("wallet_sagas", "INSERT", String::new()),
        ("transactions", "INSERT", String::new()),
    ] {
        fixture.raw.execute_batch(&format!("CREATE TRIGGER fail_issue BEFORE {event} ON {table} {condition} BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;")).unwrap();
        assert!(db
            .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
            .await
            .is_err());
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        assert!(db.get_keys(&keyset.id).await.unwrap().is_none());
        assert!(db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
        let counters: u64 = fixture
            .raw
            .query_row("SELECT COUNT(*) FROM keyset_counter", [], |row| row.get(0))
            .unwrap();
        assert_eq!(counters, 0, "failed issue must roll back the counter row");
        assert!(db
            .list_transactions(None, None, None)
            .await
            .unwrap()
            .is_empty());
        for quote in &quotes {
            assert_eq!(
                db.get_mint_quote(&quote.id).await.unwrap(),
                Some(quote.clone())
            );
        }
        fixture
            .raw
            .execute_batch("DROP TRIGGER fail_issue;")
            .unwrap();
    }
    assert!(db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .unwrap());
    assert_eq!(db.increment_keyset_counter(&keyset.id, 0).await.unwrap(), 2);
    assert_eq!(db.get_keys(&keyset.id).await.unwrap(), Some(keyset.keys));
    for receipt in issue_receipts(&saga).unwrap() {
        assert_eq!(
            db.get_transaction(receipt.id()).await.unwrap(),
            Some(receipt)
        );
    }
}

#[tokio::test]
async fn issue_acquisition_rejects_corrupt_quotes_and_existing_receipts() {
    let fixture = Fixture::new().await;
    let (keyset, quotes, saga) = issue_acquisition_fixture();
    fixture
        .db
        .add_mint(saga.mint_url.clone(), None)
        .await
        .unwrap();
    for quote in &quotes {
        fixture.db.add_mint_quote(quote.clone()).await.unwrap();
    }
    fixture
        .raw
        .execute(
            "UPDATE mint_quote SET secret_key='invalid' WHERE id=?1",
            [&quotes[1].id],
        )
        .unwrap();
    assert!(fixture
        .db
        .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
        .await
        .is_err());
    assert!(fixture.db.get_saga(&saga.id).await.unwrap().is_none());
    let reserved: u32 = fixture
        .raw
        .query_row(
            "SELECT COUNT(*) FROM mint_quote WHERE used_by_operation IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reserved, 0);

    fixture
        .raw
        .execute(
            "UPDATE mint_quote SET secret_key=?1 WHERE id=?2",
            rusqlite::params![
                quotes[1].secret_key.as_ref().unwrap().to_secret_hex(),
                quotes[1].id
            ],
        )
        .unwrap();
    let receipt = issue_receipts(&saga).unwrap().remove(0);
    for metadata in ["{}".to_owned(), "x".repeat(4 * 1024 * 1024)] {
        fixture.db.add_transaction(receipt.clone()).await.unwrap();
        fixture
            .raw
            .execute(
                "UPDATE transactions SET metadata=?1 WHERE id=?2",
                rusqlite::params![metadata, receipt.id().as_slice()],
            )
            .unwrap();
        let rejected = fixture
            .db
            .acquire_issue(&saga, &quotes, keyset.clone(), 0, 2)
            .await;
        assert!(
            matches!(rejected, Ok(false)),
            "existing receipt rejection must not decode its payload: {rejected:?}"
        );
        let retained: bool = fixture
            .raw
            .query_row(
                "SELECT metadata=?1 FROM transactions WHERE id=?2",
                rusqlite::params![metadata, receipt.id().as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(retained);
        assert!(fixture.db.get_saga(&saga.id).await.unwrap().is_none());
        assert!(fixture.db.get_keys(&keyset.id).await.unwrap().is_none());
        let counters: u32 = fixture
            .raw
            .query_row("SELECT COUNT(*) FROM keyset_counter", [], |row| row.get(0))
            .unwrap();
        assert_eq!(counters, 0);
        for quote in &quotes {
            assert_eq!(
                fixture.db.get_mint_quote(&quote.id).await.unwrap(),
                Some(quote.clone())
            );
        }
    }
}
