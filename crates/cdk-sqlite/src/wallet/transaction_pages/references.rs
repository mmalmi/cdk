//! Ownership-only projections retain global ordering and receipt byte bounds.
use super::*;

#[tokio::test]
async fn reference_pages_cross_scopes_without_decoding_unrelated_receipts() {
    let fixture = Fixture::new().await;
    let mut records: Vec<_> = (0..129).map(transaction).collect();
    let last = records.last_mut().unwrap();
    last.mint_url = "https://foreign.example".parse().unwrap();
    last.unit = CurrencyUnit::Msat;
    last.direction = TransactionDirection::Outgoing;
    for tx in &records {
        fixture.db.add_transaction(tx.clone()).await.unwrap();
    }
    fixture
        .raw
        .execute("UPDATE transactions SET metadata='{invalid'", [])
        .unwrap();
    let mut nonpoint = vec![0xffu8; 33];
    nonpoint[0] = 2;
    fixture
        .raw
        .execute(
            "UPDATE transactions SET ys=?1 WHERE id=?2",
            rusqlite::params![nonpoint, records[0].id().as_slice()],
        )
        .unwrap();
    records.sort_by_key(|tx| (tx.timestamp, tx.id()));
    let first = fixture
        .db
        .get_transaction_proof_references_page(None, 128)
        .await
        .unwrap();
    assert_eq!(first.len(), 128);
    assert_eq!(
        first.iter().map(|row| row.cursor).collect::<Vec<_>>(),
        records[..128].iter().map(cursor).collect::<Vec<_>>()
    );
    let after = first.last().unwrap().cursor;
    fixture.db.remove_transaction(after.id).await.unwrap();
    let second = fixture
        .db
        .get_transaction_proof_references_page(Some(after), 128)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].cursor, cursor(records.last().unwrap()));
    assert!(fixture
        .db
        .get_transaction_proof_references_page(Some(second[0].cursor), 128)
        .await
        .unwrap()
        .is_empty());
    // Full APIs keep their metadata/key validation; the projection grants no
    // authority to use any of these malformed receipts as ownership evidence.
    assert!(fixture
        .db
        .get_transactions_page(None, None, None, None, 128)
        .await
        .is_err());
}

#[tokio::test]
async fn reference_pages_reject_invalid_cursor_types_and_key_framing() {
    use rusqlite::types::Value;

    let fixture = Fixture::new().await;
    let tx = transaction(0);
    for limit in [0, MAX_TRANSACTION_PAGE_SIZE + 1] {
        assert!(fixture
            .db
            .get_transaction_proof_references_page(None, limit)
            .await
            .is_err());
    }
    let mut after = cursor(&tx);
    after.timestamp = u64::MAX;
    assert!(fixture
        .db
        .get_transaction_proof_references_page(Some(after), 1)
        .await
        .is_err());
    for (column, invalid) in [
        ("id", Value::Null),
        ("id", Value::Blob(vec![0; 31])),
        ("timestamp", Value::Text("invalid".to_owned())),
        ("timestamp", Value::Integer(-1)),
        ("ys", Value::Text("02".repeat(33))),
        ("ys", Value::Blob(vec![2; 34])),
        ("ys", Value::Blob(vec![0; 33])),
    ] {
        fixture.db.add_transaction(tx.clone()).await.unwrap();
        fixture
            .raw
            .execute(&format!("UPDATE transactions SET {column}=?1"), [invalid])
            .unwrap();
        assert!(
            fixture
                .db
                .get_transaction_proof_references_page(None, 1)
                .await
                .is_err(),
            "invalid {column}"
        );
        fixture.raw.execute("DELETE FROM transactions", []).unwrap();
    }
}
