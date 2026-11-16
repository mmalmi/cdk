//! Indexed receipt reads: storage fixtures carry no spendable currency.
use std::collections::HashMap;
use std::str::FromStr;

use cdk_common::database::wallet::{
    TransactionCursor, MAX_TRANSACTION_PAGE_BYTES, MAX_TRANSACTION_PAGE_SIZE,
};
use cdk_common::database::WalletDatabase;
use cdk_common::mint_url::MintUrl;
use cdk_common::wallet::{Transaction, TransactionDirection, TransactionRole, TransactionStatus};
use cdk_common::{CurrencyUnit, SecretKey};

use super::test_db::{open, Fixture};
use super::WalletSqliteDatabase;

mod references;

fn mint() -> MintUrl {
    MintUrl::from_str("https://selected.example").unwrap()
}

fn transaction(index: u64) -> Transaction {
    Transaction {
        role: TransactionRole::Primary,
        mint_url: mint(),
        direction: TransactionDirection::Incoming,
        amount: 8.into(),
        fee: 1.into(),
        unit: CurrencyUnit::Sat,
        ys: vec![SecretKey::generate().public_key()],
        timestamp: 1_000 + index / 4,
        memo: Some(format!("receipt {index}")),
        metadata: HashMap::from([("fixture".to_owned(), index.to_string())]),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(uuid::Uuid::from_u128(u128::from(index) + 1)),
        status: TransactionStatus::Completed,
    }
}

fn cursor(transaction: &Transaction) -> TransactionCursor {
    TransactionCursor {
        timestamp: transaction.timestamp,
        id: transaction.id(),
    }
}

async fn page(
    db: &WalletSqliteDatabase,
    after: Option<TransactionCursor>,
    limit: u16,
) -> Result<Vec<Transaction>, cdk_common::database::Error> {
    db.get_transactions_page(
        Some(mint()),
        Some(TransactionDirection::Incoming),
        Some(CurrencyUnit::Sat),
        after,
        limit,
    )
    .await
}

#[tokio::test]
async fn transaction_pages_preserve_optional_filters_across_global_cursor() {
    let fixture = Fixture::new().await;
    let mut records: Vec<_> = (0..259).map(transaction).collect();
    for (index, tx) in records.iter_mut().enumerate() {
        if index % 2 == 0 {
            tx.mint_url = MintUrl::from_str("https://another.example").unwrap();
        }
        if index % 3 == 0 {
            tx.direction = TransactionDirection::Outgoing;
        }
        if index % 5 == 0 {
            tx.unit = CurrencyUnit::Msat;
        }
        fixture.db.add_transaction(tx.clone()).await.unwrap();
    }
    records.sort_by_key(|tx| (tx.timestamp, tx.id()));
    for filters in 0..8 {
        let mint = (filters & 1 != 0).then(mint);
        let direction = (filters & 2 != 0).then_some(TransactionDirection::Outgoing);
        let unit = (filters & 4 != 0).then_some(CurrencyUnit::Msat);
        let expected: Vec<_> = records
            .iter()
            .filter(|tx| tx.matches_conditions(&mint, &direction, &unit))
            .cloned()
            .collect();
        let mut after = None;
        let mut actual = Vec::new();
        loop {
            let page = fixture
                .db
                .get_transactions_page(
                    mint.clone(),
                    direction,
                    unit.clone(),
                    after,
                    MAX_TRANSACTION_PAGE_SIZE,
                )
                .await
                .unwrap();
            assert!(page.len() <= usize::from(MAX_TRANSACTION_PAGE_SIZE));
            let Some(last) = page.last() else { break };
            after = Some(cursor(last));
            actual.extend(page);
        }
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn transaction_pages_seek_exact_scope_with_ties_deleted_cursor_and_reopen() {
    let fixture = Fixture::new().await;
    let mut expected: Vec<_> = (0..259).map(transaction).collect();
    // Hash-derived refund IDs interleave with primary saga IDs in binary order.
    for tx in expected.iter_mut().step_by(3) {
        tx.role = TransactionRole::SendRefund;
    }
    for tx in expected.iter().rev() {
        fixture.db.add_transaction(tx.clone()).await.unwrap();
    }
    for (index, direction, unit) in [
        (1_000, TransactionDirection::Outgoing, CurrencyUnit::Sat),
        (1_001, TransactionDirection::Incoming, CurrencyUnit::Msat),
    ] {
        let mut excluded = transaction(index);
        excluded.direction = direction;
        excluded.unit = unit;
        fixture.db.add_transaction(excluded).await.unwrap();
    }
    // A long, corrupt foreign-mint prefix must neither be scanned into a page
    // nor poison the selected scope. These are intentionally invalid records.
    fixture.raw.execute(
        "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<2047)
         INSERT INTO transactions (id,mint_url,direction,unit,amount,fee,ys,timestamp,metadata)
         SELECT randomblob(32),'https://foreign.example','Incoming','sat',0,0,x'',i,'invalid' FROM n",
        [],
    ).unwrap();
    expected.sort_by_key(|tx| (tx.timestamp, tx.id()));
    let first = page(&fixture.db, None, MAX_TRANSACTION_PAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(first, expected[..128]);
    let after = cursor(first.last().unwrap());
    fixture
        .raw
        .execute(
            "DELETE FROM transactions WHERE id=?1",
            [after.id.as_slice()],
        )
        .unwrap();
    let reopened = open(&fixture.path).await;
    let second = page(&reopened, Some(after), MAX_TRANSACTION_PAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(second, expected[128..256]);
    let third = page(&reopened, Some(cursor(second.last().unwrap())), 128)
        .await
        .unwrap();
    assert_eq!(third, expected[256..]);
    assert!(page(&reopened, Some(cursor(third.last().unwrap())), 128)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fixture
            .raw
            .query_row("SELECT count(*) FROM transactions", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        259 + 2 + 2048 - 1
    );
}

#[tokio::test]
async fn transaction_pages_reject_invalid_limits_and_cursor_without_writes() {
    let fixture = Fixture::new().await;
    let tx = transaction(0);
    fixture.db.add_transaction(tx.clone()).await.unwrap();
    for limit in [0, MAX_TRANSACTION_PAGE_SIZE + 1, u16::MAX] {
        assert!(page(&fixture.db, None, limit).await.is_err());
    }
    let mut after = cursor(&tx);
    after.timestamp = i64::MAX as u64 + 1;
    assert!(page(&fixture.db, Some(after), 1).await.is_err());
    after.timestamp = u64::MAX;
    assert!(page(&fixture.db, Some(after), 1).await.is_err());
    after.timestamp = i64::MAX as u64;
    assert!(page(&fixture.db, Some(after), 1).await.unwrap().is_empty());
    assert_eq!(page(&fixture.db, None, 1).await.unwrap(), vec![tx.clone()]);
    assert_eq!(fixture.db.get_transaction(tx.id()).await.unwrap(), Some(tx));
}

#[tokio::test]
async fn transaction_pages_reject_only_selected_corrupt_payload_and_identity() {
    let fixture = Fixture::new().await;
    let first = transaction(0);
    let second = transaction(4);
    fixture.db.add_transaction(first.clone()).await.unwrap();
    fixture.db.add_transaction(second.clone()).await.unwrap();
    for (field, corrupt) in [
        (
            "metadata",
            rusqlite::types::Value::Text("invalid".to_owned()),
        ),
        ("ys", rusqlite::types::Value::Blob(vec![0; 34])),
        (
            "timestamp",
            rusqlite::types::Value::Text("invalid".to_owned()),
        ),
        ("id", rusqlite::types::Value::Blob(vec![0x42; 32])),
        ("id", rusqlite::types::Value::Null),
    ] {
        fixture
            .raw
            .execute(
                &format!("UPDATE transactions SET {field}=?1 WHERE id=?2"),
                rusqlite::params![corrupt, second.id().as_slice()],
            )
            .unwrap();
        assert_eq!(
            page(&fixture.db, None, 1).await.unwrap(),
            vec![first.clone()]
        );
        assert!(
            page(&fixture.db, Some(cursor(&first)), 1).await.is_err(),
            "selected corrupt {field} was accepted"
        );
        assert!(page(&fixture.db, None, 2).await.is_err());
        fixture
            .raw
            .execute(
                "DELETE FROM transactions WHERE id IS NULL OR id!=?1",
                [first.id().as_slice()],
            )
            .unwrap();
        fixture.db.add_transaction(second.clone()).await.unwrap();
    }
    assert_eq!(
        page(&fixture.db, None, 2).await.unwrap(),
        vec![first, second]
    );
}

#[tokio::test]
async fn transaction_pages_bound_selected_bytes_before_decoding_and_do_not_skip() {
    let fixture = Fixture::new().await;
    let first = transaction(0);
    let mut second = transaction(4);
    fixture.db.add_transaction(first.clone()).await.unwrap();
    fixture.db.add_transaction(second.clone()).await.unwrap();
    // The over-budget row is deliberately also invalid JSON. The explicit size
    // error must precede decoding, and an earlier page must still be readable.
    fixture
        .raw
        .execute(
            "UPDATE transactions SET metadata=zeroblob(?1) WHERE id=?2",
            rusqlite::params![MAX_TRANSACTION_PAGE_BYTES + 1, second.id().as_slice()],
        )
        .unwrap();
    assert_eq!(
        page(&fixture.db, None, 1).await.unwrap(),
        vec![first.clone()]
    );
    let error = page(&fixture.db, Some(cursor(&first)), 1)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Transaction page payload exceeds"),
        "unexpected error: {error}"
    );
    assert!(page(&fixture.db, None, 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("Transaction page payload exceeds"));
    assert!(matches!(
        fixture
            .db
            .get_transaction_proof_references_page(Some(cursor(&first)), 1)
            .await,
        Err(cdk_common::database::Error::TransactionPageTooLarge { .. })
    ));
    assert_eq!(
        fixture
            .raw
            .query_row(
                "SELECT length(metadata) FROM transactions WHERE id=?1",
                [second.id().as_slice()],
                |row| row.get::<_, u64>(0)
            )
            .unwrap(),
        MAX_TRANSACTION_PAGE_BYTES + 1
    );

    // Ordinary recovery reads share the bound on a configured reopened database.
    // An unrelated oversized row cannot hide a match or turn absence into an error.
    let bounded = open(&fixture.path).await.with_bounded_transaction_reads();
    assert!(matches!(
        bounded.get_transaction(second.id()).await,
        Err(cdk_common::database::Error::TransactionPageTooLarge { bytes, limit })
            if bytes > limit && limit == MAX_TRANSACTION_PAGE_BYTES
    ));
    assert_eq!(
        bounded.get_transaction(first.id()).await.unwrap(),
        Some(first.clone())
    );
    assert!(bounded
        .get_transaction(transaction(100).id())
        .await
        .unwrap()
        .is_none());

    // Aggregate bytes, not characters or per-record size, govern the limit.
    let mut first_large = first.clone();
    first_large.memo = Some("é".repeat((MAX_TRANSACTION_PAGE_BYTES / 4) as usize));
    second.memo = first_large.memo.clone();
    fixture
        .db
        .add_transaction(first_large.clone())
        .await
        .unwrap();
    fixture.db.add_transaction(second.clone()).await.unwrap();
    assert_eq!(
        bounded.get_transaction(second.id()).await.unwrap(),
        Some(second.clone())
    );
    assert_eq!(
        page(&fixture.db, None, 1).await.unwrap(),
        vec![first_large.clone()]
    );
    assert_eq!(
        page(&fixture.db, Some(cursor(&first_large)), 1)
            .await
            .unwrap(),
        vec![second]
    );
    assert!(page(&fixture.db, None, 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("Transaction page payload exceeds"));
    assert_eq!(
        fixture
            .db
            .get_transaction_proof_references_page(None, 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        fixture
            .db
            .get_transaction_proof_references_page(None, 2)
            .await,
        Err(cdk_common::database::Error::TransactionPageTooLarge { .. })
    ));
    let mut foreign = transaction(10);
    foreign.mint_url = MintUrl::from_str("https://foreign.example").unwrap();
    fixture.db.add_transaction(foreign.clone()).await.unwrap();
    fixture
        .raw
        .execute(
            "UPDATE transactions SET memo=zeroblob(?1) WHERE id=?2",
            rusqlite::params![MAX_TRANSACTION_PAGE_BYTES + 1, foreign.id().as_slice()],
        )
        .unwrap();
    assert_eq!(page(&fixture.db, None, 1).await.unwrap(), vec![first_large]);
}

#[tokio::test]
async fn transaction_pages_seek_indexes_without_sorting_history() {
    let fixture = Fixture::new().await;
    let tx = transaction(0);
    fixture.db.add_transaction(tx.clone()).await.unwrap();
    for scoped in [true, false] {
        for after in [None, Some(cursor(&tx))] {
            let (scope, index, mut values) = if scoped {
                (
                    "mint_url=?1 AND direction=?2 AND unit=?3",
                    "transactions_scope_cursor_index",
                    vec![
                        rusqlite::types::Value::Text(mint().to_string()),
                        rusqlite::types::Value::Text("Incoming".to_owned()),
                        rusqlite::types::Value::Text("sat".to_owned()),
                    ],
                )
            } else {
                ("1=1", "transactions_global_cursor_index", vec![])
            };
            let mut predicate = String::new();
            if let Some(after) = after {
                predicate = format!(
                    "AND (timestamp,id) > (?{},?{})",
                    values.len() + 1,
                    values.len() + 2
                );
                values.push(rusqlite::types::Value::Integer(after.timestamp as i64));
                values.push(rusqlite::types::Value::Blob(after.id.as_slice().to_vec()));
            }
            let sql = format!("EXPLAIN QUERY PLAN SELECT id,timestamp FROM transactions WHERE {scope} {predicate} ORDER BY timestamp,id LIMIT 128");
            let details: Vec<String> = fixture
                .raw
                .prepare(&sql)
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |row| row.get(3))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert!(
                details
                    .iter()
                    .any(|line| line.contains(&format!("USING COVERING INDEX {index}"))),
                "missing cursor index: {details:?}"
            );
            assert!(
                !details.iter().any(|line| line.contains("TEMP B-TREE")),
                "unbounded sort: {details:?}"
            );
            if after.is_some() {
                assert!(
                    details
                        .iter()
                        .any(|line| line.contains("(timestamp,id)>(?,?)")),
                    "cursor was not an indexed seek: {details:?}"
                );
            }
        }
    }
    assert_eq!(page(&fixture.db, None, 1).await.unwrap(), vec![tx]);
}
