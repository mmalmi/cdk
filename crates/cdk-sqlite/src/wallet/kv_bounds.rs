use cdk_common::database::{Error, WalletDatabase};
use rusqlite::types::Value;

use super::test_db::{open, Fixture};

#[tokio::test]
async fn kv_read_bounded_rejects_malformed_values_and_survives_reopen() {
    let fixture = Fixture::new().await;
    let db = &fixture.db;
    db.kv_write("bounded", "", "key", b"kept").await.unwrap();
    // SQLite permits TEXT and numeric values in BLOB columns. They must never
    // look absent or count Unicode characters instead of stored bytes.
    for value in [
        Value::Text("éé".to_owned()),
        Value::Integer(42),
        Value::Real(1.5),
    ] {
        fixture
            .raw
            .execute(
                "UPDATE kv_store SET value = ? WHERE primary_namespace = 'bounded'",
                [value],
            )
            .unwrap();
        assert!(matches!(
            db.kv_read_bounded("bounded", "", "key", 100).await,
            Err(Error::InvalidDbResponse)
        ));
    }
    fixture
        .raw
        .execute(
            "UPDATE kv_store SET value = 'éé' WHERE primary_namespace = 'bounded'",
            [],
        )
        .unwrap();
    assert!(matches!(
        db.kv_read_bounded("bounded", "", "key", 3).await,
        Err(Error::KvValueTooLarge { bytes: 4, limit: 3 })
    ));
    db.kv_write("bounded", "", "key", &[0xff; 1024])
        .await
        .unwrap();
    let reopened = open(&fixture.path).await;
    assert!(matches!(
        reopened.kv_read_bounded("bounded", "", "key", 1023).await,
        Err(Error::KvValueTooLarge {
            bytes: 1024,
            limit: 1023
        })
    ));
    assert_eq!(
        reopened
            .kv_read_bounded("bounded", "", "key", 1024)
            .await
            .unwrap(),
        Some(vec![0xff; 1024])
    );
}
