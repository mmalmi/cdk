use cdk_common::database::wallet::test::melt_acquisition_fixture;
use cdk_common::database::WalletDatabase;

use super::WalletSqliteDatabase;

#[tokio::test]
async fn acquire_melt_write_failures_roll_back_every_record() {
    let path =
        std::env::temp_dir().join(format!("cdk-acquisition-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let (quote, saga, local) = melt_acquisition_fixture();
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    for table in ["proof", "melt_quote", "wallet_sagas"] {
        let event = if table == "melt_quote" {
            "UPDATE"
        } else {
            "INSERT"
        };
        raw.execute_batch(&format!("CREATE TRIGGER fail_acquisition BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;")).unwrap();
        assert!(db.acquire_melt(&saga, &quote).await.is_err(), "{table}");
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        assert_eq!(
            db.get_melt_quote(&quote.id).await.unwrap(),
            Some(quote.clone())
        );
        let proofs = db.get_proofs(None, None, None, None).await.unwrap();
        assert_eq!(proofs, vec![local.clone()]);
        raw.execute_batch("DROP TRIGGER fail_acquisition;").unwrap();
    }
    assert!(db.acquire_melt(&saga, &quote).await.unwrap());
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn acquire_melt_rejects_corrupt_optional_quote_scope() {
    let path =
        std::env::temp_dir().join(format!("cdk-quote-scope-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let (mut quote, saga, local) = melt_acquisition_fixture();
    quote.mint_url = None;
    db.add_melt_quote(quote.clone()).await.unwrap();
    db.update_proofs(vec![local.clone()], vec![]).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        "UPDATE melt_quote SET mint_url='not a URL' WHERE id=?1",
        [&quote.id],
    )
    .unwrap();
    assert!(db.acquire_melt(&saga, &quote).await.is_err());
    assert!(db.get_saga(&saga.id).await.unwrap().is_none());
    assert_eq!(
        db.get_proofs(None, None, None, None).await.unwrap(),
        vec![local]
    );
    let owner: Option<String> = raw
        .query_row(
            "SELECT used_by_operation FROM melt_quote WHERE id=?1",
            [&quote.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner, None);
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
