//! Metadata budgets reject stored growth before values cross the SQL driver.
use cdk_common::database::wallet::test::issue_acquisition_fixture;
use cdk_common::database::{Error, WalletDatabase};
use cdk_common::mint_url::MintUrl;
use cdk_common::{KeySetInfo, MintInfo};

use super::test_db::{open, Fixture};

fn refusal<T>(result: Result<T, Error>, records: u64, max_records: u32)
where
    T: std::fmt::Debug,
{
    assert!(
        matches!(result, Err(Error::MintMetadataReadTooLarge {
            records: actual, max_records: maximum, ..
        }) if actual == records && maximum == max_records),
        "metadata must refuse before decoding: {result:?}"
    );
}

#[tokio::test]
async fn metadata_reads_bound_bytes_before_decoding_and_preserve_records() {
    let f = Fixture::new().await;
    let mint: MintUrl = "https://metadata.example".parse().unwrap();
    let (keys, _, _) = issue_acquisition_fixture();
    let info = KeySetInfo {
        id: keys.id,
        unit: keys.unit.clone(),
        active: true,
        input_fee_ppk: keys.input_fee_ppk,
        final_expiry: keys.final_expiry,
    };
    f.db.add_mint(mint.clone(), Some(MintInfo::new()))
        .await
        .unwrap();
    f.db.add_mint_keysets(mint.clone(), vec![info.clone()])
        .await
        .unwrap();
    f.db.add_keys(keys.clone()).await.unwrap();
    let max_bytes = 16 * 1024;
    let bounded = open(&f.path)
        .await
        .with_mint_metadata_read_limits(2, 2, max_bytes);
    assert_eq!(
        bounded.get_mint_keysets(mint.clone()).await.unwrap(),
        Some(vec![info.clone()])
    );
    assert_eq!(
        bounded.get_keys(&keys.id).await.unwrap(),
        Some(keys.keys.clone())
    );

    // UTF-8 and a leading NUL defeat character counts and NUL-terminated lengths.
    let corrupt = format!("\0{}", "é".repeat(max_bytes as usize / 2));
    for (table, column, identity, id) in [
        ("mint", "pubkey", "mint_url", mint.to_string()),
        ("keyset", "input_fee_ppk", "id", keys.id.to_string()),
        ("key", "keys", "id", keys.id.to_string()),
    ] {
        let select = format!("SELECT {column} FROM {table} WHERE {identity}=?1");
        let original: rusqlite::types::Value =
            f.raw.query_row(&select, [&id], |row| row.get(0)).unwrap();
        let update = format!("UPDATE {table} SET {column}=?1 WHERE {identity}=?2");
        f.raw
            .execute(&update, rusqlite::params![corrupt, id])
            .unwrap();
        let reopened = open(&f.path)
            .await
            .with_mint_metadata_read_limits(2, 2, max_bytes);
        for db in [&bounded, &reopened] {
            match table {
                "mint" => {
                    refusal(db.get_mint(mint.clone()).await, 1, 1);
                    refusal(db.get_mints().await, 1, 2);
                }
                "keyset" => {
                    refusal(db.get_mint_keysets(mint.clone()).await, 1, 2);
                    refusal(db.get_keyset_by_id(&keys.id).await, 1, 1);
                }
                _ => refusal(db.get_keys(&keys.id).await, 1, 1),
            }
        }
        let retained: String = f.raw.query_row(&select, [&id], |row| row.get(0)).unwrap();
        assert_eq!(retained, corrupt);
        f.raw
            .execute(&update, rusqlite::params![original, id])
            .unwrap();
    }
    assert_eq!(bounded.get_keys(&keys.id).await.unwrap(), Some(keys.keys));
    assert_eq!(
        bounded.get_keyset_by_id(&info.id).await.unwrap(),
        Some(info)
    );
    assert!(bounded.get_mint(mint).await.unwrap().is_some());
}

#[tokio::test]
async fn metadata_catalog_and_keyset_counts_reject_before_decoding() {
    let f = Fixture::new().await;
    let empty = open(&f.path).await.with_mint_metadata_read_limits(0, 0, 0);
    assert!(empty.get_mints().await.unwrap().is_empty());
    let mint: MintUrl = "https://metadata.example".parse().unwrap();
    assert!(empty
        .get_mint_keysets(mint.clone())
        .await
        .unwrap()
        .is_none());
    for index in 0..8 {
        let url = format!("https://metadata-{index}.example");
        f.db.add_mint(url.parse().unwrap(), Some(MintInfo::new()))
            .await
            .unwrap();
    }
    let bytes = match open(&f.path)
        .await
        .with_mint_metadata_read_limits(8, 8, 0)
        .get_mints()
        .await
    {
        Err(Error::MintMetadataReadTooLarge {
            records: 8,
            bytes,
            max_bytes: 0,
            ..
        }) => u32::try_from(bytes).unwrap(),
        other => panic!("expected stored-byte observation: {other:?}"),
    };
    assert_eq!(
        open(&f.path)
            .await
            .with_mint_metadata_read_limits(8, 8, bytes)
            .get_mints()
            .await
            .unwrap()
            .len(),
        8
    );
    refusal(
        open(&f.path)
            .await
            .with_mint_metadata_read_limits(8, 8, bytes - 1)
            .get_mints()
            .await,
        8,
        8,
    );
    // Every selected row would fail ordinary pubkey decoding.
    f.raw.execute("UPDATE mint SET pubkey=X'00'", []).unwrap();
    let bounded = open(&f.path)
        .await
        .with_mint_metadata_read_limits(1, 1, 16 * 1024);
    refusal(bounded.get_mints().await, 2, 1);
    refusal(empty.get_mints().await, 1, 0);
    f.db.add_mint(mint.clone(), None).await.unwrap();
    for index in 0..8 {
        f.raw
            .execute(
                "INSERT INTO keyset(id,mint_url,keyset_u32,unit,active) VALUES(?1,?2,?3,'sat',1)",
                rusqlite::params![format!("invalid-{index}"), mint.to_string(), index],
            )
            .unwrap();
    }
    refusal(bounded.get_mint_keysets(mint.clone()).await, 2, 1);
    refusal(empty.get_mint_keysets(mint).await, 1, 0);
    let records: u64 = f
        .raw
        .query_row("SELECT COUNT(*) FROM keyset", [], |row| row.get(0))
        .unwrap();
    assert_eq!(records, 8);
}
