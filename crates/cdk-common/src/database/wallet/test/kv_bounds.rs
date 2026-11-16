use super::*;

/// Bounded reads preserve exact values, namespace isolation and rejected records.
pub async fn kv_read_bounded_contract<D>(db: D)
where
    D: Database<Error> + Sync,
{
    assert_eq!(
        db.kv_read_bounded("bounded", "", "missing", 0)
            .await
            .unwrap(),
        None
    );
    for (primary, secondary, bytes) in [
        ("bounded", "", &b""[..]),
        ("bounded", "one", &b"\x00\xffab"[..]),
        ("other", "one", &b"a"[..]),
    ] {
        db.kv_write(primary, secondary, "key", bytes).await.unwrap();
        let limit = bytes.len() as u32;
        assert_eq!(
            db.kv_read_bounded(primary, secondary, "key", limit)
                .await
                .unwrap(),
            Some(bytes.to_vec())
        );
        if limit > 0 {
            assert!(matches!(
                db.kv_read_bounded(primary, secondary, "key", limit - 1).await,
                Err(Error::KvValueTooLarge { bytes: size, limit: cap })
                    if size == u64::from(limit) && cap == limit - 1
            ));
        }
        assert_eq!(
            db.kv_read(primary, secondary, "key").await.unwrap(),
            Some(bytes.to_vec())
        );
    }
    assert!(db
        .kv_read_bounded("invalid namespace", "", "key", 4)
        .await
        .is_err());
}
