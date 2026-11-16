mod filtered;
mod reservations;

use std::path::PathBuf;
use std::str::FromStr;

use cdk_common::database::WalletDatabase;
use cdk_common::mint_url::MintUrl;
use cdk_common::secret::Secret;
use cdk_common::wallet::ProofInfo;
use cdk_common::{Amount, CurrencyUnit, Id, Proof, SecretKey, State};

use super::WalletSqliteDatabase;

fn proof(operation: uuid::Uuid) -> ProofInfo {
    let mut proof = ProofInfo::new(
        Proof {
            amount: Amount::from(8),
            keyset_id: Id::from_str("00916bbf7ef91a36").unwrap(),
            secret: Secret::generate(),
            c: SecretKey::generate().public_key(),
            witness: None,
            dleq: None,
            p2pk_e: None,
        },
        MintUrl::from_str("https://mint.example").unwrap(),
        State::Unspent,
        CurrencyUnit::Sat,
    )
    .unwrap();
    proof.created_by_operation = Some(operation);
    proof
}

async fn database() -> (PathBuf, WalletSqliteDatabase, rusqlite::Connection) {
    let path = std::env::temp_dir().join(format!("cdk-proof-page-{}.sqlite", uuid::Uuid::new_v4()));
    #[cfg(not(feature = "sqlcipher"))]
    let config = path.clone();
    #[cfg(feature = "sqlcipher")]
    let config = (path.clone(), "page-test".to_owned());
    let db = WalletSqliteDatabase::new(config).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    #[cfg(feature = "sqlcipher")]
    raw.pragma_update(None, "key", "page-test").unwrap();

    (path, db, raw)
}

#[tokio::test]
async fn proof_operation_owners_bound_headers_without_reading_payloads() {
    use crate::ProofOperationOwners;
    use cdk_common::database::Error;

    let f = super::test_db::Fixture::new().await;
    let mut coin = proof(uuid::Uuid::new_v4());
    coin.used_by_operation = Some(uuid::Uuid::new_v4());
    let missing = proof(uuid::Uuid::new_v4());
    f.db.update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    let oversized = "x".repeat(4 * 1024 * 1024 + 1);
    f.raw
        .execute(
            "UPDATE proof SET witness=?1 WHERE y=?2",
            rusqlite::params![oversized, coin.y.to_bytes().to_vec()],
        )
        .unwrap();
    assert!(f
        .db
        .get_proof_operation_owners(&[])
        .await
        .unwrap()
        .is_empty());
    assert!(f
        .db
        .get_proof_operation_owners(&[coin.y; 129])
        .await
        .is_err());
    assert_eq!(
        f.db.get_proof_operation_owners(&[missing.y, coin.y, coin.y])
            .await
            .unwrap(),
        vec![ProofOperationOwners {
            y: coin.y,
            used_by_operation: coin.used_by_operation,
            created_by_operation: coin.created_by_operation,
        }]
    );
    f.raw
        .execute("UPDATE proof SET created_by_operation=NULL", [])
        .unwrap();
    assert_eq!(
        f.db.get_proof_operation_owners(&[coin.y]).await.unwrap()[0].created_by_operation,
        None
    );
    for column in ["used_by_operation", "created_by_operation"] {
        f.raw
            .execute(&format!("UPDATE proof SET {column}=?1"), [&oversized])
            .unwrap();
        assert!(matches!(
            f.db.get_proof_operation_owners(&[coin.y]).await.unwrap_err(),
            Error::ProofPageTooLarge { bytes, limit: 105 } if bytes > 105
        ));
        f.raw
            .execute(&format!("UPDATE proof SET {column}='invalid'"), [])
            .unwrap();
        assert!(matches!(
            f.db.get_proof_operation_owners(&[coin.y])
                .await
                .unwrap_err(),
            Error::InvalidUuid(_)
        ));
        f.raw
            .execute(&format!("UPDATE proof SET {column}=NULL"), [])
            .unwrap();
    }
    assert_eq!(
        f.raw
            .query_row("SELECT witness FROM proof", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        oversized
    );
}

#[tokio::test]
async fn selected_proof_pages_bound_identity_count_and_return_exact_present_rows() {
    let f = super::test_db::Fixture::new().await;
    let mut coins = [proof(uuid::Uuid::new_v4()), proof(uuid::Uuid::new_v4())];
    coins.sort_by_key(|coin| coin.y.to_bytes());
    let missing = proof(uuid::Uuid::new_v4());
    f.db.update_proofs(coins.to_vec(), vec![]).await.unwrap();
    assert!(f.db.get_proofs_by_ys_bounded(&[]).await.unwrap().is_empty());
    assert!(f
        .db
        .get_proofs_by_ys_bounded(&[coins[0].y; 129])
        .await
        .is_err());
    assert_eq!(
        f.db.get_proofs_by_ys_bounded(&[coins[1].y, missing.y, coins[0].y, coins[1].y])
            .await
            .unwrap(),
        coins
    );
    // An exact page never reads an unrelated corrupt payload.
    f.db.update_proofs(vec![missing.clone()], vec![])
        .await
        .unwrap();
    f.raw
        .execute(
            "UPDATE proof SET witness='{' WHERE y=?1",
            [missing.y.to_bytes().to_vec()],
        )
        .unwrap();
    assert_eq!(
        f.db.get_proofs_by_ys_bounded(&[coins[0].y]).await.unwrap(),
        [coins[0].clone()]
    );
    assert!(f.db.get_proofs_by_ys_bounded(&[missing.y]).await.is_err());
}

#[tokio::test]
async fn proof_operation_pages_use_compound_index() {
    let (path, db, raw) = database().await;
    let operation = uuid::Uuid::new_v4();
    let selected = proof(operation);
    db.update_proofs(vec![selected.clone()], vec![])
        .await
        .unwrap();
    let old_index: i64 = raw
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
            ["proof_created_by_operation_index"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        old_index, 0,
        "compound index must replace the old prefix index"
    );
    for after in [None, Some(selected.y)] {
        // Match the production page-selection predicates. Do not force an
        // index: the planner must choose the compound seek without sorting.
        let (sql, values) = match after {
            None => (
                "EXPLAIN QUERY PLAN SELECT * FROM proof
                 WHERE created_by_operation = ?1 ORDER BY y LIMIT ?2",
                vec![
                    rusqlite::types::Value::Text(operation.to_string()),
                    rusqlite::types::Value::Integer(1),
                ],
            ),
            Some(y) => (
                "EXPLAIN QUERY PLAN SELECT * FROM proof
                 WHERE created_by_operation = ?1 AND y > ?2 ORDER BY y LIMIT ?3",
                vec![
                    rusqlite::types::Value::Text(operation.to_string()),
                    rusqlite::types::Value::Blob(y.to_bytes().to_vec()),
                    rusqlite::types::Value::Integer(1),
                ],
            ),
        };
        let details: Vec<String> = raw
            .prepare(sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(values), |row| row.get(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            details.iter().any(|detail| {
                detail.contains("SEARCH proof USING INDEX proof_created_by_operation_y_index")
            }),
            "proof page must use the compound index"
        );
        assert!(
            details.iter().all(|detail| !detail.contains("TEMP B-TREE")),
            "proof page must not materialize a temporary sort"
        );
        if after.is_some() {
            assert!(
                details.iter().any(|detail| detail.contains("y>?")),
                "next page must seek past the binary cursor in the index"
            );
        }
    }
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn proof_operation_page_rejects_corrupt_selected_evidence() {
    let (path, db, raw) = database().await;
    let operation = uuid::Uuid::new_v4();
    let mut selected = [proof(operation), proof(operation)];
    selected.sort_by_key(|proof| proof.y.to_bytes());
    let unrelated = proof(uuid::Uuid::new_v4());
    for column in [
        "witness",
        "used_by_operation",
        "spending_condition",
        "p2pk_e",
    ] {
        db.update_proofs(
            vec![selected[0].clone(), selected[1].clone(), unrelated.clone()],
            vec![],
        )
        .await
        .unwrap();
        raw.execute(
            &format!("UPDATE proof SET {column} = ?1 WHERE y IN (?2, ?3)"),
            rusqlite::params![
                "invalid-evidence",
                selected[1].y.to_bytes().to_vec(),
                unrelated.y.to_bytes().to_vec(),
            ],
        )
        .unwrap();
        let first = db
            .get_proofs_created_by_operation_page(operation, None, 1)
            .await
            .unwrap();
        assert!(first == selected[..1], "bounded first page changed");
        assert!(
            db.get_proofs_created_by_operation_page(operation, Some(first[0].y), 1)
                .await
                .is_err(),
            "corrupt {column} must fail the selected page"
        );
        assert!(db
            .get_proofs_created_by_operation_page(operation, None, 2)
            .await
            .is_err());
        let unchanged: String = raw
            .query_row(
                &format!("SELECT {column} FROM proof WHERE y = ?1"),
                [selected[1].y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            unchanged == "invalid-evidence",
            "read changed corrupt evidence"
        );
    }
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn proof_operation_page_bounds_bytes_before_decoding_and_preserves_evidence() {
    use cdk_common::database::wallet::MAX_PROOF_PAGE_BYTES;
    use cdk_common::database::Error;

    let (path, db, raw) = database().await;
    let operation = uuid::Uuid::new_v4();
    let mut selected = [proof(operation), proof(operation)];
    selected.sort_by_key(|proof| proof.y.to_bytes());
    let unrelated = proof(uuid::Uuid::new_v4());
    db.update_proofs(
        vec![selected[0].clone(), selected[1].clone(), unrelated.clone()],
        vec![],
    )
    .await
    .unwrap();
    let oversized = "x".repeat(MAX_PROOF_PAGE_BYTES as usize + 1);
    raw.execute(
        "UPDATE proof SET witness = ?1 WHERE y IN (?2, ?3)",
        rusqlite::params![
            oversized,
            selected[1].y.to_bytes().to_vec(),
            unrelated.y.to_bytes().to_vec(),
        ],
    )
    .unwrap();
    assert_eq!(
        db.get_proofs_created_by_operation_page(operation, None, 1)
            .await
            .unwrap(),
        selected[..1]
    );
    for (cursor, limit) in [(None, 2), (Some(selected[0].y), 1)] {
        assert!(
            matches!(
                db.get_proofs_created_by_operation_page(operation, cursor, limit).await,
                Err(Error::ProofPageTooLarge { bytes, limit })
                    if bytes > MAX_PROOF_PAGE_BYTES && limit == MAX_PROOF_PAGE_BYTES
            ),
            "stored-byte limit must precede decoding oversized malformed evidence"
        );
    }
    let retained: String = raw
        .query_row(
            "SELECT witness FROM proof WHERE y = ?1",
            [selected[1].y.to_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, oversized);
    db.update_proofs(vec![selected[1].clone()], vec![])
        .await
        .unwrap();
    assert_eq!(
        db.get_proofs_created_by_operation_page(operation, None, 2)
            .await
            .unwrap(),
        selected
    );
    // UTF-8 bytes and the aggregate page size matter, not characters or the
    // largest individual record. Unrelated oversized evidence remains excluded.
    let original_ys = selected.iter().map(|row| row.y).collect();
    for (index, row) in selected.iter_mut().enumerate() {
        row.proof.secret = Secret::new(format!(
            "{index}{}",
            "é".repeat(MAX_PROOF_PAGE_BYTES as usize / 4)
        ));
        row.y = row.proof.y().unwrap();
    }
    selected.sort_by_key(|row| row.y.to_bytes());
    db.update_proofs(selected.to_vec(), original_ys)
        .await
        .unwrap();
    for (cursor, expected) in [(None, &selected[0]), (Some(selected[0].y), &selected[1])] {
        assert_eq!(
            db.get_proofs_created_by_operation_page(operation, cursor, 1)
                .await
                .unwrap(),
            std::slice::from_ref(expected)
        );
    }
    assert!(matches!(
        db.get_proofs_created_by_operation_page(operation, None, 2)
            .await,
        Err(Error::ProofPageTooLarge { .. })
    ));
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn proof_inventory_budget_precedes_payload_decoding_and_preserves_exact_scope() {
    use cdk_common::database::Error;

    let (path, db, raw) = database().await;
    let mut selected = [proof(uuid::Uuid::new_v4()), proof(uuid::Uuid::new_v4())];
    selected[0].derivation_index = Some(7);
    selected.sort_by_key(|proof| proof.y.to_bytes());
    let mint = selected[0].mint_url.clone();
    let mut excluded = Vec::new();
    for index in 0..3 {
        let mut row = proof(uuid::Uuid::new_v4());
        match index {
            0 => row.mint_url = MintUrl::from_str("https://foreign.example").unwrap(),
            1 => row.unit = CurrencyUnit::Msat,
            _ => row.state = State::Spent,
        }
        excluded.push(row);
    }
    let mut inserted = selected.to_vec();
    inserted.extend(excluded.iter().cloned());
    db.update_proofs(inserted, vec![]).await.unwrap();
    for row in &excluded {
        raw.execute(
            "UPDATE proof SET witness = ?1 WHERE y = ?2",
            rusqlite::params!["invalid unrelated evidence", row.y.to_bytes().to_vec()],
        )
        .unwrap();
    }
    let details: Vec<String> = raw
        .prepare(
            "EXPLAIN QUERY PLAN SELECT y, length(CAST(secret AS BLOB)) FROM proof
         WHERE mint_url = ?1 AND unit = 'sat' AND state = 'UNSPENT' ORDER BY y LIMIT 3",
        )
        .unwrap()
        .query_map([mint.to_string()], |row| row.get(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(details
        .iter()
        .any(|detail| detail.contains("SEARCH proof USING INDEX proof_inventory_scope_y_index")));
    assert!(details.iter().all(|detail| !detail.contains("TEMP B-TREE")));
    let extra: Vec<_> = (0..16).map(|_| proof(uuid::Uuid::new_v4())).collect();
    db.update_proofs(extra.clone(), vec![]).await.unwrap();
    assert!(
        matches!(
            db.get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 1, 4096)
                .await,
            Err(Error::ProofInventoryTooLarge { proofs: 2, .. })
        ),
        "counting must stop at limit plus one"
    );
    db.update_proofs(vec![], extra.iter().map(|row| row.y).collect())
        .await
        .unwrap();
    let bytes = match db
        .get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 1, 0)
        .await
    {
        Err(Error::ProofInventoryTooLarge {
            proofs: 2,
            bytes,
            max_proofs: 1,
            max_bytes: 0,
        }) => bytes,
        other => panic!("expected bounded rejection, got {other:?}"),
    };
    let exact = u32::try_from(bytes).unwrap();
    assert_eq!(
        db.get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 2, exact)
            .await
            .unwrap(),
        selected
    );
    assert!(matches!(
        db.get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 2, exact - 1)
            .await,
        Err(Error::ProofInventoryTooLarge { proofs: 2, .. })
    ));
    raw.execute(
        "UPDATE proof SET witness = ?1 WHERE y = ?2",
        rusqlite::params!["x".repeat(8192), selected[1].y.to_bytes().to_vec()],
    )
    .unwrap();
    assert!(
        matches!(
            db.get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 2, 4096)
                .await,
            Err(Error::ProofInventoryTooLarge { .. })
        ),
        "budget must reject before decoding invalid oversized JSON"
    );
    assert!(
        db.get_unspent_proofs_bounded(mint.clone(), CurrencyUnit::Sat, 2, 16384)
            .await
            .is_err(),
        "selected corrupt evidence must not be skipped"
    );
    db.update_proofs(selected.to_vec(), vec![]).await.unwrap();
    assert_eq!(
        db.get_unspent_proofs_bounded(mint, CurrencyUnit::Sat, 2, exact)
            .await
            .unwrap(),
        selected
    );
    assert!(db
        .get_unspent_proofs_bounded(
            MintUrl::from_str("https://empty.example").unwrap(),
            CurrencyUnit::Sat,
            0,
            0
        )
        .await
        .unwrap()
        .is_empty());
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
