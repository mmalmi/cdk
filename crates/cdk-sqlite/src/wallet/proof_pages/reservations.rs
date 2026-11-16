//! Reserved inventories retain every owned scope and reject before decoding.
use super::*;
use cdk_common::database::Error;

#[tokio::test]
async fn configured_proof_reads_reject_oversized_batches_without_losing_owners() {
    let f = super::super::test_db::Fixture::new().await;
    let operation = uuid::Uuid::new_v4();
    let mut coin = proof(operation);
    coin.used_by_operation = Some(operation);
    coin.state = State::Pending;
    f.db.update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    let bytes = match f.db.get_reserved_proofs_bounded(&operation, 1, 0).await {
        Err(Error::ProofInventoryTooLarge { bytes, .. }) => u32::try_from(bytes).unwrap(),
        _ => panic!("expected the stored proof size"),
    };
    let bounded = crate::wallet::test_db::open(&f.path)
        .await
        .with_proof_read_limit(bytes);
    let ys = vec![coin.y];
    assert_eq!(
        bounded.get_reserved_proofs(&operation).await.unwrap(),
        [coin.clone()]
    );
    assert_eq!(
        bounded.get_proofs_by_ys(ys.clone()).await.unwrap(),
        [coin.clone()]
    );
    let oversized = "é\0".repeat(bytes as usize);
    for witness in [
        serde_json::to_string(&cdk_common::Witness::P2PKWitness(cdk_common::P2PKWitness {
            signatures: vec![oversized.clone()],
        }))
        .unwrap(),
        oversized,
    ] {
        f.raw
            .execute("UPDATE proof SET witness=?1", [&witness])
            .unwrap();
        for result in [
            bounded.get_reserved_proofs(&operation).await,
            bounded.get_proofs_by_ys(ys.clone()).await,
        ] {
            assert!(
                matches!(result, Err(Error::ProofInventoryTooLarge { .. })),
                "configured proof reads must reject before decoding"
            );
        }
        let retained: String = f
            .raw
            .query_row("SELECT witness FROM proof", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retained, witness);
        f.db.update_proofs(vec![coin.clone()], vec![])
            .await
            .unwrap();
    }
    let too_small = crate::wallet::test_db::open(&f.path)
        .await
        .with_proof_read_limit(bytes - 1);
    assert_eq!(
        too_small
            .get_proofs_by_ys_with_limit(&ys, u64::from(bytes))
            .await
            .unwrap(),
        [coin.clone()]
    );
    assert!(matches!(
        bounded
            .get_proofs_by_ys_with_limit(&ys, u64::from(bytes - 1))
            .await,
        Err(Error::ProofPageTooLarge { .. })
    ));
    // An explicit per-call allowance must not change the configured policy.
    assert!(matches!(
        too_small.get_proofs_by_ys(ys.clone()).await,
        Err(Error::ProofInventoryTooLarge { .. })
    ));
    assert!(bounded
        .get_reserved_proofs(&uuid::Uuid::new_v4())
        .await
        .unwrap()
        .is_empty());
    assert!(bounded.get_proofs_by_ys(vec![]).await.unwrap().is_empty());
    assert_eq!(
        bounded
            .get_proofs_by_ys(vec![coin.y, coin.y])
            .await
            .unwrap(),
        [coin.clone()]
    );
    let mut extra = proof(operation);
    extra.used_by_operation = Some(operation);
    extra.state = State::Spent;
    extra.unit = CurrencyUnit::Msat;
    extra.mint_url = "https://different.example".parse().unwrap();
    f.db.update_proofs(vec![extra.clone()], vec![])
        .await
        .unwrap();
    assert!(matches!(
        bounded.get_reserved_proofs(&operation).await,
        Err(Error::ProofInventoryTooLarge { .. })
    ));
    assert!(matches!(
        bounded.get_proofs_by_ys(vec![coin.y, extra.y]).await,
        Err(Error::ProofInventoryTooLarge { .. })
    ));
    assert_eq!(f.db.get_reserved_proofs(&operation).await.unwrap().len(), 2);
    f.db.update_proofs(vec![], vec![extra.y]).await.unwrap();
    // Cancellation must still see a later owner; this reader must not silently
    // restrict selected identities to the original operation or coin state.
    coin.used_by_operation = Some(uuid::Uuid::new_v4());
    coin.state = State::Spent;
    f.db.update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    assert_eq!(bounded.get_proofs_by_ys(ys).await.unwrap(), [coin]);
}

#[tokio::test]
async fn reserved_inventory_bounds_rows_and_bytes_without_filtering_ownership() {
    let f = super::super::test_db::Fixture::new().await;
    let operation = uuid::Uuid::new_v4();
    let mut coins: Vec<_> = [State::Reserved, State::Pending, State::Spent]
        .into_iter()
        .map(|state| {
            let mut coin = proof(uuid::Uuid::new_v4());
            coin.used_by_operation = Some(operation);
            coin.state = state;
            coin
        })
        .collect();
    coins[1].mint_url = "https://different.example".parse().unwrap();
    coins[2].unit = CurrencyUnit::Msat;
    coins.sort_by_key(|coin| coin.y);
    f.db.update_proofs(coins.clone(), vec![]).await.unwrap();
    let mut unrelated = proof(operation);
    unrelated.used_by_operation = Some(uuid::Uuid::new_v4());
    f.db.update_proofs(vec![unrelated.clone()], vec![])
        .await
        .unwrap();
    f.raw
        .execute(
            "UPDATE proof SET witness='broken' WHERE y=?1",
            [unrelated.y.to_bytes().to_vec()],
        )
        .unwrap();
    let index: Vec<String> = f
        .raw
        .prepare(
            "EXPLAIN QUERY PLAN SELECT y,length(CAST(secret AS BLOB)) FROM proof
         WHERE used_by_operation=?1 LIMIT 2",
        )
        .unwrap()
        .query_map([operation.to_string()], |row| row.get(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(index
        .iter()
        .any(|row| row.contains("SEARCH proof USING INDEX proof_used_by_operation_index")));
    assert!(index.iter().all(|row| !row.contains("TEMP B-TREE")));
    assert!(f
        .db
        .get_reserved_proofs_bounded(&uuid::Uuid::new_v4(), 0, 0)
        .await
        .unwrap()
        .is_empty());
    assert!(
        matches!(
            f.db.get_reserved_proofs_bounded(&operation, 1, 0).await,
            Err(Error::ProofInventoryTooLarge {
                proofs: 2,
                max_proofs: 1,
                ..
            })
        ),
        "the row probe must stop at the first extra owner"
    );
    let bytes = match f.db.get_reserved_proofs_bounded(&operation, 3, 0).await {
        Err(Error::ProofInventoryTooLarge {
            proofs: 3, bytes, ..
        }) => u32::try_from(bytes).unwrap(),
        _ => panic!("expected the complete reserved payload budget"),
    };
    assert_eq!(
        f.db.get_reserved_proofs_bounded(&operation, 3, bytes)
            .await
            .unwrap(),
        coins
    );
    assert!(matches!(
        f.db.get_reserved_proofs_bounded(&operation, 3, bytes - 1)
            .await,
        Err(Error::ProofInventoryTooLarge { proofs: 3, .. })
    ));
    let selected = &coins[0];
    let oversized = "é\0".repeat(8192);
    let valid = serde_json::to_string(&cdk_common::Witness::P2PKWitness(cdk_common::P2PKWitness {
        signatures: vec![oversized.clone()],
    }))
    .unwrap();
    for witness in [valid.as_str(), oversized.as_str(), "{broken"] {
        f.raw
            .execute(
                "UPDATE proof SET witness=?1 WHERE y=?2",
                rusqlite::params![witness, selected.y.to_bytes().to_vec()],
            )
            .unwrap();
        let result = f.db.get_reserved_proofs_bounded(&operation, 3, bytes).await;
        if witness.len() > bytes as usize {
            assert!(
                matches!(result, Err(Error::ProofInventoryTooLarge { .. })),
                "oversized records must fail before decoding"
            );
        } else {
            assert!(result.is_err(), "a corrupt owned coin must not be skipped");
        }
        let retained: String = f
            .raw
            .query_row(
                "SELECT witness FROM proof WHERE y=?1",
                [selected.y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, witness);
        f.db.update_proofs(coins.clone(), vec![]).await.unwrap();
        assert_eq!(
            f.db.get_reserved_proofs_bounded(&operation, 3, bytes)
                .await
                .unwrap(),
            coins
        );
    }
    // Corrupt owner identities must remain visible rather than disappearing in
    // the join from the bounded probe back to the payload table.
    f.raw
        .execute(
            "UPDATE proof SET y=NULL WHERE y=?1",
            [selected.y.to_bytes().to_vec()],
        )
        .unwrap();
    assert!(f
        .db
        .get_reserved_proofs_bounded(&operation, 3, bytes)
        .await
        .is_err());
}
