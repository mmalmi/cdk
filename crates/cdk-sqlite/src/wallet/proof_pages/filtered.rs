use cdk_common::database::{Error, WalletDatabase};
use cdk_common::{CurrencyUnit, SpendingConditions, State};

use super::super::test_db::open;
use super::proof;

#[tokio::test]
async fn ordinary_proof_lists_bound_matching_payloads_without_reading_other_scopes() {
    let f = super::super::test_db::Fixture::new().await;
    let coin = proof(uuid::Uuid::new_v4());
    let mut foreign = proof(uuid::Uuid::new_v4());
    foreign.mint_url = "https://other.example".parse().unwrap();
    let mut other_unit = proof(uuid::Uuid::new_v4());
    other_unit.unit = CurrencyUnit::Msat;
    let mut spent = proof(uuid::Uuid::new_v4());
    spent.state = State::Spent;
    f.db.update_proofs(
        vec![
            coin.clone(),
            foreign.clone(),
            other_unit.clone(),
            spent.clone(),
        ],
        vec![],
    )
    .await
    .unwrap();
    let db = open(&f.path).await.with_proof_read_limit(4096);
    let growth = "x".repeat(4097);
    for row in [&foreign, &other_unit, &spent] {
        f.raw
            .execute(
                "UPDATE proof SET witness = ?1 WHERE y = ?2",
                rusqlite::params![growth, row.y.to_bytes().to_vec()],
            )
            .unwrap();
    }
    assert_eq!(
        db.get_proofs(
            Some(coin.mint_url.clone()),
            Some(CurrencyUnit::Sat),
            Some(vec![State::Unspent, State::Pending]),
            None,
        )
        .await
        .unwrap(),
        vec![coin.clone()]
    );
    for (mint, unit, states) in [
        (None, None, None),
        (
            Some(foreign.mint_url.clone()),
            Some(CurrencyUnit::Sat),
            None,
        ),
        (Some(coin.mint_url.clone()), Some(CurrencyUnit::Msat), None),
        (
            Some(coin.mint_url.clone()),
            Some(CurrencyUnit::Sat),
            Some(vec![State::Spent]),
        ),
    ] {
        assert!(
            matches!(
                db.get_proofs(mint, unit, states, None).await,
                Err(Error::ProofInventoryTooLarge {
                    max_bytes: 4096,
                    ..
                })
            ),
            "ordinary proof query must enforce its configured read budget"
        );
    }
    assert!(db
        .get_proofs(None, None, Some(vec![]), None)
        .await
        .unwrap()
        .is_empty());
    let bytes: usize = f
        .raw
        .query_row(
            "SELECT length(witness) FROM proof WHERE y=?1",
            [foreign.y.to_bytes().to_vec()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bytes, growth.len());
}

#[tokio::test]
async fn ordinary_proof_lists_preserve_conditions_and_reject_corrupt_matching_rows() {
    let f = super::super::test_db::Fixture::new().await;
    let plain = proof(uuid::Uuid::new_v4());
    let mut locked = proof(uuid::Uuid::new_v4());
    let condition = SpendingConditions::P2PKConditions {
        data: locked.proof.c,
        conditions: None,
    };
    locked.spending_condition = Some(condition.clone());
    f.db.update_proofs(vec![plain.clone(), locked.clone()], vec![])
        .await
        .unwrap();
    let db = open(&f.path).await.with_proof_read_limit(4096);
    for (conditions, expected) in [
        (Some(vec![]), vec![plain.clone()]),
        (Some(vec![condition]), vec![locked.clone()]),
    ] {
        assert_eq!(
            db.get_proofs(None, None, None, conditions).await.unwrap(),
            expected
        );
    }
    f.raw
        .execute(
            "UPDATE proof SET witness='not-json' WHERE y=?1",
            [plain.y.to_bytes().to_vec()],
        )
        .unwrap();
    assert!(
        db.get_proofs(None, None, None, None).await.is_err(),
        "corrupt matching proof must not silently disappear"
    );
    let witness: String = f
        .raw
        .query_row(
            "SELECT witness FROM proof WHERE y=?1",
            [plain.y.to_bytes().to_vec()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(witness, "not-json");
    f.raw
        .execute(
            "UPDATE proof SET witness=NULL WHERE y=?1",
            [plain.y.to_bytes().to_vec()],
        )
        .unwrap();
    let restored = db.get_proofs(None, None, None, None).await.unwrap();
    assert_eq!(restored.len(), 2);
    assert!(restored.contains(&plain) && restored.contains(&locked));
    assert!(open(&f.path)
        .await
        .with_proof_read_limit(0)
        .get_proofs(None, None, None, None)
        .await
        .is_err());
    assert!(open(&f.path)
        .await
        .with_proof_read_limit(0)
        .get_proofs(None, None, Some(vec![]), None)
        .await
        .unwrap()
        .is_empty());
}
