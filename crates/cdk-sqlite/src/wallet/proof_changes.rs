use std::str::FromStr;

use cdk_common::database::wallet::ProofChange;
use cdk_common::database::WalletDatabase;
use cdk_common::mint_url::MintUrl;
use cdk_common::secret::Secret;
use cdk_common::wallet::ProofInfo;
use cdk_common::{Amount, CurrencyUnit, Id, Proof, SecretKey, State};

use super::WalletSqliteDatabase;

mod acquisition;
mod history;
mod kv;
mod read_bounds;

pub(super) fn proof() -> ProofInfo {
    ProofInfo::new(
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
    .unwrap()
}

#[tokio::test]
async fn failed_proof_write_rolls_back_the_whole_batch() {
    let db = super::memory::empty().await.unwrap();
    let first = proof();
    let mut overflow = proof();
    overflow.proof.amount = Amount::from(u64::MAX);
    let changes = [first.clone(), overflow.clone()]
        .into_iter()
        .map(|p| ProofChange {
            y: p.y,
            before: None,
            after: Some(p),
        })
        .collect();
    assert!(db.compare_and_swap_proofs(changes, None).await.is_err());
    assert!(db
        .get_proofs_by_ys(vec![first.y, overflow.y])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn corrupt_optional_evidence_cannot_match_an_absent_value() {
    let path = std::env::temp_dir().join(format!("cdk-proof-cas-{}.sqlite", uuid::Uuid::new_v4()));
    let db = WalletSqliteDatabase::new(path.clone()).await.unwrap();
    let raw = rusqlite::Connection::open(&path).unwrap();
    let original = proof();
    let other = proof();
    let mut spent = original.clone();
    spent.state = State::Spent;
    for column in [
        "witness",
        "used_by_operation",
        "created_by_operation",
        "spending_condition",
        "dleq_e",
        "p2pk_e",
        "derivation_index",
    ] {
        db.update_proofs(vec![original.clone(), other.clone()], vec![])
            .await
            .unwrap();
        raw.execute(
            &format!("UPDATE proof SET {column} = ?1 WHERE y = ?2"),
            rusqlite::params!["invalid-evidence", original.y.to_bytes().to_vec()],
        )
        .unwrap();
        let changes = vec![
            ProofChange {
                y: other.y,
                before: Some(other.clone()),
                after: None,
            },
            ProofChange {
                y: original.y,
                before: Some(original.clone()),
                after: Some(spent.clone()),
            },
        ];
        assert!(
            db.compare_and_swap_proofs(changes, None).await.is_err(),
            "corrupt {column} must reject the batch"
        );
        assert_eq!(
            db.get_proofs_by_ys(vec![other.y]).await.unwrap(),
            std::slice::from_ref(&other)
        );
        let unchanged: String = raw
            .query_row(
                &format!("SELECT {column} FROM proof WHERE y = ?1"),
                [original.y.to_bytes().to_vec()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unchanged, "invalid-evidence");
    }
    drop(raw);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
