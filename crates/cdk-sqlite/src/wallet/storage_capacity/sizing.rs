use cdk_common::secret::Secret;
use cdk_common::wallet::ProofInfo;
use cdk_common::{CurrencyUnit, Proof, ProofDleq, SecretKey, State};

use super::*;

#[tokio::test]
async fn measured_proof_and_future_kv_rows_fill_exactly_their_reserved_space() {
    let f = Fixture::new().await;
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let owner = Uuid::new_v4();
    let key = SecretKey::generate();
    let mut coin = ProofInfo::new(
        Proof::new(
            8.into(),
            "00916bbf7ef91a36".parse().unwrap(),
            Secret::generate(),
            key.public_key(),
        ),
        "https://mint.example".parse().unwrap(),
        State::PendingSpent,
        CurrencyUnit::Sat,
    )
    .unwrap();
    coin.proof.dleq = Some(ProofDleq::new(key.clone(), key.clone(), key.clone()));
    coin.proof.p2pk_e = Some(key.public_key());
    coin.created_by_operation = Some(Uuid::new_v4());
    coin.used_by_operation = Some(Uuid::new_v4());
    coin.derivation_index = Some(u32::MAX);
    let proof_bytes = f.db.proof_storage_bytes(owner, coin.clone()).await.unwrap();
    let value = vec![0xab; 1024];
    let kv_bytes =
        f.db.kv_storage_bytes(owner, "capacity-test", "", "sized", value.len() as u64)
            .await
            .unwrap();
    f.db.reserve_storage(
        owner,
        proof_bytes + kv_bytes,
        &[
            WalletStorageClaim {
                table: "proof".to_owned(),
                primary_key: vec![coin.y.to_bytes().to_vec()],
            },
            kv("sized"),
        ],
    )
    .await
    .unwrap();
    let charged =
        f.db.storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes;
    f.db.configure_storage_capacity(charged).await.unwrap();
    let reopened = open(&f.path).await;
    reopened
        .update_proofs(vec![coin.clone()], vec![])
        .await
        .unwrap();
    reopened
        .kv_write("capacity-test", "", "sized", &value)
        .await
        .unwrap();
    let used: u64 = f
        .raw
        .query_row(
            "SELECT used FROM wallet_storage_accounts WHERE owner=?1",
            [owner.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(used, proof_bytes + kv_bytes);
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        charged
    );
    assert!(reopened
        .kv_write("capacity-test", "", "sized", &vec![0xab; value.len() + 1])
        .await
        .is_err());
    assert_eq!(
        reopened
            .kv_read("capacity-test", "", "sized")
            .await
            .unwrap(),
        Some(value)
    );
    assert_eq!(
        reopened.get_proofs_by_ys(vec![coin.y]).await.unwrap(),
        vec![coin]
    );
}
