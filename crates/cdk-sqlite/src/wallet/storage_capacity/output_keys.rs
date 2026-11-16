use cdk_common::{CurrencyUnit, Id, KeySet, Keys, SecretKey};

use super::*;

pub(super) fn keys() -> KeySet {
    let keys: Keys = serde_json::from_value(
        serde_json::json!({"1": SecretKey::generate().public_key().to_string()}),
    )
    .unwrap();
    KeySet {
        id: Id::v1_from_keys(&keys),
        keys,
        unit: CurrencyUnit::Sat,
        active: Some(true),
        input_fee_ppk: 0,
        final_expiry: None,
    }
}

#[tokio::test]
async fn output_metadata_admission_is_atomic_and_preserves_cancellation() {
    let f = Fixture::new().await;
    f.db.add_mint("https://mint.example".parse().unwrap(), None)
        .await
        .unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let mut intent = saga(50);
    intent.state = WalletSagaState::Send(SendSagaState::PreparedAcquiring);
    intent.kind = intent.state.kind();
    intent.data = OperationData::Send(SendOperationData {
        preparation: Some(Box::new(SendPreparation {
            inputs: vec![],
            metadata: Default::default(),
            retain_cancellation: true,
        })),
        confirmation: None,
        reclaim: None,
        amount: 1.into(),
        memo: None,
        counter_start: None,
        counter_end: None,
        token: None,
        proofs: None,
    });
    f.db.add_saga(intent.clone()).await.unwrap();
    intent.update_state(WalletSagaState::Send(SendSagaState::ProofsReserved));
    assert!(f.db.update_saga(intent.clone()).await.unwrap());
    let before = f.db.storage_capacity().await.unwrap().unwrap();
    let keyset = keys();
    f.db.configure_storage_capacity(before.charged_bytes)
        .await
        .unwrap();
    assert!(f
        .db
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .is_err());
    assert!(f.db.get_keys(&keyset.id).await.unwrap().is_none());
    assert!(f.db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    assert_eq!(
        f.db.get_saga(&intent.id).await.unwrap(),
        Some(intent.clone())
    );
    f.db.configure_storage_capacity(before.maximum_bytes)
        .await
        .unwrap();
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(before));
    f.raw.execute_batch("CREATE TRIGGER reject_keys BEFORE INSERT ON key BEGIN SELECT RAISE(ABORT,'injected key persistence failure'); END").unwrap();
    assert!(f
        .db
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(before));
    assert!(f.db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    assert_eq!(
        f.raw
            .query_row("SELECT count(*) FROM keyset_counter", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    f.raw.execute_batch("DROP TRIGGER reject_keys").unwrap();
    f.db.prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .unwrap();
    assert_eq!(
        f.db.get_keys(&keyset.id).await.unwrap(),
        Some(keyset.keys.clone())
    );
    let admitted = f.db.storage_capacity().await.unwrap().unwrap();
    f.db.configure_storage_capacity(admitted.charged_bytes)
        .await
        .unwrap();
    let reopened = open(&f.path).await;
    reopened
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .increment_keyset_counter(&keyset.id, 12)
            .await
            .unwrap(),
        12
    );
    for state in [
        SendSagaState::PreparedCancelling,
        SendSagaState::PreparedCancelled,
    ] {
        intent.update_state(WalletSagaState::Send(state));
        assert!(reopened.update_saga(intent.clone()).await.unwrap());
    }
    assert_eq!(
        reopened
            .storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        admitted.charged_bytes
    );
    reopened.delete_saga(&intent.id).await.unwrap();
    assert_eq!(
        reopened.get_keys(&keyset.id).await.unwrap(),
        Some(keyset.keys)
    );
}

#[tokio::test]
async fn recovery_consumes_existing_allowance_without_enlarging_it() {
    let f = Fixture::new().await;
    f.db.add_mint("https://mint.example".parse().unwrap(), None)
        .await
        .unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let intent = saga(51);
    let keyset = keys();
    assert!(f
        .db
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .is_err());
    f.db.reserve_saga_storage(&intent, 8192, &[]).await.unwrap();
    let used: u64 = f
        .raw
        .query_row(
            "SELECT used FROM wallet_storage_accounts WHERE owner=?1",
            [intent.id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    f.db.reserve_storage(intent.id, used, &[]).await.unwrap();
    let before = f.db.storage_capacity().await.unwrap();
    assert!(f
        .db
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), before);
    f.db.reserve_storage(intent.id, 8192, &[]).await.unwrap();
    let admitted = f.db.storage_capacity().await.unwrap().unwrap();
    f.db.configure_storage_capacity(admitted.charged_bytes)
        .await
        .unwrap();
    f.db.prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
        .await
        .unwrap();
    assert_eq!(
        f.db.storage_capacity()
            .await
            .unwrap()
            .unwrap()
            .charged_bytes,
        admitted.charged_bytes
    );
    let mut corrupt = keyset;
    corrupt.keys = keys().keys;
    assert!(f
        .db
        .prepare_output_keyset(intent.id, intent.mint_url.clone(), corrupt)
        .await
        .is_err());
}

#[tokio::test]
async fn future_keyset_bound_covers_exact_rows_and_rolls_back_one_byte_short() {
    let f = Fixture::new().await;
    let mint: cdk_common::mint_url::MintUrl = "https://mint.example/recovery".parse().unwrap();
    f.db.add_mint(mint.clone(), None).await.unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let owner = Uuid::new_v4();
    let mut keyset = keys();
    keyset.final_expiry = Some(i64::MAX as u64);
    keyset.id = Id::v2_from_data(
        &keyset.keys,
        &keyset.unit,
        keyset.input_fee_ppk,
        keyset.final_expiry,
    );
    let json_bytes = serde_json::to_vec(&keyset.keys).unwrap().len() as u64;
    let bytes =
        f.db.output_keyset_storage_bytes(owner, mint.clone(), CurrencyUnit::Sat, json_bytes)
            .await
            .unwrap();
    let before = f.db.reserve_storage(owner, bytes - 1, &[]).await.unwrap();
    let full =
        f.db.configure_storage_capacity(before.charged_bytes)
            .await
            .unwrap();
    assert!(f
        .db
        .prepare_output_keyset(owner, mint.clone(), keyset.clone())
        .await
        .is_err());
    assert_eq!(f.db.storage_capacity().await.unwrap(), Some(full));
    assert!(f.db.get_keys(&keyset.id).await.unwrap().is_none());
    assert!(f.db.get_keyset_by_id(&keyset.id).await.unwrap().is_none());
    let counters: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM keyset_counter WHERE keyset_id=?1",
            [keyset.id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(counters, 0);
    let claimed: u64 = f
        .raw
        .query_row(
            "SELECT count(*) FROM wallet_storage_objects WHERE owner=?1",
            [owner.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(claimed, 0);

    f.db.configure_storage_capacity(full.maximum_bytes + 1)
        .await
        .unwrap();
    let full = f.db.reserve_storage(owner, bytes, &[]).await.unwrap();
    let reopened = open(&f.path).await;
    reopened
        .prepare_output_keyset(owner, mint.clone(), keyset.clone())
        .await
        .unwrap();
    let used: u64 = f
        .raw
        .query_row(
            "SELECT used FROM wallet_storage_accounts WHERE owner=?1",
            [owner.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(used, bytes);
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(
        reopened
            .increment_keyset_counter(&keyset.id, u32::MAX)
            .await
            .unwrap(),
        u32::MAX
    );
    reopened
        .prepare_output_keyset(owner, mint, keyset.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.get_keys(&keyset.id).await.unwrap(),
        Some(keyset.keys)
    );
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
}

#[tokio::test]
async fn keyset_comparison_bounds_growth_and_preserves_known_originals() {
    for large in [false, true] {
        let f = Fixture::new().await;
        let intent = saga(81);
        f.db.add_mint(intent.mint_url.clone(), None).await.unwrap();
        f.db.configure_storage_capacity(128 * 1024 * 1024)
            .await
            .unwrap();
        f.db.reserve_saga_storage(&intent, 64 * 1024 * 1024, &[])
            .await
            .unwrap();
        let mut keyset = keys();
        if large {
            let public_key = SecretKey::generate().public_key();
            keyset.keys = Keys::new(
                (1..=65536u64)
                    .map(|amount| (amount.into(), public_key))
                    .collect(),
            );
            keyset.id = Id::v1_from_keys(&keyset.keys);
        }
        let original = serde_json::to_string(&keyset.keys).unwrap();
        assert_eq!(original.len() > 4 * 1024 * 1024, large);
        f.db.prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
            .await
            .unwrap();
        assert_eq!(
            f.db.increment_keyset_counter(&keyset.id, 2).await.unwrap(),
            2
        );
        if !large {
            let corrupt = "x".repeat(4 * 1024 * 1024 + 1);
            f.raw
                .execute(
                    "UPDATE key SET keys=?1 WHERE id=?2",
                    rusqlite::params![corrupt, keyset.id.to_string()],
                )
                .unwrap();
            let before = f.db.storage_capacity().await.unwrap();
            let info = f.db.get_keyset_by_id(&keyset.id).await.unwrap();
            let mut proposal = keyset.clone();
            proposal.active = Some(false);
            proposal.input_fee_ppk = 17;
            let error =
                f.db.prepare_output_keyset(intent.id, intent.mint_url.clone(), proposal)
                    .await
                    .unwrap_err();
            assert!(
                matches!(error, cdk_common::database::Error::KeysetTooLarge { bytes, limit }
                if bytes == corrupt.len() as u64 && limit == 4 * 1024 * 1024),
                "keyset comparison must reject stored growth before decoding: {error}"
            );
            assert_eq!(f.db.storage_capacity().await.unwrap(), before);
            assert_eq!(f.db.get_keyset_by_id(&keyset.id).await.unwrap(), info);
            let retained: String = f
                .raw
                .query_row(
                    "SELECT keys FROM key WHERE id=?1",
                    [keyset.id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, corrupt);
            // Typed comparison still accepts an equivalent noncanonical encoding.
            let equivalent = format!(" {} ", original);
            f.raw
                .execute(
                    "UPDATE key SET keys=?1 WHERE id=?2",
                    rusqlite::params![equivalent, keyset.id.to_string()],
                )
                .unwrap();
        }
        let before = f.db.storage_capacity().await.unwrap().unwrap();
        f.db.configure_storage_capacity(before.charged_bytes)
            .await
            .unwrap();
        let full = f.db.storage_capacity().await.unwrap();
        let reopened = open(&f.path).await;
        reopened
            .prepare_output_keyset(intent.id, intent.mint_url.clone(), keyset.clone())
            .await
            .unwrap();
        assert_eq!(
            reopened.get_keys(&keyset.id).await.unwrap(),
            Some(keyset.keys)
        );
        assert_eq!(reopened.storage_capacity().await.unwrap(), full);
        let counter: u64 = f
            .raw
            .query_row(
                "SELECT counter FROM keyset_counter WHERE keyset_id=?1",
                [keyset.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(counter, 2);
        assert_eq!(reopened.get_saga(&intent.id).await.unwrap(), Some(intent));
    }
}
