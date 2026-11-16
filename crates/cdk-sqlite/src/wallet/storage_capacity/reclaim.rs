use cdk_common::secret::Secret;
use cdk_common::wallet::{
    ProofInfo, SendReclaim, Transaction, TransactionDirection, TransactionRole, TransactionStatus,
};
use cdk_common::{
    BlindedMessage, CurrencyUnit, PreMint, PreMintSecrets, PreSwap, Proof, ProofDleq, SecretKey,
    State, SwapRequest,
};

use super::*;

// Only exercise storage with sizing records; none of these proofs are currency.
#[tokio::test]
async fn quoted_reclaim_fits_production_records_after_atomic_allowance_transfer() {
    let f = Fixture::new().await;
    let mut send = saga(70);
    f.db.add_mint(send.mint_url.clone(), None).await.unwrap();
    f.db.configure_storage_capacity(1024 * 1024).await.unwrap();
    let key = SecretKey::generate();
    let keyset = super::output_keys::keys();
    let id = keyset.id;
    let counter = u32::MAX - 1;
    f.db.increment_keyset_counter(&id, counter).await.unwrap();
    let mut input = ProofInfo::new(
        Proof::new(1.into(), id, Secret::new("original"), key.public_key()),
        send.mint_url.clone(),
        State::PendingSpent,
        CurrencyUnit::Sat,
    )
    .unwrap();
    input.used_by_operation = Some(send.id);
    input.created_by_operation = Some(send.id);
    send.state = WalletSagaState::Send(SendSagaState::TokenCreated);
    send.kind = send.state.kind();
    send.data = OperationData::Send(SendOperationData {
        preparation: None,
        confirmation: None,
        reclaim: None,
        amount: 1.into(),
        memo: None,
        counter_start: None,
        counter_end: None,
        token: Some("retained token".to_owned()),
        proofs: Some(vec![input.proof.clone()]),
    });
    let primary = Transaction {
        role: TransactionRole::Primary,
        status: TransactionStatus::Pending,
        mint_url: send.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: send.amount,
        fee: 0.into(),
        unit: send.unit.clone(),
        ys: vec![input.y],
        timestamp: send.created_at,
        memo: None,
        metadata: Default::default(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(send.id),
    };
    let secret = PreMint {
        blinded_message: BlindedMessage::new(1.into(), id, key.public_key()),
        secret: Secret::new("f".repeat(64)),
        r: key.clone(),
        amount: 1.into(),
        derivation_index: Some(counter),
    };
    let refund = Transaction {
        role: TransactionRole::SendRefund,
        status: TransactionStatus::Completed,
        direction: TransactionDirection::Incoming,
        ..primary.clone()
    };
    let reclaim = SendReclaim {
        inputs: vec![input.clone()],
        pre_swap: PreSwap {
            pre_mint_secrets: PreMintSecrets {
                secrets: vec![secret.clone()],
                keyset_id: id,
            },
            swap_request: SwapRequest::new(
                vec![input.proof.clone()],
                vec![secret.blinded_message.clone()],
            ),
            derived_secret_count: 1,
            fee: 0.into(),
            p2bk_secret_keys: None,
        },
        transaction: refund.clone(),
        outputs: None,
    };
    let quoted =
        f.db.send_reclaim_storage_bytes(&send, &reclaim)
            .await
            .unwrap();
    f.db.reserve_saga_storage(&send, 32768, &[]).await.unwrap();
    f.db.update_proofs(vec![input.clone()], vec![])
        .await
        .unwrap();
    f.db.add_transaction(primary.clone()).await.unwrap();
    let retained: u64 = f
        .raw
        .query_row(
            "SELECT used FROM wallet_storage_accounts WHERE owner=?1",
            [send.id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    f.db.reserve_storage(send.id, retained, &[]).await.unwrap();
    let source = Uuid::new_v4();
    let marker =
        f.db.storage_reservation_transfer_bytes(source)
            .await
            .unwrap();
    let capacity =
        f.db.reserve_storage(source, quoted + marker, &[])
            .await
            .unwrap();
    let full =
        f.db.configure_storage_capacity(capacity.charged_bytes)
            .await
            .unwrap();
    let reopened = open(&f.path).await.with_bounded_transaction_reads();
    assert_eq!(
        reopened
            .transfer_storage_reservation(source, send.id)
            .await
            .unwrap(),
        full
    );
    let original = send.clone();
    let OperationData::Send(data) = &mut send.data else {
        panic!("send fixture")
    };
    data.reclaim = Some(Box::new(reclaim));
    send.update_state(WalletSagaState::Send(SendSagaState::ReclaimPreparing));

    // Unexpected stored growth must stop before decoding or moving any allowance.
    reopened
        .configure_storage_capacity(full.maximum_bytes + 10 * 1024 * 1024)
        .await
        .unwrap();
    let original_allowance: u64 = f
        .raw
        .query_row(
            "SELECT reserved FROM wallet_storage_accounts WHERE owner=?1",
            [send.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    reopened
        .reserve_storage(send.id, original_allowance + 5 * 1024 * 1024, &[])
        .await
        .unwrap();
    let oversized = "x".repeat(4 * 1024 * 1024);
    f.raw
        .execute(
            "UPDATE proof SET witness=?1 WHERE y=?2",
            rusqlite::params![oversized, input.y.to_bytes().to_vec()],
        )
        .unwrap();
    let retained = reopened.storage_capacity().await.unwrap();
    let error = reopened.update_saga(send.clone()).await.unwrap_err();
    assert!(
        matches!(error, cdk_common::database::Error::ProofPageTooLarge { bytes, limit }
        if bytes > limit && limit == 4 * 1024 * 1024),
        "completion admission must bound unexpected evidence before decoding: {error}"
    );
    assert_eq!(
        reopened.get_saga(&send.id).await.unwrap(),
        Some(original.clone())
    );
    assert_eq!(reopened.storage_capacity().await.unwrap(), retained);
    let saved: String = f
        .raw
        .query_row(
            "SELECT witness FROM proof WHERE y=?1",
            [input.y.to_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(saved, oversized);
    reopened
        .update_proofs(vec![input.clone()], vec![])
        .await
        .unwrap();

    // Both admission entry points must bound the internal receipt reload before
    // changing the parent, allowance, coins, output keys or derivation counter.
    for (field, original_field) in [("memo", None), ("metadata", Some("{}"))] {
        f.raw
            .execute(
                &format!("UPDATE transactions SET {field}=?1 WHERE id=?2"),
                rusqlite::params![oversized, primary.id().as_slice()],
            )
            .unwrap();
        let retained = reopened.storage_capacity().await.unwrap();
        for prepare in [false, true] {
            let result = if prepare {
                reopened
                    .prepare_saga_outputs(
                        Some(&original),
                        send.clone(),
                        keyset.clone(),
                        counter,
                        1,
                        None,
                    )
                    .await
            } else {
                reopened.update_saga(send.clone()).await
            };
            assert!(
                matches!(result, Err(cdk_common::database::Error::TransactionPageTooLarge { bytes, limit })
                    if bytes > limit && limit == 4 * 1024 * 1024),
                "reclaim admission must bound the original receipt before decoding: {result:?}"
            );
            assert_eq!(
                reopened.get_saga(&send.id).await.unwrap(),
                Some(original.clone())
            );
            assert_eq!(reopened.storage_capacity().await.unwrap(), retained);
            assert_eq!(
                reopened.get_proofs_by_ys(vec![input.y]).await.unwrap(),
                vec![input.clone()]
            );
            assert!(reopened.get_keys(&id).await.unwrap().is_none());
            assert!(reopened.get_keyset_by_id(&id).await.unwrap().is_none());
            assert_eq!(
                reopened.increment_keyset_counter(&id, 0).await.unwrap(),
                counter
            );
            let saved: String = f
                .raw
                .query_row(
                    &format!("SELECT {field} FROM transactions WHERE id=?1"),
                    [primary.id().as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(saved == oversized);
        }
        f.raw
            .execute(
                &format!("UPDATE transactions SET {field}=?1 WHERE id=?2"),
                rusqlite::params![original_field, primary.id().as_slice()],
            )
            .unwrap();
    }
    assert_eq!(
        reopened.get_transaction(primary.id()).await.unwrap(),
        Some(primary)
    );
    reopened
        .reserve_storage(send.id, original_allowance, &[])
        .await
        .unwrap();
    assert_eq!(
        reopened
            .configure_storage_capacity(full.maximum_bytes)
            .await
            .unwrap(),
        full
    );
    assert!(reopened.update_saga(send.clone()).await.unwrap());
    let mut coin = Proof::new(1.into(), id, secret.secret, key.public_key());
    coin.dleq = Some(ProofDleq::new(key.clone(), key.clone(), key.clone()));
    coin.p2pk_e = Some(key.public_key());
    let mut output = ProofInfo::new(
        coin,
        send.mint_url.clone(),
        State::Unspent,
        send.unit.clone(),
    )
    .unwrap();
    output.derivation_index = secret.derivation_index;
    output.created_by_operation = Some(send.id);
    let OperationData::Send(data) = &mut send.data else {
        panic!("send fixture")
    };
    data.reclaim.as_mut().unwrap().outputs = Some(vec![output.clone()]);
    send.update_state(WalletSagaState::Send(SendSagaState::Reclaiming));
    assert!(reopened.update_saga(send.clone()).await.unwrap());
    reopened
        .update_proofs(vec![output.clone()], vec![])
        .await
        .unwrap();
    reopened.add_transaction(refund).await.unwrap();
    assert_eq!(reopened.storage_capacity().await.unwrap(), Some(full));
    assert_eq!(
        reopened.get_proofs_by_ys(vec![output.y]).await.unwrap(),
        vec![output]
    );
    assert_eq!(reopened.get_saga(&send.id).await.unwrap(), Some(send));
}
