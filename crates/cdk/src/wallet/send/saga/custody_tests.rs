use cdk_common::wallet::{OperationData, Transaction, TransactionDirection, TransactionStatus};

use super::confirmation_tests::{interrupted, save_ready};
use crate::nuts::{ProofsMethods, State};

#[tokio::test]
async fn confirmation_rejects_changed_input_owner_without_importing_outputs() {
    let (wallet, _, saga) = interrupted().await;
    let saga = save_ready(&wallet, saga).await;
    let OperationData::Send(data) = &saga.data else {
        panic!("send");
    };
    let intent = data.confirmation.as_ref().unwrap();
    let input_y = intent
        .pre_swap
        .as_ref()
        .unwrap()
        .swap_request
        .inputs()
        .ys()
        .unwrap()[0];
    let mut changed = wallet
        .localstore
        .get_proofs_by_ys(vec![input_y])
        .await
        .unwrap()
        .remove(0);
    changed.used_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![changed.clone()], vec![])
        .await
        .unwrap();
    assert!(wallet
        .complete_send_confirmation(saga.clone(), true)
        .await
        .is_err());
    let mut ys = intent.ready.as_ref().unwrap().send.ys().unwrap();
    ys.extend(intent.ready.as_ref().unwrap().change.ys().unwrap());
    assert!(wallet
        .localstore
        .get_proofs_by_ys(ys)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![input_y])
            .await
            .unwrap(),
        vec![changed]
    );
    assert_eq!(
        wallet.localstore.get_saga(&saga.id).await.unwrap(),
        Some(saga)
    );
}

#[tokio::test]
async fn confirmation_keeps_a_matching_terminal_receipt_status() {
    let (wallet, _, saga) = interrupted().await;
    let saga = save_ready(&wallet, saga).await;
    let OperationData::Send(data) = &saga.data else {
        panic!("send");
    };
    let intent = data.confirmation.as_ref().unwrap();
    let receipt = Transaction {
        role: cdk_common::wallet::TransactionRole::Primary,
        mint_url: wallet.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: intent.pre_swap.as_ref().unwrap().fee + intent.send_fee,
        unit: wallet.unit.clone(),
        ys: intent.ready.as_ref().unwrap().send.ys().unwrap(),
        timestamp: saga.created_at,
        memo: intent.token_memo.clone(),
        metadata: intent.metadata.clone(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(saga.id),
        status: TransactionStatus::Completed,
    };
    wallet
        .localstore
        .add_transaction(receipt.clone())
        .await
        .unwrap();
    wallet.complete_send_confirmation(saga, true).await.unwrap();
    assert_eq!(
        wallet
            .localstore
            .get_transaction(receipt.id())
            .await
            .unwrap(),
        Some(receipt)
    );
    assert!(!wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::PendingSpent]), None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn confirmation_fences_racing_ownership_and_parent_changes() {
    use std::sync::Arc;

    use super::interposed_store::{Action, Interposed};
    for fault in 0..5 {
        let (mut wallet, _, saga) = interrupted().await;
        let saga = save_ready(&wallet, saga).await;
        let OperationData::Send(data) = &saga.data else {
            panic!("send");
        };
        let intent = data.confirmation.as_ref().unwrap();
        let input_y = intent
            .pre_swap
            .as_ref()
            .unwrap()
            .swap_request
            .inputs()
            .ys()
            .unwrap()[0];
        let input = wallet
            .localstore
            .get_proofs_by_ys(vec![input_y])
            .await
            .unwrap()
            .remove(0);
        let mut output_ys = intent.ready.as_ref().unwrap().send.ys().unwrap();
        output_ys.extend(intent.ready.as_ref().unwrap().change.ys().unwrap());
        let db = Arc::new(Interposed::new(wallet.localstore.clone()));
        let action = match fault {
            0 => {
                let mut changed = input.clone();
                changed.used_by_operation = Some(uuid::Uuid::new_v4());
                Action::ReplaceProof(Box::new(changed))
            }
            1 => Action::RemoveSaga(saga.id),
            2 => {
                let mut changed = saga.clone();
                changed.update_state(changed.state);
                Action::ReplaceSaga(changed)
            }
            3 => {
                let mut changed = saga.clone();
                if let OperationData::Send(data) = &mut changed.data {
                    data.memo = Some("new writer".to_owned());
                }
                changed.update_state(changed.state);
                Action::ReplaceSaga(changed)
            }
            _ => {
                let mut info = cdk_common::wallet::ProofInfo::new(
                    intent.ready.as_ref().unwrap().send[0].clone(),
                    wallet.mint_url.clone(),
                    State::Reserved,
                    wallet.unit.clone(),
                )
                .unwrap();
                info.used_by_operation = Some(uuid::Uuid::new_v4());
                Action::ReplaceProof(Box::new(info))
            }
        };
        *db.before_proofs.lock().unwrap() = Some(action);
        wallet.localstore = db;
        assert!(wallet.complete_send_confirmation(saga, true).await.is_err());
        let input_after = wallet
            .localstore
            .get_proofs_by_ys(vec![input_y])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(input_after.state, input.state, "no partial input spend");
        let outputs = wallet.localstore.get_proofs_by_ys(output_ys).await.unwrap();
        assert_eq!(
            outputs.len(),
            usize::from(fault == 4),
            "no partial output import"
        );
        assert!(wallet
            .localstore
            .list_transactions(None, None, None)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn stale_confirmation_cannot_resurrect_retired_history() {
    use std::sync::Arc;

    use super::interposed_store::{Action, Interposed};
    let (mut wallet, _, saga) = interrupted().await;
    let saga = save_ready(&wallet, saga).await;
    let OperationData::Send(data) = &saga.data else {
        panic!("send");
    };
    let intent = data.confirmation.as_ref().unwrap();
    let receipt = Transaction {
        role: cdk_common::wallet::TransactionRole::Primary,
        mint_url: wallet.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: intent.pre_swap.as_ref().unwrap().fee + intent.send_fee,
        unit: wallet.unit.clone(),
        ys: intent.ready.as_ref().unwrap().send.ys().unwrap(),
        timestamp: saga.created_at,
        memo: intent.token_memo.clone(),
        metadata: intent.metadata.clone(),
        quote_id: None,
        payment_request: None,
        payment_proof: None,
        payment_method: None,
        saga_id: Some(saga.id),
        status: TransactionStatus::Pending,
    };
    let db = Arc::new(Interposed::new(wallet.localstore.clone()));
    *db.before_receipt.lock().unwrap() = Some(Action::Retire(receipt.clone()));
    wallet.localstore = db;
    assert!(wallet
        .complete_send_confirmation(saga.clone(), true)
        .await
        .is_err());
    assert!(wallet
        .localstore
        .get_saga(&saga.id)
        .await
        .unwrap()
        .is_none());
    assert!(wallet
        .localstore
        .get_transaction(receipt.id())
        .await
        .unwrap()
        .is_none());
    assert!(wallet
        .localstore
        .get_proofs_by_ys(receipt.ys)
        .await
        .unwrap()
        .iter()
        .all(|p| p.state == State::Spent));
}

#[tokio::test]
async fn unsupported_send_storage_makes_no_mint_request() {
    use std::sync::Arc;

    use super::interposed_store::Interposed;
    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
        MockMintConnector,
    };
    for proof_supported in [false, true] {
        let db = create_test_db().await;
        let proof = test_proof_info(test_keyset().id, 64, test_mint_url());
        db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
        let mint = Arc::new(MockMintConnector::new());
        mint.reset_default_mint_state();
        let mut adapter = Interposed::new(db.clone());
        adapter.proof_cas_supported = proof_supported;
        adapter.completion_supported = !proof_supported;
        let wallet = create_test_wallet_with_mock(Arc::new(adapter), mint.clone()).await;
        assert!(wallet
            .prepare_send(32.into(), Default::default())
            .await
            .is_err());
        assert!(mint.captured_swap_requests().is_empty());
        assert_eq!(
            db.get_proofs_by_ys(vec![proof.y]).await.unwrap(),
            vec![proof]
        );
    }
}

#[tokio::test]
async fn signed_p2bk_send_preserves_local_evidence() {
    use std::sync::Arc;

    use crate::nuts::nut28::{blind_public_key, ecdh_kdf};
    use crate::nuts::{CurrencyUnit, Proof, SecretKey, SpendingConditions};
    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, MockMintConnector,
    };
    use crate::wallet::{P2PKLockedProofSendMode, SendOptions};

    for reclaim in [false, true] {
        let receiver = SecretKey::generate();
        let ephemeral = SecretKey::generate();
        let tweak = ecdh_kdf(&ephemeral, &receiver.public_key(), 0).unwrap();
        let blinded = blind_public_key(&receiver.public_key(), &tweak).unwrap();
        let secret: crate::nuts::nut10::Secret = SpendingConditions::new_p2pk(blinded, None)
            .try_into()
            .unwrap();
        let mut keyset = test_keyset();
        keyset.input_fee_ppk = 0;
        let mut proof = Proof::new(
            32.into(),
            keyset.id,
            secret.try_into().unwrap(),
            SecretKey::generate().public_key(),
        );
        proof.p2pk_e = Some(ephemeral.public_key());
        let mut original = cdk_common::wallet::ProofInfo::new(
            proof,
            test_mint_url(),
            State::Unspent,
            CurrencyUnit::Sat,
        )
        .unwrap();
        original.derivation_index = Some(42);
        original.created_by_operation = Some(uuid::Uuid::new_v4());
        let db = create_test_db().await;
        db.update_proofs(vec![original.clone()], vec![])
            .await
            .unwrap();
        let mint = Arc::new(MockMintConnector::new());
        mint.set_active_keyset(keyset);
        let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
        let prepared = wallet
            .prepare_send(
                32.into(),
                SendOptions {
                    p2pk_locked_proof_send_mode: P2PKLockedProofSendMode::SignAndSend,
                    p2pk_signing_keys: vec![receiver],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let operation = prepared.operation_id();
        assert!(prepared.proofs_to_swap().is_empty());
        let _ = prepared.confirm(None).await.unwrap();
        let stored = db
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap()
            .remove(0);
        stored.proof.verify_p2pk().unwrap();
        original.state = State::PendingSpent;
        original.used_by_operation = Some(operation);
        original.proof.witness = stored.proof.witness.clone();
        assert_eq!(stored, original);
        assert!(mint.captured_swap_requests().is_empty());
        let parent = db.get_saga(&operation).await.unwrap().unwrap();
        let OperationData::Send(data) = &parent.data else {
            panic!("send");
        };
        assert!(data.proofs.as_ref().unwrap()[0].p2pk_e.is_none());
        mint.set_check_state_response(Ok(crate::nuts::CheckStateResponse {
            states: vec![crate::nuts::ProofState {
                y: original.y,
                state: if reclaim {
                    State::Unspent
                } else {
                    State::Spent
                },
                witness: None,
            }],
        }));
        if reclaim {
            mint.set_post_swap_response(Err(crate::Error::Timeout));
            assert!(matches!(
                wallet.revoke_send(operation).await,
                Err(crate::Error::Timeout)
            ));
            let saved = db.get_saga(&operation).await.unwrap().unwrap();
            let OperationData::Send(data) = &saved.data else {
                panic!("send");
            };
            let swap = &data.reclaim.as_ref().unwrap().pre_swap;
            swap.swap_request.inputs()[0].verify_p2pk().unwrap();
            let outputs = swap.swap_request.outputs().clone();
            let signatures = outputs
                .iter()
                .map(|output| crate::nuts::BlindSignature {
                    amount: output.amount,
                    keyset_id: output.keyset_id,
                    c: output.blinded_secret,
                    dleq: None,
                })
                .collect();
            mint._set_restore_response(Ok(crate::nuts::RestoreResponse {
                outputs,
                signatures,
            }));
            assert_eq!(wallet.revoke_send(operation).await.unwrap(), 32.into());
        } else {
            assert!(wallet.check_send_status(operation).await.unwrap());
        }
        original.state = State::Spent;
        assert_eq!(
            db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            [original]
        );
        assert!(db.get_saga(&operation).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn reserved_batch_metadata_cannot_move_a_send_receipt() {
    use std::sync::Arc;

    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
        MockMintConnector,
    };
    use crate::wallet::SendOptions;
    let db = create_test_db().await;
    let original = test_proof_info(test_keyset().id, 64, test_mint_url());
    db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let mut options = SendOptions::default();
    options
        .metadata
        .insert("batch_quote_id".to_owned(), "reserved".to_owned());
    assert!(wallet.prepare_send(32.into(), options).await.is_err());
    assert!(mint.captured_swap_requests().is_empty());
    assert_eq!(
        db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
        vec![original]
    );
    assert!(db.get_incomplete_sagas().await.unwrap().is_empty());

    let (wallet, mint, mut saga) = interrupted().await;
    let OperationData::Send(data) = &mut saga.data else {
        panic!("send");
    };
    data.confirmation
        .as_mut()
        .unwrap()
        .metadata
        .insert("batch_quote_id".to_owned(), "reserved".to_owned());
    saga.update_state(saga.state);
    wallet.localstore.update_saga(saga.clone()).await.unwrap();
    assert!(wallet
        .complete_send_confirmation(saga.clone(), true)
        .await
        .is_err());
    assert_eq!(
        wallet.localstore.get_saga(&saga.id).await.unwrap(),
        Some(saga)
    );
    assert_eq!(mint.captured_swap_requests().len(), 1);
    assert!(wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
}
