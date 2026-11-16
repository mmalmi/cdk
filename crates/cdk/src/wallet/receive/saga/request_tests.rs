//! Receive custody and recovery regressions.
use std::sync::Arc;

use super::test_utils::{fixture, options, ready};
use crate::nuts::{BlindSignature, State, SwapResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::Amount;

#[tokio::test]
#[allow(clippy::use_debug)]
async fn receive_checkpoint_roundtrip_redacts_request_secrets() {
    let (_, _, saga) = ready().await;
    let encoded = serde_json::to_string(&saga).unwrap();
    assert_eq!(
        serde_json::from_str::<cdk_common::wallet::WalletSaga>(&encoded).unwrap(),
        saga
    );
    let saved = super::receipt::checkpoint(&saga).unwrap();
    let secret = saved.pre_swap.as_ref().unwrap().swap_request.inputs()[0]
        .secret
        .to_string();
    assert!(encoded.contains(&secret));
    assert!(!format!("{saga:?}").contains(&secret));
    assert!(!format!("{saved:?}").contains(&secret));
}

#[tokio::test]
async fn receive_recovery_replays_exact_sig_all_request_without_counter_changes() {
    use crate::nuts::nut10::SpendingConditionVerification;
    use crate::nuts::{Conditions, RestoreResponse, SecretKey, SigFlag, SpendingConditions};
    let (wallet, client) = fixture().await;
    let key = SecretKey::generate();
    let mut proof = test_proof_info(test_keyset().id, 8, test_mint_url()).proof;
    proof.secret = SpendingConditions::new_p2pk(
        key.public_key(),
        Some(Conditions {
            sig_flag: SigFlag::SigAll,
            ..Default::default()
        }),
    )
    .try_into()
    .unwrap();
    let mut opts = options();
    opts.p2pk_signing_keys.push(key);
    client.set_post_swap_response(Err(crate::Error::TokenAlreadySpent));
    assert!(wallet
        .receive_proofs(vec![proof], opts, None, None)
        .await
        .is_err());
    let saga = wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let saved = super::receipt::checkpoint(&saga)
        .unwrap()
        .pre_swap
        .as_ref()
        .unwrap();
    saved.swap_request.verify_spending_conditions().unwrap();
    crate::wallet::swap::outputs::validate_request(saved).unwrap();
    assert_eq!(client.captured_swap_requests()[0], saved.swap_request);
    client._set_restore_response(Ok(RestoreResponse {
        outputs: vec![],
        signatures: vec![],
    }));
    client.set_post_swap_response(Ok(SwapResponse {
        signatures: vec![BlindSignature {
            amount: 8.into(),
            keyset_id: test_keyset().id,
            c: SecretKey::generate().public_key(),
            dleq: None,
        }],
    }));
    wallet.resume_receive_saga(&saga).await.unwrap();
    assert_eq!(
        client.captured_swap_requests(),
        [saved.swap_request.clone(), saved.swap_request.clone()]
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn self_receive_preserves_source_send_and_distinct_terminal_receipts() {
    use cdk_common::wallet::{TransactionDirection, TransactionStatus};
    let (wallet, client) = fixture().await;
    let original = test_proof_info(test_keyset().id, 8, test_mint_url());
    wallet
        .localstore
        .update_proofs(vec![original], vec![])
        .await
        .unwrap();
    wallet
        .prepare_send(8.into(), Default::default())
        .await
        .unwrap()
        .confirm(None)
        .await
        .unwrap();
    let original = wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::PendingSpent]), None)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let send_id = original.used_by_operation.unwrap();
    wallet
        .receive_proofs(vec![original.proof.clone()], options(), None, None)
        .await
        .unwrap();
    let mut spent = original.clone();
    spent.state = State::Spent;
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![spent.y])
            .await
            .unwrap(),
        [spent]
    );
    client.set_check_state_response(Ok(crate::nuts::CheckStateResponse {
        states: vec![(original.y, State::Spent).into()],
    }));
    assert!(wallet.check_send_status(send_id).await.unwrap());
    let receipts = wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(receipts
        .iter()
        .all(|t| t.status == TransactionStatus::Completed));
    assert_ne!(receipts[0].id(), receipts[1].id());
    assert!(receipts
        .iter()
        .any(|t| t.direction == TransactionDirection::Incoming));
    assert!(receipts
        .iter()
        .any(|t| t.direction == TransactionDirection::Outgoing));
    let spendable = wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::Unspent]), None)
        .await
        .unwrap();
    assert_eq!(spendable.len(), 1);
    assert_eq!(spendable[0].proof.amount, 8.into());
}

#[tokio::test]
async fn p2bk_sig_all_receive_restores_signed_request_and_full_original_evidence() {
    use cdk_common::wallet::ProofInfo;

    use crate::nuts::nut10::SpendingConditionVerification;
    use crate::nuts::{Conditions, RestoreResponse, SecretKey, SigFlag, SpendingConditions};
    let client = Arc::new(MockMintConnector::new());
    client.enable_mint_signing();
    let keyset = client.keysets.lock().unwrap()[0].id;
    let wallet = create_test_wallet_with_mock(create_test_db().await, client.clone()).await;
    wallet.active_keyset().await.unwrap();
    let recipient = SecretKey::generate();
    let ephemeral = SecretKey::generate();
    let tweak = crate::nuts::nut28::ecdh_kdf(&ephemeral, &recipient.public_key(), 0).unwrap();
    let blinded_key =
        crate::nuts::nut28::blind_public_key(&recipient.public_key(), &tweak).unwrap();
    let derived =
        crate::nuts::nut28::derive_signing_key_bip340(&recipient, &tweak, &blinded_key).unwrap();
    let secret: crate::secret::Secret = SpendingConditions::new_p2pk(
        blinded_key,
        Some(Conditions {
            sig_flag: SigFlag::SigAll,
            ..Default::default()
        }),
    )
    .try_into()
    .unwrap();
    let (blinded, r) = crate::dhke::blind_message(secret.as_bytes(), None).unwrap();
    let mint_key =
        client.mint_signing_keys.lock().unwrap().as_ref().unwrap()[&Amount::from(8)].clone();
    let signature = BlindSignature::new(
        8.into(),
        crate::dhke::sign_message(&mint_key, &blinded).unwrap(),
        keyset,
        &blinded,
        &mint_key,
    )
    .unwrap();
    let mut proof = crate::dhke::construct_proofs(
        vec![signature],
        vec![r],
        vec![secret],
        &wallet.keyset(keyset).await.unwrap().keys,
    )
    .unwrap()
    .pop()
    .unwrap();
    proof.p2pk_e = Some(ephemeral.public_key());
    proof.sign_p2pk(derived).unwrap();
    let mut original = ProofInfo::new(
        proof.clone(),
        test_mint_url(),
        State::Unspent,
        wallet.unit.clone(),
    )
    .unwrap();
    original.derivation_index = Some(42);
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    client.set_post_swap_response(Err(crate::Error::Custom("lost signed reply".to_owned())));
    let mut opts = options();
    opts.p2pk_signing_keys.push(recipient);
    assert!(wallet
        .receive_proofs(vec![proof], opts, None, None)
        .await
        .is_err());
    let saga = wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let saved = super::receipt::checkpoint(&saga).unwrap();
    assert_eq!(saved.inputs[0].before.as_ref(), Some(&original));
    assert_eq!(
        saved.inputs[0].after.as_ref().unwrap().proof,
        original.proof
    );
    let request = &saved.pre_swap.as_ref().unwrap().swap_request;
    request.verify_spending_conditions().unwrap();
    assert!(request.inputs()[0].dleq.is_none());
    assert_eq!(request.inputs()[0].p2pk_e, original.proof.p2pk_e);
    let signatures = request
        .outputs()
        .iter()
        .map(|output| {
            BlindSignature::new(
                output.amount,
                crate::dhke::sign_message(&mint_key, &output.blinded_secret).unwrap(),
                keyset,
                &output.blinded_secret,
                &mint_key,
            )
            .unwrap()
        })
        .collect();
    client._set_restore_response(Ok(RestoreResponse {
        outputs: request.outputs().clone(),
        signatures,
    }));
    wallet.resume_receive_saga(&saga).await.unwrap();
    original.state = State::Spent;
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [original]
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&keyset, 0)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn reordered_multi_output_restore_preserves_each_allocated_derivation_index() {
    use crate::nuts::RestoreResponse;
    let (wallet, client) = fixture().await;
    client.set_post_swap_response(Err(crate::Error::Custom(
        "lost multi-output reply".to_owned(),
    )));
    let proof = test_proof_info(test_keyset().id, 8, test_mint_url()).proof;
    let mut opts = options();
    opts.amount_split_target =
        crate::amount::SplitTarget::Values(vec![4.into(), 2.into(), 1.into(), 1.into()]);
    assert!(wallet
        .receive_proofs(vec![proof], opts, None, None)
        .await
        .is_err());
    let saga = wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let swap = super::receipt::checkpoint(&saga)
        .unwrap()
        .pre_swap
        .as_ref()
        .unwrap();
    let outputs: Vec<_> = swap.swap_request.outputs().iter().rev().cloned().collect();
    let signatures = outputs
        .iter()
        .map(|o| BlindSignature {
            amount: o.amount,
            keyset_id: o.keyset_id,
            c: crate::nuts::SecretKey::generate().public_key(),
            dleq: None,
        })
        .collect();
    client._set_restore_response(Ok(RestoreResponse {
        outputs,
        signatures,
    }));
    wallet.resume_receive_saga(&saga).await.unwrap();
    let stored = wallet
        .localstore
        .get_proofs(None, None, Some(vec![State::Unspent]), None)
        .await
        .unwrap();
    assert_eq!(stored.len(), 4);
    for proof in stored {
        let expected = swap
            .pre_mint_secrets
            .secrets
            .iter()
            .find(|s| s.secret == proof.proof.secret)
            .unwrap();
        assert_eq!(proof.derivation_index, expected.derivation_index);
        assert!(proof.derivation_index.is_some());
    }
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        4
    );
}
