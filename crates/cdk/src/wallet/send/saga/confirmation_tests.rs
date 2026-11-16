use std::sync::Arc;

use cdk_common::wallet::{OperationData, SendSagaState, WalletSaga, WalletSagaState};

use crate::nuts::{BlindSignature, ProofsMethods, RestoreResponse, SecretKey, State, SwapResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::wallet::SendOptions;
use crate::{Amount, Error, Wallet};

pub(super) async fn interrupted() -> (Wallet, Arc<MockMintConnector>, WalletSaga) {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 0;
    db.update_proofs(
        vec![test_proof_info(keyset.id, 64, test_mint_url())],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    mint.set_active_keyset(keyset);
    mint.set_post_swap_response(Err(Error::Timeout));
    let mut wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    wallet.set_target_proof_count(0);
    let prepared = wallet
        .prepare_send(
            32.into(),
            SendOptions {
                max_total_amount: Some(32.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id = prepared.operation_id();
    let error = prepared.confirm(None).await.err().unwrap();
    assert!(matches!(error, Error::Timeout), "{error}");
    let saga = db.get_saga(&id).await.unwrap().unwrap();
    assert_eq!(saga.state, WalletSagaState::Send(SendSagaState::Confirming));
    assert_eq!(
        db.get_incomplete_sagas().await.unwrap().len(),
        1,
        "one send owns the swap"
    );
    let encoded = serde_json::to_vec(&saga).unwrap();
    assert!(serde_json::from_slice::<WalletSaga>(&encoded).unwrap() == saga);
    let OperationData::Send(data) = &saga.data else {
        panic!("send");
    };
    let swap = data
        .confirmation
        .as_ref()
        .unwrap()
        .pre_swap
        .as_ref()
        .unwrap();
    let local = serde_json::to_value(swap).unwrap();
    assert!(local["pre_mint_secrets"]["secrets"]
        .as_array()
        .unwrap()
        .iter()
        .all(|secret| secret["derivation_index"].is_u64()));
    let wire = serde_json::to_value(&mint.captured_swap_requests()[0]).unwrap();
    assert_eq!(wire, local["swap_request"]);
    assert_eq!(
        wire.as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["inputs", "outputs"]
    );
    assert!(!wire.to_string().contains("derivation_index"));
    (wallet, mint, saga)
}

pub(super) fn response(saga: &WalletSaga) -> RestoreResponse {
    let OperationData::Send(data) = &saga.data else {
        panic!("send")
    };
    let swap = data
        .confirmation
        .as_ref()
        .unwrap()
        .pre_swap
        .as_ref()
        .unwrap();
    let outputs = swap.swap_request.outputs().clone();
    let signatures = outputs
        .iter()
        .map(|p| BlindSignature {
            amount: p.amount,
            keyset_id: p.keyset_id,
            c: SecretKey::generate().public_key(),
            dleq: None,
        })
        .collect();
    RestoreResponse {
        outputs,
        signatures,
    }
}

#[tokio::test]
async fn restore_requires_exact_complete_outputs_before_importing() {
    let (wallet, mint, saga) = interrupted().await;
    let good = response(&saga);
    assert_eq!(good.outputs.len(), 2);
    for fault in 0..6 {
        let mut bad = good.clone();
        match fault {
            0 => {
                bad.outputs.pop();
                bad.signatures.pop();
            }
            1 => {
                bad.outputs[1] = bad.outputs[0].clone();
            }
            2 => {
                bad.outputs[0].blinded_secret = SecretKey::generate().public_key();
            }
            3 => {
                bad.outputs[0].amount = 16.into();
            }
            4 => {
                bad.signatures[0].amount = 16.into();
            }
            _ => {
                bad.signatures.clear();
            }
        }
        mint._set_restore_response(Ok(bad));
        assert!(wallet
            .complete_send_confirmation(saga.clone(), true)
            .await
            .is_err());
        assert!(wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap() == saga);
        assert_eq!(wallet.total_balance().await.unwrap(), Amount::ZERO);
        assert_eq!(
            wallet
                .get_reserved_proofs()
                .await
                .unwrap()
                .total_amount()
                .unwrap(),
            Amount::from(64)
        );
        assert_eq!(mint.captured_swap_requests().len(), 1);
    }
    let mut reordered = good;
    reordered.outputs.reverse();
    reordered.signatures.reverse();
    mint._set_restore_response(Ok(reordered));
    let (token, proofs, completed) = wallet
        .complete_send_confirmation(saga.clone(), true)
        .await
        .unwrap();
    assert_eq!(completed.id, saga.id);
    assert_eq!(
        completed.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    );
    assert_eq!(proofs.total_amount().unwrap(), Amount::from(32));
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(32));
    let OperationData::Send(original) = &saga.data else {
        panic!("send");
    };
    let pre_swap = original
        .confirmation
        .as_ref()
        .unwrap()
        .pre_swap
        .as_ref()
        .unwrap();
    for premint in &pre_swap.pre_mint_secrets.secrets {
        let y = crate::dhke::hash_to_curve(premint.secret.as_bytes()).unwrap();
        let info = wallet
            .localstore
            .get_proofs_by_ys(vec![y])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(info.derivation_index, premint.derivation_index);
        assert_eq!(info.created_by_operation, Some(saga.id));
    }
    let transaction = wallet
        .get_transaction(cdk_common::wallet::TransactionId::from_saga_id(saga.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        transaction.status,
        cdk_common::wallet::TransactionStatus::Pending
    );

    assert!(!token.to_string().is_empty());
    assert_eq!(mint.captured_swap_requests().len(), 1);
}

#[tokio::test]
async fn empty_restore_retries_only_the_saved_request() {
    let (wallet, mint, saga) = interrupted().await;
    let restored = response(&saga);
    mint._set_restore_response(Ok(RestoreResponse {
        outputs: vec![],
        signatures: vec![],
    }));
    mint.set_post_swap_response(Ok(SwapResponse {
        signatures: restored.signatures,
    }));
    wallet.complete_send_confirmation(saga, true).await.unwrap();
    let requests = mint.captured_swap_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0] == requests[1],
        "retry must preserve original inputs and outputs"
    );
}

pub(super) async fn save_ready(wallet: &Wallet, mut saga: WalletSaga) -> WalletSaga {
    let signatures = response(&saga).signatures;
    let OperationData::Send(data) = &mut saga.data else {
        panic!("send")
    };
    let intent = data.confirmation.as_mut().unwrap();
    intent.ready = Some(
        crate::wallet::swap::outputs::resolve(
            wallet,
            intent.pre_swap.as_ref().unwrap(),
            Some(intent.swap_amount),
            &crate::amount::SplitTarget::None,
            intent.spending_conditions.as_ref(),
            SwapResponse { signatures },
        )
        .await
        .unwrap(),
    );
    saga.update_state(saga.state);
    assert!(wallet.localstore.update_saga(saga.clone()).await.unwrap());
    saga
}

#[tokio::test]
async fn ready_outputs_resume_without_network_and_preserve_used_change() {
    let (wallet, mint, saga) = interrupted().await;
    let saga = save_ready(&wallet, saga).await;
    let OperationData::Send(data) = &saga.data else {
        panic!("send")
    };
    let outputs = data.confirmation.as_ref().unwrap().ready.as_ref().unwrap();
    // Import through the production transaction before another send reserves change.
    wallet
        .import_send_outputs(&saga, data.confirmation.as_ref().unwrap(), outputs)
        .await
        .unwrap();
    let mut change = wallet
        .localstore
        .get_proofs_by_ys(vec![outputs.change[0].y().unwrap()])
        .await
        .unwrap()
        .remove(0);
    change.state = State::Reserved;
    change.used_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![change.clone()], vec![])
        .await
        .unwrap();
    let (_, proofs, _) = wallet
        .complete_send_confirmation(saga.clone(), true)
        .await
        .unwrap();
    assert_eq!(proofs.total_amount().unwrap(), Amount::from(32));
    assert!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![change.y])
            .await
            .unwrap()[0]
            == change
    );
    assert_eq!(mint.captured_swap_requests().len(), 1);
    // A stale recovery may not recreate an already claimed token.
    wallet
        .localstore
        .update_proofs_state(proofs.ys().unwrap(), State::Spent)
        .await
        .unwrap();
    assert!(wallet.complete_send_confirmation(saga, true).await.is_err());
    assert!(wallet
        .localstore
        .get_proofs_by_ys(proofs.ys().unwrap())
        .await
        .unwrap()
        .iter()
        .all(|p| p.state == State::Spent));
}

#[tokio::test]
async fn changed_ready_secrets_or_request_secrets_cannot_import_coins() {
    let (wallet, _, saga) = interrupted().await;
    let saga = save_ready(&wallet, saga).await;
    for change_request in [false, true] {
        let mut changed = wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap();
        let OperationData::Send(data) = &mut changed.data else {
            panic!("send")
        };
        let intent = data.confirmation.as_mut().unwrap();
        let foreign = test_proof_info(test_keyset().id, 32, test_mint_url()).proof;
        if change_request {
            intent.pre_swap.as_mut().unwrap().pre_mint_secrets.secrets[0].secret = foreign.secret;
        } else {
            intent.ready.as_mut().unwrap().send[0] = foreign;
        }
        changed.update_state(changed.state);
        assert!(wallet
            .localstore
            .update_saga(changed.clone())
            .await
            .unwrap());
        assert!(wallet
            .complete_send_confirmation(changed.clone(), true)
            .await
            .is_err());
        assert_eq!(wallet.total_balance().await.unwrap(), Amount::ZERO);
        assert!(wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap() == changed);
        let mut original = changed;
        original.data = saga.data.clone();
        original.update_state(original.state);
        assert!(wallet.localstore.update_saga(original).await.unwrap());
    }
}

#[tokio::test]
async fn direct_confirmation_recovers_without_mint_requests() {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 0;
    let mut original = test_proof_info(keyset.id, 32, test_mint_url());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.derivation_index = Some(7);
    original.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
        crate::nuts::P2PKWitness {
            signatures: vec!["existing witness".to_owned()],
        },
    ));
    original.proof.dleq = Some(crate::nuts::ProofDleq::new(
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    ));
    original.proof.p2pk_e = Some(SecretKey::generate().public_key());
    db.update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.set_active_keyset(keyset);
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let prepared = wallet
        .prepare_send(32.into(), SendOptions::default())
        .await
        .unwrap();
    assert!(prepared.proofs_to_swap().is_empty());
    let mut saga = db
        .get_saga(&prepared.operation_id())
        .await
        .unwrap()
        .unwrap();
    let OperationData::Send(data) = &mut saga.data else {
        panic!("send")
    };
    // The token's saved intent may omit optional local proof evidence.
    let mut retained = prepared.proofs_to_send().clone();
    retained[0].dleq = None;
    retained[0].witness = None;
    retained[0].p2pk_e = None;
    data.confirmation = Some(Box::new(cdk_common::wallet::SendConfirmation {
        pre_swap: None,
        swap_amount: Amount::ZERO,
        spending_conditions: None,
        retained_proofs: retained,
        send_fee: Amount::ZERO,
        include_fee: false,
        max_total_amount: Some(32.into()),
        required_keyset_id: None,
        token_memo: None,
        metadata: Default::default(),
        ready: None,
    }));
    saga.update_state(WalletSagaState::Send(SendSagaState::Confirming));
    assert!(db.update_saga(saga.clone()).await.unwrap());
    drop(prepared);
    let (_, proofs, done) = wallet.complete_send_confirmation(saga, true).await.unwrap();
    assert_eq!(proofs.total_amount().unwrap(), Amount::from(32));
    assert_eq!(
        done.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    );
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::ZERO);
    let mut expected = original;
    expected.state = State::PendingSpent;
    expected.used_by_operation = Some(done.id);
    assert_eq!(
        db.get_proofs_by_ys(vec![expected.y]).await.unwrap(),
        vec![expected]
    );
    assert!(mint.captured_swap_requests().is_empty());
}

#[tokio::test]
async fn ambiguous_send_keeps_one_durable_confirmation() {
    let db = create_test_db().await;
    let mut keyset = test_keyset();
    keyset.input_fee_ppk = 0;
    db.update_proofs(
        vec![test_proof_info(keyset.id, 64, test_mint_url())],
        vec![],
    )
    .await
    .unwrap();
    let mint = Arc::new(MockMintConnector::new());
    mint.reset_default_mint_state();
    mint.set_active_keyset(keyset);
    mint.set_post_swap_response(Err(Error::Timeout));
    let mut wallet = create_test_wallet_with_mock(db.clone(), mint).await;
    wallet.set_target_proof_count(0);
    let prepared = wallet
        .prepare_send(32.into(), SendOptions::default())
        .await
        .unwrap();
    let id = prepared.operation_id();
    let error = prepared.confirm(None).await.err().unwrap();
    assert!(matches!(error, Error::Timeout), "{error}");
    let saga = db.get_saga(&id).await.unwrap().unwrap();
    assert_ne!(
        saga.state,
        WalletSagaState::Send(SendSagaState::ProofsReserved),
        "ambiguous send cannot remain a compensatable reservation"
    );
    assert_eq!(
        db.get_incomplete_sagas().await.unwrap().len(),
        1,
        "one send must own the exact pending mint request"
    );
}
