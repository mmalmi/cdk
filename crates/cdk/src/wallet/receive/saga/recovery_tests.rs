//! Receive custody and recovery regressions.
use super::test_utils::{fixture, options};
use crate::nuts::{BlindSignature, State};
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};

#[tokio::test]
async fn cancellation_preserves_foreign_winner_before_acquisition_or_after_rollback() {
    use cdk_common::wallet::{ReceiveSagaState, WalletSagaState};
    for cancelling in [false, true] {
        let (wallet, _) = fixture().await;
        let original = test_proof_info(test_keyset().id, 8, test_mint_url());
        wallet
            .localstore
            .update_proofs(vec![original.clone()], vec![])
            .await
            .unwrap();
        let prepared = super::ReceiveSaga::new(&wallet)
            .prepare(vec![original.proof.clone()], options(), None)
            .await
            .unwrap();
        let inputs = super::admission::plan(&wallet, &prepared.state_data)
            .await
            .unwrap();
        let mut saga = super::receipt::start(&wallet, &prepared.state_data, inputs)
            .await
            .unwrap();
        if cancelling {
            saga.update_state(WalletSagaState::Receive(ReceiveSagaState::Cancelling));
            assert!(wallet.localstore.update_saga(saga.clone()).await.unwrap());
        }
        // Equivalent durable state after a foreign winner before acquisition,
        // or after our rollback committed but before cleanup.
        let mut foreign = original;
        foreign.state = State::Reserved;
        foreign.used_by_operation = Some(uuid::Uuid::new_v4());
        wallet
            .localstore
            .update_proofs(vec![foreign.clone()], vec![])
            .await
            .unwrap();
        wallet.resume_receive_saga(&saga).await.unwrap();
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![foreign.y])
                .await
                .unwrap(),
            [foreign]
        );
        assert!(wallet
            .localstore
            .get_saga(&saga.id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn receive_recovers_after_public_spent_check_preserving_original_metadata() {
    use crate::nuts::{CheckStateResponse, RestoreResponse};
    let (wallet, client) = fixture().await;
    let mut original = test_proof_info(test_keyset().id, 8, test_mint_url());
    original.derivation_index = Some(42);
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    client.set_post_swap_response(Err(crate::Error::Custom("lost reply".to_owned())));
    assert!(wallet
        .receive_proofs(vec![original.proof.clone()], options(), None, None)
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
    client.set_check_state_response(Ok(CheckStateResponse {
        states: vec![(original.y, State::Spent).into()],
    }));
    wallet
        .check_proofs_spent(vec![original.proof.clone()])
        .await
        .unwrap();
    client._set_restore_response(Ok(RestoreResponse {
        outputs: saved.swap_request.outputs().clone(),
        signatures: vec![BlindSignature {
            amount: 8.into(),
            keyset_id: test_keyset().id,
            c: crate::nuts::SecretKey::generate().public_key(),
            dleq: None,
        }],
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
}

#[tokio::test]
async fn cancellation_retains_observed_spent_evidence_without_inventing_a_receipt() {
    for stored in [false, true] {
        let (wallet, client) = fixture().await;
        let mut original = test_proof_info(test_keyset().id, 8, test_mint_url());
        original.derivation_index = Some(42);
        if stored {
            wallet
                .localstore
                .update_proofs(vec![original.clone()], vec![])
                .await
                .unwrap();
        }
        let prepared = super::ReceiveSaga::new(&wallet)
            .prepare(vec![original.proof.clone()], options(), None)
            .await
            .unwrap();
        let inputs = super::admission::plan(&wallet, &prepared.state_data)
            .await
            .unwrap();
        let saga = super::receipt::start(&wallet, &prepared.state_data, inputs)
            .await
            .unwrap();
        client.set_check_state_response(Ok(crate::nuts::CheckStateResponse {
            states: vec![(original.y, State::Spent).into()],
        }));
        wallet
            .check_proofs_spent(vec![original.proof.clone()])
            .await
            .unwrap();
        wallet.resume_receive_saga(&saga).await.unwrap();
        let mut expected = if stored {
            original.clone()
        } else {
            super::receipt::checkpoint(&saga).unwrap().inputs[0]
                .after
                .clone()
                .unwrap()
        };
        expected.state = State::Spent;
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![original.y])
                .await
                .unwrap(),
            [expected]
        );
        assert!(wallet
            .localstore
            .list_transactions(None, None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(wallet
            .localstore
            .get_saga(&saga.id)
            .await
            .unwrap()
            .is_none());
    }
}
