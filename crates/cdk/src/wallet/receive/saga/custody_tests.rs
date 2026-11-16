//! Receive custody and recovery regressions.
use super::test_utils::{fixture, options};
use crate::nuts::State;
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};
use crate::Amount;

#[tokio::test]
async fn unfamiliar_receive_checks_mint_state_before_acquisition() {
    use crate::nuts::CheckStateResponse;
    use crate::Error;

    let (wallet, client) = fixture().await;
    let input = test_proof_info(test_keyset().id, 8, test_mint_url());
    for response in [
        Ok(CheckStateResponse {
            states: vec![(input.y, State::Spent).into()],
        }),
        Ok(CheckStateResponse {
            states: vec![(input.y, State::Pending).into()],
        }),
        Ok(CheckStateResponse { states: vec![] }),
        Ok(CheckStateResponse {
            states: vec![(input.y, State::Reserved).into()],
        }),
        Err(Error::Custom("state query unavailable".to_owned())),
    ] {
        client.set_check_state_response(response);
        assert!(wallet
            .receive_proofs(vec![input.proof.clone()], options(), None, None)
            .await
            .is_err());
        assert!(wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(wallet
            .localstore
            .get_incomplete_sagas()
            .await
            .unwrap()
            .is_empty());
        assert!(client.captured_swap_requests().is_empty());
        assert_eq!(
            wallet
                .localstore
                .increment_keyset_counter(&test_keyset().id, 0)
                .await
                .unwrap(),
            0
        );
    }
    client.set_check_state_response(Ok(CheckStateResponse {
        states: vec![(input.y, State::Unspent).into()],
    }));
    assert_eq!(
        wallet
            .receive_proofs(vec![input.proof], options(), None, None)
            .await
            .unwrap(),
        Amount::from(8)
    );
    assert_eq!(client.captured_swap_requests().len(), 1);
}

#[tokio::test]
async fn receive_admission_preserves_spent_and_reserved_records_and_counter() {
    for state in [State::Spent, State::Reserved] {
        let (wallet, client) = fixture().await;
        let mut input = test_proof_info(test_keyset().id, 8, test_mint_url());
        input.state = state;
        input.used_by_operation = Some(uuid::Uuid::new_v4());
        input.created_by_operation = Some(uuid::Uuid::new_v4());
        input.derivation_index = Some(42);
        wallet
            .localstore
            .update_proofs(vec![input.clone()], vec![])
            .await
            .unwrap();
        let result = wallet
            .receive_proofs(vec![input.proof.clone()], options(), None, None)
            .await;
        assert!(result.is_err());
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![input.y])
                .await
                .unwrap(),
            [input]
        );
        assert!(client.captured_swap_requests().is_empty());
        assert_eq!(
            wallet
                .localstore
                .increment_keyset_counter(&test_keyset().id, 0)
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn receive_handoff_preserves_complete_original_input() {
    let (wallet, _) = fixture().await;
    let mut input = test_proof_info(test_keyset().id, 8, test_mint_url());
    input.created_by_operation = Some(uuid::Uuid::new_v4());
    input.derivation_index = Some(42);
    wallet
        .localstore
        .update_proofs(vec![input.clone()], vec![])
        .await
        .unwrap();
    let amount = wallet
        .receive_proofs(vec![input.proof.clone()], options(), None, None)
        .await
        .unwrap();
    assert_eq!(amount, Amount::from(8));
    input.state = State::Spent;
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![input.y])
            .await
            .unwrap(),
        [input]
    );
}

#[tokio::test]
async fn receive_allocates_its_output_counter_range_only_once() {
    let (wallet, _) = fixture().await;
    let input = test_proof_info(test_keyset().id, 8, test_mint_url());
    wallet
        .receive_proofs(vec![input.proof], options(), None, None)
        .await
        .unwrap();
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
async fn receive_cancellation_restores_full_original_and_rejects_stale_acquisition() {
    let (wallet, client) = fixture().await;
    let mut original = test_proof_info(test_keyset().id, 8, test_mint_url());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.derivation_index = Some(42);
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
    let saga = super::receipt::start(&wallet, &prepared.state_data, inputs.clone())
        .await
        .unwrap();
    wallet.resume_receive_saga(&saga).await.unwrap();
    assert!(!wallet
        .localstore
        .compare_and_swap_proofs(inputs, Some(&saga))
        .await
        .unwrap());
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [original.clone()]
    );
    assert!(client.captured_swap_requests().is_empty());
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        0
    );
    let retry = super::ReceiveSaga::new(&wallet)
        .prepare(vec![original.proof], options(), None)
        .await
        .unwrap();
    let inputs = super::admission::plan(&wallet, &retry.state_data)
        .await
        .unwrap();
    let next = super::receipt::start(&wallet, &retry.state_data, inputs)
        .await
        .unwrap();
    assert_ne!(next.id, saga.id);
    assert_eq!(
        super::receipt::checkpoint(&next).unwrap().transaction.id(),
        super::receipt::checkpoint(&saga).unwrap().transaction.id()
    );
    assert!(super::receipt::cancel(&wallet, saga.clone()).await.is_err());
    assert!(prepared.execute().await.is_err());
    assert_eq!(
        wallet.localstore.get_saga(&next.id).await.unwrap(),
        Some(next.clone())
    );
    assert!(client.captured_swap_requests().is_empty());
    wallet.resume_receive_saga(&next).await.unwrap();
}

#[tokio::test]
async fn rejected_acquisition_does_not_allocate_secrets_or_contact_mint() {
    let (wallet, client) = fixture().await;
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
    let mut reserved = original.clone();
    reserved.state = State::Reserved;
    reserved.used_by_operation = Some(uuid::Uuid::new_v4());
    wallet
        .localstore
        .update_proofs(vec![reserved.clone()], vec![])
        .await
        .unwrap();
    assert!(super::receipt::start(&wallet, &prepared.state_data, inputs)
        .await
        .is_err());
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [reserved]
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        0
    );
    assert!(client.captured_swap_requests().is_empty());
}
