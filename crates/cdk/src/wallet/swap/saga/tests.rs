use std::sync::Arc;

use cdk_common::nuts::State;

use super::SwapSaga;
use crate::amount::SplitTarget;
use crate::nuts::{BlindSignature, SecretKey as Nut01SecretKey, SwapResponse};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, test_proof_info,
    MockMintConnector,
};
use crate::{Amount, Error};

#[tokio::test]
async fn test_prepare_swap_reserves_proofs_for_operation() {
    let db = create_test_db().await;
    let mint_url = test_mint_url();
    let keyset_id = test_keyset_id();
    let proof_info = test_proof_info(keyset_id, 100, mint_url);
    let proof_y = proof_info.y;
    db.update_proofs(vec![proof_info], vec![]).await.unwrap();

    let mock_client = Arc::new(MockMintConnector::new());
    mock_client.reset_default_mint_state();
    let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

    let saga = SwapSaga::new(&wallet);
    let prepared = saga
        .prepare(
            None,
            SplitTarget::default(),
            wallet.get_unspent_proofs().await.unwrap(),
            None,
            false,
            false,
        )
        .await
        .unwrap();

    let reserved = db
        .get_reserved_proofs(&prepared.state_data.saga.id)
        .await
        .unwrap();
    assert_eq!(reserved.len(), 1);
    assert_eq!(reserved[0].y, proof_y);
    assert_eq!(reserved[0].state, State::Reserved);

    let stored = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].state, State::Reserved);
    assert_eq!(
        stored[0].used_by_operation,
        Some(prepared.state_data.saga.id)
    );
}

/// A standalone swap cannot acquire another operation's reserved proofs.
#[tokio::test]
async fn test_swap_prepare_fails_on_already_reserved_proofs() {
    let db = create_test_db().await;
    let mint_url = test_mint_url();
    let keyset_id = test_keyset_id();
    let proof_info = test_proof_info(keyset_id, 100, mint_url);
    let proof_y = proof_info.y;
    db.update_proofs(vec![proof_info], vec![]).await.unwrap();

    // Reserve the proof under a "parent" operation
    let parent_op_id = uuid::Uuid::new_v4();
    db.reserve_proofs(vec![proof_y], &parent_op_id)
        .await
        .unwrap();

    let mock_client = Arc::new(MockMintConnector::new());
    mock_client.reset_default_mint_state();
    let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

    // Get the (now reserved) proofs directly from DB for the swap input
    let reserved_proofs: Vec<_> = db
        .get_reserved_proofs(&parent_op_id)
        .await
        .unwrap()
        .into_iter()
        .map(|pi| pi.proof)
        .collect();
    assert_eq!(reserved_proofs.len(), 1);

    let saga = SwapSaga::new(&wallet);
    let result = saga
        .prepare(
            None,
            SplitTarget::default(),
            reserved_proofs,
            None,
            false,
            false,
        )
        .await;

    // Should fail because the proofs are already Reserved, not Unspent
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Proof not in unspent state"),
        "Expected ProofNotUnspent error, got: {}",
        err
    );
}

#[tokio::test]
async fn test_execute_rejects_signature_with_mismatched_amount() {
    let db = create_test_db().await;
    let mint_url = test_mint_url();
    let keyset_id = test_keyset_id();

    let proof_info = test_proof_info(keyset_id, 3, mint_url);
    db.update_proofs(vec![proof_info], vec![]).await.unwrap();

    let mock_client = Arc::new(MockMintConnector::new());
    mock_client.reset_default_mint_state();
    let wallet = create_test_wallet_with_mock(db, mock_client.clone()).await;

    let input_proofs = wallet.get_unspent_proofs().await.unwrap();
    let prepared = SwapSaga::new(&wallet)
        .prepare(
            None,
            SplitTarget::Values(vec![Amount::from(2)]),
            input_proofs,
            None,
            false,
            false,
        )
        .await
        .expect("prepare swap saga");

    let outputs = super::checkpoint::get(&prepared.state_data.saga)
        .unwrap()
        .pre_swap
        .swap_request
        .outputs()
        .clone();
    let bad_signatures = outputs
        .iter()
        .map(|blinded_message| BlindSignature {
            amount: Amount::from(1),
            keyset_id: blinded_message.keyset_id,
            c: Nut01SecretKey::generate().public_key(),
            dleq: None,
        })
        .collect();

    mock_client.set_post_swap_response(Ok(SwapResponse {
        signatures: bad_signatures,
    }));

    let result = prepared.execute().await;

    assert!(matches!(result, Err(Error::InvalidMintResponse(_))));
}
