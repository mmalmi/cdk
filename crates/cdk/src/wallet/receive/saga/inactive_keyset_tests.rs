//! Submitted errors retain the original request even if mint keys have rotated.
use cdk_common::wallet::{KeysetLoadPolicy, ReceiveSagaState, WalletSagaState};

use super::test_utils::{fixture, options};
use crate::nuts::{BlindSignature, RestoreResponse, SecretKey, State};
use crate::wallet::test_utils::{
    make_inactive_keyset, test_keyset, test_mint_url, test_proof_info,
};
use crate::Error;

#[tokio::test]
async fn submitted_inactive_keyset_retains_exact_receive_error_and_recoverable_intent() {
    let (wallet, client) = fixture().await;
    let mut original = test_proof_info(test_keyset().id, 8, test_mint_url());
    original.created_by_operation = Some(uuid::Uuid::new_v4());
    original.derivation_index = Some(42);
    wallet
        .localstore
        .update_proofs(vec![original.clone()], vec![])
        .await
        .unwrap();
    // The cached keyset selected for preparation remains valid historical
    // evidence even if a connector's final replay sees a rotated active keyset.
    let mut old = test_keyset();
    old.active = Some(false);
    old.input_fee_ppk = 0;
    let mut rotated = make_inactive_keyset();
    rotated.active = Some(true);
    client.set_mint_keys_response(Ok(vec![old, rotated]));
    client.set_post_swap_response(Err(Error::InactiveKeyset));
    let error = wallet
        .receive_proofs(
            vec![original.proof.clone()],
            options(),
            Some("stable memo".to_owned()),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::InactiveKeyset),
        "original mint rejection was replaced: {error}"
    );
    assert_eq!(
        wallet
            .active_keyset_with_policy(KeysetLoadPolicy::CacheOnly)
            .await
            .unwrap()
            .id,
        test_keyset().id
    );
    let sagas = wallet.localstore.get_incomplete_sagas().await.unwrap();
    assert_eq!(sagas.len(), 1);
    let saga = &sagas[0];
    assert_eq!(
        saga.state,
        WalletSagaState::Receive(ReceiveSagaState::Redeeming)
    );
    let checkpoint = super::receipt::checkpoint(saga).unwrap();
    let request = &checkpoint.pre_swap.as_ref().unwrap().swap_request;
    assert_eq!(checkpoint.inputs[0].before.as_ref(), Some(&original));
    assert_eq!(
        client.captured_swap_requests().as_slice(),
        std::slice::from_ref(request)
    );
    assert_eq!(
        wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        [checkpoint.inputs[0].after.clone().unwrap()]
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        1
    );
    assert!(wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
    // Exact restore evidence may still exist from an earlier successful request
    // whose reply was lost. It must complete this receipt, without a fresh swap.
    client._set_restore_response(Ok(RestoreResponse {
        outputs: request.outputs().clone(),
        signatures: request
            .outputs()
            .iter()
            .map(|output| BlindSignature {
                amount: output.amount,
                keyset_id: output.keyset_id,
                c: SecretKey::generate().public_key(),
                dleq: None,
            })
            .collect(),
    }));
    wallet.resume_receive_saga(saga).await.unwrap();
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
            .get_transaction(checkpoint.transaction.id())
            .await
            .unwrap(),
        Some(checkpoint.transaction.clone())
    );
    assert_eq!(
        wallet
            .localstore
            .increment_keyset_counter(&test_keyset().id, 0)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        client.captured_swap_requests().as_slice(),
        std::slice::from_ref(request)
    );
    assert!(wallet
        .localstore
        .get_saga(&saga.id)
        .await
        .unwrap()
        .is_none());
}
