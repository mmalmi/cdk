//! Parent swap interruption and custody regressions.
use std::collections::HashMap;

use cdk_common::wallet::{MeltSagaState, OperationData, WalletSagaState};

use super::swap_fixture::Fixture;
use crate::nuts::{ProofsMethods, State};
use crate::{Amount, Error};

#[tokio::test]
async fn lost_swap_reply_keeps_only_parent_melt_journal() {
    let f = Fixture::new().await;
    let prepared = super::MeltSaga::new(&f.wallet)
        .prepare(&f.quote.id, HashMap::new())
        .await
        .unwrap();
    let id = prepared.operation_id();
    assert!(!prepared.proofs_to_swap().is_empty());
    f.client.set_post_swap_response(Err(Error::Timeout));
    assert!(prepared
        .request_melt_with_options(Default::default())
        .await
        .is_err());
    assert_eq!(f.client.captured_swap_requests().len(), 1);
    let sagas = f.wallet.localstore.get_incomplete_sagas().await.unwrap();
    assert_eq!(
        sagas.len(),
        1,
        "a parent melt must own the only recovery journal"
    );
    assert_eq!(sagas[0].id, id);
    assert!(matches!(sagas[0].state, WalletSagaState::Melt(_)));
}

#[tokio::test]
async fn parent_restore_releases_replacements_and_preserves_original_evidence() {
    let f = Fixture::new().await;
    let saga = f.interrupted().await;
    let swap = super::swap::saved(&saga).unwrap();
    let original = swap.inputs[0].clone();
    let counter = f
        .wallet
        .localstore
        .increment_keyset_counter(&original.proof.keyset_id, 0)
        .await
        .unwrap();
    f.client._set_restore_response(Ok(f.restore(&saga)));
    let result = f.wallet.resume_melt_saga(&saga).await.unwrap().unwrap();
    assert_eq!(result.state(), crate::nuts::MeltQuoteState::Unpaid);
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(32));
    assert!(f
        .wallet
        .localstore
        .get_incomplete_sagas()
        .await
        .unwrap()
        .is_empty());
    let mut spent = original.clone();
    spent.state = State::Spent;
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(vec![original.y])
            .await
            .unwrap(),
        vec![spent]
    );
    assert_eq!(
        f.wallet
            .localstore
            .increment_keyset_counter(&original.proof.keyset_id, 0)
            .await
            .unwrap(),
        counter
    );
    assert_eq!(f.client.captured_swap_requests().len(), 1);
    assert!(f
        .wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap()
        .is_empty());
    let outputs = f
        .wallet
        .localstore
        .get_proofs(
            Some(f.wallet.mint_url.clone()),
            Some(f.wallet.unit.clone()),
            Some(vec![State::Unspent]),
            None,
        )
        .await
        .unwrap();
    for output in outputs {
        let secret = swap
            .pre_swap
            .pre_mint_secrets
            .secrets
            .iter()
            .find(|s| s.secret == output.proof.secret)
            .unwrap();
        assert_eq!(output.derivation_index, secret.derivation_index);
        assert_eq!(output.created_by_operation, Some(saga.id));
    }
}

#[tokio::test]
async fn partial_parent_restore_retains_exact_intent_and_all_custody() {
    let f = Fixture::new().await;
    let saga = f.interrupted().await;
    let originals = super::swap::saved(&saga).unwrap().inputs.clone();
    let mut response = f.restore(&saga);
    response.outputs.pop();
    response.signatures.pop();
    f.client._set_restore_response(Ok(response));
    assert!(f.wallet.resume_melt_saga(&saga).await.is_err());
    let retained = f
        .wallet
        .localstore
        .get_saga(&saga.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.state,
        WalletSagaState::Melt(MeltSagaState::SwapCancelling)
    );
    assert_eq!(retained.data, saga.data);
    assert_eq!(
        f.wallet
            .localstore
            .get_reserved_proofs(&saga.id)
            .await
            .unwrap(),
        originals
    );
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::ZERO);
    f.client._set_restore_response(Ok(f.restore(&retained)));
    f.wallet.resume_melt_saga(&retained).await.unwrap().unwrap();
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(32));
}

#[tokio::test]
async fn retry_preserves_later_output_owners_and_metadata_without_partial_handoff() {
    let f = Fixture::new().await;
    let mut saga = f.ready().await;
    let records = super::swap_custody::records(&f.wallet, &saga)
        .await
        .unwrap();
    let target = records
        .iter()
        .find(|p| p.used_by_operation.is_none())
        .unwrap();
    let mut later = target.clone();
    later.state = State::Spent;
    later.used_by_operation = Some(uuid::Uuid::new_v4());
    later.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
        crate::nuts::nut11::P2PKWitness {
            signatures: vec!["later-evidence".to_owned()],
        },
    ));
    f.wallet
        .localstore
        .update_proofs(vec![later.clone()], vec![])
        .await
        .unwrap();
    // An altered signature with the same secret/Y is not this verified output.
    let send = records
        .iter()
        .find(|p| p.used_by_operation == Some(saga.id))
        .unwrap();
    let mut changed = send.clone();
    changed.proof.c = crate::nuts::SecretKey::generate().public_key();
    f.wallet
        .localstore
        .update_proofs(vec![changed.clone()], vec![])
        .await
        .unwrap();
    assert!(super::swap::cancel(&f.wallet, &mut saga).await.is_err());
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(vec![send.y])
            .await
            .unwrap(),
        vec![changed]
    );
    f.wallet
        .localstore
        .update_proofs(vec![send.clone()], vec![])
        .await
        .unwrap();
    super::swap::cancel(&f.wallet, &mut saga).await.unwrap();
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(vec![later.y])
            .await
            .unwrap(),
        vec![later]
    );
}

#[tokio::test]
async fn cancelled_parent_fences_stale_response_before_later_spending() {
    let f = Fixture::new().await;
    let mut stale = f.interrupted().await;
    f.client._set_restore_response(Ok(f.restore(&stale)));
    f.wallet.resume_melt_saga(&stale).await.unwrap().unwrap();
    let proofs = f.wallet.get_unspent_proofs().await.unwrap();
    f.wallet
        .localstore
        .reserve_proofs(proofs.ys().unwrap(), &uuid::Uuid::new_v4())
        .await
        .unwrap();
    let later = f
        .wallet
        .localstore
        .get_proofs_by_ys(proofs.ys().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        super::swap::finish(&f.wallet, &mut stale, true).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap(),
        later
    );
    assert_eq!(f.client.captured_swap_requests().len(), 1);
}

#[tokio::test]
async fn payment_phase_wins_against_stale_swap_cancellation() {
    let f = Fixture::new().await;
    let mut saga = f.ready().await;
    let mut stale = saga.clone();
    let proofs = super::swap::saved(&saga)
        .unwrap()
        .outputs
        .as_ref()
        .unwrap()
        .send
        .clone();
    super::swap_custody::pending(&f.wallet, &saga, &proofs)
        .await
        .unwrap();
    saga.update_state(WalletSagaState::Melt(MeltSagaState::MeltRequested));
    if let OperationData::Melt(data) = &mut saga.data {
        data.final_proof_ys = Some(proofs.ys().unwrap());
    }
    assert!(f.wallet.localstore.update_saga(saga.clone()).await.unwrap());
    let before = f
        .wallet
        .localstore
        .get_proofs_by_ys(proofs.ys().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        super::swap::cancel(&f.wallet, &mut stale).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert_eq!(
        f.wallet
            .localstore
            .get_proofs_by_ys(proofs.ys().unwrap())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        f.wallet.localstore.get_saga(&saga.id).await.unwrap(),
        Some(saga)
    );
}
