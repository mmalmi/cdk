use std::sync::Arc;

use cdk_common::wallet::KeysetLoadPolicy;

use super::test_utils::{prepare, response, submitted, wallet};
use super::*;
use crate::nuts::{BlindSignature, RestoreResponse, SecretKey, SwapResponse};
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_mint_url, test_proof_info, MockMintConnector,
};

#[tokio::test]
async fn repeated_denominations_require_every_restored_output_identity() {
    let (wallet, mint) = wallet().await;
    let prepared = SwapSaga::new(&wallet)
        .prepare(
            None,
            SplitTarget::Value(4.into()),
            wallet.get_unspent_proofs().await.unwrap(),
            None,
            false,
            false,
        )
        .await
        .unwrap();
    let id = prepared.state_data.saga.id;
    mint.set_post_swap_response(Err(Error::Timeout));
    assert!(prepared.execute().await.is_err());
    let saved = wallet.localstore.get_saga(&id).await.unwrap().unwrap();
    let full = response(&saved);
    let mut duplicate = full.clone();
    let (a, b) = (0..full.outputs.len())
        .find_map(|a| {
            ((a + 1)..full.outputs.len())
                .find(|b| full.outputs[a].amount == full.outputs[*b].amount)
                .map(|b| (a, b))
        })
        .unwrap();
    duplicate.outputs[b] = duplicate.outputs[a].clone();
    duplicate.signatures[b] = duplicate.signatures[a].clone();
    mint._set_restore_response(Ok(duplicate));
    assert!(wallet.resume_swap_saga(&saved).await.is_err());
    assert_eq!(wallet.total_balance().await.unwrap().to_u64(), 0);
    let current = wallet.localstore.get_saga(&id).await.unwrap().unwrap();
    mint._set_restore_response(Ok(RestoreResponse {
        outputs: full.outputs.into_iter().rev().collect(),
        signatures: full.signatures.into_iter().rev().collect(),
    }));
    assert!(matches!(
        wallet.resume_swap_saga(&current).await.unwrap(),
        RecoveryAction::Recovered
    ));
}

#[tokio::test]
async fn saved_outputs_reverify_present_dleq_before_import() {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    mint.enable_mint_signing();
    let keyset = mint.keysets.lock().unwrap()[0].id;
    db.update_proofs(vec![test_proof_info(keyset, 32, test_mint_url())], vec![])
        .await
        .unwrap();
    let mut wallet = create_test_wallet_with_mock(db, mint.clone()).await;
    wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();
    let mut saved = prepare(&wallet, None, false).await.state_data.saga;
    transition(&wallet, &mut saved, SwapSagaState::SwapRequested)
        .await
        .unwrap();
    let signatures = checkpoint::get(&saved)
        .unwrap()
        .pre_swap
        .swap_request
        .outputs()
        .iter()
        .map(|output| {
            let keys = mint.mint_signing_keys.lock().unwrap();
            let key = &keys.as_ref().unwrap()[&output.amount];
            let c = crate::dhke::sign_message(key, &output.blinded_secret).unwrap();
            BlindSignature::new(
                output.amount,
                c,
                output.keyset_id,
                &output.blinded_secret,
                key,
            )
            .unwrap()
        })
        .collect();
    let ready = checkpoint::resolve(&wallet, &saved, SwapResponse { signatures })
        .await
        .unwrap();
    let store = Arc::new(Interposed::new(wallet.localstore.clone()));
    *store.before_proofs.lock().unwrap() = Some(Action::Fail);
    wallet.localstore = store;
    assert!(checkpoint::finish(&wallet, &mut saved, ready.clone())
        .await
        .is_err());
    let mut altered = saved.clone();
    if let OperationData::Swap(data) = &mut altered.data {
        data.checkpoint
            .as_mut()
            .unwrap()
            .ready
            .as_mut()
            .unwrap()
            .send[0]
            .c = SecretKey::generate().public_key();
    }
    altered.update_state(altered.state);
    wallet
        .localstore
        .update_saga(altered.clone())
        .await
        .unwrap();
    assert!(wallet.resume_swap_saga(&altered).await.is_err());
    assert_eq!(wallet.total_balance().await.unwrap().to_u64(), 0);
    let mut current = wallet
        .localstore
        .get_saga(&saved.id)
        .await
        .unwrap()
        .unwrap();
    if let OperationData::Swap(data) = &mut current.data {
        data.checkpoint.as_mut().unwrap().ready = Some(ready);
    }
    current.update_state(current.state);
    assert!(wallet
        .localstore
        .update_saga(current.clone())
        .await
        .unwrap());
    assert!(matches!(
        wallet.resume_swap_saga(&current).await.unwrap(),
        RecoveryAction::Recovered
    ));
}

#[tokio::test]
async fn existing_output_provenance_must_match_before_handoff_retry() {
    for condition in [false, true] {
        let (mut wallet, mint) = wallet().await;
        let conditions =
            condition.then(|| SpendingConditions::new_htlc("00".repeat(32), None).unwrap());
        let mut saved = submitted(&wallet, &mint, conditions, false).await;
        let ready = checkpoint::resolve(
            &wallet,
            &saved,
            SwapResponse {
                signatures: response(&saved).signatures,
            },
        )
        .await
        .unwrap();
        let store = Arc::new(Interposed::new(wallet.localstore.clone()));
        *store.after_proofs.lock().unwrap() = Some(Action::Fail);
        wallet.localstore = store;
        assert!(checkpoint::finish(&wallet, &mut saved, ready)
            .await
            .is_err());
        let mut changed = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            .into_iter()
            .find(|p| {
                p.created_by_operation == Some(saved.id)
                    && if condition {
                        p.spending_condition.is_some()
                    } else {
                        p.derivation_index.is_some()
                    }
            })
            .unwrap();
        if condition {
            changed.spending_condition = None;
        } else {
            changed.derivation_index = Some(u32::MAX);
        }
        wallet
            .localstore
            .update_proofs(vec![changed.clone()], vec![])
            .await
            .unwrap();
        assert!(wallet.resume_swap_saga(&saved).await.is_err());
        assert!(wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![changed.y])
                .await
                .unwrap(),
            [changed]
        );
    }
}
