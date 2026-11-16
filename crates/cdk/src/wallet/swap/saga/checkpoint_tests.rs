use std::sync::Arc;

use cdk_common::wallet::{SwapSagaState, WalletSaga, WalletSagaState};

use super::test_utils::{prepare, response, submitted, wallet};
use super::*;
use crate::nuts::{SecretKey, SpendingConditions, State, SwapResponse};
use crate::wallet::recovery::RecoveryAction;
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::Error;

#[tokio::test]
async fn exact_random_secrets_recipient_split_and_evidence_survive_restart() {
    for case in 0..3 {
        let (wallet, mint) = wallet().await;
        let conditions = match case {
            0 => None,
            1 => Some(SpendingConditions::new_htlc("00".repeat(32), None).unwrap()),
            _ => Some(SpendingConditions::new_p2pk(
                SecretKey::generate().public_key(),
                None,
            )),
        };
        let saved = submitted(&wallet, &mint, conditions, case == 2).await;
        let saved: WalletSaga =
            serde_json::from_str(&serde_json::to_string(&saved).unwrap()).unwrap();
        let expected = checkpoint::get(&saved).unwrap().clone();
        mint._set_restore_response(Ok(response(&saved)));
        assert!(matches!(
            wallet.resume_swap_saga(&saved).await.unwrap(),
            RecoveryAction::Recovered
        ));
        let all = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap();
        for input in &expected.acquisition {
            let mut spent = input.after.clone().unwrap();
            spent.state = State::Spent;
            assert!(all.contains(&spent));
        }
        let outputs: Vec<_> = all
            .iter()
            .filter(|p| p.created_by_operation == Some(saved.id))
            .collect();
        assert_eq!(
            outputs.len(),
            expected.pre_swap.pre_mint_secrets.secrets.len()
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|p| p.state == State::Reserved)
                .map(|p| p.proof.amount.to_u64())
                .sum::<u64>(),
            12
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|p| p.state == State::Unspent)
                .map(|p| p.proof.amount.to_u64())
                .sum::<u64>(),
            20 - expected.pre_swap.fee.to_u64()
        );
        for output in outputs {
            let secret = expected
                .pre_swap
                .pre_mint_secrets
                .secrets
                .iter()
                .find(|s| s.secret == output.proof.secret)
                .unwrap();
            assert_eq!(output.derivation_index, secret.derivation_index);
            if case == 2 && output.state == State::Reserved {
                assert!(output.proof.p2pk_e.is_some());
            }
        }
        assert_eq!(
            mint.captured_swap_requests(),
            [expected.pre_swap.swap_request]
        );
    }
}

#[tokio::test]
async fn partial_restore_missing_inputs_and_changed_secrets_retain_intent() {
    let (wallet, mint) = wallet().await;
    let mut saved = submitted(&wallet, &mint, None, false).await;
    let request = checkpoint::get(&saved)
        .unwrap()
        .pre_swap
        .swap_request
        .clone();
    let mut changed = saved.clone();
    if let OperationData::Swap(data) = &mut changed.data {
        data.checkpoint
            .as_mut()
            .unwrap()
            .pre_swap
            .pre_mint_secrets
            .secrets[0]
            .amount = 100.into();
    }
    assert!(wallet.resume_swap_saga(&changed).await.is_err());
    for case in 0..3 {
        let mut restore = response(&saved);
        match case {
            0 => {
                restore.outputs.pop();
                restore.signatures.pop();
            }
            1 => restore.outputs[0].blinded_secret = SecretKey::generate().public_key(),
            _ => {
                restore.outputs[1] = restore.outputs[0].clone();
                restore.signatures[1] = restore.signatures[0].clone();
            }
        }
        mint._set_restore_response(Ok(restore));
        assert!(wallet.resume_swap_saga(&saved).await.is_err());
        saved = wallet
            .localstore
            .get_saga(&saved.id)
            .await
            .unwrap()
            .unwrap();
        assert!(checkpoint::get(&saved).unwrap().ready.is_none());
        assert_eq!(wallet.total_balance().await.unwrap().to_u64(), 0);
    }
    let input = checkpoint::get(&saved).unwrap().acquisition[0]
        .after
        .clone()
        .unwrap();
    wallet
        .localstore
        .update_proofs(vec![], vec![input.y])
        .await
        .unwrap();
    assert!(wallet.resume_swap_saga(&saved).await.is_err());
    assert!(wallet
        .localstore
        .get_saga(&saved.id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(mint.captured_swap_requests(), [request]);
}

#[tokio::test]
async fn ready_outputs_finish_without_mint_and_fence_a_stale_live_worker() {
    let (mut wallet, mint) = wallet().await;
    let mut saved = submitted(&wallet, &mint, None, false).await;
    let stale = saved.clone();
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
    *store.before_proofs.lock().unwrap() = Some(Action::Fail);
    wallet.localstore = store;
    assert!(checkpoint::finish(&wallet, &mut saved, ready.clone())
        .await
        .is_err());
    let current = wallet
        .localstore
        .get_saga(&saved.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.state,
        WalletSagaState::Swap(SwapSagaState::OutputsReady)
    );
    assert_eq!(
        checkpoint::get(&current).unwrap().ready,
        Some(ready.clone())
    );
    assert!(matches!(
        wallet.resume_swap_saga(&current).await.unwrap(),
        RecoveryAction::Recovered
    ));
    assert!(matches!(
        checkpoint::finish(&wallet, &mut stale.clone(), ready).await,
        Err(Error::ConcurrentUpdate)
    ));
    assert_eq!(mint.captured_swap_requests().len(), 1);
}

#[tokio::test]
async fn handoff_retry_preserves_outputs_spent_or_owned_by_later_operations() {
    let (mut wallet, mint) = wallet().await;
    let mut saved = submitted(&wallet, &mint, None, false).await;
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
    let mut outputs: Vec<_> = wallet
        .localstore
        .get_proofs(None, None, None, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.created_by_operation == Some(saved.id))
        .collect();
    for (index, p) in outputs.iter_mut().enumerate() {
        p.state = if index % 2 == 0 {
            State::Spent
        } else {
            State::Reserved
        };
        p.used_by_operation = Some(uuid::Uuid::new_v4());
    }
    wallet
        .localstore
        .update_proofs(outputs.clone(), vec![])
        .await
        .unwrap();
    assert!(matches!(
        wallet.resume_swap_saga(&saved).await.unwrap(),
        RecoveryAction::Recovered
    ));
    let actual = wallet
        .localstore
        .get_proofs_by_ys(outputs.iter().map(|p| p.y).collect())
        .await
        .unwrap();
    assert!(outputs.iter().all(|p| actual.contains(p)));
}

#[allow(clippy::use_debug)]
#[tokio::test]
async fn checkpoint_debug_redacts_private_material_and_missing_checkpoint_fails_closed() {
    let (wallet, _) = wallet().await;
    let prepared = prepare(
        &wallet,
        Some(SpendingConditions::new_htlc("00".repeat(32), None).unwrap()),
        false,
    )
    .await;
    let mut saved = prepared.state_data.saga;
    let debug = format!("{saved:?}");
    for secret in &checkpoint::get(&saved)
        .unwrap()
        .pre_swap
        .pre_mint_secrets
        .secrets
    {
        assert!(!debug.contains(&secret.secret.to_string()));
    }
    if let OperationData::Swap(data) = &mut saved.data {
        data.checkpoint = None;
    }
    assert!(matches!(
        wallet.resume_swap_saga(&saved).await,
        Err(Error::InvalidOperationState)
    ));
}

#[tokio::test]
async fn final_inactive_keyset_error_recovers_exact_prior_outputs() {
    let (wallet, mint) = wallet().await;
    let prepared = prepare(&wallet, None, false).await;
    let original = prepared.state_data.saga.clone();
    mint.set_post_swap_response(Err(Error::InactiveKeyset));
    assert!(matches!(
        prepared.execute().await,
        Err(Error::InactiveKeyset)
    ));
    let saved = wallet
        .localstore
        .get_saga(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        checkpoint::get(&saved).unwrap(),
        checkpoint::get(&original).unwrap()
    );
    for change in &checkpoint::get(&saved).unwrap().acquisition {
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![change.y])
                .await
                .unwrap(),
            [change.after.clone().unwrap()]
        );
    }
    mint._set_restore_response(Ok(response(&saved)));
    assert!(matches!(
        wallet.resume_swap_saga(&saved).await.unwrap(),
        RecoveryAction::Recovered
    ));
    assert_eq!(
        mint.captured_swap_requests(),
        [checkpoint::get(&original)
            .unwrap()
            .pre_swap
            .swap_request
            .clone()]
    );
}
