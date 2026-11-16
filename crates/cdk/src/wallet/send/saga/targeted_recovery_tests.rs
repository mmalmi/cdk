use cdk_common::wallet::{IssueSagaState, OperationData, SendSagaState, WalletSagaState};
use uuid::Uuid;

use super::confirmation_tests::{interrupted, response};
use crate::nuts::{CurrencyUnit, ProofsMethods, RestoreResponse, SwapResponse};
use crate::wallet::test_utils::{test_keyset, test_mint_url, test_proof_info};
use crate::wallet::SendOptions;
use crate::Amount;

#[tokio::test]
async fn targeted_recovery_leaves_another_prepared_send_unchanged() {
    let (wallet, mint, saga) = interrupted().await;
    wallet
        .localstore
        .update_proofs(
            vec![test_proof_info(test_keyset().id, 16, test_mint_url())],
            vec![],
        )
        .await
        .unwrap();
    let prepared = wallet
        .prepare_send(16.into(), SendOptions::default())
        .await
        .unwrap();
    let other_id = prepared.operation_id();
    drop(prepared);
    let other = wallet
        .localstore
        .get_saga(&other_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        other.state,
        WalletSagaState::Send(SendSagaState::ProofsReserved)
    );
    let reserved = wallet
        .localstore
        .get_reserved_proofs(&other_id)
        .await
        .unwrap();

    mint._set_restore_response(Ok(response(&saga)));
    wallet.recover_send_confirmation(saga.id).await.unwrap();

    let done = wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap();
    assert_eq!(
        done.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    );
    let OperationData::Send(send) = &done.data else {
        panic!("send");
    };
    assert!(send.token.as_ref().is_some_and(|token| !token.is_empty()));
    assert_eq!(
        send.proofs.as_ref().unwrap().total_amount().unwrap(),
        Amount::from(32)
    );
    assert!(
        wallet
            .localstore
            .get_saga(&other_id)
            .await
            .unwrap()
            .unwrap()
            == other
    );
    assert!(
        wallet
            .localstore
            .get_reserved_proofs(&other_id)
            .await
            .unwrap()
            == reserved
    );
    assert_eq!(mint.captured_swap_requests().len(), 1);
    assert_eq!(wallet.total_balance().await.unwrap(), Amount::from(32));
}

#[tokio::test]
async fn targeted_recovery_retries_only_the_original_confirmation() {
    let (wallet, mint, saga) = interrupted().await;
    let restored = response(&saga);
    mint._set_restore_response(Ok(RestoreResponse {
        outputs: vec![],
        signatures: vec![],
    }));
    mint.set_post_swap_response(Ok(SwapResponse {
        signatures: restored.signatures,
    }));
    wallet.recover_send_confirmation(saga.id).await.unwrap();
    let requests = mint.captured_swap_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0] == requests[1]);
    let done = wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap();
    assert_eq!(
        done.state,
        WalletSagaState::Send(SendSagaState::TokenCreated)
    );
    assert!(wallet.recover_send_confirmation(saga.id).await.is_err());
    assert!(wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap() == done);
    assert_eq!(mint.captured_swap_requests().len(), 2);
}

#[tokio::test]
async fn targeted_recovery_rejects_missing_foreign_and_nonconfirming_operations() {
    let (wallet, mint, saga) = interrupted().await;
    assert!(wallet
        .recover_send_confirmation(Uuid::new_v4())
        .await
        .is_err());
    for case in 0..12 {
        let mut invalid = saga.clone();
        invalid.id = Uuid::new_v4();
        match case {
            0 => invalid.mint_url = "https://other.invalid".parse().unwrap(),
            1 => invalid.unit = CurrencyUnit::Msat,
            2 => invalid.state = WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
            value => {
                let state = [
                    SendSagaState::ProofsReserved,
                    SendSagaState::TokenCreated,
                    SendSagaState::RollingBack,
                    SendSagaState::ReclaimPreparing,
                    SendSagaState::ReclaimCancelling,
                    SendSagaState::Reclaiming,
                    SendSagaState::Reclaimed,
                    SendSagaState::Claimed,
                    SendSagaState::Confirming,
                ][value - 3];
                invalid.state = WalletSagaState::Send(state);
                if state == SendSagaState::Confirming {
                    let OperationData::Send(data) = &mut invalid.data else {
                        panic!("send");
                    };
                    data.confirmation = None;
                }
            }
        }
        wallet.localstore.add_saga(invalid.clone()).await.unwrap();
        let proofs = wallet.get_reserved_proofs().await.unwrap();
        assert!(wallet.recover_send_confirmation(invalid.id).await.is_err());
        assert!(
            wallet
                .localstore
                .get_saga(&invalid.id)
                .await
                .unwrap()
                .unwrap()
                == invalid
        );
        assert!(wallet.get_reserved_proofs().await.unwrap() == proofs);
        assert!(wallet.localstore.get_saga(&saga.id).await.unwrap().unwrap() == saga);
        assert_eq!(mint.captured_swap_requests().len(), 1);
        // No restore reply is configured: any attempted mint restore would panic.
    }
}
