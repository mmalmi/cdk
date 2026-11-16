use cdk_common::wallet::ProofInfo;

use super::test_utils::{response, wallet};
use super::*;
use crate::nuts::nut10::SpendingConditionVerification;
use crate::nuts::{SecretKey, State};
use crate::wallet::recovery::RecoveryAction;

#[tokio::test]
async fn signed_inputs_keep_exact_wire_witness_and_original_local_evidence() {
    for valid in [false, true] {
        let (wallet, mint) = wallet().await;
        let plain = wallet
            .localstore
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            .remove(0);
        let key = SecretKey::generate();
        let mut proof = plain.proof.clone();
        proof.secret = SpendingConditions::new_p2pk(key.public_key(), None)
            .try_into()
            .unwrap();
        if valid {
            proof.sign_p2pk(key).unwrap();
        }
        let mut original = ProofInfo::new(
            proof.clone(),
            wallet.mint_url.clone(),
            State::Unspent,
            wallet.unit.clone(),
        )
        .unwrap();
        original.created_by_operation = Some(uuid::Uuid::new_v4());
        original.derivation_index = Some(42);
        wallet
            .localstore
            .update_proofs(vec![original.clone()], vec![plain.y])
            .await
            .unwrap();
        let result = SwapSaga::new(&wallet)
            .prepare(None, Default::default(), vec![proof], None, false, false)
            .await;
        if !valid {
            assert!(result.is_err());
            assert_eq!(
                wallet
                    .localstore
                    .get_proofs_by_ys(vec![original.y])
                    .await
                    .unwrap(),
                [original]
            );
            assert!(wallet
                .localstore
                .get_incomplete_sagas()
                .await
                .unwrap()
                .is_empty());
            assert!(mint.captured_swap_requests().is_empty());
            continue;
        }
        let prepared = result.unwrap();
        let id = prepared.state_data.saga.id;
        let expected = checkpoint::get(&prepared.state_data.saga)
            .unwrap()
            .pre_swap
            .swap_request
            .clone();
        expected.verify_spending_conditions().unwrap();
        assert_eq!(expected.inputs()[0].witness, original.proof.witness);
        mint.set_post_swap_response(Err(Error::Timeout));
        assert!(prepared.execute().await.is_err());
        let saved = wallet.localstore.get_saga(&id).await.unwrap().unwrap();
        let restored = response(&saved);
        mint._set_restore_response(Ok(restored));
        assert!(matches!(
            wallet.resume_swap_saga(&saved).await.unwrap(),
            RecoveryAction::Recovered
        ));
        assert_eq!(mint.captured_swap_requests(), [expected]);
        let mut spent = original;
        spent.state = State::Spent;
        spent.used_by_operation = Some(id);
        assert_eq!(
            wallet
                .localstore
                .get_proofs_by_ys(vec![spent.y])
                .await
                .unwrap(),
            [spent]
        );
    }
}
