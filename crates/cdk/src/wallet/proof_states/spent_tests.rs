use std::sync::Arc;

use crate::nuts::{CheckStateResponse, ProofState, State};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, test_proof_info,
    MockMintConnector,
};

#[tokio::test]
async fn checking_spent_proofs_keeps_original_metadata_and_other_states() {
    let db = create_test_db().await;
    let mut proofs: Vec<_> = [8, 4, 2]
        .into_iter()
        .map(|amount| test_proof_info(test_keyset_id(), amount, test_mint_url()))
        .collect();
    proofs[0].used_by_operation = Some(uuid::Uuid::new_v4());
    proofs[0].created_by_operation = Some(uuid::Uuid::new_v4());
    proofs[0].derivation_index = Some(7);
    proofs[1].state = State::PendingSpent;
    db.update_proofs(proofs.clone(), vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let states = vec![
        ProofState {
            y: proofs[0].y,
            state: State::Spent,
            witness: None,
        },
        ProofState {
            y: proofs[1].y,
            state: State::Pending,
            witness: None,
        },
    ];
    for _ in 0..2 {
        mint.set_check_state_response(Ok(CheckStateResponse {
            states: states.clone(),
        }));
        assert_eq!(
            wallet
                .check_proofs_spent(proofs[..2].iter().map(|p| p.proof.clone()).collect())
                .await
                .unwrap(),
            states
        );
        for (index, original) in proofs.iter().enumerate() {
            let mut expected = original.clone();
            if index == 0 {
                expected.state = State::Spent;
            }
            assert_eq!(
                db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
                [expected]
            );
        }
    }
}

#[tokio::test]
async fn pending_check_keeps_spent_evidence_and_active_reservations() {
    let db = create_test_db().await;
    let mut proofs: Vec<_> = [8, 4, 2]
        .into_iter()
        .map(|amount| test_proof_info(test_keyset_id(), amount, test_mint_url()))
        .collect();
    for proof in &mut proofs {
        proof.state = State::PendingSpent;
    }
    proofs[2].used_by_operation = Some(uuid::Uuid::new_v4());
    db.update_proofs(proofs.clone(), vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    // Match the backend's read order, just as a real mint echoes request order.
    let states = db
        .get_proofs(
            Some(test_mint_url()),
            None,
            Some(vec![State::PendingSpent]),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.used_by_operation.is_none())
        .map(|p| ProofState {
            y: p.y,
            state: if p.y == proofs[0].y {
                State::Spent
            } else {
                State::Unspent
            },
            witness: None,
        })
        .collect();
    mint.set_check_state_response(Ok(CheckStateResponse { states }));
    assert_eq!(wallet.check_all_pending_proofs().await.unwrap().to_u64(), 4);
    for (index, original) in proofs.iter().enumerate() {
        let mut expected = original.clone();
        if index == 0 {
            expected.state = State::Spent;
        }
        assert_eq!(
            db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            [expected]
        );
    }
}

#[tokio::test]
async fn malformed_spent_answer_cannot_delete_or_modify_local_proofs() {
    let db = create_test_db().await;
    let proofs: Vec<_> = [8, 4]
        .into_iter()
        .map(|amount| test_proof_info(test_keyset_id(), amount, test_mint_url()))
        .collect();
    db.update_proofs(proofs.clone(), vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    mint.set_check_state_response(Ok(CheckStateResponse {
        states: vec![ProofState {
            y: proofs[0].y,
            state: State::Spent,
            witness: None,
        }],
    }));
    assert!(wallet
        .check_proofs_spent(proofs.iter().map(|p| p.proof.clone()).collect())
        .await
        .is_err());
    for original in proofs {
        assert_eq!(
            db.get_proofs_by_ys(vec![original.y]).await.unwrap(),
            [original]
        );
    }
}

#[tokio::test]
async fn checking_unknown_spent_proofs_does_not_import_them() {
    let db = create_test_db().await;
    let known = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let unknown = test_proof_info(test_keyset_id(), 4, test_mint_url());
    db.update_proofs(vec![known.clone()], vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    mint.set_check_state_response(Ok(CheckStateResponse {
        states: [known.y, unknown.y]
            .into_iter()
            .map(|y| ProofState {
                y,
                state: State::Spent,
                witness: None,
            })
            .collect(),
    }));
    wallet
        .check_proofs_spent(vec![known.proof.clone(), unknown.proof])
        .await
        .unwrap();
    let mut expected = known;
    expected.state = State::Spent;
    assert_eq!(
        db.get_proofs_by_ys(vec![expected.y, unknown.y])
            .await
            .unwrap(),
        [expected]
    );
}
