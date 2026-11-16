use std::sync::Arc;

use crate::nuts::{CheckStateResponse, ProofState, SecretKey, State};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset_id, test_mint_url, test_proof_info,
    MockMintConnector,
};

#[tokio::test]
async fn read_only_mint_states_preserve_known_records_and_accept_unknown_proofs() {
    let db = create_test_db().await;
    let mut known = test_proof_info(test_keyset_id(), 4, test_mint_url());
    known.state = State::Reserved;
    known.used_by_operation = Some(uuid::Uuid::new_v4());
    db.update_proofs(vec![known.clone()], vec![]).await.unwrap();
    let unknown = test_proof_info(test_keyset_id(), 8, test_mint_url());
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let proofs = vec![known.proof.clone(), unknown.proof];
    let ys = vec![known.y, unknown.y];
    for state in [State::Unspent, State::Pending, State::Spent] {
        let states: Vec<_> = ys
            .iter()
            .map(|y| ProofState {
                y: *y,
                state,
                witness: None,
            })
            .collect();
        mint.set_check_state_response(Ok(CheckStateResponse {
            states: states.clone(),
        }));
        assert_eq!(
            wallet.check_proofs_state(proofs.clone()).await.unwrap(),
            states
        );
        assert_eq!(
            db.get_proofs_by_ys(ys.clone()).await.unwrap(),
            [known.clone()]
        );
    }
}

#[tokio::test]
async fn invalid_mint_state_answers_never_change_wallet_records() {
    let db = create_test_db().await;
    let proofs: Vec<_> = [8, 4]
        .into_iter()
        .map(|amount| test_proof_info(test_keyset_id(), amount, test_mint_url()))
        .collect();
    db.update_proofs(proofs.clone(), vec![]).await.unwrap();
    let ys: Vec<_> = proofs.iter().map(|p| p.y).collect();
    let before = db.get_proofs_by_ys(ys.clone()).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let state = |y, state| ProofState {
        y,
        state,
        witness: None,
    };
    for states in [
        vec![],
        vec![state(ys[0], State::Spent)],
        vec![state(ys[0], State::Spent), state(ys[0], State::Spent)],
        vec![
            state(ys[0], State::Spent),
            state(SecretKey::generate().public_key(), State::Spent),
        ],
        vec![state(ys[1], State::Spent), state(ys[0], State::Spent)],
        vec![state(ys[0], State::Reserved), state(ys[1], State::Spent)],
        vec![
            state(ys[0], State::PendingSpent),
            state(ys[1], State::Spent),
        ],
    ] {
        mint.set_check_state_response(Ok(CheckStateResponse { states }));
        assert!(wallet
            .check_proofs_state(proofs.iter().map(|p| p.proof.clone()).collect())
            .await
            .is_err());
        assert_eq!(db.get_proofs_by_ys(ys.clone()).await.unwrap(), before);
    }
}

#[tokio::test]
async fn duplicate_queries_and_failed_requests_preserve_local_custody() {
    let db = create_test_db().await;
    let proof = test_proof_info(test_keyset_id(), 8, test_mint_url());
    db.update_proofs(vec![proof.clone()], vec![]).await.unwrap();
    let mint = Arc::new(MockMintConnector::new());
    let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
    let state = ProofState {
        y: proof.y,
        state: State::Spent,
        witness: None,
    };
    mint.set_check_state_response(Ok(CheckStateResponse {
        states: vec![state.clone(), state],
    }));
    assert!(wallet
        .check_proofs_state(vec![proof.proof.clone(); 2])
        .await
        .is_err());
    mint.set_check_state_response(Err(crate::Error::Custom("mint unavailable".to_owned())));
    assert!(wallet
        .check_proofs_state(vec![proof.proof.clone()])
        .await
        .is_err());
    assert_eq!(
        db.get_proofs_by_ys(vec![proof.y]).await.unwrap(),
        vec![proof]
    );
}
