//! Recovery must retain the exact original secrets and signed mint request.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::tests::paid_signed_mint_quote;
use super::*;
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock_seed, test_mint_url, MockMintConnector,
};

#[allow(clippy::use_debug)]
async fn recover_original(
    batch: bool,
    conditioned: bool,
    external: bool,
    interleaved: bool,
    restore: bool,
) {
    let db = create_test_db().await;
    let mint = Arc::new(MockMintConnector::new());
    mint.enable_mint_signing();
    let seed = [42; 64];
    let wallet = create_test_wallet_with_mock_seed(db.clone(), mint.clone(), seed).await;
    let mut quotes = Vec::new();
    let mut external_keys = HashMap::new();
    for _ in 0..if batch { 2 } else { 1 } {
        let key = SecretKey::generate();
        let mut quote = paid_signed_mint_quote(test_mint_url(), 8.into(), key.clone());
        if external {
            quote.secret_key = None;
            external_keys.insert(quote.id.clone(), key);
        }
        db.add_mint_quote(quote.clone()).await.unwrap();
        quotes.push(quote);
    }
    let conditions =
        conditioned.then(|| SpendingConditions::new_p2pk(SecretKey::generate().public_key(), None));
    let names: Vec<_> = quotes.iter().map(|q| q.id.as_str()).collect();
    let prepared = if batch {
        MintSaga::new(&wallet)
            .prepare_batch(&names, SplitTarget::None, conditions, Some(&external_keys))
            .await
    } else {
        MintSaga::new(&wallet)
            .prepare(names[0], SplitTarget::None, conditions)
            .await
    }
    .unwrap();
    let id = prepared.state_data.saga.id;
    let originals = prepared
        .state_data
        .checkpoint()
        .unwrap()
        .premint_secrets
        .clone();
    assert!(!format!("{:?}", prepared.state_data.saga)
        .contains(&originals.secrets[0].secret.to_string()));
    let expected: HashSet<_> = originals
        .secrets
        .iter()
        .map(|p| p.secret.to_string())
        .collect();
    if interleaved {
        db.increment_keyset_counter(
            &prepared
                .state_data
                .checkpoint()
                .unwrap()
                .premint_secrets
                .keyset_id,
            11,
        )
        .await
        .unwrap();
    }
    let lost = Error::Custom("lost mint response".to_owned());
    if batch {
        mint.push_post_batch_mint_response(Err(lost));
    } else {
        mint.push_post_mint_response(Err(lost));
    }
    assert!(prepared.execute().await.is_err());
    let saga = db.get_saga(&id).await.unwrap().unwrap();
    drop(wallet);
    drop(external_keys);
    if restore {
        let rejected = Error::Custom("already issued".to_owned());
        if batch {
            mint.push_post_batch_mint_response(Err(rejected));
        } else {
            mint.push_post_mint_response(Err(rejected));
        }
        let outputs = originals.blinded_messages();
        let signatures = outputs
            .iter()
            .map(|output| crate::nuts::BlindSignature {
                amount: output.amount,
                keyset_id: output.keyset_id,
                c: crate::dhke::sign_message(
                    &mint.mint_signing_keys.lock().unwrap().as_ref().unwrap()[&output.amount],
                    &output.blinded_secret,
                )
                .unwrap(),
                dleq: None,
            })
            .collect::<Vec<_>>();
        mint._set_restore_response(Ok(crate::nuts::RestoreResponse {
            outputs: outputs.into_iter().rev().collect(),
            signatures: signatures.into_iter().rev().collect(),
        }));
    }
    let reopened = create_test_wallet_with_mock_seed(db.clone(), mint.clone(), seed).await;
    assert_eq!(
        reopened.resume_issue_saga(&saga).await.unwrap(),
        crate::wallet::recovery::RecoveryAction::Recovered
    );
    let proofs = db.get_proofs(None, None, None, None).await.unwrap();
    let actual: HashSet<_> = proofs.iter().map(|p| p.proof.secret.to_string()).collect();
    assert!(
        actual == expected,
        "recovery must preserve original bearer secrets"
    );
    if batch {
        let requests = mint.post_batch_mint_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0], requests[1],
            "replay must retain original signatures"
        );
    } else {
        let requests = mint.post_mint_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
    assert!(db.get_saga(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn issue_checkpoint_replay_survives_interleaved_counter() {
    for batch in [false, true] {
        for restore in [false, true] {
            recover_original(batch, false, false, true, restore).await;
        }
    }
}

#[tokio::test]
async fn issue_checkpoint_conditioned_secrets_survive_restart() {
    for batch in [false, true] {
        for restore in [false, true] {
            recover_original(batch, true, false, false, restore).await;
        }
    }
}

#[tokio::test]
async fn issue_checkpoint_external_batch_signatures_survive_restart() {
    for restore in [false, true] {
        recover_original(true, false, true, false, restore).await;
    }
}

#[tokio::test]
async fn issue_checkpoint_invalid_or_stale_never_submits() {
    for case in ["missing", "secret", "request", "stale", "foreign_owner"] {
        let db = create_test_db().await;
        let mint = Arc::new(MockMintConnector::new());
        mint.enable_mint_signing();
        let wallet = create_test_wallet_with_mock_seed(db.clone(), mint.clone(), [42; 64]).await;
        let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
        db.add_mint_quote(quote.clone()).await.unwrap();
        let prepared = MintSaga::new(&wallet)
            .prepare(&quote.id, SplitTarget::None, None)
            .await
            .unwrap();
        let mut saved = prepared.state_data.saga;
        saved.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
        let OperationData::Mint(data) = &mut saved.data else {
            unreachable!()
        };
        match case {
            "missing" => data.checkpoint = None,
            "secret" => {
                data.checkpoint.as_mut().unwrap().premint_secrets.secrets[0].r =
                    SecretKey::generate()
            }
            "request" => {
                let MintRequestData::Single { request, .. } =
                    &mut data.checkpoint.as_mut().unwrap().request
                else {
                    unreachable!()
                };
                request.quote.push('x');
            }
            "foreign_owner" => {
                let mut owned = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
                owned.used_by_operation = Some(uuid::Uuid::new_v4().to_string());
                db.add_mint_quote(owned).await.unwrap();
            }
            _ => {}
        }
        assert!(db.update_saga(saved.clone()).await.unwrap());
        let mut expected = saved.clone();
        if case == "stale" {
            expected.version -= 1;
        }
        let receipts = db.list_transactions(None, None, None).await.unwrap();
        assert!(wallet.resume_issue_saga(&expected).await.is_err(), "{case}");
        assert_eq!(db.get_saga(&saved.id).await.unwrap(), Some(saved));
        assert!(mint.post_mint_requests().is_empty(), "{case}");
        assert!(db
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            receipts
        );
    }
}
