//! A delayed mint response cannot overwrite a completed operation's later owners.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::tests::paid_signed_mint_quote;
use super::*;
use crate::wallet::send::saga::interposed_store::{Action, Interposed};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_mint_url, MockMintConnector,
};

async fn concurrent_completion(batch: bool, recovering: bool, state: State, at_commit: bool) {
    let db = create_test_db().await;
    let store = Arc::new(Interposed::new(db.clone()));
    let mint = Arc::new(MockMintConnector::new());
    mint.enable_mint_signing();
    let wallet = create_test_wallet_with_mock(store.clone(), mint.clone()).await;
    let mut quotes = Vec::new();
    for _ in 0..if batch { 2 } else { 1 } {
        let quote = paid_signed_mint_quote(test_mint_url(), 8.into(), SecretKey::generate());
        db.add_mint_quote(quote.clone()).await.unwrap();
        quotes.push(quote);
    }
    let names: Vec<_> = quotes.iter().map(|quote| quote.id.as_str()).collect();
    let prepared = if batch {
        MintSaga::new(&wallet)
            .prepare_batch(&names, SplitTarget::None, None, None)
            .await
    } else {
        MintSaga::new(&wallet)
            .prepare(names[0], SplitTarget::None, None)
            .await
    }
    .unwrap();
    let id = prepared.state_data.saga.id;
    let output_count = prepared
        .state_data
        .checkpoint()
        .unwrap()
        .premint_secrets
        .len();
    let mut prepared = Some(prepared);
    if recovering {
        let lost = Err(Error::Custom("lost mint reply".to_owned()));
        if batch {
            mint.push_post_batch_mint_response(lost);
        } else {
            mint.push_post_mint_response(lost);
        }
        assert!(prepared.take().unwrap().execute().await.is_err());
    }
    let original = db.get_saga(&id).await.unwrap().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let proceed = Arc::new(tokio::sync::Notify::new());
    let (commit_entered, commit_wait) = tokio::sync::oneshot::channel();
    let (commit_release, commit_proceed) = tokio::sync::oneshot::channel();
    if at_commit {
        *store.before_issue_retirement.lock().unwrap() = Some(Action::Pause {
            reached: commit_entered,
            release: commit_proceed,
        });
    } else {
        *mint.mint_gate.lock().unwrap() = Some((entered.clone(), proceed.clone()));
    }
    let delayed = async {
        match prepared {
            Some(prepared) => prepared.execute().await.map(|_| ()),
            None => wallet.resume_issue_saga(&original).await.map(|_| ()),
        }
    };
    tokio::pin!(delayed);
    tokio::select! {
        _ = async { if at_commit { commit_wait.await.unwrap(); } else { entered.notified().await; } } => {}
        _ = &mut delayed => panic!("completion was not held"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("completion barrier timed out"),
    }
    let submitted = db.get_saga(&id).await.unwrap().unwrap();
    wallet.resume_issue_saga(&submitted).await.unwrap();
    assert!(db.get_saga(&id).await.unwrap().is_none());
    let mut later = db.get_proofs(None, None, None, None).await.unwrap();
    assert_eq!(later.len(), output_count);
    for proof in &mut later {
        proof.state = state;
        proof.used_by_operation = (state == State::Pending).then(uuid::Uuid::new_v4);
    }
    db.update_proofs(later.clone(), vec![]).await.unwrap();
    let expected: HashMap<_, _> = later.into_iter().map(|proof| (proof.y, proof)).collect();
    let receipts = db.list_transactions(None, None, None).await.unwrap();
    let owned_quotes = db.get_mint_quotes().await.unwrap();
    if at_commit {
        commit_release.send(()).unwrap();
    } else {
        proceed.notify_one();
    }
    assert!(delayed.await.is_err());
    let actual: HashMap<_, _> = db
        .get_proofs(None, None, None, None)
        .await
        .unwrap()
        .into_iter()
        .map(|proof| (proof.y, proof))
        .collect();
    assert!(
        actual == expected,
        "stale completion overwrote later coin state"
    );
    assert_eq!(
        db.list_transactions(None, None, None).await.unwrap(),
        receipts
    );
    assert_eq!(db.get_mint_quotes().await.unwrap(), owned_quotes);
    assert!(db.get_saga(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn issue_publication_stale_recovery_preserves_completed_coins() {
    for batch in [false, true] {
        for state in [State::Spent, State::Pending] {
            for at_commit in [false, true] {
                concurrent_completion(batch, true, state, at_commit).await;
            }
        }
    }
}

#[tokio::test]
async fn issue_publication_stale_submission_preserves_completed_coins() {
    for batch in [false, true] {
        for state in [State::Spent, State::Pending] {
            for at_commit in [false, true] {
                concurrent_completion(batch, false, state, at_commit).await;
            }
        }
    }
}
