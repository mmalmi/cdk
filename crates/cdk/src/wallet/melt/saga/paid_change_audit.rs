//! Regression probes for interrupted paid melts, using the native SQLite store.
use cdk_common::wallet::KeysetLoadPolicy;

use super::finalize_melt_common;
use super::paid_change_fixture::Fixture;
use crate::nuts::{MeltQuoteState, State};

async fn retry_preserves_change(state: State) {
    let f = Fixture::new().await;
    let original = f.existing_change(state).await;
    f.restore(&[16, 8]).await.unwrap().unwrap();
    let ys = original.iter().map(|p| p.y).collect();
    let actual = f.wallet.localstore.get_proofs_by_ys(ys).await.unwrap();
    for expected in original {
        assert_eq!(
            actual.iter().find(|p| p.y == expected.y),
            Some(&expected),
            "retry revived change or stole a later owner's reservation"
        );
    }
}

#[tokio::test]
async fn audit_paid_retry_preserves_spent_change() {
    retry_preserves_change(State::Spent).await;
}

#[tokio::test]
async fn audit_paid_retry_preserves_reserved_change() {
    retry_preserves_change(State::Reserved).await;
}

#[tokio::test]
async fn audit_paid_delayed_live_completion_preserves_spent_change() {
    let f = Fixture::new().await;
    f.restore(&[16, 8]).await.unwrap().unwrap();
    let original = f.existing_change(State::Spent).await;
    let _result = finalize_melt_common(
        &f.wallet,
        f.saga.id,
        &f.quote,
        &f.inputs,
        &f.secrets,
        MeltQuoteState::Paid,
        Some("preimage".to_owned()),
        Some(f.response(&[16, 8]).signatures),
        match &f.saga.data {
            cdk_common::wallet::OperationData::Melt(d) => d.metadata.clone(),
            _ => unreachable!(),
        },
        KeysetLoadPolicy::CacheThenNetwork,
    )
    .await;
    if let Err(error) = _result {
        assert!(
            matches!(error, crate::Error::ConcurrentUpdate),
            "unexpected fixture error: {error:?}"
        );
    }
    let actual = f
        .wallet
        .localstore
        .get_proofs_by_ys(original.iter().map(|p| p.y).collect())
        .await
        .unwrap();
    for expected in original {
        assert_eq!(
            actual.iter().find(|p| p.y == expected.y),
            Some(&expected),
            "late live completion revived already-spent change"
        );
    }
}

#[tokio::test]
async fn audit_paid_live_completion_preserves_later_change_owner() {
    for state in [State::Reserved, State::Spent] {
        let f = Fixture::new().await;
        let original = f.existing_change(state).await;
        let input = f
            .wallet
            .localstore
            .get_reserved_proofs(&f.saga.id)
            .await
            .unwrap()[0]
            .clone();
        finalize_melt_common(
            &f.wallet,
            f.saga.id,
            &f.quote,
            &f.inputs,
            &f.secrets,
            MeltQuoteState::Paid,
            Some("preimage".to_owned()),
            Some(f.response(&[16, 8]).signatures),
            match &f.saga.data {
                cdk_common::wallet::OperationData::Melt(d) => d.metadata.clone(),
                _ => unreachable!(),
            },
            KeysetLoadPolicy::CacheThenNetwork,
        )
        .await
        .unwrap();
        let actual = f
            .wallet
            .localstore
            .get_proofs_by_ys(original.iter().map(|proof| proof.y).collect())
            .await
            .unwrap();
        for expected in original {
            assert!(actual.contains(&expected));
        }
        let mut expected = input;
        expected.state = State::Spent;
        assert_eq!(
            f.wallet
                .localstore
                .get_proofs_by_ys(vec![expected.y])
                .await
                .unwrap(),
            [expected]
        );
    }
}
