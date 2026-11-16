//! Paid quotes already contain enough information to recover their change.
use super::paid_change_fixture::Fixture;
use crate::nuts::State;
use crate::Amount;

#[tokio::test]
async fn test_paid_quote_change_recovers_without_restore_and_keeps_metadata() {
    let f = Fixture::new().await;
    f.status(Some(f.response(&[16, 8]).signatures));
    f.client._set_restore_response(Err(crate::Error::Timeout));
    let result = f.wallet.resume_melt_saga(&f.saga).await.unwrap().unwrap();
    assert_eq!(result.fee_paid(), Amount::ZERO);
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(24));
    assert!(
        f.client.restore_response.lock().unwrap().is_some(),
        "quote change must not make a restore request"
    );
    let txs = f
        .wallet
        .localstore
        .list_transactions(None, None, None)
        .await
        .unwrap();
    assert_eq!(
        txs[0].metadata.get("purpose").map(String::as_str),
        Some("recovery test")
    );
    let outputs = f.wallet.get_unspent_proofs().await.unwrap();
    let infos = f
        .wallet
        .localstore
        .get_proofs_by_ys(outputs.iter().map(|proof| proof.y().unwrap()).collect())
        .await
        .unwrap();
    assert!(infos.iter().all(|proof| proof.derivation_index.is_some()));
}

#[tokio::test]
async fn test_paid_quote_change_rejects_malformed_signatures() {
    for case in [
        "too_many",
        "wrong_keyset",
        "unsupported_amount",
        "invalid_dleq",
    ] {
        let f = Fixture::new().await;
        let mut signatures = f.response(&[16, 8]).signatures;
        match case {
            "too_many" => signatures.resize(f.secrets.len() + 1, signatures[0].clone()),
            "wrong_keyset" => signatures[0].keyset_id = "00ffffffffffffff".parse().unwrap(),
            "unsupported_amount" => signatures[0].amount = Amount::from(3),
            "invalid_dleq" => {
                signatures[0].dleq = Some(crate::nuts::nut12::BlindSignatureDleq {
                    e: crate::nuts::SecretKey::from_slice(&[1; 32]).unwrap(),
                    s: crate::nuts::SecretKey::from_slice(&[2; 32]).unwrap(),
                });
            }

            _ => unreachable!(),
        }
        f.status(Some(signatures));
        // An invalid quote response must not silently fall back to a valid restore.
        f.client._set_restore_response(Ok(f.response(&[16, 8])));
        assert!(
            f.wallet.resume_melt_saga(&f.saga).await.is_err(),
            "accepted {case}"
        );
        assert!(f.client.restore_response.lock().unwrap().is_some());
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::ZERO);
        assert!(f
            .wallet
            .localstore
            .get_reserved_proofs(&f.saga.id)
            .await
            .unwrap()
            .iter()
            .all(|proof| proof.state == State::Pending));
    }
}

#[tokio::test]
async fn test_paid_quote_change_omitted_uses_restore() {
    let f = Fixture::new().await;
    let result = f.restore(&[16, 8]).await.unwrap().unwrap();
    assert_eq!(result.fee_paid(), Amount::ZERO);
    assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(24));
    assert!(f.client.restore_response.lock().unwrap().is_none());
}

#[tokio::test]
async fn paid_quote_incomplete_change_keeps_the_original_fee_bound_and_can_retry() {
    for amounts in [vec![], vec![16]] {
        let f = Fixture::new().await;
        let original = f
            .wallet
            .localstore
            .get_reserved_proofs(&f.saga.id)
            .await
            .unwrap();
        f.status(Some(f.response(&amounts).signatures));
        f.client._set_restore_response(Err(crate::Error::Timeout));
        assert!(f.wallet.resume_melt_saga(&f.saga).await.is_err());
        assert_eq!(
            f.wallet
                .localstore
                .get_reserved_proofs(&f.saga.id)
                .await
                .unwrap(),
            original
        );
        assert!(f
            .wallet
            .localstore
            .get_saga(&f.saga.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::ZERO);
        assert!(f.client.restore_response.lock().unwrap().is_some());
        f.status(Some(f.response(&[16, 8]).signatures));
        let result = f.wallet.resume_melt_saga(&f.saga).await.unwrap().unwrap();
        assert_eq!(result.fee_paid(), Amount::ZERO);
        assert_eq!(f.wallet.total_balance().await.unwrap(), Amount::from(24));
    }
}
