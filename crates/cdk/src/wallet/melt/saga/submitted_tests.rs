//! Submitted payments retain custody until a verified paid outcome is saved.
use std::sync::Arc;
use std::time::Duration;

use cdk_common::wallet::{
    KeysetLoadPolicy, MeltQuote, OperationData, ProofInfo, Transaction, TransactionStatus,
    WalletSaga,
};
use tokio::sync::Notify;

use super::paid_change_fixture::Fixture;
use super::state::MeltRequested;
use super::{finalize_melt_common, MeltSaga, MeltSagaResult};
use crate::nuts::{MeltQuoteBolt11Response, MeltQuoteState, ProofsMethods, State};
use crate::{Amount, Error};

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    quote: Option<MeltQuote>,
    receipts: Vec<Transaction>,
    proofs: Vec<ProofInfo>,
    saga: Option<WalletSaga>,
}

impl Snapshot {
    async fn load(f: &Fixture) -> Self {
        let db = &f.wallet.localstore;
        let mut proofs = db.get_proofs(None, None, None, None).await.unwrap();
        proofs.sort_by_key(|proof| proof.y.to_string());
        Self {
            quote: db.get_melt_quote(&f.quote.id).await.unwrap(),
            receipts: db.list_transactions(None, None, None).await.unwrap(),
            proofs,
            saga: db.get_saga(&f.saga.id).await.unwrap(),
        }
    }

    fn assert_paid(&self, f: &Fixture) {
        let quote = self.quote.as_ref().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.payment_proof.as_deref(), Some("preimage"));
        assert_eq!(quote.used_by_operation, None);
        assert!(self.saga.is_none());
        assert_eq!(self.receipts.len(), 1);
        let receipt = &self.receipts[0];
        assert_eq!(receipt.status, TransactionStatus::Completed);
        assert_eq!(receipt.fee, Amount::ZERO);
        assert_eq!(receipt.payment_proof.as_deref(), Some("preimage"));
        assert_eq!(receipt.timestamp, f.saga.created_at);
        assert_eq!(self.proofs.len(), 3);
        let OperationData::Melt(data) = &f.saga.data else {
            panic!("expected melt data");
        };
        assert_eq!(receipt.metadata, data.metadata);
        let payment = data.payment.as_ref().unwrap();
        let mut spent = payment.inputs[0].clone();
        spent.state = State::Spent;
        assert_eq!(
            self.proofs.iter().find(|proof| proof.y == spent.y),
            Some(&spent)
        );
        let change: Vec<_> = self
            .proofs
            .iter()
            .filter(|proof| proof.y != spent.y)
            .collect();
        assert_eq!(
            Amount::try_sum(change.iter().map(|proof| proof.proof.amount)).unwrap(),
            Amount::from(24)
        );
        assert!(change.iter().all(|proof| {
            proof.state == State::Unspent
                && proof.created_by_operation == Some(f.saga.id)
                && proof.used_by_operation.is_none()
                && proof.derivation_index.is_some()
        }));
    }
}

fn negative_status(f: &Fixture, state: MeltQuoteState) {
    f.client
        .set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: f.quote.id.clone(),
            state,
            expiry: f.quote.expiry,
            fee_reserve: f.quote.fee_reserve,
            amount: f.quote.amount,
            request: Some(f.quote.request.clone()),
            payment_preimage: None,
            change: None,
            unit: Some(f.quote.unit.clone()),
            method: f.quote.payment_method.clone(),
        }));
}

#[tokio::test]
async fn delayed_negative_status_preserves_completed_payment() {
    for state in [MeltQuoteState::Unpaid, MeltQuoteState::Failed] {
        let f = Fixture::new().await;
        negative_status(&f, state);
        let captured = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *f.client.melt_quote_status_gate.lock().unwrap() =
            Some((captured.clone(), release.clone()));
        let complete = async {
            captured.notified().await;
            let OperationData::Melt(data) = &f.saga.data else {
                panic!("expected melt data");
            };
            let result = finalize_melt_common(
                &f.wallet,
                f.saga.id,
                &f.quote,
                &f.inputs,
                &f.secrets,
                MeltQuoteState::Paid,
                Some("preimage".to_owned()),
                Some(f.response(&[16, 8]).signatures),
                data.metadata.clone(),
                KeysetLoadPolicy::CacheOnly,
            )
            .await
            .unwrap();
            assert_eq!(result.state(), MeltQuoteState::Paid);
            let completed = Snapshot::load(&f).await;
            completed.assert_paid(&f);
            release.notify_one();
            completed
        };
        let (stale, completed) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(f.wallet.resume_melt_saga(&f.saga), complete)
        })
        .await
        .expect("status/finalization race timed out");
        assert!(stale.unwrap().is_none());
        assert_eq!(Snapshot::load(&f).await, completed);
        assert!(f.client.melt_quote_status_gate.lock().unwrap().is_none());
        assert!(f.client.last_post_melt_request().is_none());
    }
}

#[tokio::test]
async fn ambiguous_submission_retains_custody_until_paid_recovery() {
    for state in [MeltQuoteState::Unpaid, MeltQuoteState::Failed] {
        for error in [Error::RequestAlreadyPaid, Error::Timeout] {
            let f = Fixture::new().await;
            let before = Snapshot::load(&f).await;
            assert_eq!(before.receipts[0].status, TransactionStatus::Pending);
            assert_eq!(before.proofs[0].state, State::Pending);
            let OperationData::Melt(data) = &f.saga.data else {
                panic!("expected melt data");
            };
            let requested = MeltSaga {
                wallet: &f.wallet,
                state_data: MeltRequested {
                    operation_id: f.saga.id,
                    quote: before.quote.clone().unwrap(),
                    final_proofs: f.inputs.clone(),
                    premint_secrets: f.secrets.clone(),
                },
            };
            f.client.set_post_melt_response(Err(error));
            negative_status(&f, state);
            let result = requested
                .execute_async(data.metadata.clone())
                .await
                .unwrap();
            assert!(matches!(result, MeltSagaResult::Pending(_)));
            assert_eq!(Snapshot::load(&f).await, before);
            let (_, sent) = f.client.last_post_melt_request().unwrap();
            assert_eq!(sent.inputs(), &f.inputs);
            assert_eq!(sent.outputs(), &Some(f.secrets.blinded_messages()));
            f.client.last_post_melt_request.lock().unwrap().take();

            f.status(Some(f.response(&[16, 8]).signatures));
            let paid = f.wallet.resume_melt_saga(&f.saga).await.unwrap().unwrap();
            assert_eq!(paid.state(), MeltQuoteState::Paid);
            assert_eq!(
                paid.change().unwrap().total_amount().unwrap(),
                Amount::from(24)
            );
            let completed = Snapshot::load(&f).await;
            completed.assert_paid(&f);
            f.client.set_melt_quote_status_response(Err(Error::Timeout));
            assert!(f.wallet.resume_melt_saga(&f.saga).await.unwrap().is_none());
            assert_eq!(Snapshot::load(&f).await, completed);
            assert!(f
                .client
                .melt_quote_status_response
                .lock()
                .unwrap()
                .is_some());
            assert!(f.client.last_post_melt_request().is_none());
        }
    }
}
