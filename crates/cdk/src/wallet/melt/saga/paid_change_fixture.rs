//! Regression probes for interrupted paid melts, using the native SQLite store.
use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::wallet::{
    MeltOperationData, MeltPayment, MeltQuote, MeltSagaState, OperationData, ProofInfo,
    Transaction, TransactionDirection, TransactionStatus, WalletSaga, WalletSagaState,
};

use crate::nuts::{
    BlindSignature, CurrencyUnit, KeySetInfo, MeltQuoteBolt11Response, MeltQuoteState,
    PaymentMethod, PreMintSecrets, Proofs, RestoreResponse, State,
};
use crate::wallet::test_utils::{
    create_test_db, create_test_wallet_with_mock, test_keyset, test_melt_quote, test_mint_url,
    test_proof_info, MockMintConnector,
};
use crate::{Amount, Wallet};

pub(super) struct Fixture {
    pub(super) wallet: Wallet,
    pub(super) client: Arc<MockMintConnector>,
    pub(super) saga: WalletSaga,
    pub(super) quote: MeltQuote,
    pub(super) secrets: PreMintSecrets,
    pub(super) inputs: Proofs,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::with_reserve(1).await
    }

    pub(super) async fn with_reserve(reserve: u64) -> Self {
        let db = create_test_db().await;
        let client = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), client.clone()).await;
        let mut keyset = test_keyset();
        keyset.input_fee_ppk = 0;
        db.add_mint(test_mint_url(), None).await.unwrap();
        db.add_mint_keysets(
            test_mint_url(),
            vec![KeySetInfo {
                id: keyset.id,
                unit: keyset.unit.clone(),
                active: true,
                input_fee_ppk: 0,
                final_expiry: None,
            }],
        )
        .await
        .unwrap();
        db.add_keys(keyset.clone()).await.unwrap();
        let id = uuid::Uuid::new_v4();
        let mut quote = test_melt_quote();
        quote.id = id.to_string();
        quote.amount = Amount::from(8);
        quote.fee_reserve = Amount::from(reserve);
        db.add_melt_quote(quote.clone()).await.unwrap();
        let mut input = test_proof_info(keyset.id, 32, test_mint_url());
        input.state = State::Pending;
        input.used_by_operation = Some(id);
        input.created_by_operation = Some(uuid::Uuid::new_v4());
        input.derivation_index = Some(42);
        input.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
            crate::nuts::nut11::P2PKWitness {
                signatures: vec!["original-input-witness".to_owned()],
            },
        ));
        let inputs = vec![input.proof.clone()];
        db.update_proofs(vec![input.clone()], vec![]).await.unwrap();
        let secrets =
            PreMintSecrets::from_seed_blank(keyset.id, 0, &wallet.seed, Amount::from(24)).unwrap();
        let mut saga = WalletSaga::new(
            id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(8),
            test_mint_url(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote.id.clone(),
                amount: quote.amount,
                fee_reserve: quote.fee_reserve,
                counter_start: Some(0),
                counter_end: Some(secrets.len() as u32),
                change_amount: Some(Amount::from(24)),
                metadata: HashMap::from([("purpose".to_owned(), "recovery test".to_owned())]),
                final_proof_ys: Some(vec![input.y]),
                change_blinded_messages: Some(secrets.blinded_messages()),
            }),
        );
        save_payment(&wallet, &mut saga, &quote, vec![input]).await;
        db.add_saga(saga.clone()).await.unwrap();
        Self {
            wallet,
            client,
            saga,
            quote,
            secrets,
            inputs,
        }
    }

    pub(super) fn response(&self, amounts: &[u64]) -> RestoreResponse {
        let outputs = self
            .secrets
            .blinded_messages()
            .into_iter()
            .take(amounts.len())
            .collect::<Vec<_>>();
        let signatures = outputs
            .iter()
            .zip(amounts)
            .map(|(message, amount)| BlindSignature {
                amount: Amount::from(*amount),
                keyset_id: message.keyset_id,
                c: message.blinded_secret,
                dleq: None,
            })
            .collect();
        RestoreResponse {
            outputs,
            signatures,
        }
    }

    pub(super) fn status(&self, signatures: Option<Vec<BlindSignature>>) {
        self.client
            .set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
                quote: self.quote.id.clone(),
                state: MeltQuoteState::Paid,
                expiry: 9999999999,
                fee_reserve: self.quote.fee_reserve,
                amount: self.quote.amount,
                request: None,
                payment_preimage: Some("preimage".to_owned()),
                change: signatures,
                unit: Some(CurrencyUnit::Sat),
                method: PaymentMethod::BOLT11,
            }));
    }

    pub(super) async fn restore(
        &self,
        amounts: &[u64],
    ) -> Result<Option<crate::types::FinalizedMelt>, crate::Error> {
        self.status(None);
        self.client
            ._set_restore_response(Ok(self.response(amounts)));
        self.wallet.resume_melt_saga(&self.saga).await
    }

    pub(super) async fn existing_change(&self, state: State) -> Vec<ProofInfo> {
        let response = self.response(&[16, 8]);
        let keys = test_keyset().keys;
        let proofs = cdk_common::dhke::construct_proofs(
            response.signatures,
            self.secrets.rs()[..2].to_vec(),
            self.secrets.secrets()[..2].to_vec(),
            &keys,
        )
        .unwrap();
        let mut infos = proofs
            .into_iter()
            .map(|proof| ProofInfo::new(proof, test_mint_url(), state, CurrencyUnit::Sat).unwrap())
            .collect::<Vec<_>>();
        let later_owner = uuid::Uuid::new_v4();
        for (info, secret) in infos.iter_mut().zip(&self.secrets.secrets) {
            info.used_by_operation = Some(later_owner);
            info.created_by_operation = Some(self.saga.id);
            info.derivation_index = secret.derivation_index;
            info.proof.witness = Some(crate::nuts::Witness::P2PKWitness(
                crate::nuts::nut11::P2PKWitness {
                    signatures: vec!["later-owner-witness".to_owned()],
                },
            ));
        }
        self.wallet
            .localstore
            .update_proofs(infos.clone(), vec![])
            .await
            .unwrap();
        self.wallet
            .localstore
            .get_proofs_by_ys(infos.iter().map(|p| p.y).collect())
            .await
            .unwrap()
    }
}

/// Persist the request boundary used by direct live-finalization tests.
pub(super) async fn save_request(
    wallet: &Wallet,
    id: uuid::Uuid,
    quote: &MeltQuote,
    proofs: &Proofs,
    secrets: &PreMintSecrets,
) {
    use crate::nuts::ProofsMethods;
    let inputs: Vec<_> = proofs
        .iter()
        .map(|proof| {
            ProofInfo::new_with_operations(
                proof.clone(),
                wallet.mint_url.clone(),
                State::Pending,
                wallet.unit.clone(),
                Some(id),
                None,
            )
            .unwrap()
        })
        .collect();
    wallet
        .localstore
        .update_proofs(inputs.clone(), vec![])
        .await
        .unwrap();
    let mut saga = WalletSaga::new(
        id,
        WalletSagaState::Melt(MeltSagaState::MeltRequested),
        quote.amount,
        wallet.mint_url.clone(),
        wallet.unit.clone(),
        OperationData::Melt(MeltOperationData {
            payment: None,
            swap: None,
            acquisition: None,
            quote_id: quote.id.clone(),
            amount: quote.amount,
            fee_reserve: quote.fee_reserve,
            counter_start: secrets.secrets.first().and_then(|s| s.derivation_index),
            counter_end: secrets
                .secrets
                .last()
                .and_then(|s| s.derivation_index)
                .map(|i| i + 1),
            change_amount: Some(proofs.total_amount().unwrap() - quote.amount),
            metadata: HashMap::new(),
            final_proof_ys: Some(proofs.ys().unwrap()),
            change_blinded_messages: Some(secrets.blinded_messages()),
        }),
    );
    save_payment(wallet, &mut saga, quote, inputs).await;
    wallet.localstore.add_saga(saga).await.unwrap();
}

pub(super) async fn save_payment(
    wallet: &Wallet,
    saga: &mut WalletSaga,
    quote: &MeltQuote,
    inputs: Vec<ProofInfo>,
) {
    let OperationData::Melt(ref mut data) = saga.data else {
        panic!("melt fixture")
    };
    let receipt = Transaction {
        role: Default::default(),
        mint_url: saga.mint_url.clone(),
        direction: TransactionDirection::Outgoing,
        amount: saga.amount,
        fee: data.fee_reserve,
        unit: saga.unit.clone(),
        ys: inputs.iter().map(|p| p.y).collect(),
        timestamp: saga.created_at,
        memo: None,
        metadata: data.metadata.clone(),
        quote_id: Some(quote.id.clone()),
        payment_request: Some(quote.request.clone()),
        payment_proof: None,
        payment_method: Some(quote.payment_method.clone()),
        saga_id: Some(saga.id),
        status: TransactionStatus::Pending,
    };
    data.payment = Some(Box::new(MeltPayment {
        inputs,
        receipt: receipt.clone(),
        change: None,
    }));
    let mut owned = quote.clone();
    owned.used_by_operation = Some(saga.id.to_string());
    wallet.localstore.add_melt_quote(owned).await.unwrap();
    wallet.localstore.add_transaction(receipt).await.unwrap();
}
