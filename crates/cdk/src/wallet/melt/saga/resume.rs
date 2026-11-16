//! Resume logic for melt sagas after crash recovery.
//!
//! Prepared operations can be cancelled. Submitted operations retain custody
//! until payment is verified; saved paid outcomes complete without mint access.

use cdk_common::wallet::{MeltSagaState, OperationData, WalletSaga};
use cdk_common::{Amount, MeltQuoteState};
use tracing::instrument;

use crate::types::FinalizedMelt;
use crate::wallet::util::escape_log_value;
use crate::{Error, Wallet};

impl Wallet {
    /// Resume an incomplete melt saga after crash recovery.
    ///
    /// Complete saved paid outcomes locally, check submitted payments with the
    /// mint, or cancel preparation that never reached the melt request boundary.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(FinalizedMelt))` - The melt was finalized or compensated
    /// - `Ok(None)` - The melt was skipped (still pending, mint unreachable)
    /// - `Err(e)` - An error occurred during recovery
    #[instrument(skip(self, saga))]
    pub(crate) async fn resume_melt_saga(
        &self,
        saga: &WalletSaga,
    ) -> Result<Option<FinalizedMelt>, Error> {
        // Callers enumerate sagas and then await while processing them. Always
        // reload this operation so a stale snapshot cannot compensate a saga
        // that another task has advanced or completed.
        let mut saga = match self.localstore.get_saga(&saga.id).await? {
            Some(saga) => saga,
            None => return Ok(None),
        };

        let state = match &saga.state {
            cdk_common::wallet::WalletSagaState::Melt(s) => s,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid saga state type for melt saga {}",
                    saga.id
                )))
            }
        };

        let data = match &saga.data {
            OperationData::Melt(d) => d,
            _ => {
                return Err(Error::Custom(format!(
                    "Invalid operation data type for melt saga {}",
                    saga.id
                )))
            }
        };

        match state {
            MeltSagaState::PaidReady | MeltSagaState::Completed => {
                super::completion::finish(self, &saga).await.map(Some)
            }
            MeltSagaState::ProofsReserved | MeltSagaState::SwapCancelling => {
                let result = FinalizedMelt::new(
                    data.quote_id.clone(),
                    MeltQuoteState::Unpaid,
                    None,
                    data.amount,
                    Amount::ZERO,
                    None,
                );
                super::swap::cancel(self, &mut saga).await?;
                Ok(Some(result))
            }
            MeltSagaState::MeltRequested | MeltSagaState::PaymentPending => {
                // Melt was requested or payment is pending - check quote state
                tracing::info!(
                    "Melt saga {} in {:?} state - checking quote state",
                    saga.id,
                    state
                );
                self.recover_submitted_melt(&saga).await
            }
        }
    }

    /// Complete only verified paid status; other submitted outcomes retain custody.
    ///
    /// Returns `Some(FinalizedMelt)` for paid melts,
    /// `None` for still-pending melts that should be retried later.
    async fn recover_submitted_melt(
        &self,
        saga: &WalletSaga,
    ) -> Result<Option<FinalizedMelt>, Error> {
        let data = super::custody::data(self, saga)?;
        let saga_id = &saga.id;
        super::custody::inputs(self, saga).await?;
        let payment = data.payment.as_ref().ok_or(Error::InvalidOperationState)?;
        if self
            .localstore
            .get_transaction(payment.receipt.id())
            .await?
            .as_ref()
            != Some(&payment.receipt)
        {
            return Err(Error::ConcurrentUpdate);
        }

        // Check quote state with the mint
        match self.internal_check_melt_status(&data.quote_id).await {
            Ok(quote_status) => match quote_status.state() {
                MeltQuoteState::Paid => {
                    // Payment succeeded - mark proofs as spent and recover change
                    tracing::info!("Melt saga {} - payment succeeded, finalizing", saga_id);
                    super::completion::record_paid(
                        self,
                        saga,
                        quote_status.payment_proof(),
                        quote_status.change(),
                        cdk_common::wallet::KeysetLoadPolicy::CacheThenNetwork,
                    )
                    .await
                    .map(Some)
                }
                MeltQuoteState::Unpaid
                | MeltQuoteState::Failed
                | MeltQuoteState::Pending
                | MeltQuoteState::Unknown => {
                    // Still pending or unknown - skip and retry later
                    tracing::info!("Melt saga {} - payment pending/unknown, skipping", saga_id);
                    Ok(None)
                }
            },
            Err(e) => {
                tracing::warn!(
                    "Melt saga {} - can't check quote state ({}), skipping",
                    saga_id,
                    escape_log_value(&e)
                );
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bip39::Mnemonic;
    use cdk_common::nuts::{CurrencyUnit, PaymentMethod, State};
    use cdk_common::wallet::{
        MeltOperationData, MeltSagaState, OperationData, Transaction, TransactionDirection,
        TransactionStatus, WalletSaga, WalletSagaState,
    };
    use cdk_common::{Amount, MeltQuoteBolt11Response, MeltQuoteState, RestoreResponse};

    use crate::nuts::{BlindSignature, BlindedMessage, PreMintSecrets};
    use crate::wallet::saga::test_utils::{create_test_db, test_mint_url, test_proof_info};
    use crate::wallet::test_utils::{
        create_test_wallet_with_mock, test_keyset, test_melt_quote, MockMintConnector,
    };

    fn restore_response_with_amounts(
        blinded_messages: &[BlindedMessage],
        amounts: &[u64],
    ) -> RestoreResponse {
        let outputs = blinded_messages
            .iter()
            .take(amounts.len())
            .cloned()
            .collect();
        let signatures = blinded_messages
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

    // Build a submitted intent once; individual tests corrupt evidence only afterward.
    async fn save_payment(wallet: &crate::Wallet, id: uuid::Uuid) -> WalletSaga {
        let db = &wallet.localstore;
        let mut saga = db.get_saga(&id).await.unwrap().unwrap();
        let OperationData::Melt(data) = &mut saga.data else {
            panic!("expected melt");
        };
        let ys = data.final_proof_ys.as_ref().unwrap();
        let mut stored: HashMap<_, _> = db
            .get_proofs_by_ys(ys.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|p| (p.y, p))
            .collect();
        let inputs = ys
            .iter()
            .map(|y| {
                let mut input = stored.remove(y).expect("fixture input must exist");
                input.state = State::Pending;
                assert_eq!(input.used_by_operation, Some(id));
                input
            })
            .collect();
        let mut quote = db
            .get_melt_quote(&data.quote_id)
            .await
            .unwrap()
            .unwrap_or_else(test_melt_quote);
        quote.id = data.quote_id.clone();
        quote.amount = data.amount;
        quote.fee_reserve = data.fee_reserve;
        quote.used_by_operation = Some(id.to_string());
        db.add_melt_quote(quote.clone()).await.unwrap();
        if let Some(messages) = &data.change_blinded_messages {
            let keyset = messages[0].keyset_id;
            let start = data.counter_start.unwrap();
            let secrets = PreMintSecrets::from_seed_blank(
                keyset,
                start,
                &wallet.seed,
                data.change_amount.unwrap(),
            )
            .unwrap();
            let count = secrets.secrets.len() as u32;
            data.counter_end = Some(start + count);
            data.change_blinded_messages = Some(secrets.blinded_messages());
            db.increment_keyset_counter(&keyset, start + count)
                .await
                .unwrap();
        }
        let receipt = Transaction {
            role: Default::default(),
            mint_url: saga.mint_url.clone(),
            direction: TransactionDirection::Outgoing,
            amount: saga.amount,
            fee: data.fee_reserve,
            unit: saga.unit.clone(),
            ys: ys.clone(),
            timestamp: saga.created_at,
            memo: None,
            metadata: data.metadata.clone(),
            quote_id: Some(quote.id),
            payment_request: Some(quote.request),
            payment_proof: None,
            payment_method: Some(quote.payment_method),
            saga_id: Some(id),
            status: TransactionStatus::Pending,
        };
        if db.get_transaction(receipt.id()).await.unwrap().is_none() {
            db.add_transaction(receipt.clone()).await.unwrap();
        }
        data.payment = Some(Box::new(cdk_common::wallet::MeltPayment {
            inputs,
            receipt,
            change: None,
        }));
        saga.update_state(saga.state);
        assert!(db.update_saga(saga.clone()).await.unwrap());
        saga
    }

    #[tokio::test]
    async fn test_recover_melt_proofs_reserved() {
        // Compensate: proofs released, quote released
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        // Create and reserve proofs
        let proof_info = test_proof_info(keyset_id, 128, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info.clone()], vec![])
            .await
            .unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();

        // Store melt quote before reserving it
        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.amount = Amount::from(100);
        melt_quote.fee_reserve = Amount::from(10);
        db.add_melt_quote(melt_quote).await.unwrap();
        db.reserve_melt_quote(&quote_id, &saga_id).await.unwrap();

        // Create saga in ProofsReserved state
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::ProofsReserved),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: Some(vec![cdk_common::database::wallet::ProofChange {
                    y: proof_y,
                    before: Some(proof_info),
                    after: Some(
                        db.get_proofs_by_ys(vec![proof_y])
                            .await
                            .unwrap()
                            .pop()
                            .unwrap(),
                    ),
                }]),
                quote_id,
                amount: Amount::from(100),
                fee_reserve: Amount::from(10),
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: None,
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        // Create wallet and recover
        let mock_client = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        // Verify compensation
        assert!(result.is_some());
        let finalized = result.unwrap();
        assert_eq!(finalized.state(), MeltQuoteState::Unpaid);

        // Proofs should be back to Unspent
        let proofs = db
            .get_proofs(None, None, Some(vec![State::Unspent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);

        // Saga should be deleted
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_melt_reloads_stale_proofs_reserved_snapshot() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Pending);
        proof_info.used_by_operation = Some(saga_id);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let stale_saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::ProofsReserved),
            Amount::from(100),
            mint_url,
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(100),
                fee_reserve: Amount::ZERO,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
                change_blinded_messages: None,
            }),
        );
        db.add_saga(stale_saga.clone()).await.unwrap();

        let mut advanced_saga = stale_saga.clone();
        advanced_saga.update_state(WalletSagaState::Melt(MeltSagaState::MeltRequested));
        assert!(db.update_saga(advanced_saga).await.unwrap());

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Pending,
            expiry: 9999999999,
            fee_reserve: Amount::ZERO,
            amount: Amount::from(100),
            request: Some("lnbc100...".to_string()),
            payment_preimage: None,
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let saved = save_payment(&wallet, saga_id).await;
        let history = db.list_transactions(None, None, None).await.unwrap();
        let inputs = db.get_reserved_proofs(&saga_id).await.unwrap();
        let quote = db
            .get_melt_quote(&saved.quote_id.clone().unwrap())
            .await
            .unwrap();
        let result = wallet.resume_melt_saga(&stale_saga).await.unwrap();
        assert!(result.is_none());

        let stored = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(stored[0].state, State::Pending);
        assert_eq!(stored[0].used_by_operation, Some(saga_id));
        assert!(matches!(
            db.get_saga(&saga_id).await.unwrap().map(|saga| saga.state),
            Some(WalletSagaState::Melt(MeltSagaState::MeltRequested))
        ));
        assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved.clone()));
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            history
        );
        assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap(), inputs);
        assert_eq!(
            db.get_melt_quote(&saved.quote_id.unwrap()).await.unwrap(),
            quote
        );
    }

    #[tokio::test]
    async fn test_recover_melt_missing_acquisition_retains_journal_and_reserved_proof() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.update_proofs_state(vec![proof_y], State::Reserved)
            .await
            .unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();
        db.reserve_melt_quote(&quote_id, &saga_id).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::ProofsReserved),
            Amount::from(100),
            mint_url,
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id,
                amount: Amount::from(100),
                fee_reserve: Amount::from(10),
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: None,
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(result.is_err());

        let reserved = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].state, State::Reserved);
        assert_eq!(reserved[0].used_by_operation, None);

        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_recover_melt_melt_requested_quote_paid() {
        // Mock: quote Paid → complete melt, get change
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        // Create pending proofs as they would be after a melt request is sent.
        let mut proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Pending);
        proof_info.used_by_operation = Some(saga_id);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        // Create saga in MeltRequested state
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(100),
                fee_reserve: Amount::ZERO,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        // Store melt quote
        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        // Mock: quote is Paid
        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::ZERO,
            amount: Amount::from(100),
            request: Some("lnbc100...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        // Verify melt completed
        assert!(result.is_some());
        let finalized = result.unwrap();
        assert_eq!(finalized.state(), MeltQuoteState::Paid);

        // Proofs should be marked spent
        let proofs = db
            .get_proofs(None, None, Some(vec![State::Spent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);

        // Saga should be deleted
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());

        // Transaction history should contain the recovered paid melt.
        let transactions = wallet
            .list_transactions(Some(TransactionDirection::Outgoing))
            .await
            .unwrap();
        assert_eq!(transactions.len(), 1);

        let transaction = &transactions[0];
        assert_eq!(transaction.quote_id.as_deref(), Some(quote_id.as_str()));
        assert_eq!(transaction.payment_request.as_deref(), Some("lnbc1000..."));
        assert_eq!(transaction.payment_proof.as_deref(), Some("preimage123"));
        assert_eq!(transaction.saga_id, Some(saga_id));
        assert_eq!(transaction.amount, Amount::from(100));
        assert_eq!(transaction.fee, Amount::ZERO);

        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.payment_proof.as_deref(), Some("preimage123"));
        assert_eq!(quote.used_by_operation, None);
    }

    #[tokio::test]
    async fn test_recover_melt_paid_preserves_stored_quote_payment_proof_when_status_has_none() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Pending);
        proof_info.used_by_operation = Some(saga_id);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(100),
            mint_url,
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(100),
                fee_reserve: Amount::ZERO,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                change_blinded_messages: None,
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.payment_proof = Some("stored-preimage".to_string());
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::ZERO,
            amount: Amount::from(100),
            request: Some("lnbc100...".to_string()),
            payment_preimage: None,
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let finalized = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap()
            .expect("paid melt should finalize");

        assert_eq!(finalized.payment_proof(), Some("stored-preimage"));

        let transactions = wallet
            .list_transactions(Some(TransactionDirection::Outgoing))
            .await
            .unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(
            transactions[0].payment_proof.as_deref(),
            Some("stored-preimage")
        );

        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.payment_proof.as_deref(), Some("stored-preimage"));
        assert_eq!(quote.used_by_operation, None);
    }

    #[tokio::test]
    async fn test_recover_melt_paid_rejects_missing_inputs_before_recording() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut input = test_proof_info(test_keyset().id, 100, mint_url.clone(), State::Pending);
        input.used_by_operation = Some(saga_id);
        let missing_y = input.y;
        db.update_proofs(vec![input], vec![]).await.unwrap();
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(100),
            mint_url,
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(100),
                fee_reserve: Amount::ZERO,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![missing_y]),
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.used_by_operation = Some(saga_id.to_string());
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::ZERO,
            amount: Amount::from(100),
            request: Some("lnbc100...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let saved = save_payment(&wallet, saga_id).await;
        db.update_proofs(vec![], vec![missing_y]).await.unwrap();
        let history = db.list_transactions(None, None, None).await.unwrap();
        let quote = db
            .get_melt_quote(&saved.quote_id.clone().unwrap())
            .await
            .unwrap();
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(matches!(result, Err(crate::Error::ConcurrentUpdate)));
        assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved.clone()));
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            history
        );
        assert_eq!(
            db.get_melt_quote(&saved.quote_id.unwrap()).await.unwrap(),
            quote
        );
    }

    #[tokio::test]
    async fn test_recover_melt_paid_rejects_missing_or_mismatched_receipt() {
        for absent in [false, true] {
            let db = create_test_db().await;
            let mint_url = test_mint_url();
            let keyset_id = test_keyset().id;
            let saga_id = uuid::Uuid::new_v4();
            let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

            let mut pending_input =
                test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
            pending_input.used_by_operation = Some(saga_id);
            let pending_input_y = pending_input.y;
            db.update_proofs(vec![pending_input], vec![]).await.unwrap();

            let mut metadata = HashMap::new();
            metadata.insert("label".to_string(), "recovery metadata".to_string());

            let saga = WalletSaga::new(
                saga_id,
                WalletSagaState::Melt(MeltSagaState::MeltRequested),
                Amount::from(1000),
                mint_url.clone(),
                CurrencyUnit::Sat,
                OperationData::Melt(MeltOperationData {
                    payment: None,
                    swap: None,
                    acquisition: None,
                    quote_id: quote_id.clone(),
                    amount: Amount::from(1000),
                    fee_reserve: Amount::from(200),
                    counter_start: Some(0),
                    counter_end: None,
                    change_amount: Some(Amount::from(200)),
                    metadata,
                    final_proof_ys: Some(vec![pending_input_y]),
                    change_blinded_messages: Some(
                        PreMintSecrets::blank(keyset_id, Amount::from(200))
                            .unwrap()
                            .blinded_messages(),
                    ),
                }),
            );
            db.add_saga(saga).await.unwrap();

            let mut melt_quote = test_melt_quote();
            melt_quote.id = quote_id.clone();
            melt_quote.used_by_operation = Some(saga_id.to_string());
            let payment_method = melt_quote.payment_method.clone();
            db.add_melt_quote(melt_quote).await.unwrap();

            let mut existing_metadata = HashMap::new();
            existing_metadata.insert("label".to_string(), "original metadata".to_string());
            db.add_transaction(Transaction {
                role: Default::default(),
                mint_url: mint_url.clone(),
                direction: TransactionDirection::Outgoing,
                amount: Amount::from(1000),
                fee: Amount::from(50),
                unit: CurrencyUnit::Sat,
                ys: vec![pending_input_y],
                timestamp: 42,
                memo: Some("original memo".to_string()),
                metadata: existing_metadata.clone(),
                quote_id: Some(quote_id.clone()),
                payment_request: Some("original request".to_string()),
                payment_proof: Some("original proof".to_string()),
                payment_method: Some(payment_method),
                saga_id: Some(saga_id),
                status: TransactionStatus::Completed,
            })
            .await
            .unwrap();

            let mock_client = Arc::new(MockMintConnector::new());
            mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
                quote: quote_id.clone(),
                state: MeltQuoteState::Paid,
                expiry: 9999999999,
                fee_reserve: Amount::from(200),
                amount: Amount::from(1000),
                request: Some("lnbc1000...".to_string()),
                payment_preimage: Some("preimage123".to_string()),
                change: None,
                unit: Some(CurrencyUnit::Sat),
                method: PaymentMethod::BOLT11,
            }));

            let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
            let saved = save_payment(&wallet, saga_id).await;
            if absent {
                let OperationData::Melt(data) = &saved.data else {
                    panic!("expected melt");
                };
                db.remove_transaction(data.payment.as_ref().unwrap().receipt.id())
                    .await
                    .unwrap();
            }
            let history = db.list_transactions(None, None, None).await.unwrap();
            let proofs = db.get_reserved_proofs(&saga_id).await.unwrap();
            let quote = db.get_melt_quote(&quote_id).await.unwrap();
            let result = wallet.resume_melt_saga(&saved).await;
            assert!(result.is_err());
            assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved));
            assert_eq!(
                db.list_transactions(None, None, None).await.unwrap(),
                history
            );
            assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap(), proofs);
            assert_eq!(db.get_melt_quote(&quote_id).await.unwrap(), quote);
        }
    }

    #[tokio::test]
    async fn test_recover_melt_paid_does_not_reconstruct_checkpoint_from_history() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut proof_info = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        proof_info.used_by_operation = Some(saga_id);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(200),
                counter_start: Some(0),
                counter_end: None,
                change_amount: Some(Amount::from(200)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
                change_blinded_messages: Some(
                    PreMintSecrets::blank(keyset_id, Amount::from(200))
                        .unwrap()
                        .blinded_messages(),
                ),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.used_by_operation = Some(saga_id.to_string());
        let payment_method = melt_quote.payment_method.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        db.add_transaction(Transaction {
            role: Default::default(),
            mint_url,
            direction: TransactionDirection::Outgoing,
            amount: Amount::from(1000),
            fee: Amount::from(50),
            unit: CurrencyUnit::Sat,
            ys: vec![proof_y],
            timestamp: 42,
            memo: Some("original memo".to_string()),
            metadata: HashMap::new(),
            quote_id: Some(quote_id.clone()),
            payment_request: Some("original request".to_string()),
            payment_proof: Some("transaction proof".to_string()),
            payment_method: Some(payment_method),
            saga_id: Some(saga_id),
            status: TransactionStatus::Completed,
        })
        .await
        .unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(200),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: None,
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let mut saved = save_payment(&wallet, saga_id).await;
        let history = db.list_transactions(None, None, None).await.unwrap();
        let proofs = db.get_reserved_proofs(&saga_id).await.unwrap();
        let quote = db.get_melt_quote(&quote_id).await.unwrap();
        let OperationData::Melt(data) = &mut saved.data else {
            panic!("expected melt");
        };
        data.payment = None;
        saved.update_state(saved.state);
        assert!(db.update_saga(saved.clone()).await.unwrap());
        let result = wallet.resume_melt_saga(&saved).await;
        assert!(result.is_err());
        assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved));
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            history
        );
        assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap(), proofs);
        assert_eq!(db.get_melt_quote(&quote_id).await.unwrap(), quote);
    }

    #[tokio::test]
    async fn test_recover_melt_paid_preserves_existing_incoming_transaction() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut proof_info = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        proof_info.used_by_operation = Some(saga_id);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(200),
                counter_start: Some(0),
                counter_end: None,
                change_amount: Some(Amount::from(200)),
                change_blinded_messages: Some(
                    PreMintSecrets::blank(keyset_id, Amount::from(200))
                        .unwrap()
                        .blinded_messages(),
                ),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.used_by_operation = Some(saga_id.to_string());
        let payment_method = melt_quote.payment_method.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        db.add_transaction(Transaction {
            role: Default::default(),
            mint_url,
            direction: TransactionDirection::Incoming,
            amount: Amount::from(1200),
            fee: Amount::ZERO,
            unit: CurrencyUnit::Sat,
            ys: vec![proof_y],
            timestamp: 42,
            memo: Some("original incoming".to_string()),
            metadata: HashMap::new(),
            quote_id: Some("mint_quote".to_string()),
            payment_request: Some("mint request".to_string()),
            payment_proof: None,
            payment_method: None,
            saga_id: Some(uuid::Uuid::new_v4()),
            status: TransactionStatus::Completed,
        })
        .await
        .unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(200),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        mock_client._set_restore_response(Ok(RestoreResponse {
            outputs: vec![],
            signatures: vec![],
        }));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        let finalized = result.expect("paid melt should finalize");
        assert_eq!(finalized.state(), MeltQuoteState::Paid);
        assert_eq!(finalized.fee_paid(), Amount::from(200));

        let incoming_transactions = wallet
            .list_transactions(Some(TransactionDirection::Incoming))
            .await
            .unwrap();
        assert_eq!(incoming_transactions.len(), 1);
        assert_eq!(
            incoming_transactions[0].memo.as_deref(),
            Some("original incoming")
        );

        let outgoing_transactions = wallet
            .list_transactions(Some(TransactionDirection::Outgoing))
            .await
            .unwrap();
        assert_eq!(outgoing_transactions.len(), 1);

        let transaction = &outgoing_transactions[0];
        assert_eq!(transaction.direction, TransactionDirection::Outgoing);
        assert_eq!(transaction.amount, Amount::from(1000));
        assert_eq!(transaction.fee, Amount::from(200));
        assert_eq!(transaction.quote_id.as_deref(), Some(quote_id.as_str()));
        assert_eq!(transaction.payment_request.as_deref(), Some("lnbc1000..."));
        assert_eq!(transaction.payment_proof.as_deref(), Some("preimage123"));
        assert_eq!(transaction.payment_method, Some(payment_method));
        assert_eq!(transaction.saga_id, Some(saga_id));

        let stored_input = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Spent);

        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.payment_proof.as_deref(), Some("preimage123"));
        assert_eq!(quote.used_by_operation, None);
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_melt_paid_uses_only_pending_melt_inputs() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let other_saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;

        let mut spent_swap_input = test_proof_info(keyset_id, 300, mint_url.clone(), State::Spent);
        spent_swap_input.used_by_operation = Some(saga_id);
        let spent_swap_input_y = spent_swap_input.y;

        let mut other_pending = test_proof_info(keyset_id, 900, mint_url.clone(), State::Pending);
        other_pending.used_by_operation = Some(other_saga_id);
        let other_pending_y = other_pending.y;

        db.update_proofs(vec![pending_input, spent_swap_input, other_pending], vec![])
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(200),
                counter_start: Some(0),
                counter_end: None,
                change_amount: Some(Amount::from(200)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: Some(
                    PreMintSecrets::blank(keyset_id, Amount::from(200))
                        .unwrap()
                        .blinded_messages(),
                ),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(200),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        mock_client._set_restore_response(Ok(RestoreResponse {
            outputs: vec![],
            signatures: vec![],
        }));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        assert!(result.is_some());

        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].ys, vec![pending_input_y]);
        assert!(!transactions[0].ys.contains(&spent_swap_input_y));
        assert!(!transactions[0].ys.contains(&other_pending_y));
        assert_eq!(transactions[0].fee, Amount::from(200));
    }

    #[tokio::test]
    async fn test_recover_melt_paid_requires_saved_final_input_ids() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut spent_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Spent);
        spent_input.used_by_operation = Some(saga_id);
        let spent_input_y = spent_input.y;

        db.update_proofs(vec![spent_input], vec![]).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(200),
                counter_start: Some(0),
                counter_end: None,
                change_amount: Some(Amount::from(200)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![spent_input_y]),
                change_blinded_messages: Some(
                    PreMintSecrets::blank(keyset_id, Amount::from(200))
                        .unwrap()
                        .blinded_messages(),
                ),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(200),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let mut saved = save_payment(&wallet, saga_id).await;
        let OperationData::Melt(data) = &mut saved.data else {
            panic!("expected melt");
        };
        data.final_proof_ys = None;
        saved.update_state(saved.state);
        assert!(db.update_saga(saved.clone()).await.unwrap());
        let history = db.list_transactions(None, None, None).await.unwrap();
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(matches!(result, Err(crate::Error::InvalidOperationState)));
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            history
        );
        assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved));
        let stored = db.get_proofs_by_ys(vec![spent_input_y]).await.unwrap();
        assert_eq!(stored[0].state, State::Spent);
        assert_eq!(stored[0].used_by_operation, Some(saga_id));
    }

    #[tokio::test]
    async fn test_recover_melt_paid_uses_final_input_ys_after_inputs_spent() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut spent_melt_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Spent);
        spent_melt_input.used_by_operation = Some(saga_id);
        let spent_melt_input_y = spent_melt_input.y;

        let mut spent_swap_input = test_proof_info(keyset_id, 300, mint_url.clone(), State::Spent);
        spent_swap_input.used_by_operation = Some(saga_id);
        let spent_swap_input_y = spent_swap_input.y;

        db.update_proofs(vec![spent_melt_input, spent_swap_input], vec![])
            .await
            .unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(200),
                counter_start: Some(0),
                counter_end: None,
                change_amount: Some(Amount::from(200)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![spent_melt_input_y]),
                change_blinded_messages: Some(
                    PreMintSecrets::blank(keyset_id, Amount::from(200))
                        .unwrap()
                        .blinded_messages(),
                ),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(200),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        mock_client._set_restore_response(Ok(RestoreResponse {
            outputs: vec![],
            signatures: vec![],
        }));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        assert!(result.is_some());

        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].ys, vec![spent_melt_input_y]);
        assert!(!transactions[0].ys.contains(&spent_swap_input_y));
        assert_eq!(transactions[0].fee, Amount::from(200));
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_melt_paid_restores_persisted_change_when_status_omits_change() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset = crate::wallet::test_utils::test_keyset();
        let keyset_id = keyset.id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());
        let seed = Mnemonic::generate(12).unwrap().to_seed_normalized("");
        db.add_mint(mint_url.clone(), None).await.unwrap();
        db.add_mint_keysets(
            mint_url.clone(),
            vec![cdk_common::nuts::KeySetInfo {
                id: keyset.id,
                unit: keyset.unit.clone(),
                active: keyset.active.unwrap_or(true),
                input_fee_ppk: keyset.input_fee_ppk,
                final_expiry: keyset.final_expiry,
            }],
        )
        .await
        .unwrap();
        db.add_keys(keyset.clone()).await.unwrap();

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;
        db.update_proofs(vec![pending_input], vec![]).await.unwrap();

        let premint_secrets =
            PreMintSecrets::from_seed_blank(keyset_id, 0, &seed, Amount::from(150))
                .expect("blank premint secrets");
        let blinded_messages = premint_secrets.blinded_messages();
        let counter_end = blinded_messages.len() as u32;

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(10),
                counter_start: Some(0),
                counter_end: Some(counter_end),
                change_amount: Some(Amount::from(150)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: Some(blinded_messages.clone()),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(10),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));
        mock_client._set_restore_response(Ok(restore_response_with_amounts(
            &blinded_messages,
            &[128, 16, 4, 2],
        )));
        let wallet = crate::wallet::WalletBuilder::new()
            .mint_url(mint_url)
            .unit(CurrencyUnit::Sat)
            .localstore(db.clone())
            .seed(seed)
            .shared_client(mock_client)
            .build()
            .unwrap();
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        let finalized = result.expect("melt should finalize");
        assert_eq!(finalized.fee_paid(), Amount::from(50));
        assert_eq!(
            Amount::try_sum(
                finalized
                    .change()
                    .expect("change should be recovered")
                    .iter()
                    .map(|proof| proof.amount)
            )
            .unwrap(),
            Amount::from(150)
        );
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].fee, Amount::from(50));

        let stored_input = db.get_proofs_by_ys(vec![pending_input_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Spent);

        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Paid);
        assert_eq!(quote.payment_proof.as_deref(), Some("preimage123"));
    }

    #[tokio::test]
    async fn test_recover_melt_paid_keeps_saga_when_empty_restore_exceeds_fee_reserve() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;
        db.update_proofs(vec![pending_input], vec![]).await.unwrap();

        let premint_secrets =
            PreMintSecrets::blank(keyset_id, Amount::from(150)).expect("blank premint secrets");
        let blinded_messages = premint_secrets.blinded_messages();
        let counter_end = blinded_messages.len() as u32;

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(10),
                counter_start: Some(0),
                counter_end: Some(counter_end),
                change_amount: Some(Amount::from(150)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: Some(blinded_messages),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(10),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));
        mock_client._set_restore_response(Ok(RestoreResponse {
            outputs: vec![],
            signatures: vec![],
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;
        assert!(result.is_err());
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
        let stored_input = db.get_proofs_by_ys(vec![pending_input_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Pending);
        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Unpaid);
        assert!(quote.payment_proof.is_none());
    }

    #[tokio::test]
    async fn test_recover_melt_paid_keeps_saga_when_restore_fails_ambiguously() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;
        db.update_proofs(vec![pending_input], vec![]).await.unwrap();

        let premint_secrets =
            PreMintSecrets::blank(keyset_id, Amount::from(150)).expect("blank premint secrets");
        let blinded_messages = premint_secrets.blinded_messages();
        let counter_end = blinded_messages.len() as u32;

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(10),
                counter_start: Some(0),
                counter_end: Some(counter_end),
                change_amount: Some(Amount::from(150)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: Some(blinded_messages),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(10),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));
        mock_client._set_restore_response(Err(crate::Error::Timeout));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;

        assert!(result.is_err());
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);

        let stored_input = db.get_proofs_by_ys(vec![pending_input_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Pending);

        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Unpaid);
        assert!(quote.payment_proof.is_none());
    }

    #[tokio::test]
    async fn test_recover_melt_paid_keeps_saga_when_change_recovery_data_is_missing() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;
        db.update_proofs(vec![pending_input], vec![]).await.unwrap();

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(10),
                counter_start: None,
                counter_end: None,
                change_amount: Some(Amount::from(150)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.used_by_operation = Some(saga_id.to_string());
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(10),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await;
        assert!(result.is_err());
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
        let stored_input = db.get_proofs_by_ys(vec![pending_input_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Pending);
        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Unpaid);
        assert!(quote.payment_proof.is_none());
        assert_eq!(quote.used_by_operation, Some(saga_id.to_string()));
    }

    #[tokio::test]
    async fn test_recover_incomplete_sagas_retains_paid_melt_with_definitive_restore_failure() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        let mut pending_input = test_proof_info(keyset_id, 1200, mint_url.clone(), State::Pending);
        pending_input.used_by_operation = Some(saga_id);
        let pending_input_y = pending_input.y;
        db.update_proofs(vec![pending_input], vec![]).await.unwrap();

        let premint_secrets =
            PreMintSecrets::blank(keyset_id, Amount::from(150)).expect("blank premint secrets");
        let blinded_messages = premint_secrets.blinded_messages();
        let counter_end = blinded_messages.len() as u32;

        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(1000),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(1000),
                fee_reserve: Amount::from(10),
                counter_start: Some(0),
                counter_end: Some(counter_end),
                change_amount: Some(Amount::from(150)),
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![pending_input_y]),
                change_blinded_messages: Some(blinded_messages),
            }),
        );
        db.add_saga(saga).await.unwrap();

        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        melt_quote.used_by_operation = Some(saga_id.to_string());
        db.add_melt_quote(melt_quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id.clone(),
            state: MeltQuoteState::Paid,
            expiry: 9999999999,
            fee_reserve: Amount::from(10),
            amount: Amount::from(1000),
            request: Some("lnbc1000...".to_string()),
            payment_preimage: Some("preimage123".to_string()),
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));
        mock_client._set_restore_response(Err(crate::Error::HttpError(
            Some(404),
            "Not Found".to_string(),
        )));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        save_payment(&wallet, saga_id).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.recovered, 0);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.failed, 1);
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
        let stored_input = db.get_proofs_by_ys(vec![pending_input_y]).await.unwrap();
        assert_eq!(stored_input.len(), 1);
        assert_eq!(stored_input[0].state, State::Pending);
        let quote = db.get_melt_quote(&quote_id).await.unwrap().unwrap();
        assert_eq!(quote.state, MeltQuoteState::Unpaid);
        assert!(quote.payment_proof.is_none());
        assert_eq!(quote.used_by_operation, Some(saga_id.to_string()));
    }

    #[tokio::test]
    async fn test_recover_melt_submitted_unpaid_or_failed_preserves_exact_intent() {
        for status in [MeltQuoteState::Unpaid, MeltQuoteState::Failed] {
            // Submitted requests remain recoverable after an Unpaid or Failed observation.
            let db = create_test_db().await;
            let mint_url = test_mint_url();
            let keyset_id = test_keyset().id;
            let saga_id = uuid::Uuid::new_v4();
            let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

            // Create and reserve proofs
            let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
            let proof_y = proof_info.y;
            db.update_proofs(vec![proof_info], vec![]).await.unwrap();
            db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();
            db.update_proofs_state(vec![proof_y], State::Pending)
                .await
                .unwrap();

            // Create saga in MeltRequested state
            let saga = WalletSaga::new(
                saga_id,
                WalletSagaState::Melt(MeltSagaState::MeltRequested),
                Amount::from(100),
                mint_url.clone(),
                CurrencyUnit::Sat,
                OperationData::Melt(MeltOperationData {
                    payment: None,
                    swap: None,
                    acquisition: None,
                    quote_id: quote_id.clone(),
                    amount: Amount::from(100),
                    fee_reserve: Amount::ZERO,
                    counter_start: None,
                    counter_end: None,
                    change_amount: None,
                    metadata: HashMap::new(),
                    final_proof_ys: Some(vec![proof_y]),
                    change_blinded_messages: None,
                }),
            );
            db.add_saga(saga).await.unwrap();

            // Store melt quote
            let mut melt_quote = test_melt_quote();
            melt_quote.id = quote_id.clone();
            db.add_melt_quote(melt_quote).await.unwrap();

            // Mock: quote is Unpaid
            let mock_client = Arc::new(MockMintConnector::new());
            mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
                quote: quote_id,
                state: status,
                expiry: 9999999999,
                fee_reserve: Amount::ZERO,
                amount: Amount::from(100),
                request: Some("lnbc100...".to_string()),
                payment_preimage: None,
                change: None,
                unit: Some(CurrencyUnit::Sat),
                method: PaymentMethod::BOLT11,
            }));

            let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
            let saved = save_payment(&wallet, saga_id).await;
            let history = db.list_transactions(None, None, None).await.unwrap();
            let proofs = db.get_reserved_proofs(&saga_id).await.unwrap();
            let quote = db
                .get_melt_quote(&saved.quote_id.clone().unwrap())
                .await
                .unwrap();
            let result = wallet
                .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
                .await
                .unwrap();

            assert!(result.is_none());
            assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved.clone()));
            assert_eq!(
                db.list_transactions(None, None, None).await.unwrap(),
                history
            );
            assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap(), proofs);
            assert_eq!(
                db.get_melt_quote(&saved.quote_id.unwrap()).await.unwrap(),
                quote
            );
        }
    }

    #[tokio::test]
    async fn test_recover_melt_melt_requested_quote_pending() {
        // Mock: quote Pending → skip
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset().id;
        let saga_id = uuid::Uuid::new_v4();
        let quote_id = format!("test_melt_quote_{}", uuid::Uuid::new_v4());

        // Create and reserve proofs
        let proof_info = test_proof_info(keyset_id, 100, mint_url.clone(), State::Unspent);
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();
        db.update_proofs_state(vec![proof_y], State::Pending)
            .await
            .unwrap();

        // Create saga in MeltRequested state
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            Amount::from(100),
            mint_url.clone(),
            CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote_id.clone(),
                amount: Amount::from(100),
                fee_reserve: Amount::ZERO,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: HashMap::new(),
                final_proof_ys: Some(vec![proof_y]),
                change_blinded_messages: None,
            }),
        );
        db.add_saga(saga).await.unwrap();

        // Store melt quote
        let mut melt_quote = test_melt_quote();
        melt_quote.id = quote_id.clone();
        db.add_melt_quote(melt_quote).await.unwrap();

        // Mock: quote is Pending (no payment_proof)
        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote_id,
            state: MeltQuoteState::Pending,
            expiry: 9999999999,
            fee_reserve: Amount::ZERO,
            amount: Amount::from(100),
            request: Some("lnbc100...".to_string()),
            payment_preimage: None,
            change: None,
            unit: Some(CurrencyUnit::Sat),
            method: PaymentMethod::BOLT11,
        }));

        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;
        let saved = save_payment(&wallet, saga_id).await;
        let history = db.list_transactions(None, None, None).await.unwrap();
        let inputs = db.get_reserved_proofs(&saga_id).await.unwrap();
        let quote = db
            .get_melt_quote(&saved.quote_id.clone().unwrap())
            .await
            .unwrap();
        let result = wallet
            .resume_melt_saga(&db.get_saga(&saga_id).await.unwrap().unwrap())
            .await
            .unwrap();

        // Should skip (None returned for pending)
        assert!(result.is_none());

        // Proofs should still be reserved
        let reserved = db.get_reserved_proofs(&saga_id).await.unwrap();
        assert_eq!(reserved.len(), 1);

        // Saga should still exist
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
        assert_eq!(db.get_saga(&saga_id).await.unwrap(), Some(saved.clone()));
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            history
        );
        assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap(), inputs);
        assert_eq!(
            db.get_melt_quote(&saved.quote_id.unwrap()).await.unwrap(),
            quote
        );
    }
}
