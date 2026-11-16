//! Wallet saga recovery module.
//!
//! This module handles recovery from incomplete wallet sagas after a crash.
//! It follows the same pattern as the mint's `start_up_check.rs` module.
//!
//! # Recovery Strategy
//!
//! The recovery logic examines the state of each incomplete saga:
//!
//! - **Safe to Rollback:** If no external calls were made (e.g. ProofsReserved),
//!   the saga is compensated by releasing resources (proofs, quotes).
//! - **Check External State:** If external calls were possible (e.g. SwapRequested),
//!   the wallet queries the mint to determine if the operation succeeded.
//!   - If successful: The wallet completes the local state update (e.g. marking proofs spent).
//!   - If failed/unknown: The wallet may rollback or retry depending on the specific state.

use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::IncompleteSagaReader;
use cdk_common::wallet::{ProofInfo, WalletSagaState};
use cdk_common::BlindedMessage;
use tracing::instrument;

use crate::dhke::construct_proofs;
use crate::nuts::{BlindSignature, PreMintSecrets, RestoreRequest, RestoreResponse, State};
use crate::wallet::blind_signature::{
    validate_mint_response_signatures, SignatureAmountValidation,
};
use crate::wallet::util::escape_log_value;
use crate::{Error, Wallet};

/// Parameters for recovering outputs using stored blinded messages.
///
/// This struct captures the common data needed by issuance and melt change
/// recovery operations.
struct OutputRecoveryParams<'a> {
    /// The blinded messages stored during the original operation
    blinded_messages: &'a [BlindedMessage],
    /// Counter start for re-deriving secrets
    counter_start: u32,
    /// Counter end for re-deriving secrets
    counter_end: u32,
}

/// Whether all requested outputs must have been signed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputRecoveryMode {
    /// Fixed outputs from swap, receive, or issuance must all be recovered.
    Complete,
    /// Melt change may leave some blank outputs unsigned.
    Partial,
}

/// Report of recovery operations performed by [`Wallet::recover_incomplete_sagas`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    /// Operations successfully completed after crash
    pub recovered: usize,
    /// Operations rolled back (resources released)
    pub compensated: usize,
    /// Operations still pending (will retry on next call)
    pub skipped: usize,
    /// Operations that could not be recovered
    pub failed: usize,
}

impl RecoveryReport {
    /// Returns true if no operations were recovered, compensated, skipped, or failed.
    pub fn is_empty(&self) -> bool {
        self.recovered == 0 && self.compensated == 0 && self.skipped == 0 && self.failed == 0
    }
}

/// Result of a saga recovery operation.
///
/// Used by individual saga resume functions to indicate the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// The saga was successfully recovered (outputs saved)
    Recovered,
    /// The saga was compensated (rolled back)
    Compensated,
    /// The saga was skipped (e.g., mint unreachable, payment pending)
    Skipped,
}

/// Detailed result of a restore attempt against the mint.
#[derive(Debug, Clone)]
pub(crate) enum OutputRecoveryResult {
    /// The mint returned signatures and the wallet reconstructed proofs.
    Restored(Vec<ProofInfo>),
    /// The mint accepted the restore request but had no matching signatures.
    EmptyResponse,
    /// The restore attempt could not be made definitively.
    Unavailable,
}

impl Wallet {
    /// Restore outputs, requiring complete coverage for fixed-output operations.
    pub(crate) async fn restore_outputs_with_result(
        &self,
        saga_id: &uuid::Uuid,
        saga_type: &str,
        blinded_messages: Option<&[BlindedMessage]>,
        counter_start: Option<u32>,
        counter_end: Option<u32>,
        mode: OutputRecoveryMode,
    ) -> Result<OutputRecoveryResult, Error> {
        let blinded_messages_owned = blinded_messages.map(|bm| bm.to_vec());

        let params = match Self::extract_recovery_params(
            saga_id,
            saga_type,
            blinded_messages_owned.as_ref(),
            counter_start,
            counter_end,
        ) {
            Some(p) => p,
            None => return Ok(OutputRecoveryResult::Unavailable),
        };

        self.recover_outputs_from_blinded_messages(saga_id, saga_type, params, mode)
            .await
    }

    /// Recover NUT-08 change signatures already included in a paid quote.
    pub(crate) async fn recover_melt_change_signatures(
        &self,
        saga_id: &uuid::Uuid,
        blinded_messages: &[BlindedMessage],
        counter_start: u32,
        counter_end: u32,
        signatures: Vec<BlindSignature>,
    ) -> Result<OutputRecoveryResult, Error> {
        // NUT-08 returns the signed prefix in the original output order.
        let outputs = blinded_messages.get(..signatures.len()).ok_or_else(|| {
            Error::InvalidMintResponse("melt change exceeds the requested output count".to_owned())
        })?;
        self.construct_recovered_outputs(
            saga_id,
            "Melt",
            OutputRecoveryParams {
                blinded_messages,
                counter_start,
                counter_end,
            },
            OutputRecoveryMode::Partial,
            RestoreResponse {
                outputs: outputs.to_vec(),
                signatures,
            },
        )
        .await
    }

    /// Recover from incomplete operations after a crash. Call on startup.
    ///
    /// Handles interrupted swap, send, receive, and melt operations to prevent
    /// proofs from being stuck in reserved states. Requires network connection.
    /// Reservations without their original operation journal remain unchanged;
    /// missing authority cannot establish that a payment was never submitted.
    #[instrument(skip(self))]
    pub async fn recover_incomplete_sagas(&self) -> Result<RecoveryReport, Error> {
        let mut sagas = IncompleteSagaReader::new(self.localstore.as_ref());
        let mut report = RecoveryReport::default();
        while let Some(saga) = sagas.next().await? {
            if saga.mint_url != self.mint_url || saga.unit != self.unit {
                continue;
            }
            tracing::info!(
                "Recovering saga {} (kind: {:?}, state: {})",
                saga.id,
                saga.kind,
                saga.state.state_str()
            );

            let result: Result<RecoveryAction, Error> = match &saga.state {
                WalletSagaState::Swap(_) => self.resume_swap_saga(&saga).await,
                WalletSagaState::Send(_) => self.resume_send_saga(&saga).await,
                WalletSagaState::Receive(_) => self.resume_receive_saga(&saga).await,
                WalletSagaState::Issue(_) => self.resume_issue_saga(&saga).await,
                WalletSagaState::Melt(_) => {
                    // Melt saga returns Option<FinalizedMelt>, convert to RecoveryAction
                    self.resume_melt_saga(&saga).await.map(|opt| match opt {
                        Some(finalized) => {
                            use cdk_common::MeltQuoteState;
                            if finalized.state() == MeltQuoteState::Paid {
                                RecoveryAction::Recovered
                            } else {
                                RecoveryAction::Compensated
                            }
                        }
                        None => RecoveryAction::Skipped,
                    })
                }
            };

            match result {
                Ok(RecoveryAction::Recovered) => {
                    tracing::info!("Saga {} recovered successfully", saga.id);
                    report.recovered += 1;
                }
                Ok(RecoveryAction::Compensated) => {
                    tracing::info!("Saga {} compensated (rolled back)", saga.id);
                    report.compensated += 1;
                }
                Ok(RecoveryAction::Skipped) => {
                    tracing::info!("Saga {} skipped", saga.id);
                    report.skipped += 1;
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to recover saga {}: {}",
                        saga.id,
                        escape_log_value(&e)
                    );
                    report.failed += 1;
                }
            }
        }

        tracing::info!(
            "Recovery complete: {} recovered, {} compensated, {} skipped, {} failed",
            report.recovered,
            report.compensated,
            report.skipped,
            report.failed
        );

        Ok(report)
    }

    /// Recover outputs using stored blinded messages.
    ///
    /// This is the core recovery logic shared between swap, receive, and issue saga
    /// recovery. It queries the mint for signatures using the stored blinded messages
    /// and reconstructs the proofs.
    ///
    /// # Returns
    ///
    /// - `Ok(OutputRecoveryResult::Restored(proofs))` - Successfully recovered proofs
    /// - `Ok(OutputRecoveryResult::EmptyResponse)` - Mint had no matching signatures
    /// - `Ok(OutputRecoveryResult::Unavailable)` - Could not make a definitive restore attempt
    /// - `Err(_)` - Unrecoverable error (e.g., cryptographic failure)
    #[instrument(skip(self, params))]
    async fn recover_outputs_from_blinded_messages(
        &self,
        saga_id: &uuid::Uuid,
        saga_type: &str,
        params: OutputRecoveryParams<'_>,
        mode: OutputRecoveryMode,
    ) -> Result<OutputRecoveryResult, Error> {
        tracing::info!(
            "{} saga {} - attempting to recover {} outputs using stored blinded messages",
            saga_type,
            saga_id,
            params.blinded_messages.len()
        );

        let restore_request = RestoreRequest {
            outputs: params.blinded_messages.to_vec(),
        };

        let restore_response = match self.client.post_restore(restore_request).await {
            Ok(response) => response,
            Err(e) => {
                if e.is_definitive_failure() {
                    tracing::warn!(
                        "{} saga {} - failed to restore from mint (definitive): {}. \
                         Run wallet.restore() to recover any missing proofs.",
                        saga_type,
                        saga_id,
                        escape_log_value(&e)
                    );
                    return Ok(OutputRecoveryResult::Unavailable);
                } else {
                    tracing::warn!(
                        "{} saga {} - failed to restore from mint (ambiguous): {}. \
                         Skipping recovery to retry later.",
                        saga_type,
                        saga_id,
                        escape_log_value(&e)
                    );
                    return Err(e);
                }
            }
        };

        self.construct_recovered_outputs(saga_id, saga_type, params, mode, restore_response)
            .await
    }

    /// Validate and construct proofs identically for restore and quote responses.
    async fn construct_recovered_outputs(
        &self,
        saga_id: &uuid::Uuid,
        saga_type: &str,
        params: OutputRecoveryParams<'_>,
        mode: OutputRecoveryMode,
        restore_response: RestoreResponse,
    ) -> Result<OutputRecoveryResult, Error> {
        // Validate coverage before constructing or saving any proofs. A nonempty
        // subset is not enough to finish an operation with fixed outputs.
        if mode == OutputRecoveryMode::Complete {
            let mut expected: HashSet<_> = params
                .blinded_messages
                .iter()
                .map(|output| output.blinded_secret)
                .collect();
            if expected.len() != params.blinded_messages.len()
                || restore_response.outputs.len() != restore_response.signatures.len()
                || restore_response.outputs.len() != expected.len()
                || !restore_response
                    .outputs
                    .iter()
                    .all(|output| expected.remove(&output.blinded_secret))
                || !expected.is_empty()
            {
                return Err(Error::InvalidMintResponse(
                    "restore response does not cover every expected output exactly once".to_owned(),
                ));
            }
        }

        if restore_response.signatures.is_empty() {
            tracing::warn!(
                "{} saga {} - mint returned no signatures. \
                 Outputs may have already been saved or mint doesn't have them.",
                saga_type,
                saga_id
            );
            return Ok(OutputRecoveryResult::EmptyResponse);
        }

        // Get keyset ID from the first blinded message
        let keyset_id = params.blinded_messages[0].keyset_id;

        let premint_secrets = PreMintSecrets::restore_batch(
            keyset_id,
            &self.seed,
            params.counter_start,
            params.counter_end,
        )?;

        let premints_by_blinded_secret = premint_secrets
            .secrets
            .iter()
            .map(|premint| (premint.blinded_message.blinded_secret, premint))
            .collect::<HashMap<_, _>>();
        let requested_outputs_by_blinded_secret = params
            .blinded_messages
            .iter()
            .map(|output| (output.blinded_secret, output))
            .collect::<HashMap<_, _>>();

        // Match the returned outputs to our premint secrets by B_ value, preserving
        // the mint response order used by the returned signatures.
        let matched: Vec<_> = restore_response
            .outputs
            .iter()
            .filter_map(|output| {
                let premint = premints_by_blinded_secret.get(&output.blinded_secret)?;
                let requested_output =
                    requested_outputs_by_blinded_secret.get(&output.blinded_secret)?;
                Some((*premint, *requested_output))
            })
            .collect();

        if matched.len() != restore_response.signatures.len() {
            tracing::warn!(
                "{} saga {} - signature count mismatch: {} secrets, {} signatures",
                saga_type,
                saga_id,
                matched.len(),
                restore_response.signatures.len()
            );
        }

        // Load keyset keys for proof construction
        let keys = self.keyset(keyset_id).await?.keys;

        validate_mint_response_signatures(
            self,
            &restore_response.signatures,
            matched
                .iter()
                .map(|(_, requested_output)| *requested_output),
            SignatureAmountValidation::AllowZeroAmountPlaceholder,
        )
        .await?;

        // Construct proofs from signatures
        let proofs = construct_proofs(
            restore_response.signatures,
            matched.iter().map(|(p, _)| p.r.clone()).collect(),
            matched.iter().map(|(p, _)| p.secret.clone()).collect(),
            &keys,
        )?;

        tracing::info!(
            "{} saga {} - recovered {} proofs",
            saga_type,
            saga_id,
            proofs.len()
        );

        // Convert to ProofInfo
        let proof_infos: Vec<ProofInfo> = proofs
            .into_iter()
            .zip(matched)
            .map(|(p, (premint, _))| {
                let proof_info =
                    ProofInfo::new(p, self.mint_url.clone(), State::Unspent, self.unit.clone())?;
                Ok::<_, Error>(match premint.derivation_index {
                    Some(index) => proof_info.with_derivation_index(index),
                    None => proof_info,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(OutputRecoveryResult::Restored(proof_infos))
    }

    /// Extract recovery parameters from operation data.
    ///
    /// Returns `None` if the required data (blinded messages, counter range) is missing.
    fn extract_recovery_params<'a>(
        saga_id: &uuid::Uuid,
        saga_type: &str,
        blinded_messages: Option<&'a Vec<BlindedMessage>>,
        counter_start: Option<u32>,
        counter_end: Option<u32>,
    ) -> Option<OutputRecoveryParams<'a>> {
        let blinded_messages = match blinded_messages {
            Some(bm) if !bm.is_empty() => bm,
            _ => {
                tracing::warn!(
                    "{} saga {} - no blinded messages stored, cannot recover outputs. \
                     Run wallet.restore() to recover any missing proofs.",
                    saga_type,
                    saga_id
                );
                return None;
            }
        };

        let (counter_start, counter_end) = match (counter_start, counter_end) {
            (Some(start), Some(end)) => (start, end),
            _ => {
                tracing::warn!(
                    "{} saga {} - no counter range stored, cannot recover outputs. \
                     Run wallet.restore() to recover any missing proofs.",
                    saga_type,
                    saga_id
                );
                return None;
            }
        };

        Some(OutputRecoveryParams {
            blinded_messages,
            counter_start,
            counter_end,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;

    use cdk_common::mint_url::MintUrl;
    use cdk_common::nuts::{MeltQuoteBolt11Response, MeltQuoteState, State};
    use cdk_common::wallet::{
        IssueSagaState, MeltOperationData, MeltPayment, MeltQuote, MeltSagaState, OperationData,
        ReceiveOperationData, ReceiveSagaState, Transaction, TransactionDirection,
        TransactionStatus, WalletSaga, WalletSagaState,
    };
    use cdk_common::Amount;

    use crate::wallet::test_utils::*;

    #[tokio::test]
    async fn test_fixed_output_recovery_rejects_incomplete_responses_and_retries() {
        use cdk_common::amount::{FeeAndAmounts, SplitTarget};
        use cdk_common::wallet::{receive_identity, TransactionStatus};

        use crate::nuts::{
            BlindSignature, CheckStateResponse, CurrencyUnit, PreMintSecrets, RestoreResponse,
        };
        use crate::Error;

        for (kind, repeated_amounts) in [
            ("Issue", false),
            ("Receive", false),
            ("Issue", true),
            ("Receive", true),
        ] {
            let issue = kind == "Issue";
            let receive = kind == "Receive";
            let db = create_test_db().await;
            let client = Arc::new(MockMintConnector::new());
            client.enable_mint_signing();
            let keyset_id = client.keysets.lock().unwrap()[0].id;
            let wallet = create_test_wallet_with_mock(db.clone(), client.clone()).await;
            let amount = Amount::from(13);
            let split = if repeated_amounts {
                SplitTarget::Values(vec![4.into(), 4.into(), 4.into(), 1.into()])
            } else {
                SplitTarget::None
            };
            let premints = PreMintSecrets::from_seed(
                keyset_id,
                0,
                &wallet.seed,
                amount,
                &split,
                &FeeAndAmounts::from((0, vec![1, 2, 4, 8])),
            )
            .unwrap();
            let outputs = premints.blinded_messages();
            let output_count = outputs.len();
            assert_eq!(output_count, if repeated_amounts { 4 } else { 3 });
            let signatures: Vec<_> = outputs
                .iter()
                .map(|output| BlindSignature {
                    amount: output.amount,
                    keyset_id,
                    c: crate::dhke::sign_message(
                        &client.mint_signing_keys.lock().unwrap().as_ref().unwrap()[&output.amount],
                        &output.blinded_secret,
                    )
                    .unwrap(),
                    dleq: None,
                })
                .collect();
            client._set_restore_response(Ok(RestoreResponse {
                outputs: outputs[..1].to_vec(),
                signatures: signatures[..1].to_vec(),
            }));
            let mut blank_outputs = outputs.clone();
            for output in &mut blank_outputs {
                output.amount = Amount::ZERO;
            }
            let melt_result = wallet
                .restore_outputs_with_result(
                    &uuid::Uuid::new_v4(),
                    "Melt",
                    Some(&blank_outputs),
                    Some(0),
                    Some(output_count as u32),
                    super::OutputRecoveryMode::Partial,
                )
                .await
                .unwrap();
            assert!(
                matches!(melt_result, super::OutputRecoveryResult::Restored(proofs) if proofs.len() == 1)
            );
            let saga_id = uuid::Uuid::new_v4();
            if issue {
                let mut quote = test_mint_quote(test_mint_url());
                quote.id = "restore-test".to_owned();
                quote.amount = Some(amount);
                quote.amount_paid = amount;
                quote.state = cdk_common::nuts::MintQuoteState::Paid;
                quote.used_by_operation = Some(saga_id.to_string());
                db.add_mint_quote(quote).await.unwrap();
            }

            let (state, mut data) = match kind {
                "Issue" => (
                    WalletSagaState::Issue(IssueSagaState::MintRequested),
                    OperationData::Mint(test_issue_data(
                        {
                            let mut quote =
                                db.get_mint_quote("restore-test").await.unwrap().unwrap();
                            quote.used_by_operation = None;
                            quote
                        },
                        premints.clone(),
                    )),
                ),
                "Receive" => (
                    WalletSagaState::Receive(ReceiveSagaState::Redeeming),
                    OperationData::Receive(ReceiveOperationData { checkpoint: None }),
                ),
                _ => unreachable!("known recovery fixture"),
            };
            let input = test_proof_info(keyset_id, 13, test_mint_url());
            if let OperationData::Receive(data) = &mut data {
                let mut acquired = input.clone();
                acquired.state = State::Pending;
                acquired.used_by_operation = Some(saga_id);
                db.update_proofs(vec![acquired.clone()], vec![])
                    .await
                    .unwrap();
                data.checkpoint = Some(Box::new(cdk_common::wallet::ReceiveCheckpoint {
                    inputs: vec![cdk_common::database::wallet::ProofChange {
                        y: input.y,
                        before: None,
                        after: Some(acquired),
                    }],
                    pre_swap: Some(crate::nuts::PreSwap {
                        pre_mint_secrets: premints.clone(),
                        swap_request: crate::nuts::SwapRequest::new(
                            vec![input.proof.clone()],
                            outputs.clone(),
                        ),
                        derived_secret_count: output_count as u32,
                        fee: Amount::ZERO,
                        p2bk_secret_keys: None,
                    }),
                    transaction: cdk_common::wallet::Transaction {
                        role: Default::default(),
                        direction: TransactionDirection::Incoming,
                        mint_url: test_mint_url(),
                        unit: CurrencyUnit::Sat,
                        amount,
                        fee: Amount::ZERO,
                        ys: vec![input.y],
                        timestamp: 1,
                        memo: None,
                        metadata: HashMap::from([(
                            receive_identity::METADATA_KEY.to_owned(),
                            receive_identity::fingerprint(
                                &test_mint_url(),
                                &CurrencyUnit::Sat,
                                std::slice::from_ref(&input.proof),
                            )
                            .unwrap(),
                        )]),
                        quote_id: None,
                        payment_request: None,
                        payment_proof: None,
                        payment_method: None,
                        saga_id: Some(saga_id),
                        status: TransactionStatus::Completed,
                    },
                    outputs: None,
                }));
            }
            let stage_replay_failure = || {
                if issue {
                    client.push_post_mint_response(Err(Error::Custom("already issued".to_owned())));
                } else {
                    client.set_post_swap_response(Err(Error::TokenAlreadySpent));
                    client.set_check_state_response(Ok(CheckStateResponse {
                        states: vec![(input.y, State::Spent).into()],
                    }));
                }
            };
            let saga = WalletSaga::new(
                saga_id,
                state,
                amount,
                test_mint_url(),
                CurrencyUnit::Sat,
                data,
            );
            if issue {
                for receipt in cdk_common::database::wallet::issue_receipts(&saga).unwrap() {
                    db.add_transaction(receipt).await.unwrap();
                }
            }
            db.add_saga(saga).await.unwrap();

            let mut duplicate_outputs = outputs.clone();
            let duplicate_index = outputs
                .iter()
                .position(|o| {
                    o.amount == outputs[1].amount && o.blinded_secret != outputs[1].blinded_secret
                })
                .unwrap_or(0);
            duplicate_outputs[1] = duplicate_outputs[duplicate_index].clone();
            let mut duplicate_signatures = signatures.clone();
            duplicate_signatures[1] = duplicate_signatures[duplicate_index].clone();
            let mut unknown_outputs = outputs.clone();
            unknown_outputs[0].blinded_secret = test_proof(keyset_id, 1).c;
            // Repeated denominations let a valid duplicate signature preserve
            // both count and total amount while omitting an expected identity.
            let invalid_responses = [
                RestoreResponse {
                    outputs: vec![],
                    signatures: vec![],
                },
                RestoreResponse {
                    outputs: outputs[..1].to_vec(),
                    signatures: signatures[..1].to_vec(),
                },
                RestoreResponse {
                    outputs: duplicate_outputs,
                    signatures: duplicate_signatures,
                },
                RestoreResponse {
                    outputs: unknown_outputs,
                    signatures: signatures.clone(),
                },
                RestoreResponse {
                    outputs: outputs.clone(),
                    signatures: signatures[..1].to_vec(),
                },
            ];
            for (case, response) in invalid_responses.into_iter().enumerate() {
                client._set_restore_response(Ok(response));
                stage_replay_failure();
                let report = wallet.recover_incomplete_sagas().await.unwrap();
                assert_eq!(
                    report.failed, 1,
                    "kind={kind}, repeated={repeated_amounts}, case={case}"
                );
                assert_eq!(report.recovered, 0);
                assert!(db.get_saga(&saga_id).await.unwrap().is_some());
                let stored = db.get_proofs(None, None, None, None).await.unwrap();
                assert_eq!(stored.len(), usize::from(receive));
                if receive {
                    assert_eq!(stored[0].y, input.y);
                    assert_eq!(db.get_reserved_proofs(&saga_id).await.unwrap().len(), 1);
                }
                if issue {
                    let transactions = db.list_transactions(None, None, None).await.unwrap();
                    assert_eq!(transactions.len(), 1);
                    assert_eq!(transactions[0].status, TransactionStatus::Pending);
                }
            }

            client._set_restore_response(Ok(RestoreResponse {
                outputs: outputs.into_iter().rev().collect(),
                signatures: signatures.into_iter().rev().collect(),
            }));
            stage_replay_failure();
            let report = wallet.recover_incomplete_sagas().await.unwrap();
            assert_eq!(report.recovered, 1);
            assert_eq!(report.failed, 0);
            assert!(db.get_saga(&saga_id).await.unwrap().is_none());
            let proofs = db
                .get_proofs(None, None, Some(vec![State::Unspent]), None)
                .await
                .unwrap();
            assert_eq!(proofs.len(), output_count);
            assert_eq!(
                Amount::try_sum(proofs.iter().map(|p| p.proof.amount)).unwrap(),
                amount
            );
            if issue || receive {
                let transactions = db.list_transactions(None, None, None).await.unwrap();
                assert_eq!(transactions[0].status, TransactionStatus::Completed);
            }
        }
    }

    #[tokio::test]
    async fn test_receive_without_checkpoint_is_retained() {
        // Missing durable evidence must never be treated as a completed receive.
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();

        // Create saga in InputsRecorded state (no proofs to reserve for this test,
        // just test that the saga gets cleaned up)
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            Amount::from(100),
            mint_url.clone(),
            cdk_common::nuts::CurrencyUnit::Sat,
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        db.add_saga(saga).await.unwrap();

        // Run recovery
        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.failed, 1);

        // Preserve the incomplete evidence.
        assert!(db.get_saga(&saga_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_recover_issue_secrets_prepared() {
        let db = create_test_db().await;
        let mut quote = test_mint_quote(test_mint_url());
        quote.state = crate::wallet::MintQuoteState::Paid;
        quote.amount = Some(8.into());
        quote.amount_paid = 8.into();
        quote.secret_key = Some(cdk_common::SecretKey::generate());
        db.add_mint_quote(quote.clone()).await.unwrap();
        let mint = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mint.clone()).await;
        crate::wallet::issue::MintSaga::new(&wallet)
            .prepare(&quote.id, crate::amount::SplitTarget::None, None)
            .await
            .unwrap();
        let sagas = db.get_incomplete_sagas().await.unwrap();
        assert_eq!(sagas.len(), 1);
        let mut receipts = cdk_common::database::wallet::issue_receipts(&sagas[0]).unwrap();
        assert_eq!(receipts.len(), 1);
        receipts[0].status = TransactionStatus::Failed;
        let report = wallet.recover_incomplete_sagas().await.unwrap();
        assert_eq!(report.compensated, 1);
        assert_eq!(report.failed, 0);
        let retrieved_quote = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
        assert!(retrieved_quote.used_by_operation.is_none());
        assert!(db.get_saga(&sagas[0].id).await.unwrap().is_none());
        assert_eq!(
            db.list_transactions(None, None, None).await.unwrap(),
            receipts
        );
        assert!(db
            .get_proofs(None, None, None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(mint.post_mint_requests().is_empty());
    }

    #[tokio::test]
    async fn test_recover_melt_proofs_reserved() {
        // Test that melt saga in ProofsReserved state gets compensated:
        // - Reserved proofs are released
        // - Quote reservation is released
        // - Saga is deleted
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let saga_id = uuid::Uuid::new_v4();

        // Create and store proofs, then reserve them
        let proof_info = test_proof_info(keyset_id, 128, mint_url.clone());
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info.clone()], vec![])
            .await
            .unwrap();
        db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();

        // Create a melt quote reserved by this operation
        let mut quote = test_melt_quote();
        quote.amount = Amount::from(100);
        quote.fee_reserve = Amount::from(10);
        quote.used_by_operation = Some(saga_id.to_string());
        db.add_melt_quote(quote.clone()).await.unwrap();

        // Create saga in ProofsReserved state
        let saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::ProofsReserved),
            Amount::from(100),
            mint_url.clone(),
            cdk_common::nuts::CurrencyUnit::Sat,
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: Some(vec![cdk_common::database::wallet::ProofChange {
                    y: proof_y,
                    before: Some(proof_info),
                    after: db.get_proofs_by_ys(vec![proof_y]).await.unwrap().pop(),
                }]),
                quote_id: quote.id.clone(),
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

        // Run recovery
        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.compensated, 1);

        // Verify proofs are released
        let proofs = db
            .get_proofs(None, None, Some(vec![State::Unspent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 1);

        // Verify melt quote reservation is released
        let retrieved_quote = db.get_melt_quote(&quote.id).await.unwrap().unwrap();
        assert!(retrieved_quote.used_by_operation.is_none());

        // Verify saga is deleted
        assert!(db.get_saga(&saga_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_no_incomplete_sagas() {
        // Test that recovery with no incomplete sagas returns empty report
        let db = create_test_db().await;
        let wallet = create_test_wallet(db.clone()).await;

        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.recovered, 0);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.failed, 0);
    }

    #[tokio::test]
    async fn test_recover_multiple_sagas() {
        // Test that recovery handles multiple sagas
        // (Note: We'll use Melt sagas here since we removed the Swap logic for testing)
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();

        // Create 3 sagas in different early states
        for i in 0..3 {
            let saga_id = uuid::Uuid::new_v4();

            let proof_info = test_proof_info(keyset_id, 128, mint_url.clone());
            let proof_y = proof_info.y;
            db.update_proofs(vec![proof_info.clone()], vec![])
                .await
                .unwrap();
            db.reserve_proofs(vec![proof_y], &saga_id).await.unwrap();

            let mut quote = test_melt_quote();
            quote.amount = Amount::from(100);
            quote.fee_reserve = Amount::from(10);
            quote.id = format!("quote_{}", i);
            quote.used_by_operation = Some(saga_id.to_string());
            db.add_melt_quote(quote.clone()).await.unwrap();

            let saga = WalletSaga::new(
                saga_id,
                WalletSagaState::Melt(MeltSagaState::ProofsReserved),
                Amount::from(100),
                mint_url.clone(),
                cdk_common::nuts::CurrencyUnit::Sat,
                OperationData::Melt(MeltOperationData {
                    payment: None,
                    swap: None,
                    acquisition: Some(vec![cdk_common::database::wallet::ProofChange {
                        y: proof_y,
                        before: Some(proof_info),
                        after: db.get_proofs_by_ys(vec![proof_y]).await.unwrap().pop(),
                    }]),
                    quote_id: quote.id.clone(),
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
        }

        // Run recovery
        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        assert_eq!(report.compensated, 3);

        // All proofs should be released
        let proofs = db
            .get_proofs(None, None, Some(vec![State::Unspent]), None)
            .await
            .unwrap();
        assert_eq!(proofs.len(), 3);

        // All sagas should be deleted
        let sagas = db.get_incomplete_sagas().await.unwrap();
        assert!(sagas.is_empty());
    }

    #[tokio::test]
    async fn test_missing_melt_journal_preserves_quote_ownership() {
        let db = create_test_db().await;
        let mut originals = Vec::new();
        for scope in 0..4 {
            let mut quote = test_melt_quote();
            quote.used_by_operation = Some(uuid::Uuid::new_v4().to_string());
            match scope {
                1 => quote.mint_url = Some("https://other-mint.example".parse().unwrap()),
                2 => quote.unit = cdk_common::CurrencyUnit::Msat,
                3 => quote.mint_url = None,
                _ => {}
            }
            db.add_melt_quote(quote.clone()).await.unwrap();
            originals.push(quote);
        }
        let wallet = create_test_wallet(db.clone()).await;
        assert!(wallet.recover_incomplete_sagas().await.unwrap().is_empty());
        for quote in originals {
            assert_eq!(
                db.get_melt_quote(&quote.id).await.unwrap(),
                Some(quote),
                "missing melt journal must retain quote ownership"
            );
        }
    }

    #[tokio::test]
    async fn test_missing_issue_journal_preserves_quote_ownership() {
        // A missing original journal is not proof that a request was unsubmitted.
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let operation_id = uuid::Uuid::new_v4();

        // Create a mint quote with reservation but NO corresponding saga
        let mut quote = test_mint_quote(mint_url);
        quote.used_by_operation = Some(operation_id.to_string());
        db.add_mint_quote(quote.clone()).await.unwrap();

        // Run recovery
        let wallet = create_test_wallet(db.clone()).await;
        let _report = wallet.recover_incomplete_sagas().await.unwrap();

        // Preserve the complete original quote; do not manufacture permission to reuse it.
        let retrieved_quote = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
        assert_eq!(retrieved_quote, quote);
    }

    async fn submitted_melt_fixture(
        status: MeltQuoteState,
    ) -> (crate::Wallet, WalletSaga, MeltQuote) {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let saga_id = uuid::Uuid::new_v4();
        let mut quote = test_melt_quote();
        quote.amount = Amount::from(100);
        quote.fee_reserve = Amount::ZERO;
        quote.mint_url = Some(mint_url.clone());
        quote.used_by_operation = Some(saga_id.to_string());
        db.add_melt_quote(quote.clone()).await.unwrap();

        let mut input = test_proof_info(test_keyset_id(), 100, mint_url.clone());
        input.state = State::Pending;
        input.used_by_operation = Some(saga_id);
        input.created_by_operation = Some(uuid::Uuid::new_v4());
        input.derivation_index = Some(42);
        db.update_proofs(vec![input.clone()], vec![]).await.unwrap();
        let metadata = HashMap::from([("label".to_owned(), "restored melt".to_owned())]);
        let mut saga = WalletSaga::new(
            saga_id,
            WalletSagaState::Melt(MeltSagaState::MeltRequested),
            quote.amount,
            mint_url.clone(),
            quote.unit.clone(),
            OperationData::Melt(MeltOperationData {
                payment: None,
                swap: None,
                acquisition: None,
                quote_id: quote.id.clone(),
                amount: quote.amount,
                fee_reserve: quote.fee_reserve,
                counter_start: None,
                counter_end: None,
                change_amount: None,
                metadata: metadata.clone(),
                final_proof_ys: Some(vec![input.y]),
                change_blinded_messages: None,
            }),
        );
        let receipt = Transaction {
            role: Default::default(),
            mint_url,
            direction: TransactionDirection::Outgoing,
            amount: quote.amount,
            fee: quote.fee_reserve,
            unit: quote.unit.clone(),
            ys: vec![input.y],
            timestamp: saga.created_at,
            memo: None,
            metadata,
            quote_id: Some(quote.id.clone()),
            payment_request: Some(quote.request.clone()),
            payment_proof: None,
            payment_method: Some(quote.payment_method.clone()),
            saga_id: Some(saga_id),
            status: TransactionStatus::Pending,
        };
        let OperationData::Melt(data) = &mut saga.data else {
            panic!("expected melt");
        };
        data.payment = Some(Box::new(MeltPayment {
            inputs: vec![input],
            receipt: receipt.clone(),
            change: None,
        }));
        db.add_transaction(receipt).await.unwrap();
        db.add_saga(saga.clone()).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.set_melt_quote_status_response(Ok(MeltQuoteBolt11Response {
            quote: quote.id.clone(),
            amount: quote.amount,
            fee_reserve: quote.fee_reserve,
            state: status,
            expiry: quote.expiry,
            payment_preimage: (status == MeltQuoteState::Paid).then(|| "preimage123".to_owned()),
            change: None,
            request: Some(quote.request.clone()),
            unit: Some(quote.unit.clone()),
            method: quote.payment_method.clone(),
        }));
        let wallet = create_test_wallet_with_mock(db, mock_client).await;
        (wallet, saga, quote)
    }

    async fn assert_submitted_melt_retained(status: MeltQuoteState) {
        let (wallet, saga, quote) = submitted_melt_fixture(status).await;
        let db = &wallet.localstore;
        let OperationData::Melt(data) = &saga.data else {
            panic!("expected melt");
        };
        let saved = data.payment.as_ref().unwrap();
        let report = wallet.recover_incomplete_sagas().await.unwrap();
        assert_eq!(report.skipped, 1);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.recovered, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(db.get_saga(&saga.id).await.unwrap(), Some(saga.clone()));
        assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(quote));
        assert_eq!(
            db.get_proofs_by_ys(saved.receipt.ys.clone()).await.unwrap(),
            saved.inputs
        );
        assert_eq!(
            db.get_transaction(saved.receipt.id()).await.unwrap(),
            Some(saved.receipt.clone())
        );
    }

    #[tokio::test]
    async fn test_recover_melt_requested_quote_failed() {
        // A submitted request can still be in flight when a negative status arrives.
        assert_submitted_melt_retained(MeltQuoteState::Failed).await;
    }

    #[tokio::test]
    async fn test_recover_melt_requested_quote_pending() {
        assert_submitted_melt_retained(MeltQuoteState::Pending).await;
    }

    #[tokio::test]
    async fn test_recover_melt_requested_quote_unpaid() {
        assert_submitted_melt_retained(MeltQuoteState::Unpaid).await;
    }

    #[tokio::test]
    async fn test_recover_melt_requested_quote_paid() {
        let (wallet, saga, mut quote) = submitted_melt_fixture(MeltQuoteState::Paid).await;
        let db = &wallet.localstore;
        let OperationData::Melt(data) = &saga.data else {
            panic!("expected melt");
        };
        let saved = data.payment.as_ref().unwrap();
        let report = wallet.recover_incomplete_sagas().await.unwrap();
        assert_eq!(report.recovered, 1);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.failed, 0);

        let mut spent = saved.inputs.clone();
        for input in &mut spent {
            input.state = State::Spent;
        }
        assert_eq!(
            db.get_proofs_by_ys(saved.receipt.ys.clone()).await.unwrap(),
            spent
        );
        assert!(db.get_saga(&saga.id).await.unwrap().is_none());
        let mut completed = saved.receipt.clone();
        completed.status = TransactionStatus::Completed;
        completed.payment_proof = Some("preimage123".to_owned());
        assert_eq!(
            db.list_transactions(None, Some(TransactionDirection::Outgoing), None)
                .await
                .unwrap(),
            vec![completed]
        );
        quote.state = MeltQuoteState::Paid;
        quote.used_by_operation = None;
        quote.payment_proof = Some("preimage123".to_owned());
        quote.version += 1;
        assert_eq!(db.get_melt_quote(&quote.id).await.unwrap(), Some(quote));
    }

    #[tokio::test]
    async fn test_recover_incomplete_sagas_filters_by_mint_and_unit() {
        // Test that recovery only processes sagas for its own mint and unit
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let other_mint_url = MintUrl::from_str("https://other-mint.example.com").unwrap();
        let saga_id_1 = uuid::Uuid::new_v4();
        let saga_id_2 = uuid::Uuid::new_v4();
        let saga_id_3 = uuid::Uuid::new_v4();

        // 1. Our incomplete checkpoint is processed and retained as failed.
        let saga_1 = WalletSaga::new(
            saga_id_1,
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            Amount::from(100),
            mint_url.clone(),
            cdk_common::nuts::CurrencyUnit::Sat,
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        db.add_saga(saga_1).await.unwrap();

        // 2. Saga for OTHER mint (should be skipped)
        let saga_2 = WalletSaga::new(
            saga_id_2,
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            Amount::from(100),
            other_mint_url.clone(),
            cdk_common::nuts::CurrencyUnit::Sat,
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        db.add_saga(saga_2).await.unwrap();

        // 3. Saga for our mint but OTHER unit (should be skipped)
        let saga_3 = WalletSaga::new(
            saga_id_3,
            WalletSagaState::Receive(ReceiveSagaState::InputsRecorded),
            Amount::from(100),
            mint_url.clone(),
            cdk_common::nuts::CurrencyUnit::Usd,
            OperationData::Receive(ReceiveOperationData { checkpoint: None }),
        );
        db.add_saga(saga_3).await.unwrap();

        // Run recovery
        let wallet = create_test_wallet(db.clone()).await;
        let report = wallet.recover_incomplete_sagas().await.unwrap();

        // Only saga_1 is processed; missing checkpoint evidence is retained.
        assert_eq!(report.failed, 1);
        assert_eq!(report.compensated, 0);
        assert_eq!(report.recovered, 0);
        assert_eq!(report.skipped, 0);

        // Verify saga_1 is retained
        assert!(db.get_saga(&saga_id_1).await.unwrap().is_some());

        // Verify saga_2 and saga_3 STILL EXIST in the DB (they were filtered out)
        assert!(db.get_saga(&saga_id_2).await.unwrap().is_some());
        assert!(db.get_saga(&saga_id_3).await.unwrap().is_some());
    }
}
