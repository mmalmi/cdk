//! Mint (Issue) Saga - Type State Pattern Implementation
//!
//! This module implements the saga pattern for mint operations using the typestate
//! pattern to enforce valid state transitions at compile-time.
//!
//! Preparation commits quote ownership with its recovery journal. Before any mint
//! request the journal advances to MintRequested. Submitted errors and missing
//! restore results retain that authority. Terminal cleanup requires the exact
//! journal, every owned quote and canonical receipts in one transaction.

#[cfg(test)]
use cdk_common::wallet::TransactionStatus;
use cdk_common::wallet::{
    IssueSagaState, MintCheckpoint, MintOperationData, MintRequestData, OperationData, WalletSaga,
    WalletSagaState,
};
use cdk_common::SecretKey;
use tracing::instrument;

use self::state::{Finalized, Initial, Prepared};
use crate::amount::SplitTarget;
#[cfg(test)]
use crate::nuts::State;
use crate::nuts::{MintRequest, PreMintSecrets, Proofs, SpendingConditions};
use crate::util::unix_time;
use crate::wallet::MintQuote;
use crate::{Amount, Error, Wallet};

mod checkpoint;
pub(crate) mod resume;
mod retirement;
pub(crate) mod state;

fn should_retry_with_legacy_quote_signature(error: &Error) -> bool {
    matches!(
        error,
        Error::SignatureMissingOrInvalid
            | Error::NUT20(crate::nuts::nut20::Error::InvalidSignature)
            | Error::NUT20(crate::nuts::nut20::Error::SignatureMissing)
    )
}

/// Saga pattern implementation for mint (issue) operations.
///
/// Uses the typestate pattern to enforce valid state transitions at compile-time.
/// Each state (Initial, Prepared, Finalized) is a distinct type, and operations
/// are only available on the appropriate type.
pub(crate) struct MintSaga<'a, S> {
    /// Wallet reference
    wallet: &'a Wallet,
    /// State-specific data
    state_data: S,
}

impl<'a> MintSaga<'a, Initial> {
    /// Create a new mint saga in the Initial state.
    pub fn new(wallet: &'a Wallet) -> Self {
        let operation_id = uuid::Uuid::now_v7();

        Self {
            wallet,
            state_data: Initial {
                operation_id,
                keyset_policy: Default::default(),
            },
        }
    }

    /// Publish the complete intent and quote ownership at one storage boundary.
    async fn acquire_quotes(
        &self,
        saga: &WalletSaga,
        quotes: &[MintQuote],
        keyset: cdk_common::Id,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<(), Error> {
        if !self.wallet.localstore.supports_atomic_issue_retirement() {
            return Err(cdk_common::database::Error::Internal(
                "Atomic issue retirement is required before acquisition".to_owned(),
            )
            .into());
        }
        if quotes.iter().any(|quote| quote.used_by_operation.is_some()) {
            return Err(cdk_common::database::Error::QuoteAlreadyInUse.into());
        }
        cdk_common::database::wallet::validate_issue_acquisition(saga, quotes)?;
        let OperationData::Mint(data) = &saga.data else {
            return Err(Error::InvalidOperationState);
        };
        let keys = self
            .wallet
            .prepared_output_keyset(
                keyset,
                counter_start,
                counter_count,
                &checkpoint::get(data)?.premint_secrets,
            )
            .await?;
        if !self
            .wallet
            .localstore
            .acquire_issue(saga, quotes, keys, counter_start, counter_count)
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        Ok(())
    }

    /// Prepare common logic for all mint types
    #[allow(clippy::too_many_arguments)]
    async fn prepare_common(
        self,
        quote_id: &str,
        quote_info: cdk_common::wallet::MintQuote,
        amount: Amount,
        amount_split_target: SplitTarget,
        spending_conditions: Option<SpendingConditions>,
        fee_and_amounts: cdk_common::amount::FeeAndAmounts,
        active_keyset_id: cdk_common::nut02::Id,
    ) -> Result<MintSaga<'a, Prepared>, Error> {
        if amount == Amount::ZERO {
            tracing::debug!("Amount mintable 0.");
            return Err(Error::AmountUndefined);
        }

        let unix_time = unix_time();
        if quote_info.expiry < unix_time && quote_info.expiry != 0 {
            tracing::warn!("Attempting to mint with expired quote.");
        }

        let split_target = match amount_split_target {
            SplitTarget::None => {
                self.wallet
                    .determine_split_target_values(amount, &fee_and_amounts)
                    .await?
            }
            s => s,
        };

        let output_count = self
            .wallet
            .check_output_count(amount.split_targeted_count(&split_target, &fee_and_amounts)?)?;

        let counter_start = self
            .wallet
            .localstore
            .increment_keyset_counter(&active_keyset_id, 0)
            .await?;
        let counter_count = if spending_conditions.is_none() {
            output_count
        } else {
            0
        };
        counter_start
            .checked_add(counter_count)
            .ok_or(Error::AmountOverflow)?;

        let premint_secrets = match &spending_conditions {
            Some(spending_conditions) => PreMintSecrets::with_conditions(
                active_keyset_id,
                amount,
                &split_target,
                spending_conditions,
                &fee_and_amounts,
            )?,
            None => PreMintSecrets::from_seed(
                active_keyset_id,
                counter_start,
                &self.wallet.seed,
                amount,
                &split_target,
                &fee_and_amounts,
            )?,
        };

        let mut request = MintRequest {
            quote: quote_id.to_string(),
            outputs: premint_secrets.blinded_messages(),
            signature: None,
        };

        let mut legacy_signature = None;
        if let Some(secret_key) = self.wallet.mint_quote_signing_key(&quote_info).await? {
            request.sign_legacy(secret_key.clone())?;
            legacy_signature = request.signature.take();
            request.sign(&secret_key)?;
        } else if quote_info.payment_method.is_bolt12() {
            // Bolt12 requires signature
            tracing::error!("Signature is required for bolt12.");
            return Err(Error::SignatureMissingOrInvalid);
        }

        // Reload the quote after signing-key resolution so the prepared saga
        // carries the latest optimistic-lock version into the post-mint
        // persistence step.
        let quote_info = self
            .wallet
            .localstore
            .get_mint_quote(quote_id)
            .await?
            .ok_or(Error::UnknownQuote)?;

        let operation_id = self.state_data.operation_id;

        #[cfg(feature = "npubcash")]
        let allow_unsigned = self.wallet.npubcash_quote_key(quote_id).await?.is_some();
        #[cfg(not(feature = "npubcash"))]
        let allow_unsigned = false;
        let mut data = MintOperationData::new_single(quote_id.to_string(), amount);
        data.checkpoint = Some(Box::new(MintCheckpoint {
            quotes: vec![quote_info.clone()],
            premint_secrets,
            payment_method: quote_info.payment_method.clone(),
            request: MintRequestData::Single {
                request,
                legacy_signature,
                allow_unsigned,
            },
        }));
        checkpoint::get(&data)?;

        // Persist saga state for crash recovery
        let saga = WalletSaga::new(
            operation_id,
            WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
            amount,
            self.wallet.mint_url.clone(),
            self.wallet.unit.clone(),
            OperationData::Mint(data),
        );

        self.acquire_quotes(
            &saga,
            std::slice::from_ref(&quote_info),
            active_keyset_id,
            counter_start,
            counter_count,
        )
        .await?;

        Ok(MintSaga {
            wallet: self.wallet,
            state_data: Prepared {
                keyset_policy: self.state_data.keyset_policy,
                saga,
            },
        })
    }

    /// Prepare the mint operation (single quote).
    ///
    /// This is the first step in the saga. It:
    /// 1. Validates the quote
    /// 2. Creates a private output proposal and signed request
    /// 3. Commits its counter range, keys, quote ownership and recovery intent
    #[instrument(skip_all)]
    pub async fn prepare(
        self,
        quote_id: &str,
        amount_split_target: SplitTarget,
        spending_conditions: Option<SpendingConditions>,
    ) -> Result<MintSaga<'a, Prepared>, Error> {
        let mut quote_info = self
            .wallet
            .localstore
            .get_mint_quote(quote_id)
            .await?
            .ok_or(Error::UnknownQuote)?;

        tracing::info!(
            "Preparing mint for quote {} with operation {} method {}",
            quote_id,
            self.state_data.operation_id,
            quote_info.payment_method
        );

        let mut amount = quote_info.amount_mintable();

        if amount == Amount::ZERO {
            self.wallet
                .inner_check_mint_quote_status(quote_info.clone())
                .await?;

            quote_info = self
                .wallet
                .localstore
                .get_mint_quote(quote_id)
                .await?
                .ok_or(Error::UnknownQuote)?;

            amount = quote_info.amount_mintable();
        }

        let keyset_policy = self.state_data.keyset_policy;
        let active_keyset_id = self
            .wallet
            .active_keyset_with_policy(keyset_policy)
            .await?
            .id;
        let fee_and_amounts = self
            .wallet
            .get_keyset_fees_and_amounts_by_id_with_policy(active_keyset_id, keyset_policy)
            .await?;

        self.prepare_common(
            quote_id,
            quote_info,
            amount,
            amount_split_target,
            spending_conditions,
            fee_and_amounts,
            active_keyset_id,
        )
        .await
    }

    /// Prepare a batch mint operation for multiple quotes.
    ///
    /// Plans each quote's outputs and signatures, then acquires every quote and
    /// its complete recovery checkpoint in one transaction.
    #[instrument(skip_all)]
    pub async fn prepare_batch(
        self,
        quote_ids: &[&str],
        amount_split_target: SplitTarget,
        spending_conditions: Option<SpendingConditions>,
        external_keys: Option<&std::collections::HashMap<String, SecretKey>>,
    ) -> Result<MintSaga<'a, Prepared>, Error> {
        use crate::nuts::BatchMintRequest;

        if quote_ids.is_empty() {
            return Err(Error::UnknownQuote);
        }

        // Check for duplicates
        let unique: std::collections::HashSet<_> = quote_ids.iter().collect();
        if unique.len() != quote_ids.len() {
            return Err(Error::DuplicateInputs);
        }

        // Load all quotes
        let mut quote_infos: Vec<MintQuote> = Vec::new();
        for quote_id in quote_ids {
            let quote = self
                .wallet
                .localstore
                .get_mint_quote(quote_id)
                .await?
                .ok_or(Error::UnknownQuote)?;
            quote_infos.push(quote);
        }

        // Validate all quotes share the same payment method and unit
        let payment_method = quote_infos[0].payment_method.clone();
        let unit = quote_infos[0].unit.clone();

        for quote in &quote_infos {
            if quote.payment_method != payment_method {
                return Err(Error::InvalidPaymentMethod);
            }
            if quote.unit != unit {
                return Err(Error::UnsupportedUnit);
            }
        }

        // Calculate total mintable amount and canonical per-quote amounts.
        // If we refresh a quote state, keep quote_infos and quote_amounts in sync.
        let mut total_amount = Amount::ZERO;
        let mut quote_amounts: Vec<Amount> = Vec::with_capacity(quote_infos.len());
        for quote in &mut quote_infos {
            let mut mintable = quote.amount_mintable();
            if mintable == Amount::ZERO {
                // Refresh quote status
                self.wallet
                    .inner_check_mint_quote_status(quote.clone())
                    .await?;

                let refreshed = self
                    .wallet
                    .localstore
                    .get_mint_quote(&quote.id)
                    .await?
                    .ok_or(Error::UnknownQuote)?;

                mintable = refreshed.amount_mintable();
                *quote = refreshed;
            }

            total_amount = total_amount
                .checked_add(mintable)
                .ok_or(Error::AmountOverflow)?;
            quote_amounts.push(mintable);
        }

        if total_amount == Amount::ZERO {
            return Err(Error::AmountUndefined);
        }

        let keyset_policy = self.state_data.keyset_policy;
        let active_keyset_id = self
            .wallet
            .active_keyset_with_policy(keyset_policy)
            .await?
            .id;
        let fee_and_amounts = self
            .wallet
            .get_keyset_fees_and_amounts_by_id_with_policy(active_keyset_id, keyset_policy)
            .await?;

        // Generate a consecutive output segment for each quote. NUT-29 sends
        // one shared output list, so retaining these boundaries is what lets
        // transaction history and crash recovery attribute proofs correctly.
        let mut output_total = 0u64;
        let mut plans = Vec::new();
        for quote_amount in &quote_amounts {
            let split_target = match &amount_split_target {
                SplitTarget::None => {
                    self.wallet
                        .determine_split_target_values(*quote_amount, &fee_and_amounts)
                        .await?
                }
                split_target => split_target.clone(),
            };
            let count = quote_amount.split_targeted_count(&split_target, &fee_and_amounts)?;
            output_total = output_total
                .checked_add(count)
                .ok_or(Error::AmountOverflow)?;
            self.wallet.check_output_count(output_total)?;
            plans.push((split_target, count));
        }
        let counter_start = self
            .wallet
            .localstore
            .increment_keyset_counter(&active_keyset_id, 0)
            .await?;
        let counter_count = if spending_conditions.is_none() {
            self.wallet.check_output_count(output_total)?
        } else {
            0
        };
        counter_start
            .checked_add(counter_count)
            .ok_or(Error::AmountOverflow)?;
        let mut next_counter = counter_start;
        let mut premint_secrets = PreMintSecrets::new(active_keyset_id);
        let mut output_counts = Vec::with_capacity(quote_amounts.len());
        for (quote_amount, (split_target, count)) in quote_amounts.iter().zip(plans) {
            let quote_secrets = match &spending_conditions {
                Some(sc) => PreMintSecrets::with_conditions(
                    active_keyset_id,
                    *quote_amount,
                    &split_target,
                    sc,
                    &fee_and_amounts,
                )?,
                None => {
                    let start = next_counter;
                    next_counter = next_counter
                        .checked_add(u32::try_from(count).map_err(|_| Error::AmountOverflow)?)
                        .ok_or(Error::AmountOverflow)?;

                    PreMintSecrets::from_seed(
                        active_keyset_id,
                        start,
                        &self.wallet.seed,
                        *quote_amount,
                        &split_target,
                        &fee_and_amounts,
                    )?
                }
            };

            output_counts.push(quote_secrets.len());
            premint_secrets.secrets.extend(quote_secrets.secrets);
        }

        let outputs = premint_secrets.blinded_messages();

        // Create batch mint request
        let mut batch_request = BatchMintRequest {
            quotes: quote_ids.iter().map(|s| s.to_string()).collect(),
            quote_amounts: Some(quote_amounts.clone()),
            outputs: outputs.clone(),
            signatures: None,
        };

        // Build signatures for each quote (NUT-20)
        let mut signatures: Vec<Option<String>> = Vec::new();
        let mut legacy_signatures = Vec::new();

        for quote in &quote_infos {
            let secret_key = match self.wallet.mint_quote_signing_key(quote).await? {
                Some(secret_key) => Some(secret_key),
                None => external_keys.and_then(|keys| keys.get(&quote.id)).cloned(),
            };

            let requires_signature = secret_key.is_some() || quote.payment_method.is_bolt12();

            if requires_signature {
                let sk = secret_key.ok_or(Error::SignatureMissingOrInvalid)?;
                let sig = batch_request
                    .sign_quote(&quote.id, &sk)
                    .map_err(|e| Error::Custom(format!("NUT-20 signing failed: {}", e)))?;
                legacy_signatures.push(Some(
                    batch_request
                        .sign_quote_legacy(&quote.id, &sk)
                        .map_err(|e| {
                            Error::Custom(format!("NUT-20 legacy signing failed: {}", e))
                        })?,
                ));
                signatures.push(Some(sig));
            } else {
                // Quote is unlocked
                signatures.push(None);
                legacy_signatures.push(None);
            }
        }

        // Refresh every snapshot after signing-key resolution so later
        // versioned writes use the latest persisted versions.
        for quote in &mut quote_infos {
            *quote = self
                .wallet
                .localstore
                .get_mint_quote(&quote.id)
                .await?
                .ok_or(Error::UnknownQuote)?;
        }

        // Check if any quote requires a signature.
        let has_locked = signatures.iter().any(Option::is_some);
        let signatures_to_send = if has_locked { Some(signatures) } else { None };
        batch_request.signatures = signatures_to_send;

        let mut data = MintOperationData::new_partitioned_batch(
            quote_ids.iter().map(|s| s.to_string()).collect(),
            total_amount,
            output_counts,
            quote_amounts,
        );
        data.checkpoint = Some(Box::new(MintCheckpoint {
            quotes: quote_infos.clone(),
            premint_secrets,
            payment_method,
            request: MintRequestData::Batch {
                request: batch_request,
                legacy_signatures: has_locked.then_some(legacy_signatures),
            },
        }));
        checkpoint::get(&data)?;

        // Persist saga state
        let saga = WalletSaga::new(
            self.state_data.operation_id,
            WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
            total_amount,
            self.wallet.mint_url.clone(),
            self.wallet.unit.clone(),
            OperationData::Mint(data),
        );

        self.acquire_quotes(
            &saga,
            &quote_infos,
            active_keyset_id,
            counter_start,
            counter_count,
        )
        .await?;

        Ok(MintSaga {
            wallet: self.wallet,
            state_data: Prepared {
                keyset_policy,
                saga,
            },
        })
    }
}

impl<'a> MintSaga<'a, Prepared> {
    /// Submit the original request and atomically publish its complete financial result.
    /// Errors retain the original journal unless the entire completion committed.
    #[instrument(skip_all)]
    pub async fn execute(self) -> Result<MintSaga<'a, Finalized>, Error> {
        let MintSaga { wallet, state_data } = self;
        let original = state_data.checkpoint()?;
        let mut submitted = state_data.saga.clone();
        submitted.update_state(WalletSagaState::Issue(IssueSagaState::MintRequested));
        if !wallet.localstore.update_saga(submitted.clone()).await? {
            return Err(Error::ConcurrentUpdate);
        }
        let response = checkpoint::post(wallet, original).await?;
        let proofs =
            checkpoint::proofs(wallet, original, state_data.keyset_policy, response).await?;
        let outputs = checkpoint::proof_infos(wallet, original, proofs.clone(), submitted.id)?;
        retirement::retire(wallet, &submitted, Some(outputs)).await?;
        Ok(MintSaga {
            wallet,
            state_data: Finalized { proofs },
        })
    }
}

impl<'a> MintSaga<'a, Finalized> {
    /// Consume the saga and return the minted proofs
    pub fn into_proofs(self) -> Proofs {
        self.state_data.proofs
    }
}

impl<S: std::fmt::Debug> std::fmt::Debug for MintSaga<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintSaga")
            .field("state_data", &self.state_data)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use bitcoin::secp256k1::schnorr::Signature;
    use cdk_common::nuts::MintQuoteState;

    use super::*;
    use crate::nuts::{BlindSignature, BlindedMessage, MintResponse};
    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, test_mint_quote, test_mint_url,
        MockMintConnector,
    };

    fn legacy_mint_quote_msg_to_sign(quote_id: &str, outputs: &[BlindedMessage]) -> Vec<u8> {
        let capacity = quote_id.len() + (outputs.len() * 66);
        let mut msg = Vec::with_capacity(capacity);

        msg.extend_from_slice(quote_id.as_bytes());
        for output in outputs {
            msg.extend_from_slice(output.blinded_secret.to_hex().as_bytes());
        }

        msg
    }

    fn parse_signature(signature: &Option<String>) -> Signature {
        Signature::from_str(signature.as_ref().expect("signature is present"))
            .expect("valid schnorr signature")
    }

    pub(super) fn paid_signed_mint_quote(
        mint_url: cdk_common::mint_url::MintUrl,
        amount: Amount,
        signing_key: SecretKey,
    ) -> MintQuote {
        let mut mint_quote = test_mint_quote(mint_url);
        mint_quote.state = MintQuoteState::Paid;
        mint_quote.amount = Some(amount);
        mint_quote.amount_paid = amount;
        mint_quote.secret_key = Some(signing_key);
        mint_quote
    }

    #[cfg(feature = "npubcash")]
    fn seed_prefix_signing_key(wallet: &Wallet) -> SecretKey {
        SecretKey::from_slice(&wallet.seed[..32]).expect("wallet seed prefix is a valid key")
    }

    #[cfg(feature = "npubcash")]
    #[tokio::test]
    async fn seed_prefix_preparation_refreshes_persisted_version() {
        for batch in [false, true] {
            let db = create_test_db().await;
            let mint = Arc::new(MockMintConnector::new());
            mint.enable_mint_signing();
            let wallet = create_test_wallet_with_mock(db.clone(), mint).await;
            let quote = paid_signed_mint_quote(
                test_mint_url(),
                64.into(),
                seed_prefix_signing_key(&wallet),
            );
            db.add_mint_quote(quote.clone()).await.unwrap();
            let split = SplitTarget::Values(vec![64.into()]);
            let prepared = if batch {
                MintSaga::new(&wallet)
                    .prepare_batch(&[quote.id.as_str()], split, None, None)
                    .await
            } else {
                MintSaga::new(&wallet).prepare(&quote.id, split, None).await
            }
            .unwrap();
            let mut reserved = prepared.state_data.checkpoint().unwrap().quotes[0].clone();
            reserved.version += 1;
            reserved.used_by_operation = Some(prepared.state_data.saga.id.to_string());
            assert_eq!(db.get_mint_quote(&quote.id).await.unwrap(), Some(reserved));
            let proofs = prepared.execute().await.unwrap().into_proofs();
            assert_eq!(
                Amount::try_sum(proofs.iter().map(|p| p.amount)).unwrap(),
                64.into()
            );
            let completed = db.get_mint_quote(&quote.id).await.unwrap().unwrap();
            assert_eq!(completed.amount_issued, 64.into());
            assert_eq!(completed.state, MintQuoteState::Issued);
            assert!(completed.used_by_operation.is_none());
        }
    }

    #[tokio::test]
    async fn test_execute_retries_single_mint_with_legacy_quote_signature() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;

        let signing_key =
            SecretKey::from_hex("50d7fd7aa2b2fe4607f41f4ce6f8794fc184dd47b8cdfbe4b3d1249aa02d35aa")
                .expect("valid signing key");
        let mint_quote = paid_signed_mint_quote(mint_url, Amount::from(64), signing_key.clone());
        let quote_id = mint_quote.id.clone();
        db.add_mint_quote(mint_quote).await.expect("add mint quote");

        let prepared = MintSaga::new(&wallet)
            .prepare(&quote_id, SplitTarget::Values(vec![Amount::from(64)]), None)
            .await
            .expect("prepare mint saga");

        mock_client.push_post_mint_response(Err(Error::SignatureMissingOrInvalid));
        mock_client.push_post_mint_response(Err(Error::Custom(
            "legacy retry should not replace original error".to_string(),
        )));

        let result = prepared.execute().await;

        assert!(matches!(result, Err(Error::SignatureMissingOrInvalid)));

        let requests = mock_client.post_mint_requests();
        assert_eq!(requests.len(), 2);

        let first_request = &requests[0].1;
        let legacy_request = &requests[1].1;

        let pubkey = signing_key.public_key();
        let new_signature = parse_signature(&first_request.signature);
        let legacy_signature = parse_signature(&legacy_request.signature);
        let legacy_msg =
            legacy_mint_quote_msg_to_sign(&legacy_request.quote, &legacy_request.outputs);

        assert!(pubkey
            .verify(&first_request.msg_to_sign(), &new_signature)
            .is_ok());
        assert!(pubkey.verify(&legacy_msg, &legacy_signature).is_ok());
        assert!(pubkey.verify(&legacy_msg, &new_signature).is_err());
        assert!(pubkey
            .verify(&legacy_request.msg_to_sign(), &legacy_signature)
            .is_err());
        assert_ne!(first_request.signature, legacy_request.signature);
        assert_eq!(first_request.outputs, legacy_request.outputs);
    }

    #[tokio::test]
    async fn test_execute_retries_batch_mint_with_legacy_quote_signature() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;

        let signing_key =
            SecretKey::from_hex("50d7fd7aa2b2fe4607f41f4ce6f8794fc184dd47b8cdfbe4b3d1249aa02d35aa")
                .expect("valid signing key");
        let mint_quote = paid_signed_mint_quote(mint_url, Amount::from(64), signing_key.clone());
        let quote_id = mint_quote.id.clone();
        db.add_mint_quote(mint_quote).await.expect("add mint quote");

        let prepared = MintSaga::new(&wallet)
            .prepare_batch(
                &[quote_id.as_str()],
                SplitTarget::Values(vec![Amount::from(64)]),
                None,
                None,
            )
            .await
            .expect("prepare batch mint saga");

        mock_client.push_post_batch_mint_response(Err(Error::SignatureMissingOrInvalid));
        mock_client.push_post_batch_mint_response(Err(Error::Custom(
            "legacy retry should not replace original error".to_string(),
        )));

        let result = prepared.execute().await;

        assert!(matches!(result, Err(Error::SignatureMissingOrInvalid)));

        let requests = mock_client.post_batch_mint_requests();
        assert_eq!(requests.len(), 2);

        let first_request = &requests[0].1;
        let legacy_request = &requests[1].1;
        let quote = first_request
            .quotes
            .first()
            .expect("batch request has quote");

        let new_signature = first_request
            .signatures
            .as_ref()
            .and_then(|signatures| signatures.first())
            .and_then(Option::as_ref)
            .expect("new signature is present");
        let legacy_signature = legacy_request
            .signatures
            .as_ref()
            .and_then(|signatures| signatures.first())
            .and_then(Option::as_ref)
            .expect("legacy signature is present");
        let new_signature =
            Signature::from_str(new_signature).expect("valid new schnorr signature");
        let legacy_signature =
            Signature::from_str(legacy_signature).expect("valid legacy schnorr signature");
        let legacy_msg = legacy_mint_quote_msg_to_sign(quote, &legacy_request.outputs);
        let pubkey = signing_key.public_key();

        assert!(pubkey
            .verify(&first_request.msg_to_sign(quote), &new_signature)
            .is_ok());
        assert!(pubkey.verify(&legacy_msg, &legacy_signature).is_ok());
        assert!(pubkey.verify(&legacy_msg, &new_signature).is_err());
        assert!(pubkey
            .verify(&legacy_request.msg_to_sign(quote), &legacy_signature)
            .is_err());
        assert_ne!(new_signature, legacy_signature);
        assert_eq!(first_request.outputs, legacy_request.outputs);
    }

    #[tokio::test]
    async fn test_execute_rejects_signature_with_mismatched_amount() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;

        let mut mint_quote = test_mint_quote(mint_url);
        mint_quote.state = MintQuoteState::Paid;
        mint_quote.amount = Some(Amount::from(64));
        mint_quote.amount_paid = Amount::from(64);
        let quote_id = mint_quote.id.clone();
        db.add_mint_quote(mint_quote).await.expect("add mint quote");

        let prepared = MintSaga::new(&wallet)
            .prepare(&quote_id, SplitTarget::Values(vec![Amount::from(64)]), None)
            .await
            .expect("prepare mint saga");

        let outputs = prepared.state_data.checkpoint().unwrap().request.outputs();

        let bad_signatures = outputs
            .iter()
            .map(|blinded_message| BlindSignature {
                amount: Amount::from(1),
                keyset_id: blinded_message.keyset_id,
                c: blinded_message.blinded_secret,
                dleq: None,
            })
            .collect();

        mock_client.set_post_mint_response(Ok(MintResponse {
            signatures: bad_signatures,
        }));

        let result = prepared.execute().await;

        assert!(matches!(result, Err(Error::InvalidMintResponse(_))));
    }

    #[cfg(feature = "npubcash")]
    #[tokio::test]
    async fn test_execute_retries_npubcash_marked_quote_unsigned_after_signature_rejection() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;

        let signing_key =
            SecretKey::from_hex("50d7fd7aa2b2fe4607f41f4ce6f8794fc184dd47b8cdfbe4b3d1249aa02d35aa")
                .expect("valid signing key");
        let mint_quote = paid_signed_mint_quote(mint_url, Amount::from(64), signing_key);
        let quote_id = mint_quote.id.clone();
        db.add_mint_quote(mint_quote).await.expect("add mint quote");

        // Mark as an npub.cash quote (NIP-06 provenance marker).
        db.kv_write("npubcash", "quotes", &quote_id, b"nip06")
            .await
            .expect("provenance marker write");

        let prepared = MintSaga::new(&wallet)
            .prepare(&quote_id, SplitTarget::Values(vec![Amount::from(64)]), None)
            .await
            .expect("prepare mint saga");

        mock_client.push_post_mint_response(Err(Error::SignatureMissingOrInvalid));
        mock_client.push_post_mint_response(Err(Error::SignatureMissingOrInvalid));
        mock_client.push_post_mint_response(Err(Error::Custom(
            "unsigned retry exercised; stop here".to_string(),
        )));

        let result = prepared.execute().await;

        // The original signature error is preserved, not the fallback's.
        assert!(matches!(result, Err(Error::SignatureMissingOrInvalid)));

        let requests = mock_client.post_mint_requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].1.signature.is_some(), "first attempt is signed");
        assert!(requests[1].1.signature.is_some(), "legacy retry is signed");
        assert!(
            requests[2].1.signature.is_none(),
            "npub.cash fallback goes out unsigned"
        );
        assert_eq!(requests[0].1.outputs, requests[2].1.outputs);
    }
}

#[cfg(test)]
mod acquisition_tests;
#[cfg(test)]
mod output_budget_tests;
#[cfg(test)]
mod retirement_tests;

#[cfg(test)]
mod checkpoint_tests;

#[cfg(test)]
mod publication_tests;
