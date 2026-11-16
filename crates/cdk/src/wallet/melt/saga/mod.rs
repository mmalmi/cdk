//! Persistent payment custody with typestate transitions for live callers.
//!
//! Prepared payments may be cancelled before submission. After the request and
//! Pending receipt are saved, non-paid replies retain custody because a payment
//! attempt may still be running. Recovery never resubmits the payment.
//!
//! Verified change and the final receipt are saved in `PaidReady` before spending
//! inputs or importing change. The quote, receipt and `Completed` state then commit
//! together. Retrying either saved outcome requires no mint access; `Completed`
//! only needs journal cleanup.

use std::collections::HashMap;

use cdk_common::wallet::{
    KeysetLoadPolicy, MeltPayment, MeltQuote, MeltSagaState, OperationData, Transaction,
    TransactionDirection, TransactionStatus, WalletSaga, WalletSagaState,
};
use cdk_common::{MeltQuoteState, PaymentMethod};
use tracing::instrument;
use uuid::Uuid;

use self::state::{Finalized, Initial, MeltRequested, PaymentPending, Prepared};
use super::MeltConfirmOptions;
use crate::nuts::nut00::{KnownMethod, ProofsMethods};
use crate::nuts::{MeltRequest, PreMintSecrets, Proofs};
use crate::{Amount, Error, Wallet};

mod acquisition;
#[cfg(test)]
mod acquisition_tests;
mod admission;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod change_fee_tests;
mod completion;
#[cfg(test)]
mod completion_boundary_tests;
mod custody;
#[cfg(test)]
mod ownership_tests;
#[cfg(test)]
mod paid_change_audit;
#[cfg(test)]
mod paid_change_fixture;
#[cfg(test)]
mod paid_change_race_tests;
#[cfg(test)]
mod quote_change_tests;
pub(crate) mod resume;
pub(crate) mod state;
#[cfg(test)]
mod submitted_tests;
pub(super) mod swap;
#[cfg(test)]
mod swap_admission_tests;
mod swap_custody;
#[cfg(test)]
mod swap_fixture;
#[cfg(test)]
mod swap_tests;

/// Result of an async melt execution
pub enum MeltSagaResult<'a> {
    /// Melt finalized (paid)
    Finalized(MeltSaga<'a, Finalized>),
    /// Melt pending
    Pending(Box<MeltSaga<'a, PaymentPending>>),
}

/// Saga pattern implementation for melt operations.
///
/// Uses the typestate pattern to enforce valid state transitions at compile-time.
/// Each state (Initial, Prepared, Confirmed) is a distinct type, and operations
/// are only available on the appropriate type.
pub(crate) struct MeltSaga<'a, S> {
    /// Wallet reference
    pub(crate) wallet: &'a Wallet,
    /// State-specific data
    pub(crate) state_data: S,
}

/// Shared helper function to perform the actual melt finalization.
/// Used by `execute_async` and `PaymentPending::finalize`.
#[allow(clippy::too_many_arguments)]
async fn finalize_melt_common<'a>(
    wallet: &'a Wallet,
    operation_id: Uuid,
    quote_info: &MeltQuote,
    final_proofs: &Proofs,
    premint_secrets: &PreMintSecrets,
    state: MeltQuoteState,
    payment_proof: Option<String>,
    change: Option<Vec<crate::nuts::BlindSignature>>,
    metadata: HashMap<String, String>,
    keyset_policy: KeysetLoadPolicy,
) -> Result<MeltSaga<'a, Finalized>, Error> {
    let saga = wallet
        .localstore
        .get_saga(&operation_id)
        .await?
        .ok_or(Error::ConcurrentUpdate)?;
    let data = custody::data(wallet, &saga)?;
    if state != MeltQuoteState::Paid
        || data.quote_id != quote_info.id
        || data.amount != quote_info.amount
        || data.fee_reserve != quote_info.fee_reserve
        || quote_info.unit != wallet.unit
        || data.metadata != metadata
        || data.change_blinded_messages.as_deref().unwrap_or_default()
            != premint_secrets.blinded_messages()
    {
        return Err(Error::InvalidOperationState);
    }
    let inputs = custody::inputs(wallet, &saga).await?;
    if inputs.len() != final_proofs.len()
        || inputs
            .iter()
            .zip(final_proofs)
            .any(|(stored, proof)| &stored.proof != proof)
    {
        return Err(Error::ConcurrentUpdate);
    }
    let result =
        completion::record_paid(wallet, &saga, payment_proof, change, keyset_policy).await?;
    Ok(MeltSaga {
        wallet,
        state_data: Finalized {
            quote_id: result.quote_id().to_owned(),
            state: result.state(),
            amount: result.amount(),
            fee: result.fee_paid(),
            payment_proof: result.payment_proof().map(str::to_owned),
            change: result.into_change(),
        },
    })
}

impl<'a> MeltSaga<'a, Initial> {
    /// Create a new melt saga in the Initial state.
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

    /// Prepare the melt operation by selecting and reserving proofs.
    ///
    /// Loads the quote, selects and reserves proofs for the required amount.
    ///
    /// The complete quote and proof reservations commit with the recovery journal.
    #[instrument(skip_all)]
    pub async fn prepare(
        self,
        quote_id: &str,
        metadata: HashMap<String, String>,
    ) -> Result<MeltSaga<'a, Prepared>, Error> {
        tracing::info!(
            "Preparing melt for quote {} with operation {}",
            quote_id,
            self.state_data.operation_id
        );

        let quote_info = acquisition::quote(self.wallet, quote_id).await?;

        let inputs_needed_amount = quote_info
            .amount
            .checked_add(quote_info.fee_reserve)
            .ok_or(Error::AmountOverflow)?;

        let keyset_policy = self.state_data.keyset_policy;

        let active_keyset_ids = self
            .wallet
            .keysets(keyset_policy)
            .await?
            .into_iter()
            .filter(|k| k.active.unwrap_or(false))
            .map(|k| k.id)
            .collect();
        let keyset_fees_and_amounts = self
            .wallet
            .get_keyset_fees_and_amounts_with_policy(keyset_policy)
            .await?;

        let inventory = self.wallet.unspent_inventory().await?;
        let derivation_indices = Wallet::proof_derivation_indices(&inventory);
        let available_proofs: Proofs = inventory.into_iter().map(|row| row.proof).collect();

        let exact_input_proofs = Wallet::select_proofs_with_derivation_indices(
            inputs_needed_amount,
            available_proofs.clone(),
            &active_keyset_ids,
            &keyset_fees_and_amounts,
            true,
            &derivation_indices,
        )?;
        let proofs_total = exact_input_proofs.total_amount()?;

        if proofs_total == inputs_needed_amount {
            let input_fee = self.wallet.get_proofs_fee(&exact_input_proofs).await?.total;
            let (quote_info, saga) = acquisition::acquire(
                self.wallet,
                self.state_data.operation_id,
                &quote_info,
                &exact_input_proofs,
                false,
                metadata,
            )
            .await?;

            return Ok(MeltSaga {
                wallet: self.wallet,
                state_data: Prepared {
                    operation_id: self.state_data.operation_id,
                    quote: quote_info,
                    proofs: exact_input_proofs,
                    proofs_to_swap: Proofs::new(),
                    swap_fee: Amount::ZERO,
                    input_fee,
                    input_fee_without_swap: input_fee,
                    keyset_policy,
                    saga,
                },
            });
        }

        let active_keyset_id = self
            .wallet
            .active_keyset_with_policy(keyset_policy)
            .await?
            .id;
        let fee_and_amounts = self
            .wallet
            .get_keyset_fees_and_amounts_by_id_with_policy(active_keyset_id, keyset_policy)
            .await?;

        let estimated_output_count = inputs_needed_amount.split(&fee_and_amounts)?.len();
        let estimated_melt_fee = self
            .wallet
            .get_keyset_count_fee_with_policy(
                &active_keyset_id,
                estimated_output_count as u64,
                keyset_policy,
            )
            .await?;

        let selection_amount = inputs_needed_amount
            .checked_add(estimated_melt_fee)
            .ok_or(Error::AmountOverflow)?;

        let input_proofs = Wallet::select_proofs_with_derivation_indices(
            selection_amount,
            available_proofs,
            &active_keyset_ids,
            &keyset_fees_and_amounts,
            true,
            &derivation_indices,
        )?;

        let input_fee = estimated_melt_fee;

        let proofs_to_send = Proofs::new();
        let proofs_to_swap = input_proofs;
        let swap_fee = self.wallet.get_proofs_fee(&proofs_to_swap).await?.total;

        let (quote_info, saga) = acquisition::acquire(
            self.wallet,
            self.state_data.operation_id,
            &quote_info,
            &proofs_to_swap,
            false,
            metadata,
        )
        .await?;

        let input_fee_without_swap = swap_fee;

        Ok(MeltSaga {
            wallet: self.wallet,
            state_data: Prepared {
                operation_id: self.state_data.operation_id,
                quote: quote_info,
                proofs: proofs_to_send,
                proofs_to_swap,
                swap_fee,
                input_fee,
                input_fee_without_swap,
                keyset_policy,
                saga,
            },
        })
    }

    /// Prepare the melt operation with specific proofs (no automatic selection).
    ///
    /// Uses the provided proofs directly without automatic proof selection.
    /// The caller must ensure the proofs cover the quote amount plus fee reserve.
    ///
    /// Known proofs retain their original metadata; external proofs are acquired
    /// without inventing an origin or derivation index.
    #[instrument(skip_all)]
    pub async fn prepare_with_proofs(
        self,
        quote_id: &str,
        proofs: Proofs,
        metadata: HashMap<String, String>,
    ) -> Result<MeltSaga<'a, Prepared>, Error> {
        tracing::info!(
            "Preparing melt with specific proofs for quote {} with operation {}",
            quote_id,
            self.state_data.operation_id
        );

        let quote_info = acquisition::quote(self.wallet, quote_id).await?;

        let input_fee = self.wallet.get_proofs_fee(&proofs).await?.total;
        if proofs.total_amount()? < acquisition::required(&quote_info, input_fee)? {
            return Err(Error::InsufficientFunds);
        }
        let (quote_info, saga) = acquisition::acquire(
            self.wallet,
            self.state_data.operation_id,
            &quote_info,
            &proofs,
            true,
            metadata,
        )
        .await?;

        Ok(MeltSaga {
            wallet: self.wallet,
            state_data: Prepared {
                operation_id: self.state_data.operation_id,
                quote: quote_info,
                proofs,
                proofs_to_swap: Proofs::new(),
                swap_fee: Amount::ZERO,
                input_fee,
                input_fee_without_swap: input_fee,
                keyset_policy: self.state_data.keyset_policy,
                saga,
            },
        })
    }
}

impl<'a> MeltSaga<'a, Prepared> {
    /// Create a new melt saga directly in the Prepared state.
    ///
    /// This constructor is used by `confirm_prepared_melt` to reconstruct
    /// a saga from stored state when confirming an already-prepared melt.
    ///
    /// Note: This bypasses the normal `prepare()` flow and assumes the caller
    /// has already properly reserved the proofs.
    #[allow(clippy::too_many_arguments)]
    pub fn from_prepared(
        wallet: &'a Wallet,
        operation_id: uuid::Uuid,
        quote: MeltQuote,
        proofs: Proofs,
        proofs_to_swap: Proofs,
        input_fee: Amount,
        input_fee_without_swap: Amount,
        saga: WalletSaga,
    ) -> Self {
        Self {
            wallet,
            state_data: Prepared {
                operation_id,
                quote,
                proofs,
                proofs_to_swap,
                swap_fee: Amount::ZERO,
                input_fee,
                input_fee_without_swap,
                keyset_policy: Default::default(),
                saga,
            },
        }
    }

    /// Get the operation ID
    pub fn operation_id(&self) -> uuid::Uuid {
        self.state_data.operation_id
    }

    /// Get the quote
    pub fn quote(&self) -> &MeltQuote {
        &self.state_data.quote
    }

    /// Get the proofs that will be used
    pub fn proofs(&self) -> &Proofs {
        &self.state_data.proofs
    }

    /// Get the proofs that need to be swapped
    pub fn proofs_to_swap(&self) -> &Proofs {
        &self.state_data.proofs_to_swap
    }

    /// Get the swap fee
    pub fn swap_fee(&self) -> Amount {
        self.state_data.swap_fee
    }

    /// Get the input fee
    pub fn input_fee(&self) -> Amount {
        self.state_data.input_fee
    }

    /// Get the input fee if swap is skipped
    pub fn input_fee_without_swap(&self) -> Amount {
        self.state_data.input_fee_without_swap
    }

    /// Build the melt request with options and transition to MeltRequested state.
    ///
    /// Performs swap if needed, sets proofs to Pending, creates pre-mint secrets for change.
    ///
    /// # Options
    ///
    /// - `skip_swap`: If true, skips the pre-melt swap and sends proofs directly.
    ///
    /// # Compensation
    ///
    /// Recovery retains a submitted preparatory swap and releases only its verified
    /// replacement coins. The originals remain spent.
    #[instrument(skip_all)]
    pub async fn request_melt_with_options(
        mut self,
        options: MeltConfirmOptions,
    ) -> Result<MeltSaga<'a, MeltRequested>, Error> {
        let operation_id = self.state_data.operation_id;
        let mut original_proofs = self.state_data.proofs.clone();
        original_proofs.extend(self.state_data.proofs_to_swap.clone());
        admission::validate_inputs(self.wallet, &self.state_data.saga, &original_proofs)?;
        admission::no_extra_inputs(self.wallet, &self.state_data.saga).await?;
        let quote_info =
            admission::quote(self.wallet, &self.state_data.saga, &self.state_data.quote).await?;
        if quote_info.payment_method == PaymentMethod::Known(KnownMethod::Onchain)
            && quote_info.fee_index.is_none()
        {
            return Err(Error::InvalidPaymentRequest);
        }
        let input_fee = self.state_data.input_fee;

        tracing::info!(
            "Building melt request for quote {} with operation {} (skip_swap: {})",
            quote_info.id,
            operation_id,
            options.skip_swap
        );

        let keyset_policy = self.state_data.keyset_policy;
        let active_keyset_id = self
            .wallet
            .active_keyset_with_policy(keyset_policy)
            .await?
            .id;
        let mut final_proofs = self.state_data.proofs.clone();

        // Handle proofs_to_swap based on skip_swap option
        if !self.state_data.proofs_to_swap.is_empty() {
            if options.skip_swap {
                // Skip swap: use proofs_to_swap directly
                // The mint will return change from the melt
                tracing::debug!(
                    "Skipping swap, using {} proofs directly (total: {})",
                    self.state_data.proofs_to_swap.len(),
                    self.state_data.proofs_to_swap.total_amount()?,
                );
                final_proofs.extend(self.state_data.proofs_to_swap.clone());
            } else {
                // Current behavior: swap first to get optimal denominations
                let target_swap_amount = quote_info
                    .amount
                    .checked_add(quote_info.fee_reserve)
                    .ok_or(Error::AmountOverflow)?
                    .checked_add(input_fee)
                    .ok_or(Error::AmountOverflow)?;

                tracing::debug!(
                    "Swapping {} proofs (total: {}) for target amount {}",
                    self.state_data.proofs_to_swap.len(),
                    self.state_data.proofs_to_swap.total_amount()?,
                    target_swap_amount
                );

                if !final_proofs.is_empty() {
                    return Err(Error::InvalidOperationState);
                }
                final_proofs = swap::start(
                    self.wallet,
                    &mut self.state_data.saga,
                    self.state_data.proofs_to_swap.clone(),
                    target_swap_amount,
                    active_keyset_id,
                    keyset_policy,
                )
                .await?;
            }
        }

        // Recalculate the actual input_fee based on final_proofs
        let actual_input_fee = self.wallet.get_proofs_fee(&final_proofs).await?.total;
        let inputs_needed_amount = quote_info
            .amount
            .checked_add(quote_info.fee_reserve)
            .ok_or(Error::AmountOverflow)?
            .checked_add(actual_input_fee)
            .ok_or(Error::AmountOverflow)?;

        let proofs_total = final_proofs.total_amount()?;
        if proofs_total < inputs_needed_amount {
            // Roll back the exact unsubmitted melt authority.
            self.compensate().await?;
            return Err(Error::InsufficientFunds);
        }

        let payment_inputs = if matches!(&self.state_data.saga.data, OperationData::Melt(data) if data.swap.is_some())
        {
            swap_custody::pending(self.wallet, &self.state_data.saga, &final_proofs).await?
        } else {
            admission::pending(self.wallet, &self.state_data.saga, &final_proofs).await?
        };

        // Calculate change accounting for input fees
        let change_amount = proofs_total - quote_info.amount - actual_input_fee;

        let (premint_secrets, counter_start, counter_end) = if change_amount <= Amount::ZERO {
            (PreMintSecrets::new(active_keyset_id), None, None)
        } else {
            let num_secrets =
                ((u64::from(change_amount) as f64).log2().ceil() as u64).max(1) as u32;

            let count = self
                .wallet
                .localstore
                .increment_keyset_counter(&active_keyset_id, 0)
                .await?;
            let new_counter = count
                .checked_add(num_secrets)
                .ok_or(Error::AmountOverflow)?;

            (
                PreMintSecrets::from_seed_blank(
                    active_keyset_id,
                    count,
                    &self.wallet.seed,
                    change_amount,
                )?,
                Some(count),
                Some(new_counter),
            )
        };

        let change_blinded_messages = if change_amount > Amount::ZERO {
            Some(premint_secrets.blinded_messages())
        } else {
            None
        };

        let final_proof_ys = final_proofs.ys()?;

        // Update saga state to MeltRequested BEFORE making the melt call
        let mut saga = self.state_data.saga.clone();
        saga.update_state(WalletSagaState::Melt(MeltSagaState::MeltRequested));
        if let OperationData::Melt(ref mut data) = saga.data {
            data.counter_start = counter_start;
            data.counter_end = counter_end;
            data.change_amount = if change_amount > Amount::ZERO {
                Some(change_amount)
            } else {
                None
            };
            data.final_proof_ys = Some(final_proof_ys);
            data.change_blinded_messages = change_blinded_messages.clone();
        }

        let metadata = match &saga.data {
            OperationData::Melt(data) => data.metadata.clone(),
            _ => HashMap::new(),
        };

        let receipt = Transaction {
            role: Default::default(),
            mint_url: self.wallet.mint_url.clone(),
            direction: TransactionDirection::Outgoing,
            amount: quote_info.amount,
            fee: quote_info.fee_reserve,
            unit: self.wallet.unit.clone(),
            ys: final_proofs.ys()?,
            timestamp: saga.created_at,
            memo: None,
            metadata,
            quote_id: Some(quote_info.id.clone()),
            payment_request: Some(quote_info.request.clone()),
            payment_proof: None,
            payment_method: Some(quote_info.payment_method.clone()),
            saga_id: Some(operation_id),
            status: TransactionStatus::Pending,
        };
        if let OperationData::Melt(ref mut data) = saga.data {
            data.payment = Some(Box::new(MeltPayment {
                inputs: payment_inputs,
                receipt: receipt.clone(),
                change: None,
            }));
        }
        let committed = match (counter_start, counter_end) {
            (Some(start), Some(end)) => {
                let keyset = self
                    .wallet
                    .keyset_with_policy(active_keyset_id, KeysetLoadPolicy::CacheOnly)
                    .await?;
                self.wallet
                    .localstore
                    .prepare_saga_outputs(
                        Some(&self.state_data.saga),
                        saga,
                        keyset,
                        start,
                        end - start,
                        Some(receipt),
                    )
                    .await?
            }
            (None, None) => {
                self.wallet
                    .localstore
                    .complete_saga(&self.state_data.saga, saga, receipt)
                    .await?
            }
            _ => return Err(Error::InvalidOperationState),
        };
        if !committed {
            return Err(Error::ConcurrentUpdate);
        }

        Ok(MeltSaga {
            wallet: self.wallet,
            state_data: MeltRequested {
                operation_id,
                quote: quote_info,
                final_proofs,
                premint_secrets,
            },
        })
    }

    /// Cancel using the current parent journal, including any submitted swap.
    async fn compensate(self) -> Result<(), Error> {
        let mut saga = self
            .wallet
            .localstore
            .get_saga(&self.state_data.operation_id)
            .await?
            .ok_or(Error::ConcurrentUpdate)?;
        swap::cancel(self.wallet, &mut saga).await
    }

    /// Cancel the prepared melt and release reserved proofs.
    pub async fn cancel(self) -> Result<(), Error> {
        self.compensate().await?;
        Ok(())
    }
}

impl std::fmt::Debug for MeltSaga<'_, Prepared> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeltSaga<Prepared>")
            .field("operation_id", &self.state_data.operation_id)
            .field("quote_id", &self.state_data.quote.id)
            .field("amount", &self.state_data.quote.amount)
            .field(
                "proofs",
                &self
                    .state_data
                    .proofs
                    .iter()
                    .map(|p| p.amount)
                    .collect::<Vec<_>>(),
            )
            .field(
                "proofs_to_swap",
                &self
                    .state_data
                    .proofs_to_swap
                    .iter()
                    .map(|p| p.amount)
                    .collect::<Vec<_>>(),
            )
            .field("swap_fee", &self.state_data.swap_fee)
            .field("input_fee", &self.state_data.input_fee)
            .finish()
    }
}

impl std::fmt::Debug for MeltSaga<'_, PaymentPending> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeltSaga<PaymentPending>")
            .field("operation_id", &self.state_data.operation_id)
            .field("quote_id", &self.state_data.quote.id)
            .field("amount", &self.state_data.quote.amount)
            .finish()
    }
}

impl<'a> MeltSaga<'a, MeltRequested> {
    /// Execute the melt request with async support.
    #[instrument(skip_all)]
    pub async fn execute_async(
        self,
        metadata: HashMap<String, String>,
    ) -> Result<MeltSagaResult<'a>, Error> {
        let operation_id = self.state_data.operation_id;
        let quote_info = &self.state_data.quote;

        tracing::info!(
            "Executing async melt request for quote {} with operation {}",
            quote_info.id,
            operation_id
        );

        let request = MeltRequest::new(
            quote_info.id.clone(),
            self.state_data.final_proofs.clone(),
            if self.state_data.premint_secrets.is_empty() {
                None
            } else {
                Some(self.state_data.premint_secrets.blinded_messages())
            },
        )
        .prefer_async(true);

        let request = if quote_info.payment_method == PaymentMethod::Known(KnownMethod::Onchain) {
            request.fee_index(quote_info.fee_index.ok_or(Error::InvalidPaymentRequest)?)
        } else {
            request
        };

        let melt_result = self
            .wallet
            .client
            .post_melt(&quote_info.payment_method, request)
            .await;

        let paid = match melt_result {
            Ok(response) if response.state() == MeltQuoteState::Paid => Some((
                response.payment_proof().map(str::to_owned),
                response.change().cloned(),
            )),
            Ok(_) => None,
            Err(error) => {
                tracing::warn!("Melt request failed: {}. Checking quote status", error);
                match self.wallet.internal_check_melt_status(&quote_info.id).await {
                    Ok(response) if response.state() == MeltQuoteState::Paid => {
                        Some((response.payment_proof(), response.change()))
                    }
                    _ => None,
                }
            }
        };
        if let Some((proof, change)) = paid {
            let finalized = finalize_melt_common(
                self.wallet,
                operation_id,
                quote_info,
                &self.state_data.final_proofs,
                &self.state_data.premint_secrets,
                MeltQuoteState::Paid,
                proof,
                change,
                metadata,
                Default::default(),
            )
            .await?;
            return Ok(MeltSagaResult::Finalized(finalized));
        }
        // An unpaid reply does not exclude an earlier or still-running attempt.
        // Retain the submitted inputs until a verified paid outcome is available.
        Ok(MeltSagaResult::Pending(Box::new(MeltSaga {
            wallet: self.wallet,
            state_data: PaymentPending {
                operation_id,
                quote: self.state_data.quote,
                final_proofs: self.state_data.final_proofs,
                premint_secrets: self.state_data.premint_secrets,
            },
        })))
    }
}

impl<'a> MeltSaga<'a, PaymentPending> {
    /// Get the quote
    pub fn quote(&self) -> &MeltQuote {
        &self.state_data.quote
    }

    /// Finalize the melt with the response from subscription
    pub async fn finalize(
        self,
        state: MeltQuoteState,
        payment_proof: Option<String>,
        change: Option<Vec<crate::nuts::BlindSignature>>,
        metadata: HashMap<String, String>,
    ) -> Result<MeltSaga<'a, Finalized>, Error> {
        finalize_melt_common(
            self.wallet,
            self.state_data.operation_id,
            &self.state_data.quote,
            &self.state_data.final_proofs,
            &self.state_data.premint_secrets,
            state,
            payment_proof,
            change,
            metadata,
            Default::default(),
        )
        .await
    }
}

impl<'a> MeltSaga<'a, Finalized> {
    /// Get the quote ID
    pub fn quote_id(&self) -> &str {
        &self.state_data.quote_id
    }

    /// Get the melt quote state
    pub fn state(&self) -> MeltQuoteState {
        self.state_data.state
    }

    /// Get the amount that was melted
    pub fn amount(&self) -> Amount {
        self.state_data.amount
    }

    /// Get the fee paid
    pub fn fee_paid(&self) -> Amount {
        self.state_data.fee
    }

    /// Get the payment proof (e.g., Lightning preimage)
    pub fn payment_proof(&self) -> Option<&str> {
        self.state_data.payment_proof.as_deref()
    }

    /// Consume the saga and return the change proofs
    pub fn into_change(self) -> Option<Proofs> {
        self.state_data.change
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cdk_common::nut00::KnownMethod;
    use cdk_common::nuts::nut30::MeltQuoteOnchainFeeOption;
    use cdk_common::nuts::{CurrencyUnit, State};
    use cdk_common::wallet::{KeysetLoadPolicy, OperationData, TransactionStatus};
    use cdk_common::{MeltQuoteOnchainResponse, MeltQuoteResponse, MeltQuoteState, PaymentMethod};
    use uuid::Uuid;

    use super::{finalize_melt_common, MeltSaga, MeltSagaResult};
    use crate::nuts::{BlindSignature, PreMintSecrets};
    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, test_keyset_id, test_melt_quote,
        test_mint_url, test_proof_info, MockMintConnector,
    };
    use crate::wallet::MeltConfirmOptions;
    use crate::{Amount, Error};

    #[tokio::test]
    async fn test_prepare_melt_reserves_exact_input_proofs_for_operation() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 2000, mint_url.clone());
        let proof_y = proof_info.y;
        let proof = proof_info.proof.clone();
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let quote = test_melt_quote();
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let saga = MeltSaga::new(&wallet);
        let prepared = saga
            .prepare_with_proofs(&quote_id, vec![proof], HashMap::new())
            .await
            .unwrap();

        let reserved = db
            .get_reserved_proofs(&prepared.state_data.operation_id)
            .await
            .unwrap();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].y, proof_y);
        assert_eq!(reserved[0].state, State::Reserved);

        let stored = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].state, State::Reserved);
        assert_eq!(
            stored[0].used_by_operation,
            Some(prepared.state_data.operation_id)
        );
    }

    #[tokio::test]
    async fn test_prepare_melt_persists_metadata_in_saga() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 2000, mint_url.clone());
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let quote = test_melt_quote();
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let mut metadata = HashMap::new();
        metadata.insert("purpose".to_string(), "ffi-pending".to_string());

        let prepared = MeltSaga::new(&wallet)
            .prepare(&quote_id, metadata.clone())
            .await
            .unwrap();

        let saga = db
            .get_saga(&prepared.state_data.operation_id)
            .await
            .unwrap()
            .expect("saga should be stored");
        match saga.data {
            OperationData::Melt(data) => assert_eq!(data.metadata, metadata),
            _ => panic!("expected melt saga data"),
        }
    }

    #[tokio::test]
    async fn test_prepare_melt_proofs_persists_metadata_in_saga() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 1024, mint_url.clone());
        let proof = proof_info.proof.clone();
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let quote = test_melt_quote();
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let mut metadata = HashMap::new();
        metadata.insert("purpose".to_string(), "ffi-pending-proofs".to_string());

        let prepared = MeltSaga::new(&wallet)
            .prepare_with_proofs(&quote_id, vec![proof], metadata.clone())
            .await
            .unwrap();

        let saga = db
            .get_saga(&prepared.state_data.operation_id)
            .await
            .unwrap()
            .expect("saga should be stored");
        match saga.data {
            OperationData::Melt(data) => assert_eq!(data.metadata, metadata),
            _ => panic!("expected melt saga data"),
        }
    }

    /// Verify the Amount arithmetic used in prepare/request_melt does not panic on overflow.
    ///
    /// The saga uses checked_add to guard against malicious mint responses returning
    /// quote amounts that would overflow u64.  Two storable i64-positive values cannot
    /// overflow u64 when added, but three can (the triple-operand path in
    /// request_melt_with_options).  This test documents both invariants.
    #[test]
    fn test_melt_amount_overflow_handled_gracefully() {
        // Two-operand path (prepare / prepare_with_proofs): i64::MAX + i64::MAX = u64::MAX - 1
        let a = Amount::from(i64::MAX as u64);
        let b = Amount::from(i64::MAX as u64);
        assert!(
            a.checked_add(b).is_some(),
            "two i64::MAX values must not overflow u64"
        );

        // Three-operand path (request_melt_with_options): can overflow when combined
        let c = Amount::from(2u64); // pushes u64::MAX-1 over the top
        assert!(
            a.checked_add(b)
                .expect("two i64::MAX fit in u64")
                .checked_add(c)
                .is_none(),
            "three amounts summing to > u64::MAX must return None"
        );

        // Single overflow: u64::MAX + 1 must return None
        assert!(Amount::from(u64::MAX)
            .checked_add(Amount::from(1u64))
            .is_none());
    }

    #[tokio::test]
    async fn test_prepare_melt_reserves_swap_input_proofs_for_operation() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 2000, mint_url.clone());
        let proof_y = proof_info.y;
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let quote = test_melt_quote();
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let saga = MeltSaga::new(&wallet);
        let prepared = saga.prepare(&quote_id, HashMap::new()).await.unwrap();

        assert_eq!(prepared.state_data.proofs_to_swap.len(), 1);

        let reserved = db
            .get_reserved_proofs(&prepared.state_data.operation_id)
            .await
            .unwrap();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].y, proof_y);
        assert_eq!(reserved[0].state, State::Reserved);

        let stored = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].state, State::Reserved);
        assert_eq!(
            stored[0].used_by_operation,
            Some(prepared.state_data.operation_id)
        );
    }

    #[tokio::test]
    async fn test_onchain_melt_sends_change_outputs_when_change_expected() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 1200, mint_url.clone());
        let proof = proof_info.proof.clone();

        let mut quote = test_melt_quote();
        quote.amount = Amount::from(1000);
        quote.fee_reserve = Amount::from(10);
        quote.payment_method = PaymentMethod::Known(KnownMethod::Onchain);
        quote.request = "bcrt1qtestaddress".to_string();
        quote.estimated_blocks = Some(6);
        quote.fee_index = Some(0);
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote.clone()).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        mock_client.set_post_melt_response(Ok(MeltQuoteResponse::Onchain(
            MeltQuoteOnchainResponse {
                quote: quote_id.clone(),
                amount: quote.amount,
                unit: CurrencyUnit::Sat,
                method: PaymentMethod::Known(KnownMethod::Onchain),
                state: MeltQuoteState::Pending,
                expiry: quote.expiry,
                request: quote.request.clone(),
                fee_options: vec![MeltQuoteOnchainFeeOption {
                    fee_index: 0,
                    fee_reserve: quote.fee_reserve,
                    estimated_blocks: 6,
                }],
                selected_fee_index: Some(0),
                outpoint: None,
                change: None,
            },
        )));
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client.clone()).await;

        let saga = MeltSaga::new(&wallet);
        let requested = saga
            .prepare_with_proofs(&quote_id, vec![proof], HashMap::new())
            .await
            .unwrap()
            .request_melt_with_options(MeltConfirmOptions::new())
            .await
            .unwrap();

        let stored_saga = db
            .get_saga(&requested.state_data.operation_id)
            .await
            .unwrap()
            .expect("melt saga must be stored");
        let OperationData::Melt(data) = stored_saga.data else {
            panic!("stored saga must contain melt operation data");
        };
        assert!(
            data.change_amount
                .is_some_and(|amount| amount > Amount::ZERO),
            "onchain melt should record expected change amount"
        );
        assert!(
            data.change_blinded_messages
                .as_ref()
                .is_some_and(|outputs| !outputs.is_empty()),
            "onchain melt should persist blinded change outputs for recovery"
        );

        let _result = requested.execute_async(HashMap::new()).await.unwrap();
        let (method, request) = mock_client
            .last_post_melt_request()
            .expect("post_melt request must be captured");

        assert_eq!(method, PaymentMethod::Known(KnownMethod::Onchain));
        assert_eq!(request.selected_fee_index(), Some(0));
        assert!(
            request
                .outputs()
                .as_ref()
                .is_some_and(|outputs| !outputs.is_empty()),
            "onchain melt request should include blinded change outputs"
        );
    }

    fn onchain_melt_response(
        quote: &cdk_common::wallet::MeltQuote,
        state: MeltQuoteState,
    ) -> MeltQuoteResponse<String> {
        MeltQuoteResponse::Onchain(MeltQuoteOnchainResponse {
            quote: quote.id.clone(),
            amount: quote.amount,
            unit: CurrencyUnit::Sat,
            method: PaymentMethod::Known(KnownMethod::Onchain),
            state,
            expiry: quote.expiry,
            request: quote.request.clone(),
            fee_options: vec![MeltQuoteOnchainFeeOption {
                fee_index: quote.fee_index.unwrap_or(0),
                fee_reserve: quote.fee_reserve,
                estimated_blocks: quote
                    .estimated_blocks
                    .expect("test onchain quote must set estimated_blocks"),
            }],
            selected_fee_index: quote.fee_index,
            outpoint: None,
            change: None,
        })
    }

    async fn requested_onchain_melt_with_response(
        state: MeltQuoteState,
    ) -> (
        Arc<dyn cdk_common::database::WalletDatabase<cdk_common::database::Error> + Send + Sync>,
        crate::nuts::PublicKey,
        super::MeltSaga<'static, super::state::MeltRequested>,
    ) {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        let keyset_id = test_keyset_id();
        let proof_info = test_proof_info(keyset_id, 1200, mint_url);
        let proof_y = proof_info.y;
        let proof = proof_info.proof.clone();

        let mut quote = test_melt_quote();
        quote.amount = Amount::from(1000);
        quote.fee_reserve = Amount::from(10);
        quote.payment_method = PaymentMethod::Known(KnownMethod::Onchain);
        quote.request = "bcrt1qtestaddress".to_string();
        quote.estimated_blocks = Some(6);
        quote.fee_index = Some(0);
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote.clone()).await.unwrap();

        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        mock_client.set_post_melt_response(Ok(onchain_melt_response(&quote, state)));
        let wallet = Box::leak(Box::new(
            create_test_wallet_with_mock(db.clone(), mock_client).await,
        ));

        let requested = MeltSaga::new(wallet)
            .prepare_with_proofs(&quote_id, vec![proof], HashMap::new())
            .await
            .unwrap()
            .request_melt_with_options(MeltConfirmOptions::new())
            .await
            .unwrap();

        (db, proof_y, requested)
    }

    #[tokio::test]
    async fn test_execute_async_negative_response_preserves_submitted_records() {
        for state in [MeltQuoteState::Unpaid, MeltQuoteState::Failed] {
            let (db, proof_y, requested) = requested_onchain_melt_with_response(state).await;
            let operation_id = requested.state_data.operation_id;
            let quote_id = requested.state_data.quote.id.clone();
            let original = (
                db.get_saga(&operation_id).await.unwrap(),
                db.get_melt_quote(&quote_id).await.unwrap(),
                db.get_proofs_by_ys(vec![proof_y]).await.unwrap(),
                db.list_transactions(None, None, None).await.unwrap(),
            );
            assert!(matches!(
                requested.execute_async(HashMap::new()).await.unwrap(),
                MeltSagaResult::Pending(_)
            ));
            assert_eq!(
                (
                    db.get_saga(&operation_id).await.unwrap(),
                    db.get_melt_quote(&quote_id).await.unwrap(),
                    db.get_proofs_by_ys(vec![proof_y]).await.unwrap(),
                    db.list_transactions(None, None, None).await.unwrap(),
                ),
                original
            );
        }
    }

    #[tokio::test]
    async fn test_execute_async_unknown_response_stays_pending_instead_of_finalizing() {
        let (db, proof_y, requested) =
            requested_onchain_melt_with_response(MeltQuoteState::Unknown).await;
        let operation_id = requested.state_data.operation_id;

        let result = requested.execute_async(HashMap::new()).await.unwrap();
        assert!(matches!(result, MeltSagaResult::Pending(_)));

        let stored = db.get_proofs_by_ys(vec![proof_y]).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].state, State::Pending);
        assert_eq!(stored[0].used_by_operation, Some(operation_id));
        assert!(
            db.get_saga(&operation_id).await.unwrap().is_some(),
            "pending melt saga should remain for recovery"
        );
        let transactions = db.list_transactions(None, None, None).await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].status, TransactionStatus::Pending);
    }

    #[tokio::test]
    async fn test_finalize_melt_rejects_oversized_change_from_mint() {
        let db = create_test_db().await;
        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db, mock_client).await;

        let keyset_id = test_keyset_id();
        let mut quote = test_melt_quote();
        quote.fee_reserve = Amount::ZERO;
        let final_proofs = vec![test_proof_info(keyset_id, 1000, test_mint_url()).proof];
        let premint_secrets =
            PreMintSecrets::from_seed_blank(keyset_id, 0, &wallet.seed, Amount::from(1)).unwrap();
        let oversized_change = vec![BlindSignature {
            amount: Amount::from(1),
            keyset_id,
            c: premint_secrets.blinded_messages()[0].blinded_secret,
            dleq: None,
        }];

        let operation_id = Uuid::new_v4();
        super::paid_change_fixture::save_request(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
        )
        .await;
        let result = finalize_melt_common(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
            MeltQuoteState::Paid,
            None,
            Some(oversized_change),
            HashMap::new(),
            Default::default(),
        )
        .await;

        assert!(matches!(result, Err(Error::AmountOverflow)));
    }

    #[tokio::test]
    async fn test_finalize_melt_accepts_change_amount_for_zero_amount_output() {
        let db = create_test_db().await;
        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db, mock_client).await;

        let keyset_id = test_keyset_id();
        let mut quote = test_melt_quote();
        quote.fee_reserve = Amount::ZERO;
        let final_proofs = vec![test_proof_info(keyset_id, 1008, test_mint_url()).proof];
        let premint_secrets =
            PreMintSecrets::from_seed_blank(keyset_id, 0, &wallet.seed, Amount::from(8))
                .expect("blank premint secrets");
        let change = vec![BlindSignature {
            amount: Amount::from(8),
            keyset_id,
            c: premint_secrets.blinded_messages()[0].blinded_secret,
            dleq: None,
        }];

        let operation_id = Uuid::new_v4();
        super::paid_change_fixture::save_request(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
        )
        .await;
        let result = finalize_melt_common(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
            MeltQuoteState::Paid,
            None,
            Some(change),
            HashMap::new(),
            Default::default(),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().into_change().expect("change proofs")[0].amount,
            Amount::from(8)
        );
    }

    #[tokio::test]
    async fn test_finalize_melt_persists_payment_proof_on_quote() {
        let db = create_test_db().await;
        let mock_client = Arc::new(MockMintConnector::new());
        mock_client.reset_default_mint_state();
        let wallet = create_test_wallet_with_mock(db.clone(), mock_client).await;

        let keyset_id = test_keyset_id();
        let operation_id = Uuid::new_v4();
        let mut proof_info = test_proof_info(keyset_id, 1000, test_mint_url());
        proof_info.state = State::Pending;
        proof_info.used_by_operation = Some(operation_id);
        let final_proofs = vec![proof_info.proof.clone()];
        db.update_proofs(vec![proof_info], vec![]).await.unwrap();

        let mut quote = test_melt_quote();
        quote.fee_reserve = Amount::ZERO;
        let quote_id = quote.id.clone();
        db.add_melt_quote(quote.clone()).await.unwrap();

        let premint_secrets = PreMintSecrets::new(keyset_id);
        super::paid_change_fixture::save_request(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
        )
        .await;
        finalize_melt_common(
            &wallet,
            operation_id,
            &quote,
            &final_proofs,
            &premint_secrets,
            MeltQuoteState::Paid,
            Some("preimage123".to_string()),
            None,
            HashMap::new(),
            KeysetLoadPolicy::default(),
        )
        .await
        .unwrap();

        let stored_quote = db
            .get_melt_quote(&quote_id)
            .await
            .unwrap()
            .expect("quote should be stored");
        assert_eq!(stored_quote.state, MeltQuoteState::Paid);
        assert_eq!(stored_quote.payment_proof.as_deref(), Some("preimage123"));
    }

    #[tokio::test]
    async fn test_add_melt_quote_preserves_existing_payment_proof_when_update_has_none() {
        let db = create_test_db().await;
        let mut quote = test_melt_quote();
        quote.payment_proof = Some("preimage123".to_string());
        db.add_melt_quote(quote.clone()).await.unwrap();

        quote.state = MeltQuoteState::Paid;
        quote.payment_proof = None;
        db.add_melt_quote(quote.clone()).await.unwrap();

        let stored_quote = db
            .get_melt_quote(&quote.id)
            .await
            .unwrap()
            .expect("quote should be stored");
        assert_eq!(stored_quote.state, MeltQuoteState::Paid);
        assert_eq!(stored_quote.payment_proof.as_deref(), Some("preimage123"));
    }
}
