//! Issue (mint) saga types

use core::fmt;

use cashu::{BatchMintRequest, BlindedMessage, MintRequest, PaymentMethod, PreMintSecrets};
use serde::{Deserialize, Serialize};

use crate::Error;

/// States specific to mint (issue) saga
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueSagaState {
    /// Original request and secrets saved; no mint submission is allowed yet.
    SecretsPrepared,
    /// Original request may have reached the mint; recover only its saved outputs.
    MintRequested,
}

impl std::fmt::Display for IssueSagaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueSagaState::SecretsPrepared => write!(f, "secrets_prepared"),
            IssueSagaState::MintRequested => write!(f, "mint_requested"),
        }
    }
}

impl std::str::FromStr for IssueSagaState {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "secrets_prepared" => Ok(IssueSagaState::SecretsPrepared),
            "mint_requested" => Ok(IssueSagaState::MintRequested),
            _ => Err(Error::InvalidOperationState),
        }
    }
}

/// Operation-specific data for Mint operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintOperationData {
    /// Quote ID (for single mint, or first quote in batch)
    quote_id: String,
    /// Quote IDs for batch operations
    ///
    /// If present, this is a batch operation. The batch may have one or more quotes.
    /// For backward compatibility with existing sagas, check this field first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quote_ids: Option<Vec<String>>,
    /// Whether this is a batch operation
    ///
    /// True if this was created by batch_mint, false for single mint.
    /// Used to determine which endpoint to use for replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_batch: Option<bool>,
    /// Amount to mint (total for batch)
    pub amount: crate::Amount,
    /// Exact original secrets and signed request; missing evidence cannot authorize replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Box<MintCheckpoint>>,
    /// Number of consecutive blinded messages assigned to each batch quote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_output_counts: Option<Vec<usize>>,
    /// Amount issued for each batch quote, in quote order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_quote_amounts: Option<Vec<crate::Amount>>,
}

impl MintOperationData {
    /// Create operation data for a single-quote mint operation.
    pub fn new_single(quote_id: String, amount: crate::Amount) -> Self {
        Self {
            quote_ids: Some(vec![quote_id.clone()]),
            quote_id,
            is_batch: Some(false),
            amount,
            checkpoint: None,
            batch_output_counts: None,
            batch_quote_amounts: None,
        }
    }

    /// Create operation data for a batch mint operation.
    pub fn new_batch(quote_ids: Vec<String>, amount: crate::Amount) -> Self {
        let quote_id = quote_ids.first().cloned().unwrap_or_default();

        Self {
            quote_id,
            quote_ids: Some(quote_ids),
            is_batch: Some(true),
            amount,
            checkpoint: None,
            batch_output_counts: None,
            batch_quote_amounts: None,
        }
    }

    /// Create operation data for a partitioned batch mint operation.
    pub fn new_partitioned_batch(
        quote_ids: Vec<String>,
        amount: crate::Amount,
        batch_output_counts: Vec<usize>,
        batch_quote_amounts: Vec<crate::Amount>,
    ) -> Self {
        let quote_id = quote_ids.first().cloned().unwrap_or_default();

        Self {
            quote_id,
            quote_ids: Some(quote_ids),
            is_batch: Some(true),
            amount,
            checkpoint: None,
            batch_output_counts: Some(batch_output_counts),
            batch_quote_amounts: Some(batch_quote_amounts),
        }
    }

    /// Validate the endpoint, quote order, output partitions and saved compatibility signatures.
    /// Callers must additionally verify the saved secrets before submitting any request.
    pub fn original_request(&self) -> Result<&MintCheckpoint, Error> {
        let checkpoint = self
            .checkpoint
            .as_deref()
            .ok_or(Error::InvalidOperationState)?;
        let outputs = checkpoint.request.outputs();
        let quotes = self.quote_ids();
        let (amounts, counts) = match &checkpoint.request {
            MintRequestData::Single {
                request,
                legacy_signature,
                ..
            } => {
                if self.is_batch()
                    || quotes != [request.quote.clone()]
                    || self.batch_quote_amounts.is_some()
                    || self.batch_output_counts.is_some()
                    || (legacy_signature.is_some() && request.signature.is_none())
                {
                    return Err(Error::InvalidOperationState);
                }
                (vec![self.amount], vec![outputs.len()])
            }
            MintRequestData::Batch {
                request,
                legacy_signatures,
            } => {
                if !self.is_batch()
                    || request.quotes != quotes
                    || request.quote_amounts != self.batch_quote_amounts
                    || request
                        .signatures
                        .as_ref()
                        .is_some_and(|s| s.len() != quotes.len())
                    || legacy_signatures.as_ref().is_some_and(|s| {
                        s.len() != quotes.len()
                            || request.signatures.as_ref().is_none_or(|current| {
                                current
                                    .iter()
                                    .map(Option::is_some)
                                    .ne(s.iter().map(Option::is_some))
                            })
                    })
                {
                    return Err(Error::InvalidOperationState);
                }
                (
                    self.batch_quote_amounts
                        .clone()
                        .ok_or(Error::InvalidOperationState)?,
                    self.batch_output_counts
                        .clone()
                        .ok_or(Error::InvalidOperationState)?,
                )
            }
        };
        if quotes.iter().any(String::is_empty)
            || quotes
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != quotes.len()
            || quotes.first().map(String::as_str) != Some(self.primary_quote_id())
            || quotes.is_empty()
            || amounts.len() != quotes.len()
            || counts.len() != quotes.len()
            || crate::Amount::try_sum(amounts.iter().copied())
                .map_err(|_| Error::InvalidOperationState)?
                != self.amount
        {
            return Err(Error::InvalidOperationState);
        }
        let mut offset = 0usize;
        for (amount, count) in amounts.into_iter().zip(counts) {
            let end = offset.checked_add(count).ok_or(Error::AmountOverflow)?;
            let part = outputs
                .get(offset..end)
                .ok_or(Error::InvalidOperationState)?;
            if crate::Amount::try_sum(part.iter().map(|output| output.amount))
                .map_err(|_| Error::InvalidOperationState)?
                != amount
            {
                return Err(Error::InvalidOperationState);
            }
            offset = end;
        }
        if offset != outputs.len() {
            return Err(Error::InvalidOperationState);
        }
        Ok(checkpoint)
    }

    /// Blinded messages from the original saved request, without a duplicate copy.
    pub fn blinded_messages(&self) -> Option<&[BlindedMessage]> {
        self.checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.request.outputs())
    }

    /// Get the representative quote ID for this operation.
    pub fn primary_quote_id(&self) -> &str {
        &self.quote_id
    }

    /// Get all quote IDs for this operation.
    ///
    /// Returns quote_ids if this is a batch, otherwise wraps quote_id in a vec.
    pub fn quote_ids(&self) -> Vec<String> {
        if let Some(ref ids) = self.quote_ids {
            ids.clone()
        } else {
            vec![self.quote_id.clone()]
        }
    }

    /// Check if this is a batch operation.
    pub fn is_batch(&self) -> bool {
        self.is_batch.unwrap_or(false)
    }
}

/// Original issue secrets and request, persisted before any mint submission.
/// Contains bearer secrets and must be stored privately.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintCheckpoint {
    /// Original unreserved quotes, including their issued totals and receipt metadata.
    #[serde(default)]
    pub quotes: Vec<crate::wallet::MintQuote>,
    /// Exact secrets, blinding factors and individual derivation indices.
    pub premint_secrets: PreMintSecrets,
    /// Payment method used for all requests in this issue.
    pub payment_method: PaymentMethod,
    /// Original signed request and precomputed compatibility signatures.
    pub request: MintRequestData,
}

impl fmt::Debug for MintCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MintCheckpoint").finish_non_exhaustive()
    }
}

/// Mint endpoint and immutable request evidence for replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MintRequestData {
    /// NUT-04 request, with compatibility authorization decided before submission.
    Single {
        /// Exact original request.
        request: MintRequest<String>,
        /// Legacy NUT-20 signature over these same outputs, when available.
        legacy_signature: Option<String>,
        /// Whether this original npub.cash quote permits an unsigned fallback.
        allow_unsigned: bool,
    },
    /// NUT-29 batch request, including signatures from externally supplied keys.
    Batch {
        /// Exact original request.
        request: BatchMintRequest<String>,
        /// Legacy NUT-20 signatures in original quote order, when available.
        legacy_signatures: Option<Vec<Option<String>>>,
    },
}

impl MintRequestData {
    /// Original blinded messages in request order.
    pub fn outputs(&self) -> &[BlindedMessage] {
        match self {
            Self::Single { request, .. } => &request.outputs,
            Self::Batch { request, .. } => &request.outputs,
        }
    }
}
