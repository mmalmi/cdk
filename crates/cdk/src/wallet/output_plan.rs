use std::collections::HashMap;

use cdk_common::wallet::{KeysetLoadPolicy, WalletSaga};
use tracing::instrument;

use crate::amount::{FeeAndAmounts, SplitTarget};
use crate::nuts::{Id, KeySet, PreMintSecrets, PreSwap};
use crate::{Amount, Error, Wallet};

impl Wallet {
    /// Commit a validated private swap proposal with its exact parent and range.
    #[instrument(skip_all)]
    pub(crate) async fn commit_swap_preparation(
        &self,
        expected: Option<&WalletSaga>,
        next: &WalletSaga,
        keyset: Id,
        counter_start: u32,
        swap: &PreSwap,
    ) -> Result<(), Error> {
        let keys = self
            .prepared_output_keyset(
                keyset,
                counter_start,
                swap.derived_secret_count,
                &swap.pre_mint_secrets,
            )
            .await?;
        if !self
            .localstore
            .prepare_saga_outputs(
                expected,
                next.clone(),
                keys,
                counter_start,
                swap.derived_secret_count,
                None,
            )
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        Ok(())
    }

    /// Validate the complete proposed range before publishing its recovery keys.
    #[instrument(skip_all)]
    pub(crate) async fn prepared_output_keyset(
        &self,
        keyset: Id,
        counter_start: u32,
        counter_count: u32,
        secrets: &PreMintSecrets,
    ) -> Result<KeySet, Error> {
        let counter_end = counter_start
            .checked_add(counter_count)
            .ok_or(Error::AmountOverflow)?;
        let mut indices: Vec<_> = secrets
            .secrets
            .iter()
            .filter_map(|secret| secret.derivation_index)
            .collect();
        indices.sort_unstable();
        if secrets.keyset_id != keyset
            || indices.into_iter().ne(counter_start..counter_end)
            || secrets
                .secrets
                .iter()
                .any(|secret| secret.blinded_message.keyset_id != keyset)
        {
            return Err(Error::InvalidOperationState);
        }
        self.keyset_with_policy(keyset, KeysetLoadPolicy::CacheOnly)
            .await
    }

    pub(crate) fn check_output_count(&self, count: u64) -> Result<u32, Error> {
        if let Some(max) = self.max_outputs {
            if count > u64::from(max) {
                return Err(Error::MaxOutputsExceeded {
                    actual: usize::try_from(count).map_err(|_| Error::AmountOverflow)?,
                    max: max as usize,
                });
            }
        }
        u32::try_from(count).map_err(|_| Error::AmountOverflow)
    }

    /// Plan refill values before expanding them into individual outputs.
    pub(crate) async fn determine_split_target_values(
        &self,
        change_amount: Amount,
        keyset: &FeeAndAmounts,
    ) -> Result<SplitTarget, Error> {
        let proofs = self.unspent_inventory().await?;
        let mut held = HashMap::<u64, u64>::new();
        for proof in proofs {
            *held.entry(proof.proof.amount.to_u64()).or_default() += 1;
        }
        let mut denominations = keyset.amounts().to_vec();
        denominations.sort_unstable();
        let mut counts = Vec::new();
        let mut remaining = change_amount.to_u64();
        let mut count = 0u64;
        for value in denominations {
            if value == 0 {
                return Err(crate::amount::Error::InvalidAmount(
                    "denominations must be greater than zero".to_owned(),
                )
                .into());
            }
            let needed = (self.target_proof_count as u64)
                .saturating_sub(held.get(&value).copied().unwrap_or(0));
            let take = needed.min(remaining / value);
            remaining -= take * value;
            // Preserve checked-add validation of an attempted value that does not fit.
            if needed > take {
                (change_amount.to_u64() - remaining)
                    .checked_add(value)
                    .ok_or(Error::AmountOverflow)?;
            }
            if take > 0 {
                counts.push((Amount::from(value), take));
                count = count.checked_add(take).ok_or(Error::AmountOverflow)?;
            }
        }
        let tail = Amount::from(remaining).split_count(keyset)?;
        self.check_output_count(count.checked_add(tail).ok_or(Error::AmountOverflow)?)?;
        let values = counts
            .into_iter()
            .flat_map(|(value, count)| (0..count).map(move |_| value))
            .collect();
        Ok(SplitTarget::Values(values))
    }
}
