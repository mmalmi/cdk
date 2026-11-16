use crate::nuts::{Id, PreSwap, Proof, Proofs, ProofsMethods};
use crate::wallet::SendOptions;
use crate::{Amount, Error, Wallet};

/// Prepare one exact swap under the parent send's approved terms.
pub(super) async fn prepare_swap(
    wallet: &Wallet,
    amount: Amount,
    proofs: Proofs,
    options: &SendOptions,
    keyset: Id,
    max_fee: Amount,
) -> Result<(PreSwap, u32), Error> {
    if wallet
        .active_keyset_with_policy(cdk_common::wallet::KeysetLoadPolicy::Refresh)
        .await?
        .id
        != keyset
    {
        return Err(Error::InactiveKeyset);
    }
    let denominations = wallet.get_keyset_fees_and_amounts_by_id(keyset).await?;
    let fee = wallet.get_proofs_fee(&proofs).await?;
    if fee.total > max_fee {
        return Err(Error::MaxFeeExceeded);
    }
    let counter = wallet
        .localstore
        .increment_keyset_counter(&keyset, 0)
        .await?;
    let proposal = wallet
        .create_swap(
            keyset,
            &denominations,
            Some(amount),
            crate::amount::SplitTarget::None,
            proofs,
            options.conditions.clone(),
            false,
            options.use_p2bk,
            &fee,
            counter,
        )
        .await?;
    Ok((proposal, counter))
}

pub(super) fn check_keyset(options: &SendOptions, active: Id) -> Result<(), Error> {
    if options.required_keyset_id.is_some_and(|id| id != active) {
        return Err(Error::InactiveKeyset);
    }
    Ok(())
}

pub(super) fn matching_proofs(options: &SendOptions, proofs: &Proofs) -> bool {
    options
        .required_keyset_id
        .is_none_or(|id| proofs.iter().all(|p| p.keyset_id == id))
}

pub(super) fn check_proof_count(
    options: &SendOptions,
    retained: usize,
    outputs: usize,
) -> Result<(), Error> {
    if let Some(max) = options.max_proofs {
        let actual = retained.checked_add(outputs).ok_or(Error::AmountOverflow)?;
        if actual > max {
            return Err(Error::MaxOutputsExceeded { actual, max });
        }
    }
    Ok(())
}

pub(super) fn check_input_bytes<'a, I>(options: &SendOptions, proofs: I) -> Result<(), Error>
where
    I: IntoIterator<Item = &'a Proof>,
{
    let Some(max) = options.max_input_bytes else {
        return Ok(());
    };
    let max = usize::try_from(max).unwrap_or(usize::MAX);
    // Count the single JSON array without copying its encoded proof contents.
    let mut bytes = InputBytes(2);
    for (index, proof) in proofs.into_iter().enumerate() {
        bytes.0 = bytes
            .0
            .checked_add(usize::from(index > 0))
            .ok_or(Error::AmountOverflow)?;
        serde_json::to_writer(&mut bytes, proof)?;
    }
    if bytes.0 > max {
        return Err(Error::RequestFieldTooLarge {
            field: "send_inputs".to_owned(),
            actual: bytes.0,
            max,
        });
    }
    Ok(())
}

struct InputBytes(usize);

impl std::io::Write for InputBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("send input size overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn check_debit(
    options: &SendOptions,
    amount: Amount,
    send_fee: Amount,
    swap_fee: Amount,
    retained: &Proofs,
    has_swap: bool,
) -> Result<(), Error> {
    // Tolerant sends can retain more than amount + send_fee, including when
    // other inputs still need a swap. Charge actual token value, never change.
    let retained_value = retained.total_amount()?;
    let token_value = if has_swap {
        amount
            .checked_add(send_fee)
            .ok_or(Error::AmountOverflow)?
            .max(retained_value)
    } else {
        retained_value
    };
    let debit = token_value
        .checked_add(swap_fee)
        .ok_or(Error::AmountOverflow)?;
    if let Some(maximum) = options.max_total_amount {
        if debit > maximum {
            return Err(Error::MaxAmountExceeded {
                amount: debit,
                maximum,
            });
        }
    }
    Ok(())
}
