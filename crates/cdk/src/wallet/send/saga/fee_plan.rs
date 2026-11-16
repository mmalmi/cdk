use std::collections::HashMap;

use crate::amount::FeeAndAmounts;
use crate::fees::calculate_fee;
use crate::nuts::{Id, Proofs, ProofsMethods};
use crate::wallet::send::{ProofSplitResult, SendAmounts};
use crate::wallet::{SendKind, SendOptions};
use crate::{Amount, Error};

pub(super) fn output_count(amount: Amount, keyset: &FeeAndAmounts) -> Result<usize, Error> {
    usize::try_from(amount.split_count(keyset)?).map_err(|_| Error::AmountOverflow)
}

pub(super) fn amounts_and_reserve(
    amount: Amount,
    include_fee: bool,
    keyset: &FeeAndAmounts,
) -> Result<(SendAmounts, Amount), Error> {
    let total = if include_fee {
        amount.split_total_with_fee(keyset)?
    } else {
        amount
    };
    let amounts = total.split_counts(keyset)?;
    // Denomination rounding can make the required reserve larger than the fee
    // charged for the chosen split. Keep the entire planned nominal amount.
    let reserve = total.checked_sub(amount).ok_or(Error::AmountOverflow)?;
    Ok((amounts, reserve))
}

pub(super) fn can_send_direct(
    requested: Amount,
    planned_total: Amount,
    proofs: &Proofs,
    redemption_fee: Amount,
    opts: &SendOptions,
) -> Result<bool, Error> {
    if opts
        .max_proofs
        .is_some_and(|maximum| proofs.len() > maximum)
    {
        return Ok(false);
    }
    let total = proofs.total_amount()?;
    let Some(net) = total.checked_sub(redemption_fee) else {
        return Ok(false);
    };
    let Some(extra) = net.checked_sub(requested) else {
        return Ok(false);
    };
    Ok(match opts.send_kind {
        SendKind::OnlineExact => extra == Amount::ZERO || total <= planned_total,
        SendKind::OfflineExact => extra == Amount::ZERO,
        SendKind::OnlineTolerance(tolerance) | SendKind::OfflineTolerance(tolerance) => {
            extra <= tolerance
        }
    })
}

fn redemption_fee(
    retained: &Proofs,
    output_count: usize,
    active_keyset: Id,
    fees: &HashMap<Id, u64>,
) -> Result<Amount, Error> {
    let mut counts = retained.count_by_keyset();
    *counts.entry(active_keyset).or_default() += output_count as u64;
    Ok(calculate_fee(&counts, fees)?.total)
}

/// Price the actual retained proofs and the swap's canonical output split.
/// Reuse old coins only when the same reserve covers their redemption fees.
pub(super) fn complete_plan(
    amount: Amount,
    total: Amount,
    mut split: ProofSplitResult,
    active_keyset: Id,
    fees: &HashMap<Id, u64>,
    active_amounts: &FeeAndAmounts,
) -> Result<(ProofSplitResult, Amount), Error> {
    if split.proofs_to_swap.is_empty() {
        let reserve = split
            .proofs_to_send
            .total_amount()?
            .checked_sub(amount)
            .ok_or(Error::InsufficientFunds)?;
        if redemption_fee(&split.proofs_to_send, 0, active_keyset, fees)? > reserve {
            return Err(Error::InsufficientFunds);
        }
        return Ok((split, reserve));
    }

    let reserve = total.checked_sub(amount).ok_or(Error::AmountOverflow)?;
    let mut needed = total
        .checked_sub(split.proofs_to_send.total_amount()?)
        .ok_or(Error::InsufficientFunds)?;
    let output_count = output_count(needed, active_amounts)?;
    if redemption_fee(&split.proofs_to_send, output_count, active_keyset, fees)? > reserve {
        split.proofs_to_swap.append(&mut split.proofs_to_send);
        split.swap_fee = calculate_fee(&split.proofs_to_swap.count_by_keyset(), fees)?.total;
        needed = total;
    }
    if split
        .proofs_to_swap
        .total_amount()?
        .checked_sub(split.swap_fee)
        .is_none_or(|available| available < needed)
    {
        return Err(Error::InsufficientFunds);
    }
    Ok((split, reserve))
}
