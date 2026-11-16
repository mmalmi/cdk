use crate::nuts::Proofs;
use crate::Amount;

pub(super) type SendAmounts = Vec<(Amount, u64)>;

pub(super) fn partition(proofs: Proofs, amounts: &[(Amount, u64)]) -> (Proofs, Proofs) {
    let mut remaining = amounts.to_vec();
    proofs.into_iter().partition(|proof| {
        match remaining
            .iter_mut()
            .find(|(amount, count)| *amount == proof.amount && *count > 0)
        {
            Some((_, count)) => {
                *count -= 1;
                true
            }
            _ => false,
        }
    })
}
