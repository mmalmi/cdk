use super::{Amount, Error, FeeAndAmounts};

impl Amount {
    /// Find the smallest greedy split covering this amount and its redemption fee.
    ///
    /// Searches scalar totals with at most 64 doublings and 64 bisections before
    /// allocating the final outputs, regardless of the requested proof count.
    /// Returns [`Error::AmountOverflow`] if no representable total can cover it.
    pub fn split_with_fee(&self, fee_and_amounts: &FeeAndAmounts) -> Result<Vec<Self>, Error> {
        self.split_total_with_fee(fee_and_amounts)?
            .split(fee_and_amounts)
    }

    /// Find the smallest nominal total whose greedy split covers this amount and its fee.
    ///
    /// Uses the same bounded search as [`Self::split_with_fee`], without allocating
    /// its outputs. Returns [`Error::AmountOverflow`] when no total can cover it.
    pub fn split_total_with_fee(&self, keyset: &FeeAndAmounts) -> Result<Self, Error> {
        let fee = u128::from(keyset.fee);
        let covered = |value| -> Result<(u128, u64), Error> {
            let mut net = 0;
            let (count, _) = Self::from(value).walk_split(keyset, |amount, count| {
                net += (u128::from(amount) * 1000).saturating_sub(fee) * u128::from(count);
            })?;
            Ok((net, count))
        };

        // Discarding outputs worth no more than their fee gives the best net
        // value at or below a nominal total. Replacing a larger greedy output
        // with smaller outputs loses value and cannot use fewer proofs, so this
        // bound is monotonic even when the untrimmed split's net value is not.
        let required = u128::from(self.value) * 1000;
        let (net, count) = covered(self.value)?;
        if net >= required {
            return Ok(*self);
        }

        let mut headroom = u64::try_from((u128::from(count) * fee).div_ceil(1000))
            .unwrap_or(u64::MAX)
            .max(1);
        let mut low = self.value.checked_add(1).ok_or(Error::AmountOverflow)?;
        let mut high = self.value.saturating_add(headroom);
        while covered(high)?.0 < required {
            low = high.checked_add(1).ok_or(Error::AmountOverflow)?;
            headroom = headroom.saturating_mul(2);
            high = self.value.saturating_add(headroom);
        }

        while low < high {
            let middle = low + (high - low) / 2;
            if covered(middle)?.0 >= required {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        Ok(Self::from(low))
    }
}

#[cfg(test)]
mod tests;
