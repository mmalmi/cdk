use super::{Amount, Error, FeeAndAmounts};

mod fee;
mod targeted;

impl Amount {
    /// Split into parts using the available denominations.
    ///
    /// Uses a greedy algorithm starting from the largest denomination, taking
    /// as many of each denomination as possible before moving to the next one.
    /// Returns an error if the amount cannot be fully represented.
    pub fn split(&self, fee_and_amounts: &FeeAndAmounts) -> Result<Vec<Self>, Error> {
        let mut parts = Vec::new();
        self.visit_split(fee_and_amounts, |amount, count| {
            parts.extend((0..count).map(|_| Self::from(amount)));
        })?;
        Ok(parts)
    }

    /// Count the greedy split without allocating one entry per output.
    ///
    /// Traverses only the available denominations, with the same representation
    /// errors as [`Self::split`]. The count remains a `u64` on 32-bit targets.
    pub fn split_count(&self, fee_and_amounts: &FeeAndAmounts) -> Result<u64, Error> {
        self.visit_split(fee_and_amounts, |_, _| {})
    }

    /// Count outputs by denomination without allocating one entry per output.
    ///
    /// Returns nonzero counts in greedy split order, with the same representation
    /// errors as [`Self::split`]. The list has at most one entry per denomination.
    pub fn split_counts(&self, fee_and_amounts: &FeeAndAmounts) -> Result<Vec<(Self, u64)>, Error> {
        let mut counts = Vec::new();
        self.visit_split(fee_and_amounts, |amount, count| {
            if count > 0 {
                counts.push((Self::from(amount), count));
            }
        })?;
        Ok(counts)
    }

    fn visit_split<F>(&self, fee_and_amounts: &FeeAndAmounts, visit: F) -> Result<u64, Error>
    where
        F: FnMut(u64, u64),
    {
        let (count, remainder) = self.walk_split(fee_and_amounts, visit)?;
        if remainder != 0 {
            return Err(Error::CannotSplitAmount(self.value, self.value - remainder));
        }
        Ok(count)
    }

    fn walk_split<F>(
        &self,
        fee_and_amounts: &FeeAndAmounts,
        mut visit: F,
    ) -> Result<(u64, u64), Error>
    where
        F: FnMut(u64, u64),
    {
        if fee_and_amounts.amounts.contains(&0) {
            return Err(Error::InvalidAmount(
                "denominations must be greater than zero".to_owned(),
            ));
        }
        let mut remainder = self.value;
        let mut count = 0;
        for &amount in fee_and_amounts.amounts.iter().rev() {
            let outputs = remainder / amount;
            visit(amount, outputs);
            // Every denomination is positive, so the count cannot exceed value.
            count += outputs;
            remainder %= amount;
        }
        Ok((count, remainder))
    }
}

#[cfg(test)]
mod tests;
