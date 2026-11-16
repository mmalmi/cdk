use super::{Amount, Error, FeeAndAmounts};
use crate::amount::SplitTarget;

impl Amount {
    /// Split with a preferred value or list of values, filling any remainder greedily.
    pub fn split_targeted(
        &self,
        target: &SplitTarget,
        keyset: &FeeAndAmounts,
    ) -> Result<Vec<Self>, Error> {
        let mut parts = Vec::new();
        self.visit_targeted_split(target, keyset, |amount, count| {
            parts.extend((0..count).map(|_| Self::from(amount)));
        })?;
        // Preserve the ordinary greedy order of this existing early-return case.
        if !matches!(target, SplitTarget::Value(value) if self <= value) {
            parts.sort();
        }
        Ok(parts)
    }

    /// Count a targeted split without allocating one entry per output.
    ///
    /// Shares representation checks with [`Self::split_targeted`]. Repeated target
    /// values are counted arithmetically, including on 32-bit platforms.
    pub fn split_targeted_count(
        &self,
        target: &SplitTarget,
        keyset: &FeeAndAmounts,
    ) -> Result<u64, Error> {
        self.visit_targeted_split(target, keyset, |_, _| {})
    }

    fn visit_targeted_split<F>(
        &self,
        target: &SplitTarget,
        keyset: &FeeAndAmounts,
        mut visit: F,
    ) -> Result<u64, Error>
    where
        F: FnMut(u64, u64),
    {
        match target {
            SplitTarget::None => self.visit_split(keyset, visit),
            SplitTarget::Value(value) => {
                if *value == Self::ZERO {
                    return Err(Error::InvalidAmount(
                        "SplitTarget::Value amount must be greater than zero".to_owned(),
                    ));
                }
                if self <= value {
                    return self.visit_split(keyset, visit);
                }
                let copies = self.value / value.value;
                let remainder = Self::from(self.value % value.value);
                // Positive denominations keep every repeated count below self.value.
                let per_copy = value.visit_split(keyset, |amount, count| {
                    visit(amount, count * copies);
                })?;
                let tail = remainder.visit_split(keyset, visit)?;
                per_copy
                    .checked_mul(copies)
                    .and_then(|n| n.checked_add(tail))
                    .ok_or(Error::AmountOverflow)
            }
            SplitTarget::Values(values) => {
                let total = Self::try_sum(values.iter().copied())?;
                let remainder = self.checked_sub(total).ok_or(Error::SplitValuesGreater)?;
                for value in values {
                    visit(value.value, 1);
                }
                let tail = if remainder == Self::ZERO {
                    0
                } else {
                    remainder.visit_split(keyset, visit)?
                };
                u64::try_from(values.len())
                    .map_err(|_| Error::AmountOverflow)?
                    .checked_add(tail)
                    .ok_or(Error::AmountOverflow)
            }
        }
    }
}
