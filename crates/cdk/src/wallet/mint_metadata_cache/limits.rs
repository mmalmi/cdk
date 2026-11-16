//! Admission of retained metadata, separate from HTTP and database read limits.
use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::Arc;

use cdk_common::{KeySetInfo, Keys};
use serde::Serialize;

use super::MintMetadata;
use crate::Error;

/// Per-cache limits for cumulative mint metadata, including inactive keysets.
///
/// Bytes are the sum of canonical JSON for the retained mint info, each keyset
/// info and each set of public keys. This is not a process-memory or SQL read
/// limit: a fetched key reply and database projections exist before admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MintMetadataLimits {
    /// Maximum distinct retained keyset identities. Must be positive.
    pub max_keysets: u32,
    /// Maximum total serialized metadata bytes. Must be positive.
    pub max_bytes: u32,
}

impl MintMetadataLimits {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if self.max_keysets == 0 || self.max_bytes == 0 {
            return Err(Error::Custom(
                "mint metadata limits must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    fn refusal(self, keysets: usize, bytes: u64) -> Error {
        Error::MintMetadataTooLarge {
            keysets,
            bytes,
            max_keysets: self.max_keysets,
            max_bytes: self.max_bytes,
        }
    }

    fn check_keysets(self, count: usize) -> Result<(), Error> {
        if count > self.max_keysets as usize {
            return Err(self.refusal(count, 0));
        }
        Ok(())
    }
}

/// Merge advertisements before fetching keys, bounding the union with old data.
pub(super) fn merge_keysets(
    metadata: &mut MintMetadata,
    keysets: &[KeySetInfo],
    limits: Option<MintMetadataLimits>,
) -> Result<(), Error> {
    let mut seen = HashSet::new();
    metadata.active_keysets.clear();
    for info in keysets {
        if !seen.insert(info.id) {
            if metadata.keysets.get(&info.id).map(Arc::as_ref) != Some(info) {
                return Err(Error::InvalidMintResponse(
                    "conflicting duplicate keyset metadata".to_owned(),
                ));
            }
            continue;
        }
        if let Some(limits) = limits {
            limits.check_keysets(
                metadata.keysets.len() + usize::from(!metadata.keysets.contains_key(&info.id)),
            )?;
        }
        let info = Arc::new(info.clone());
        metadata.keysets.insert(info.id, info.clone());
        if info.active {
            metadata.active_keysets.push(info);
        }
    }
    Ok(())
}

pub(super) struct Budget {
    limits: Option<MintMetadataLimits>,
    keysets: usize,
    bytes: u64,
}

impl Budget {
    pub(super) fn new(
        metadata: &MintMetadata,
        limits: Option<MintMetadataLimits>,
    ) -> Result<Self, Error> {
        let mut budget = Self {
            limits,
            keysets: metadata.keysets.len(),
            bytes: 0,
        };
        if let Some(limits) = limits {
            limits.check_keysets(budget.keysets)?;
            budget.add(&metadata.mint_info)?;
            for info in metadata.keysets.values() {
                budget.add(info.as_ref())?;
            }
            for keys in metadata.keys.values() {
                budget.add(keys.as_ref())?;
            }
        }
        Ok(budget)
    }

    pub(super) fn replace_keys(&mut self, old: Option<&Keys>, keys: &Keys) -> Result<(), Error> {
        if let Some(limits) = self.limits {
            if let Some(old) = old {
                self.bytes = self
                    .bytes
                    .checked_sub(encoded_size(old, u64::from(limits.max_bytes))?)
                    .ok_or(Error::Internal)?;
            }
            self.add(keys)?;
        }
        Ok(())
    }

    fn add<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize,
    {
        if let Some(limits) = self.limits {
            let remaining = u64::from(limits.max_bytes) - self.bytes;
            let size = encoded_size(value, remaining)?;
            if size > remaining {
                return Err(limits.refusal(self.keysets, u64::from(limits.max_bytes) + 1));
            }
            self.bytes += size;
        }
        Ok(())
    }
}

fn encoded_size<T>(value: &T, maximum: u64) -> Result<u64, Error>
where
    T: Serialize,
{
    let mut counter = Counter { bytes: 0, maximum };
    let result = serde_json::to_writer(&mut counter, value);
    if counter.bytes > maximum {
        return Ok(maximum + 1);
    }
    result?;
    Ok(counter.bytes)
}

struct Counter {
    bytes: u64,
    maximum: u64,
}

impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        if self.bytes > self.maximum {
            return Err(io::Error::other("mint metadata budget exceeded"));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wallet::test_utils::test_keyset;

    #[test]
    fn serialized_budget_counts_escaped_fields_and_replacement_keys() {
        let keyset = test_keyset();
        let mut metadata = MintMetadata::default();
        metadata.mint_info.description = Some("quote \" newline\n nul\0 café".to_owned());
        let info = KeySetInfo {
            id: keyset.id,
            unit: keyset.unit,
            active: true,
            input_fee_ppk: keyset.input_fee_ppk,
            final_expiry: keyset.final_expiry,
        };
        let expected = serde_json::to_vec(&metadata.mint_info).unwrap().len()
            + serde_json::to_vec(&info).unwrap().len()
            + serde_json::to_vec(&keyset.keys).unwrap().len();
        metadata.keysets.insert(info.id, Arc::new(info));
        metadata
            .keys
            .insert(keyset.id, Arc::new(keyset.keys.clone()));
        let limits = MintMetadataLimits {
            max_keysets: 1,
            max_bytes: expected as u32,
        };
        let mut budget = Budget::new(&metadata, Some(limits)).unwrap();
        assert_eq!(budget.bytes, expected as u64);
        budget
            .replace_keys(Some(&keyset.keys), &keyset.keys)
            .unwrap();
        assert_eq!(budget.bytes, expected as u64);
        assert!(matches!(
            Budget::new(
                &metadata,
                Some(MintMetadataLimits {
                    max_bytes: limits.max_bytes - 1,
                    ..limits
                })
            ),
            Err(Error::MintMetadataTooLarge { .. })
        ));
        assert!(!limits.refusal(1, expected as u64).is_definitive_failure());
    }
}
