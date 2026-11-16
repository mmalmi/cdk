//! State types for the Mint (Issue) saga.
//!
//! Each state is a distinct type that holds the data relevant to that stage
//! of the mint operation. The type state pattern ensures that only valid
//! operations are available at each stage.

use core::fmt;

use cdk_common::wallet::{KeysetLoadPolicy, MintCheckpoint, OperationData, WalletSaga};
use uuid::Uuid;

use crate::nuts::Proofs;
use crate::Error;

/// Initial state - operation ID assigned, no work done yet.
#[derive(Debug)]
pub struct Initial {
    /// Unique operation identifier for tracking and crash recovery
    pub operation_id: Uuid,
    /// Policy controlling how keysets are loaded during this saga
    pub keyset_policy: KeysetLoadPolicy,
}

/// Prepared state - the original request and secrets are held only in the saved saga.
#[derive(Debug)]
pub struct Prepared {
    /// Policy controlling how keysets are loaded
    pub keyset_policy: KeysetLoadPolicy,
    /// Persisted saga for optimistic locking and recovery
    pub saga: WalletSaga,
}

impl Prepared {
    pub(super) fn checkpoint(&self) -> Result<&MintCheckpoint, Error> {
        let OperationData::Mint(data) = &self.saga.data else {
            return Err(Error::InvalidOperationState);
        };
        super::checkpoint::get(data)
    }
}

/// Finalized state - mint completed successfully, proofs available.
pub struct Finalized {
    /// Minted proofs
    pub proofs: Proofs,
}

impl fmt::Debug for Finalized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Finalized")
            .field(
                "proof_amounts",
                &self
                    .proofs
                    .iter()
                    .map(|proof| proof.amount)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use cdk_common::wallet::{
        IssueSagaState, KeysetLoadPolicy, OperationData, WalletSaga, WalletSagaState,
    };
    use cdk_common::CurrencyUnit;
    use uuid::Uuid;

    use super::{Finalized, Prepared};
    use crate::nuts::{PreMintSecrets, SecretKey};
    use crate::secret::Secret;
    use crate::wallet::test_utils::{
        test_issue_data, test_keyset_id, test_mint_quote, test_mint_url, test_proof,
    };

    #[allow(clippy::use_debug)]
    #[test]
    fn issue_state_debug_redacts_secrets_but_keeps_quote_ids() {
        let marker = "super_secret_spending_material_xyz";
        let keyset_id = test_keyset_id();
        let premint_secrets =
            PreMintSecrets::from_secrets(keyset_id, vec![1.into()], vec![Secret::new(marker)])
                .unwrap();
        let signing_key = SecretKey::generate();
        let signing_key_hex = signing_key.to_secret_hex();
        let mut quote = test_mint_quote(test_mint_url());
        quote.secret_key = Some(signing_key);
        let quote_id = quote.id.clone();
        let prepared = Prepared {
            keyset_policy: KeysetLoadPolicy::default(),
            saga: WalletSaga::new(
                Uuid::new_v4(),
                WalletSagaState::Issue(IssueSagaState::SecretsPrepared),
                1.into(),
                test_mint_url(),
                CurrencyUnit::Sat,
                OperationData::Mint(test_issue_data(quote, premint_secrets)),
            ),
        };
        let mut proof = test_proof(keyset_id, 1);
        proof.secret = Secret::new(marker);
        let finalized = Finalized {
            proofs: vec![proof],
        };
        let prepared_debug = format!("{prepared:?}");
        let finalized_debug = format!("{finalized:?}");
        assert!(!prepared_debug.contains(marker));
        assert!(!prepared_debug.contains(&signing_key_hex));
        assert!(!finalized_debug.contains(marker));
        assert!(prepared_debug.contains(&quote_id));
    }
}
