//! Durable receive preparation and typestate entry points.

use std::collections::HashMap;

use bitcoin::hashes::sha256::Hash as Sha256Hash;
use bitcoin::hashes::Hash;
use bitcoin::XOnlyPublicKey;
use tracing::instrument;

use self::state::{Finalized, Initial, Prepared};
use super::ReceiveOptions;
use crate::nuts::nut00::ProofsMethods;
use crate::nuts::nut10::Kind;
use crate::nuts::{Conditions, Proofs, PublicKey, SecretKey, SigFlag};
use crate::util::hex;
use crate::{Amount, Error, Wallet, SECP256K1};

mod admission;
#[cfg(test)]
mod completion_tests;
#[cfg(test)]
mod custody_tests;
mod execute;
mod finish;
#[cfg(test)]
mod inactive_keyset_tests;
mod receipt;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod replay_tests;
#[cfg(test)]
mod request_tests;
pub(crate) mod resume;
pub(crate) mod state;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod test_utils;

/// Saga pattern implementation for receive operations.
///
/// Uses the typestate pattern to enforce valid state transitions at compile-time.
/// Each state (Initial, Prepared, Finalized) is a distinct type, and operations
/// are only available on the appropriate type.
pub(crate) struct ReceiveSaga<'a, S> {
    /// Wallet reference
    wallet: &'a Wallet,
    /// State-specific data
    state_data: S,
}

impl<'a> ReceiveSaga<'a, Initial> {
    /// Create a new receive saga in the Initial state.
    pub fn new(wallet: &'a Wallet) -> Self {
        let operation_id = uuid::Uuid::now_v7();

        Self {
            wallet,
            state_data: Initial {
                operation_id,
                keyset_policy: Default::default(),
            },
        }
    }

    /// Prepare proofs for receiving.
    ///
    /// Verifies DLEQ proofs, signs P2PK proofs if keys provided, and adds HTLC preimages.
    /// No database changes are made in this step.
    #[instrument(skip_all)]
    pub async fn prepare(
        self,
        proofs: Proofs,
        opts: ReceiveOptions,
        memo: Option<String>,
    ) -> Result<ReceiveSaga<'a, Prepared>, Error> {
        tracing::info!(
            "Preparing receive for {} proofs with operation {}",
            proofs.len(),
            self.state_data.operation_id
        );

        let _mint_info = self.wallet.load_mint_info().await?;

        let keyset_policy = self.state_data.keyset_policy;
        let active_keyset_id = self
            .wallet
            .active_keyset_with_policy(keyset_policy)
            .await?
            .id;

        let mut proofs = proofs;
        let proofs_amount = proofs.total_amount()?;

        let mut derived_signing_keys = HashMap::new();

        // Map hash of preimage to preimage
        let hashed_to_preimage: HashMap<String, &String> = opts
            .preimages
            .iter()
            .map(|p| {
                let hex_bytes = hex::decode(p)?;
                Ok::<(String, &String), Error>((Sha256Hash::hash(&hex_bytes).to_string(), p))
            })
            .collect::<Result<HashMap<String, &String>, _>>()?;

        let mut p2pk_signing_keys: HashMap<XOnlyPublicKey, SecretKey> = opts
            .p2pk_signing_keys
            .iter()
            .map(|s| (s.x_only_public_key(&SECP256K1).0, s.clone()))
            .collect();

        // Process each proof: verify DLEQ, handle P2PK/HTLC
        for proof in &mut proofs {
            // Verify that proof DLEQ is valid
            if proof.dleq.is_some() {
                let keys = self
                    .wallet
                    .keyset_with_policy(proof.keyset_id, keyset_policy)
                    .await?
                    .keys;
                let key = keys.amount_key(proof.amount).ok_or(Error::AmountKey)?;
                proof.verify_dleq(key)?;
            }

            if let Ok(secret) =
                <crate::secret::Secret as TryInto<crate::nuts::nut10::Secret>>::try_into(
                    proof.secret.clone(),
                )
            {
                let conditions: Result<Conditions, _> = secret
                    .secret_data()
                    .tags()
                    .cloned()
                    .unwrap_or_default()
                    .try_into();
                if let Ok(conditions) = conditions {
                    let mut pubkeys = Vec::new();

                    match secret.kind() {
                        Kind::P2PK => {
                            let data_key = PublicKey::from_str(secret.secret_data().data())?;
                            pubkeys.push(data_key);
                        }
                        Kind::HTLC => {
                            // HTLC data is a hash, not a pubkey.
                            // Add the pre-image and skip slot 0 pubkey.
                            let hashed_preimage = secret.secret_data().data();
                            let preimage = hashed_to_preimage
                                .get(hashed_preimage)
                                .ok_or(Error::PreimageNotProvided)?;
                            proof.add_preimage(preimage.to_string());

                            // For HTLC, there is no slot 0 pubkey. But slot index for the tags still starts at 1!
                        }
                    }
                    if let Some(mut cond_pubkeys) = conditions.pubkeys {
                        pubkeys.append(&mut cond_pubkeys);
                    }
                    if let Some(mut refund_keys) = conditions.refund_keys {
                        pubkeys.append(&mut refund_keys);
                    }

                    for (i, pubkey) in pubkeys.iter().enumerate() {
                        let slot = match secret.kind() {
                            Kind::P2PK => i as u8,
                            _ => (i + 1) as u8, // HTLC skips slot 0 since it's a hash, not a pubkey
                        };
                        let x_only_pubkey = pubkey.x_only_public_key();

                        if let std::collections::hash_map::Entry::Vacant(entry) =
                            p2pk_signing_keys.entry(x_only_pubkey)
                        {
                            if let Some(secret_key) = self.wallet.get_signing_key(pubkey).await? {
                                entry.insert(secret_key.clone());
                            }
                        }

                        if let Some(ephemeral_key) = proof.p2pk_e {
                            for signing_key in p2pk_signing_keys.values() {
                                if let Ok(r) =
                                    crate::nuts::nut28::ecdh_kdf(signing_key, &ephemeral_key, slot)
                                {
                                    if let Ok(derived_key) =
                                        crate::nuts::nut28::derive_signing_key_bip340(
                                            signing_key,
                                            &r,
                                            pubkey,
                                        )
                                    {
                                        derived_signing_keys
                                            .insert(x_only_pubkey, derived_key.clone());
                                        proof.sign_p2pk(derived_key)?;
                                        break;
                                    }
                                }
                            }
                        } else if let Some(signing) = p2pk_signing_keys.get(&x_only_pubkey) {
                            proof.sign_p2pk(signing.to_owned().clone())?;
                        }
                    }

                    if conditions.sig_flag != SigFlag::SigAll {
                        match secret.kind() {
                            Kind::P2PK => proof.verify_p2pk()?,
                            Kind::HTLC => proof.verify_htlc()?,
                        }
                    }
                }
            }
        }

        p2pk_signing_keys.extend(derived_signing_keys);
        Ok(ReceiveSaga {
            wallet: self.wallet,
            state_data: Prepared {
                operation_id: self.state_data.operation_id,
                options: opts,
                memo,
                proofs,
                proofs_amount,
                active_keyset_id,
                p2pk_signing_keys,
            },
        })
    }
}

impl<'a> ReceiveSaga<'a, Prepared> {
    /// Determine the signature flag based on the proofs
    fn determine_sig_flag(&self) -> Result<SigFlag, Error> {
        for proof in &self.state_data.proofs {
            if let Ok(secret) =
                <crate::secret::Secret as TryInto<crate::nuts::nut10::Secret>>::try_into(
                    proof.secret.clone(),
                )
            {
                let conditions: Result<Conditions, _> = secret
                    .secret_data()
                    .tags()
                    .cloned()
                    .unwrap_or_default()
                    .try_into();
                if let Ok(conditions) = conditions {
                    if conditions.sig_flag == SigFlag::SigAll {
                        return Ok(SigFlag::SigAll);
                    }
                }
            }
        }
        Ok(SigFlag::SigInputs)
    }
}

impl<'a> ReceiveSaga<'a, Finalized> {
    /// Consume the saga and return the received amount
    pub fn into_amount(self) -> Amount {
        self.state_data.amount
    }
}

// Required import for PublicKey::from_str
use std::str::FromStr;

impl<S> std::fmt::Debug for ReceiveSaga<'_, S>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveSaga")
            .field("state_data", &self.state_data)
            .finish_non_exhaustive()
    }
}
