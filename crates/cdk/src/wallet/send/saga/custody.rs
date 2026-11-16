//! One fenced proof transaction: consume swap inputs and publish verified outputs.
use std::collections::{HashMap, HashSet};

use cdk_common::database::wallet::ProofChange;
use cdk_common::wallet::{ProofInfo, SendConfirmation, SwapOutputs, WalletSaga};

use crate::nuts::{Proof, ProofsMethods, State, Witness};
use crate::wallet::util::same_coin;
use crate::{Error, Wallet};

pub(super) fn validate_send_metadata(metadata: &HashMap<String, String>) -> Result<(), Error> {
    // This key selects the mint-batch receipt namespace; sends own one primary ID.
    if metadata.contains_key("batch_quote_id") {
        return Err(Error::Custom(
            "Send metadata cannot contain reserved batch_quote_id".to_owned(),
        ));
    }
    Ok(())
}

// Signing may extend a witness, but an older journal must never remove local
// signatures or replace an already known HTLC preimage.
fn contains_witness(new: &Witness, old: &Witness) -> bool {
    match (new, old) {
        (Witness::P2PKWitness(new), Witness::P2PKWitness(old)) => {
            old.signatures.iter().all(|s| new.signatures.contains(s))
        }
        (Witness::HTLCWitness(new), Witness::HTLCWitness(old)) => {
            new.preimage == old.preimage
                && old
                    .signatures
                    .iter()
                    .flatten()
                    .all(|s| new.signatures.iter().flatten().any(|n| n == s))
        }
        _ => false,
    }
}

// A retained coin keeps its local evidence even when the outgoing token omits
// DLEQ or P2BK metadata. Only a verified witness extension is adopted.
fn retain_evidence(original: &ProofInfo, outgoing: &Proof) -> Result<ProofInfo, Error> {
    let mut result = original.clone();
    let Some(new) = &outgoing.witness else {
        return Ok(result);
    };
    if let Some(old) = &original.proof.witness {
        if contains_witness(old, new) {
            return Ok(result);
        }
        if !contains_witness(new, old) {
            return Err(Error::ConcurrentUpdate);
        }
    }
    match new {
        Witness::P2PKWitness(_) => {
            if !crate::wallet::util::is_p2pk_locked(outgoing) {
                return Err(Error::ConcurrentUpdate);
            }
            outgoing.verify_p2pk()?;
        }
        Witness::HTLCWitness(_) => outgoing.verify_htlc()?,
    }
    result.proof.witness = outgoing.witness.clone();
    Ok(result)
}

// Token transport may omit local metadata or carry a narrower known witness.
// It must still identify the same coin without replacing stored authorization.
pub(super) fn validate_retained_proof(original: &ProofInfo, outgoing: &Proof) -> Result<(), Error> {
    if !same_coin(&original.proof, outgoing) || retain_evidence(original, outgoing)? != *original {
        return Err(Error::ConcurrentUpdate);
    }
    Ok(())
}

impl Wallet {
    pub(crate) async fn ensure_send_storage(&self, saga: Option<&WalletSaga>) -> Result<(), Error> {
        if !self.localstore.supports_atomic_saga_completion() {
            return Err(cdk_common::database::Error::Internal(
                "Atomic saga completion is required for sends".to_owned(),
            )
            .into());
        }
        if !self
            .localstore
            .compare_and_swap_proofs(vec![], saga)
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        Ok(())
    }

    pub(super) async fn import_send_outputs(
        &self,
        saga: &WalletSaga,
        intent: &SendConfirmation,
        outputs: &SwapOutputs,
    ) -> Result<(), Error> {
        let inputs = intent
            .pre_swap
            .as_ref()
            .map(|p| p.swap_request.inputs().as_slice())
            .unwrap_or_default();
        let retained: HashSet<_> = intent.retained_proofs.ys()?.into_iter().collect();
        let mut ys = HashSet::new();
        for proof in inputs.iter().chain(&outputs.send).chain(&outputs.change) {
            if !ys.insert(proof.y()?) {
                return Err(Error::ConcurrentUpdate);
            }
        }
        let mut existing: HashMap<_, _> = self
            .localstore
            .get_proofs_by_ys(ys.into_iter().collect())
            .await?
            .into_iter()
            .map(|info| (info.y, info))
            .collect();
        let in_scope = |info: &ProofInfo, proof: &Proof| {
            info.mint_url == self.mint_url
                && info.unit == self.unit
                && same_coin(&info.proof, proof)
        };
        let derived = |proof: &Proof, state| -> Result<ProofInfo, Error> {
            let mut info = ProofInfo::new(
                proof.clone(),
                self.mint_url.clone(),
                state,
                self.unit.clone(),
            )?;
            info.created_by_operation = Some(saga.id);
            info.derivation_index = intent.pre_swap.as_ref().and_then(|swap| {
                swap.pre_mint_secrets
                    .secrets
                    .iter()
                    .find(|p| p.secret == proof.secret)
                    .and_then(|p| p.derivation_index)
            });
            Ok(info)
        };
        let mut changes = Vec::new();
        for proof in inputs {
            let y = proof.y()?;
            let before = existing.remove(&y).ok_or(Error::ConcurrentUpdate)?;
            if !in_scope(&before, proof)
                || before.used_by_operation != Some(saga.id)
                || !matches!(
                    before.state,
                    State::Reserved | State::Pending | State::Spent
                )
            {
                return Err(Error::ConcurrentUpdate);
            }
            let mut after = before.clone();
            after.state = State::Spent;
            changes.push(ProofChange {
                y,
                before: Some(before),
                after: Some(after),
            });
        }
        for proof in &outputs.send {
            let y = proof.y()?;
            let before = existing.remove(&y);
            let mut after = if retained.contains(&y) {
                let info = before.as_ref().ok_or(Error::ConcurrentUpdate)?;
                if !in_scope(info, proof)
                    || info.used_by_operation != Some(saga.id)
                    || !matches!(info.state, State::Reserved | State::PendingSpent)
                {
                    return Err(Error::ConcurrentUpdate);
                }
                retain_evidence(info, proof)?
            } else {
                let mut info = derived(proof, State::PendingSpent)?;
                info.used_by_operation = Some(saga.id);
                if before.as_ref().is_some_and(|old| old != &info) {
                    return Err(Error::ConcurrentUpdate);
                }
                info
            };
            after.state = State::PendingSpent;
            after.used_by_operation = Some(saga.id);
            changes.push(ProofChange {
                y,
                before,
                after: Some(after),
            });
        }
        for proof in &outputs.change {
            let y = proof.y()?;
            let before = existing.remove(&y);
            let expected = derived(proof, State::Unspent)?;
            if before.as_ref().is_some_and(|old| {
                !in_scope(old, proof)
                    || old.created_by_operation != expected.created_by_operation
                    || old.derivation_index != expected.derivation_index
            }) {
                return Err(Error::ConcurrentUpdate);
            }
            // Another operation may already own or have spent previously imported change.
            let after = before.clone().unwrap_or(expected);
            changes.push(ProofChange {
                y,
                before,
                after: Some(after),
            });
        }
        if !self
            .localstore
            .compare_and_swap_proofs(changes, Some(saga))
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nuts::{CurrencyUnit, ProofDleq, SecretKey, SpendingConditions};
    use crate::wallet::test_utils::{test_keyset, test_mint_url};

    #[test]
    fn retained_evidence_accepts_verified_signing_without_losing_metadata() {
        let signing_key = SecretKey::generate();
        let conditions = SpendingConditions::new_p2pk(signing_key.public_key(), None);
        let secret: crate::nuts::nut10::Secret = conditions.try_into().unwrap();
        let proof = Proof::new(
            8.into(),
            test_keyset().id,
            secret.try_into().unwrap(),
            SecretKey::generate().public_key(),
        );
        let mut original =
            ProofInfo::new(proof, test_mint_url(), State::Reserved, CurrencyUnit::Sat).unwrap();
        original.created_by_operation = Some(uuid::Uuid::new_v4());
        original.used_by_operation = Some(uuid::Uuid::new_v4());
        original.derivation_index = Some(42);
        original.proof.dleq = Some(ProofDleq::new(
            SecretKey::generate(),
            SecretKey::generate(),
            SecretKey::generate(),
        ));
        original.proof.p2pk_e = Some(SecretKey::generate().public_key());
        let mut outgoing = original.proof.clone();
        outgoing.dleq = None;
        outgoing.p2pk_e = None;
        outgoing.sign_p2pk(signing_key).unwrap();
        let mut expected = original.clone();
        expected.proof.witness = outgoing.witness.clone();
        assert_eq!(retain_evidence(&original, &outgoing).unwrap(), expected);
        // An older saved witness cannot erase newer locally retained signatures.
        outgoing.witness = None;
        assert_eq!(retain_evidence(&expected, &outgoing).unwrap(), expected);
        outgoing.witness = Some(Witness::P2PKWitness(crate::nuts::P2PKWitness {
            signatures: vec!["invalid".to_owned()],
        }));
        assert!(retain_evidence(&original, &outgoing).is_err());
    }
}
