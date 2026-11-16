//! Swap module for the wallet.
//!
//! This module provides functionality for swapping proofs.

use cdk_common::amount::FeeAndAmounts;
use cdk_common::Id;
use tracing::instrument;

use crate::amount::SplitTarget;
use crate::fees::ProofsFeeBreakdown;
use crate::nuts::nut00::ProofsMethods;
use crate::nuts::{PreMintSecrets, PreSwap, Proofs, SpendingConditions, SwapRequest};
use crate::{Amount, Error, Wallet};

pub(crate) mod outputs;
pub(crate) mod saga;

use saga::SwapSaga;

impl Wallet {
    /// Swap proofs using the saga pattern.
    ///
    /// This method reserves the input proofs before performing the swap,
    /// ensuring they cannot be used by concurrent operations.
    #[instrument(skip(self, input_proofs))]
    pub async fn swap(
        &self,
        amount: Option<Amount>,
        amount_split_target: SplitTarget,
        input_proofs: Proofs,
        spending_conditions: Option<SpendingConditions>,
        include_fees: bool,
        use_p2bk: bool,
    ) -> Result<Option<Proofs>, Error> {
        tracing::info!("Swapping");

        let saga = SwapSaga::new(self)
            .prepare(
                amount,
                amount_split_target,
                input_proofs,
                spending_conditions,
                use_p2bk,
                include_fees,
            )
            .await?;
        Ok(saga.execute().await?.into_send_proofs())
    }

    /// Construct a private swap proposal without reserving inputs or outputs.
    #[instrument(skip(self, proofs))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_swap(
        &self,
        active_keyset_id: Id,
        fee_and_amounts: &FeeAndAmounts,
        amount: Option<Amount>,
        amount_split_target: SplitTarget,
        proofs: Proofs,
        spending_conditions: Option<SpendingConditions>,
        include_fees: bool,
        use_p2bk: bool,
        proofs_fee_breakdown: &ProofsFeeBreakdown,
        starting_counter: u32,
    ) -> Result<PreSwap, Error> {
        tracing::info!("Creating swap");

        // Desired amount is either amount passed or value of all proof
        let proofs_total = proofs.total_amount()?;

        let total_to_subtract = amount
            .unwrap_or(Amount::ZERO)
            .checked_add(proofs_fee_breakdown.total)
            .ok_or(Error::AmountOverflow)?;

        let change_amount: Amount = proofs_total
            .checked_sub(total_to_subtract)
            .ok_or(Error::InsufficientFunds)?;

        let (send_amount, change_amount) = match include_fees {
            true => {
                let split_count = amount
                    .unwrap_or(Amount::ZERO)
                    .split_count(fee_and_amounts)?;

                let fee_to_redeem = self
                    .get_keyset_count_fee(&active_keyset_id, split_count)
                    .await?;

                (
                    amount
                        .map(|a| a.checked_add(fee_to_redeem).ok_or(Error::AmountOverflow))
                        .transpose()?,
                    change_amount
                        .checked_sub(fee_to_redeem)
                        .ok_or(Error::InsufficientFunds)?,
                )
            }
            false => (amount, change_amount),
        };

        let send_count = send_amount
            .unwrap_or(Amount::ZERO)
            .split_count(fee_and_amounts)?;
        self.check_output_count(send_count)?;

        // If a non None split target is passed use that
        // else use state refill
        let change_split_target = match amount_split_target {
            SplitTarget::None => {
                self.determine_split_target_values(change_amount, fee_and_amounts)
                    .await?
            }
            s => s,
        };

        let derived_secret_count;

        let change_count =
            change_amount.split_targeted_count(&change_split_target, fee_and_amounts)?;
        let output_count = self.check_output_count(
            send_count
                .checked_add(change_count)
                .ok_or(Error::AmountOverflow)?,
        )?;
        let total_secrets_needed = if spending_conditions.is_some() {
            u32::try_from(change_count).map_err(|_| Error::AmountOverflow)?
        } else {
            output_count
        };

        // The caller publishes this private range with its recovery plan.
        starting_counter
            .checked_add(total_secrets_needed)
            .ok_or(Error::AmountOverflow)?;

        let mut count = starting_counter;

        let mut p2bk_ephemeral_key = None;
        let (mut desired_messages, change_messages) = match spending_conditions {
            Some(conditions) => {
                let change_premint_secrets = PreMintSecrets::from_seed(
                    active_keyset_id,
                    count,
                    &self.seed,
                    change_amount,
                    &change_split_target,
                    fee_and_amounts,
                )?;

                derived_secret_count = change_premint_secrets.len();

                let (send_secrets, ephemeral_key) = if use_p2bk {
                    if let SpendingConditions::P2PKConditions { data, conditions } = conditions {
                        let is_sig_all = conditions
                            .as_ref()
                            .is_some_and(|c| c.sig_flag == crate::nuts::nut11::SigFlag::SigAll);
                        let keys_count = if is_sig_all { 1 } else { send_count as usize };
                        let ephemeral_keys: Vec<_> = (0..keys_count)
                            .map(|_| crate::nuts::nut01::SecretKey::generate())
                            .collect();
                        (
                            PreMintSecrets::with_p2bk(
                                active_keyset_id,
                                send_amount.unwrap_or(Amount::ZERO),
                                &SplitTarget::default(),
                                data,
                                conditions,
                                &ephemeral_keys,
                                fee_and_amounts,
                            )?,
                            Some(ephemeral_keys),
                        )
                    } else {
                        return Err(Error::Custom("P2BK requires P2PK conditions".to_string()));
                    }
                } else {
                    (
                        PreMintSecrets::with_conditions(
                            active_keyset_id,
                            send_amount.unwrap_or(Amount::ZERO),
                            &SplitTarget::default(),
                            &conditions,
                            fee_and_amounts,
                        )?,
                        None,
                    )
                };

                p2bk_ephemeral_key = ephemeral_key;
                (send_secrets, change_premint_secrets)
            }
            None => {
                let premint_secrets = PreMintSecrets::from_seed(
                    active_keyset_id,
                    count,
                    &self.seed,
                    send_amount.unwrap_or(Amount::ZERO),
                    &SplitTarget::default(),
                    fee_and_amounts,
                )?;

                count += premint_secrets.len() as u32;

                let change_premint_secrets = PreMintSecrets::from_seed(
                    active_keyset_id,
                    count,
                    &self.seed,
                    change_amount,
                    &change_split_target,
                    fee_and_amounts,
                )?;

                derived_secret_count = change_premint_secrets.len() + premint_secrets.len();

                (premint_secrets, change_premint_secrets)
            }
        };

        // Combine the BlindedMessages totaling the desired amount with change
        desired_messages.combine(change_messages);
        // Sort the premint secrets to avoid finger printing
        desired_messages.sort_secrets();

        let swap_request = SwapRequest::new(proofs, desired_messages.blinded_messages());

        Ok(PreSwap {
            pre_mint_secrets: desired_messages,
            swap_request,
            derived_secret_count: derived_secret_count as u32,
            fee: proofs_fee_breakdown.total,
            p2bk_secret_keys: p2bk_ephemeral_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cdk_common::wallet::{KeysetLoadPolicy, ProofInfo};
    use cdk_common::CurrencyUnit;

    use crate::amount::SplitTarget;
    use crate::nuts::State;
    use crate::wallet::test_utils::{
        create_test_db, create_test_wallet_with_mock, make_inactive_keyset, test_keyset,
        test_keyset_id, test_mint_url, test_proof, MockMintConnector,
    };
    use crate::Error;

    /// A final inactive-keyset response cannot exclude an earlier successful retry.
    #[tokio::test]
    async fn inactive_keyset_after_submission_retains_exact_request_and_inputs() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        db.add_mint(mint_url.clone(), None).await.unwrap();

        let mock = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mock.clone()).await;

        // Prime cache with keyset A as active
        wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();

        let keyset_a_id = test_keyset_id();

        // Store proofs in the DB so reserve_proofs can find them.
        // Use amounts 1+2=3 so that after fee (1 sat for 2 inputs), the output
        // amount of 2 is expressible in keyset B's shifted denominations (min 2).
        let proof1 = test_proof(keyset_a_id, 1);
        let proof2 = test_proof(keyset_a_id, 2);
        let pi1 = ProofInfo::new(
            proof1.clone(),
            mint_url.clone(),
            State::Unspent,
            CurrencyUnit::Sat,
        )
        .unwrap();
        let pi2 =
            ProofInfo::new(proof2.clone(), mint_url, State::Unspent, CurrencyUnit::Sat).unwrap();
        let proof_ys = vec![pi1.y, pi2.y];
        db.update_proofs(vec![pi1, pi2], vec![]).await.unwrap();

        // Rotate keysets on the mock: A becomes inactive, B becomes active
        let mut old = test_keyset();
        old.active = Some(false);
        let mut rotated = make_inactive_keyset();
        rotated.active = Some(true);
        mock.set_mint_keys_response(Ok(vec![old, rotated]));

        // A final rejection may follow a successful internal connector retry.
        mock.push_post_swap_response(Err(Error::InactiveKeyset));
        mock.push_post_swap_response(Err(Error::TokenAlreadySpent));

        let result = wallet
            .swap(
                None,
                SplitTarget::default(),
                vec![proof1, proof2],
                None,
                false,
                false,
            )
            .await;

        assert!(matches!(result, Err(Error::InactiveKeyset)));
        let requests = mock.captured_swap_requests();
        assert_eq!(
            requests.len(),
            1,
            "do not create a new request after submission"
        );
        assert!(requests[0]
            .outputs()
            .iter()
            .all(|o| o.keyset_id == keyset_a_id));
        let saved = db.get_incomplete_sagas().await.unwrap();
        assert_eq!(saved.len(), 1);
        let cdk_common::wallet::OperationData::Swap(data) = &saved[0].data else {
            panic!("swap");
        };
        assert_eq!(
            data.checkpoint.as_ref().unwrap().pre_swap.swap_request,
            requests[0]
        );

        let stored = db.get_proofs_by_ys(proof_ys).await.unwrap();
        assert!(stored.iter().all(|proof| proof.state == State::Reserved));
        assert!(stored.iter().all(|proof| proof.used_by_operation.is_some()));
    }

    #[tokio::test]
    async fn swap_already_signed_error_keeps_inputs_reserved() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        db.add_mint(mint_url.clone(), None).await.unwrap();

        let mock = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mock.clone()).await;
        wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();

        let proof1 = test_proof(test_keyset_id(), 1);
        let proof2 = test_proof(test_keyset_id(), 2);
        let pi1 = ProofInfo::new(
            proof1.clone(),
            mint_url.clone(),
            State::Unspent,
            CurrencyUnit::Sat,
        )
        .unwrap();
        let pi2 =
            ProofInfo::new(proof2.clone(), mint_url, State::Unspent, CurrencyUnit::Sat).unwrap();
        let proof_ys = vec![pi1.y, pi2.y];
        db.update_proofs(vec![pi1, pi2], vec![]).await.unwrap();

        mock.set_post_swap_response(Err(Error::BlindedMessageAlreadySigned));

        let result = wallet
            .swap(
                None,
                SplitTarget::default(),
                vec![proof1, proof2],
                None,
                false,
                false,
            )
            .await;

        assert!(matches!(result, Err(Error::BlindedMessageAlreadySigned)));
        let stored = db.get_proofs_by_ys(proof_ys).await.unwrap();
        assert!(stored.iter().all(|proof| proof.state == State::Reserved));
        assert!(stored.iter().all(|proof| proof.used_by_operation.is_some()));
    }

    /// When the mint returns InactiveKeyset but the active keyset hasn't
    /// changed, the wallet should NOT retry and return the error immediately.
    #[tokio::test]
    async fn swap_does_not_retry_when_keyset_unchanged() {
        let db = create_test_db().await;
        let mint_url = test_mint_url();
        db.add_mint(mint_url.clone(), None).await.unwrap();

        let mock = Arc::new(MockMintConnector::new());
        let wallet = create_test_wallet_with_mock(db.clone(), mock.clone()).await;

        // Prime cache with keyset A as active
        wallet.keysets(KeysetLoadPolicy::Refresh).await.unwrap();

        // Store proofs in the DB
        let proof1 = test_proof(test_keyset_id(), 1);
        let proof2 = test_proof(test_keyset_id(), 2);
        let pi1 = ProofInfo::new(
            proof1.clone(),
            mint_url.clone(),
            State::Unspent,
            CurrencyUnit::Sat,
        )
        .unwrap();
        let pi2 =
            ProofInfo::new(proof2.clone(), mint_url, State::Unspent, CurrencyUnit::Sat).unwrap();
        db.update_proofs(vec![pi1, pi2], vec![]).await.unwrap();

        // Don't rotate keysets — mock still returns keyset A as active
        mock.push_post_swap_response(Err(Error::InactiveKeyset));

        let result = wallet
            .swap(
                None,
                SplitTarget::default(),
                vec![proof1, proof2],
                None,
                false,
                false,
            )
            .await;

        assert!(
            matches!(result, Err(Error::InactiveKeyset)),
            "expected InactiveKeyset without rotation, got: {result:?}"
        );

        let requests = mock.captured_swap_requests();
        assert_eq!(
            requests.len(),
            1,
            "post_swap should be called only once (no retry)"
        );
    }
}
