//! One durable send owns its exact mint request through confirmation and recovery.
use std::collections::HashMap;

use cdk_common::wallet::{
    OperationData, SendConfirmation, SendSagaState, SwapOutputs, Transaction, TransactionDirection,
    WalletSaga, WalletSagaState,
};

use crate::amount::SplitTarget;
use crate::nuts::{Id, Proofs, ProofsMethods, Token};
use crate::{Amount, Error, Wallet};

fn invalid(message: &str) -> Error {
    Error::InvalidMintResponse(message.to_string())
}

pub(super) async fn save_preparation(
    wallet: &Wallet,
    expected: &WalletSaga,
    next: &WalletSaga,
    proposal: Option<(Id, u32)>,
) -> Result<(), Error> {
    let OperationData::Send(data) = &next.data else {
        return Err(Error::InvalidOperationState);
    };
    let swap = data
        .confirmation
        .as_ref()
        .ok_or(Error::InvalidOperationState)?
        .pre_swap
        .as_ref();
    match (proposal, swap) {
        (Some((keyset, counter)), Some(swap)) => {
            crate::wallet::swap::outputs::validate_request(swap)?;
            wallet
                .commit_swap_preparation(Some(expected), next, keyset, counter, swap)
                .await
        }
        (None, None) => {
            if !wallet.localstore.update_saga(next.clone()).await? {
                return Err(Error::ConcurrentUpdate);
            }
            Ok(())
        }
        _ => Err(Error::InvalidOperationState),
    }
}

impl Wallet {
    pub(crate) async fn complete_send_confirmation(
        &self,
        mut saga: WalletSaga,
        recovering: bool,
    ) -> Result<(Token, Proofs, WalletSaga), Error> {
        if saga.state != WalletSagaState::Send(SendSagaState::Confirming)
            || saga.mint_url != self.mint_url
            || saga.unit != self.unit
        {
            return Err(invalid("send confirmation identity mismatch"));
        }
        self.ensure_send_storage(Some(&saga)).await?;
        let OperationData::Send(data) = &saga.data else {
            return Err(invalid("missing send confirmation data"));
        };
        let mut intent = data
            .confirmation
            .clone()
            .ok_or_else(|| invalid("missing exact send confirmation"))?;
        super::custody::validate_send_metadata(&intent.metadata)?;
        if let Some(swap) = &intent.pre_swap {
            crate::wallet::swap::outputs::validate_request(swap)?;
        }
        if intent.ready.is_none() {
            let mut outputs = match &intent.pre_swap {
                Some(pre_swap) => {
                    let response = if recovering {
                        crate::wallet::swap::outputs::restore(self, pre_swap).await?
                    } else {
                        self.client.post_swap(pre_swap.swap_request.clone()).await?
                    };
                    crate::wallet::swap::outputs::resolve(
                        self,
                        pre_swap,
                        Some(intent.swap_amount),
                        &SplitTarget::None,
                        intent.spending_conditions.as_ref(),
                        response,
                    )
                    .await?
                }
                None => SwapOutputs {
                    send: Vec::new(),
                    change: Vec::new(),
                },
            };
            let mut send = intent.retained_proofs.clone();
            send.append(&mut outputs.send);
            outputs.send = send;
            intent.ready = Some(outputs);
            self.validate_send_result(saga.amount, &intent).await?;
            let OperationData::Send(ref mut data) = saga.data else {
                return Err(invalid("missing send confirmation data"));
            };
            data.confirmation = Some(intent.clone());
            saga.update_state(WalletSagaState::Send(SendSagaState::Confirming));
            if !self.localstore.update_saga(saga.clone()).await? {
                return Err(Error::ConcurrentUpdate);
            }
        } else {
            self.validate_send_result(saga.amount, &intent).await?;
        }
        let outputs = intent
            .ready
            .as_ref()
            .ok_or_else(|| invalid("missing verified outputs"))?;
        self.import_send_outputs(&saga, &intent, outputs).await?;
        let swap_fee = intent.pre_swap.as_ref().map_or(Amount::ZERO, |p| p.fee);
        let receipt = Transaction {
            role: cdk_common::wallet::TransactionRole::Primary,
            mint_url: self.mint_url.clone(),
            direction: TransactionDirection::Outgoing,
            amount: saga.amount,
            fee: swap_fee
                .checked_add(intent.send_fee)
                .ok_or(Error::AmountOverflow)?,
            unit: self.unit.clone(),
            ys: outputs.send.ys()?,
            timestamp: saga.created_at,
            memo: intent.token_memo.clone(),
            metadata: intent.metadata.clone(),
            quote_id: None,
            payment_request: None,
            payment_proof: None,
            payment_method: None,
            saga_id: Some(saga.id),
            status: cdk_common::wallet::TransactionStatus::Pending,
        };
        let expected = saga.clone();
        let token = Token::new(
            self.mint_url.clone(),
            outputs.send.clone(),
            intent.token_memo,
            self.unit.clone(),
        );
        let OperationData::Send(ref mut data) = saga.data else {
            return Err(invalid("missing send confirmation data"));
        };
        data.token = Some(token.to_string());
        data.proofs = Some(outputs.send.clone());
        data.confirmation = None;
        saga.update_state(WalletSagaState::Send(SendSagaState::TokenCreated));
        if !self
            .localstore
            .complete_saga(&expected, saga.clone(), receipt)
            .await?
        {
            return Err(Error::ConcurrentUpdate);
        }
        Ok((token, outputs.send.clone(), saga))
    }

    async fn validate_send_result(
        &self,
        amount: Amount,
        intent: &SendConfirmation,
    ) -> Result<(), Error> {
        let outputs = intent
            .ready
            .as_ref()
            .ok_or_else(|| invalid("missing verified send outputs"))?;
        let token_value = outputs.send.total_amount()?;
        let swap_fee = intent.pre_swap.as_ref().map_or(Amount::ZERO, |p| p.fee);
        let debit = token_value
            .checked_add(swap_fee)
            .ok_or(Error::AmountOverflow)?;
        if let Some(maximum) = intent.max_total_amount {
            if debit > maximum {
                return Err(Error::MaxAmountExceeded {
                    amount: debit,
                    maximum,
                });
            }
        }
        if intent
            .required_keyset_id
            .is_some_and(|id| outputs.send.iter().any(|p| p.keyset_id != id))
        {
            return Err(Error::InactiveKeyset);
        }
        let redemption = if intent.include_fee {
            self.get_proofs_fee(&outputs.send).await?.total
        } else {
            Amount::ZERO
        };
        if token_value
            .checked_sub(redemption)
            .is_none_or(|net| net < amount)
        {
            return Err(Error::MaxFeeExceeded);
        }
        let input = intent
            .retained_proofs
            .total_amount()?
            .checked_add(
                intent
                    .pre_swap
                    .as_ref()
                    .map(|p| p.swap_request.inputs().total_amount())
                    .transpose()?
                    .unwrap_or(Amount::ZERO),
            )
            .ok_or(Error::AmountOverflow)?;
        let output = token_value
            .checked_add(outputs.change.total_amount()?)
            .ok_or(Error::AmountOverflow)?;
        if input.checked_sub(swap_fee) != Some(output) {
            return Err(invalid("send confirmation value mismatch"));
        }
        let mut expected = HashMap::new();
        for proof in &intent.retained_proofs {
            if !outputs.send.contains(proof)
                || expected
                    .insert(proof.y()?, (proof.keyset_id, proof.amount))
                    .is_some()
            {
                return Err(invalid("changed retained send proof"));
            }
        }
        if let Some(swap) = &intent.pre_swap {
            for secret in &swap.pre_mint_secrets.secrets {
                let y = crate::dhke::hash_to_curve(secret.secret.as_bytes())?;
                if expected
                    .insert(y, (secret.blinded_message.keyset_id, secret.amount))
                    .is_some()
                {
                    return Err(invalid("duplicate send output secret"));
                }
            }
        }
        for proof in outputs.send.iter().chain(&outputs.change) {
            if expected.remove(&proof.y()?) != Some((proof.keyset_id, proof.amount)) {
                return Err(invalid("unexpected send confirmation proof"));
            }
            if proof.dleq.is_some() {
                let keys = self.keyset(proof.keyset_id).await?.keys;
                let key = keys.amount_key(proof.amount).ok_or(Error::AmountKey)?;
                proof
                    .verify_dleq(key)
                    .map_err(|_| Error::CouldNotVerifyDleq)?;
            }
        }
        if !expected.is_empty() {
            return Err(invalid("missing send confirmation proof"));
        }
        Ok(())
    }
}
