//! Test-only interposition at real storage transaction boundaries.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bitcoin::bip32::DerivationPath;
use cdk_common::database::wallet::ProofChange;
use cdk_common::database::{Error as DbError, WalletDatabase};
use cdk_common::mint_url::MintUrl;
use cdk_common::nuts::{
    CurrencyUnit, Id, KeySetInfo, Keys, MintInfo, PublicKey, SpendingConditions, State,
};
use cdk_common::wallet::{
    self, MintQuote as WalletMintQuote, ProofInfo, Transaction, TransactionDirection, TransactionId,
};
use cdk_common::KeySet;

#[derive(Debug)]
pub(crate) enum Action {
    ReplaceProof(Box<ProofInfo>),
    RemoveSaga(uuid::Uuid),
    ReplaceSaga(wallet::WalletSaga),
    Retire(Transaction),
    Fail,
    CompareProofs(Vec<ProofChange>, wallet::WalletSaga),
    Pause {
        reached: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    },
}

#[derive(Debug)]
pub(crate) struct Interposed {
    pub inner: Arc<dyn WalletDatabase<DbError> + Send + Sync>,
    pub completion_supported: bool,
    pub reject_bulk_history: bool,
    pub melt_completion_supported: bool,
    pub proof_cas_supported: bool,
    pub before_acquisition: Mutex<Option<Action>>,
    pub before_proofs: Mutex<Option<Action>>,
    pub before_receipt: Mutex<Option<Action>>,
    pub before_melt_completion: Mutex<Option<Action>>,
    pub after_melt_completion: Mutex<Option<Action>>,
    pub before_delete_saga: Mutex<Option<Action>>,
    pub before_issue_retirement: Mutex<Option<Action>>,
    pub after_counter_increment: Mutex<Option<u32>>,
    pub before_saga: Mutex<Option<Action>>,
    pub after_saga: Mutex<Option<Action>>,
    pub after_proofs: Mutex<Option<Action>>,
    pub before_add_saga: Mutex<Option<Action>>,
    pub after_add_saga: Mutex<Option<Action>>,
    pub after_empty_proof_check: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl Interposed {
    pub fn new(inner: Arc<dyn WalletDatabase<DbError> + Send + Sync>) -> Self {
        Self {
            inner,
            completion_supported: true,
            reject_bulk_history: false,
            melt_completion_supported: true,
            proof_cas_supported: true,
            before_acquisition: Mutex::new(None),
            before_proofs: Mutex::new(None),
            before_receipt: Mutex::new(None),
            before_melt_completion: Mutex::new(None),
            after_melt_completion: Mutex::new(None),
            before_delete_saga: Mutex::new(None),
            before_issue_retirement: Mutex::new(None),
            after_counter_increment: Mutex::new(None),
            before_saga: Mutex::new(None),
            after_saga: Mutex::new(None),
            after_proofs: Mutex::new(None),
            before_add_saga: Mutex::new(None),
            after_add_saga: Mutex::new(None),
            after_empty_proof_check: Mutex::new(None),
        }
    }
    async fn after_allocation(&self, keyset_id: Id, count: u32) -> Result<(), DbError> {
        if count > 0 {
            let additional = self.after_counter_increment.lock().unwrap().take();
            if let Some(additional) = additional {
                self.inner
                    .increment_keyset_counter(&keyset_id, additional)
                    .await?;
            }
        }
        Ok(())
    }
    async fn fire(&self, hook: &Mutex<Option<Action>>) -> Result<(), DbError> {
        let action = hook.lock().unwrap().take();
        match action {
            Some(Action::ReplaceProof(proof)) => {
                self.inner.update_proofs(vec![*proof], vec![]).await?
            }
            Some(Action::RemoveSaga(id)) => self.inner.delete_saga(&id).await?,
            Some(Action::ReplaceSaga(saga)) => {
                self.inner.update_saga(saga).await?;
            }
            Some(Action::Retire(mut receipt)) => {
                receipt.status = wallet::TransactionStatus::Completed;
                self.inner.add_transaction(receipt.clone()).await?;
                self.inner
                    .update_proofs_state(receipt.ys.clone(), State::Spent)
                    .await?;
                self.inner.delete_saga(&receipt.saga_id.unwrap()).await?;
                assert!(self.inner.retire_send_transactions(vec![receipt]).await?);
            }
            Some(Action::CompareProofs(changes, saga)) => {
                assert!(
                    self.inner
                        .compare_and_swap_proofs(changes, Some(&saga))
                        .await?
                );
            }
            Some(Action::Fail) => return Err(DbError::Internal("Interrupted".to_owned())),
            Some(Action::Pause { reached, release }) => {
                reached.send(()).expect("storage observer remains alive");
                release.await.expect("storage observer releases the caller");
            }
            None => {}
        }
        Ok(())
    }
}
#[async_trait]
impl WalletDatabase<DbError> for Interposed {
    async fn get_mint(&self, mint_url: MintUrl) -> Result<Option<MintInfo>, DbError> {
        self.inner.get_mint(mint_url).await
    }

    async fn get_mints(&self) -> Result<HashMap<MintUrl, Option<MintInfo>>, DbError> {
        self.inner.get_mints().await
    }

    async fn get_mint_keysets(
        &self,
        mint_url: MintUrl,
    ) -> Result<Option<Vec<KeySetInfo>>, DbError> {
        self.inner.get_mint_keysets(mint_url).await
    }

    async fn get_keyset_by_id(&self, keyset_id: &Id) -> Result<Option<KeySetInfo>, DbError> {
        self.inner.get_keyset_by_id(keyset_id).await
    }

    async fn get_mint_quote(&self, quote_id: &str) -> Result<Option<WalletMintQuote>, DbError> {
        self.inner.get_mint_quote(quote_id).await
    }

    async fn get_mint_quotes(&self) -> Result<Vec<WalletMintQuote>, DbError> {
        self.inner.get_mint_quotes().await
    }

    async fn get_unissued_mint_quotes(&self) -> Result<Vec<WalletMintQuote>, DbError> {
        self.inner.get_unissued_mint_quotes().await
    }

    async fn get_unissued_mint_quotes_page(
        &self,
        mint_url: MintUrl,
        unit: CurrencyUnit,
        after: Option<String>,
        limit: u16,
    ) -> Result<Vec<WalletMintQuote>, DbError> {
        self.inner
            .get_unissued_mint_quotes_page(mint_url, unit, after, limit)
            .await
    }

    async fn get_melt_quote(&self, quote_id: &str) -> Result<Option<wallet::MeltQuote>, DbError> {
        self.inner.get_melt_quote(quote_id).await
    }

    async fn get_melt_quotes(&self) -> Result<Vec<wallet::MeltQuote>, DbError> {
        if self.reject_bulk_history {
            return Err(DbError::Internal(
                "bulk quote read forbidden by fixture".to_owned(),
            ));
        }
        self.inner.get_melt_quotes().await
    }

    async fn get_keys(&self, id: &Id) -> Result<Option<Keys>, DbError> {
        self.inner.get_keys(id).await
    }

    async fn get_proofs(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<State>>,
        spending_conditions: Option<Vec<SpendingConditions>>,
    ) -> Result<Vec<ProofInfo>, DbError> {
        self.inner
            .get_proofs(mint_url, unit, state, spending_conditions)
            .await
    }

    async fn get_proofs_by_ys(&self, ys: Vec<PublicKey>) -> Result<Vec<ProofInfo>, DbError> {
        self.inner.get_proofs_by_ys(ys).await
    }

    async fn get_balance(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<State>>,
    ) -> Result<u64, DbError> {
        self.inner.get_balance(mint_url, unit, state).await
    }

    async fn get_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<Transaction>, DbError> {
        self.inner.get_transaction(transaction_id).await
    }

    async fn list_transactions(
        &self,
        mint_url: Option<MintUrl>,
        direction: Option<TransactionDirection>,
        unit: Option<CurrencyUnit>,
    ) -> Result<Vec<Transaction>, DbError> {
        self.inner
            .list_transactions(mint_url, direction, unit)
            .await
    }

    async fn update_proofs(
        &self,
        added: Vec<ProofInfo>,
        removed_ys: Vec<PublicKey>,
    ) -> Result<(), DbError> {
        self.inner.update_proofs(added, removed_ys).await
    }

    async fn compare_and_swap_proofs(
        &self,
        changes: Vec<ProofChange>,
        expected_saga: Option<&wallet::WalletSaga>,
    ) -> Result<bool, DbError> {
        if !self.proof_cas_supported {
            return Err(DbError::Internal("Unsupported".to_owned()));
        }
        if !changes.is_empty() {
            self.fire(&self.before_proofs).await?;
        }
        let nonempty = !changes.is_empty();
        let updated = self
            .inner
            .compare_and_swap_proofs(changes, expected_saga)
            .await?;
        if updated && nonempty {
            self.fire(&self.after_proofs).await?;
        }
        if updated && !nonempty && expected_saga.is_some() {
            let pause = self.after_empty_proof_check.lock().unwrap().take();
            if let Some((reached, release)) = pause {
                reached
                    .send(())
                    .expect("proof-check observer remains alive");
                release
                    .await
                    .expect("proof-check observer releases the sender");
            }
        }
        Ok(updated)
    }

    async fn update_proofs_state(&self, ys: Vec<PublicKey>, state: State) -> Result<(), DbError> {
        self.inner.update_proofs_state(ys, state).await
    }

    async fn add_transaction(&self, transaction: Transaction) -> Result<(), DbError> {
        self.inner.add_transaction(transaction).await
    }

    async fn update_mint_url(
        &self,
        old_mint_url: MintUrl,
        new_mint_url: MintUrl,
    ) -> Result<(), DbError> {
        self.inner.update_mint_url(old_mint_url, new_mint_url).await
    }

    async fn prepare_saga_outputs(
        &self,
        expected: Option<&wallet::WalletSaga>,
        next: wallet::WalletSaga,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<Transaction>,
    ) -> Result<bool, DbError> {
        if expected.is_none() {
            self.fire(&self.before_add_saga).await?;
        }
        if receipt.is_some() {
            self.fire(&self.before_receipt).await?;
        }
        let keyset_id = keyset.id;
        let result = self
            .inner
            .prepare_saga_outputs(
                expected,
                next,
                keyset,
                counter_start,
                counter_count,
                receipt,
            )
            .await?;
        if result {
            self.after_allocation(keyset_id, counter_count).await?;
            if expected.is_none() {
                self.fire(&self.after_add_saga).await?;
            }
            self.fire(&self.after_saga).await?;
        }
        Ok(result)
    }

    async fn increment_keyset_counter(&self, keyset_id: &Id, count: u32) -> Result<u32, DbError> {
        let allocated = self
            .inner
            .increment_keyset_counter(keyset_id, count)
            .await?;
        self.after_allocation(*keyset_id, count).await?;
        Ok(allocated)
    }

    async fn increment_derivation_counter(
        &self,
        namespace: &str,
        count: u32,
    ) -> Result<u32, DbError> {
        self.inner
            .increment_derivation_counter(namespace, count)
            .await
    }

    async fn add_mint(
        &self,
        mint_url: MintUrl,
        mint_info: Option<MintInfo>,
    ) -> Result<(), DbError> {
        self.inner.add_mint(mint_url, mint_info).await
    }

    async fn remove_mint(&self, mint_url: MintUrl) -> Result<(), DbError> {
        self.inner.remove_mint(mint_url).await
    }

    async fn add_mint_keysets(
        &self,
        mint_url: MintUrl,
        keysets: Vec<KeySetInfo>,
    ) -> Result<(), DbError> {
        self.inner.add_mint_keysets(mint_url, keysets).await
    }

    async fn add_mint_quote(&self, quote: WalletMintQuote) -> Result<(), DbError> {
        self.inner.add_mint_quote(quote).await
    }

    async fn remove_mint_quote(&self, quote_id: &str) -> Result<(), DbError> {
        self.inner.remove_mint_quote(quote_id).await
    }

    async fn add_melt_quote(&self, quote: wallet::MeltQuote) -> Result<(), DbError> {
        self.inner.add_melt_quote(quote).await
    }

    async fn remove_melt_quote(&self, quote_id: &str) -> Result<(), DbError> {
        self.inner.remove_melt_quote(quote_id).await
    }

    async fn add_keys(&self, keyset: KeySet) -> Result<(), DbError> {
        self.inner.add_keys(keyset).await
    }

    async fn remove_keys(&self, id: &Id) -> Result<(), DbError> {
        self.inner.remove_keys(id).await
    }

    async fn remove_transaction(&self, transaction_id: TransactionId) -> Result<(), DbError> {
        self.inner.remove_transaction(transaction_id).await
    }

    async fn retire_send_transactions(&self, _expected: Vec<Transaction>) -> Result<bool, DbError> {
        self.inner.retire_send_transactions(_expected).await
    }

    async fn retire_incoming_transactions(
        &self,
        _expected: Vec<Transaction>,
    ) -> Result<bool, DbError> {
        self.inner.retire_incoming_transactions(_expected).await
    }

    async fn add_saga(&self, saga: wallet::WalletSaga) -> Result<(), DbError> {
        self.fire(&self.before_add_saga).await?;
        self.inner.add_saga(saga).await?;
        self.fire(&self.after_add_saga).await
    }

    async fn get_saga(&self, id: &uuid::Uuid) -> Result<Option<wallet::WalletSaga>, DbError> {
        self.inner.get_saga(id).await
    }

    async fn update_saga(&self, saga: wallet::WalletSaga) -> Result<bool, DbError> {
        self.fire(&self.before_saga).await?;
        let updated = self.inner.update_saga(saga).await?;
        if updated {
            self.fire(&self.after_saga).await?;
        }
        Ok(updated)
    }

    fn supports_atomic_saga_completion(&self) -> bool {
        self.completion_supported && self.inner.supports_atomic_saga_completion()
    }

    async fn complete_saga(
        &self,
        _expected: &wallet::WalletSaga,
        _next: wallet::WalletSaga,
        _receipt: Transaction,
    ) -> Result<bool, DbError> {
        self.fire(&self.before_receipt).await?;
        self.inner.complete_saga(_expected, _next, _receipt).await
    }

    fn supports_atomic_melt_completion(&self) -> bool {
        self.melt_completion_supported && self.inner.supports_atomic_melt_completion()
    }

    async fn complete_melt(
        &self,
        expected: &wallet::WalletSaga,
        next: wallet::WalletSaga,
        quote: &wallet::MeltQuote,
    ) -> Result<bool, DbError> {
        if !self.melt_completion_supported {
            return Err(DbError::Internal("Unsupported".to_owned()));
        }
        self.fire(&self.before_melt_completion).await?;
        let updated = self.inner.complete_melt(expected, next, quote).await?;
        if updated {
            self.fire(&self.after_melt_completion).await?;
        }
        Ok(updated)
    }

    async fn delete_saga(&self, id: &uuid::Uuid) -> Result<(), DbError> {
        self.fire(&self.before_delete_saga).await?;
        self.inner.delete_saga(id).await
    }

    async fn get_incomplete_saga_ids(
        &self,
        after: Option<cdk_common::database::wallet::SagaCursor>,
        limit: u16,
    ) -> Result<Vec<cdk_common::database::wallet::SagaCursor>, DbError> {
        self.inner.get_incomplete_saga_ids(after, limit).await
    }

    async fn get_incomplete_sagas(&self) -> Result<Vec<wallet::WalletSaga>, DbError> {
        if self.reject_bulk_history {
            return Err(DbError::Internal(
                "bulk saga read forbidden by fixture".to_owned(),
            ));
        }
        self.inner.get_incomplete_sagas().await
    }

    async fn reserve_proofs(
        &self,
        ys: Vec<PublicKey>,
        operation_id: &uuid::Uuid,
    ) -> Result<(), DbError> {
        self.inner.reserve_proofs(ys, operation_id).await
    }

    async fn release_proofs(&self, operation_id: &uuid::Uuid) -> Result<(), DbError> {
        self.inner.release_proofs(operation_id).await
    }

    async fn get_reserved_proofs(
        &self,
        operation_id: &uuid::Uuid,
    ) -> Result<Vec<ProofInfo>, DbError> {
        self.inner.get_reserved_proofs(operation_id).await
    }

    fn supports_atomic_issue_retirement(&self) -> bool {
        self.completion_supported && self.inner.supports_atomic_issue_retirement()
    }

    async fn retire_issue(
        &self,
        expected: &wallet::WalletSaga,
        quotes: &[wallet::MintQuote],
        proofs: Option<Vec<ProofChange>>,
    ) -> Result<bool, DbError> {
        self.fire(&self.before_issue_retirement).await?;
        self.inner.retire_issue(expected, quotes, proofs).await
    }

    async fn acquire_issue(
        &self,
        saga: &wallet::WalletSaga,
        quotes: &[wallet::MintQuote],
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, DbError> {
        self.fire(&self.before_acquisition).await?;
        let id = keyset.id;
        let acquired = self
            .inner
            .acquire_issue(saga, quotes, keyset, counter_start, counter_count)
            .await?;
        if acquired {
            self.after_allocation(id, counter_count).await?;
        }
        self.fire(&self.after_add_saga).await?;
        Ok(acquired)
    }

    async fn acquire_melt(
        &self,
        saga: &wallet::WalletSaga,
        quote: &wallet::MeltQuote,
    ) -> Result<bool, DbError> {
        self.fire(&self.before_acquisition).await?;
        self.inner.acquire_melt(saga, quote).await
    }

    async fn reserve_melt_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), DbError> {
        self.inner.reserve_melt_quote(quote_id, operation_id).await
    }

    async fn release_melt_quote(&self, operation_id: &uuid::Uuid) -> Result<(), DbError> {
        self.inner.release_melt_quote(operation_id).await
    }

    async fn reserve_mint_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), DbError> {
        self.inner.reserve_mint_quote(quote_id, operation_id).await
    }

    async fn release_mint_quote(&self, operation_id: &uuid::Uuid) -> Result<(), DbError> {
        self.inner.release_mint_quote(operation_id).await
    }

    async fn kv_read(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, DbError> {
        self.inner
            .kv_read(primary_namespace, secondary_namespace, key)
            .await
    }

    async fn kv_read_bounded(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, DbError> {
        self.inner
            .kv_read_bounded(primary_namespace, secondary_namespace, key, max_bytes)
            .await
    }

    async fn kv_list(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
    ) -> Result<Vec<String>, DbError> {
        self.inner
            .kv_list(primary_namespace, secondary_namespace)
            .await
    }

    async fn kv_write(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        value: &[u8],
    ) -> Result<(), DbError> {
        self.inner
            .kv_write(primary_namespace, secondary_namespace, key, value)
            .await
    }

    async fn kv_remove(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<(), DbError> {
        self.inner
            .kv_remove(primary_namespace, secondary_namespace, key)
            .await
    }

    async fn add_p2pk_key(
        &self,
        pubkey: &PublicKey,
        derivation_path: DerivationPath,
        derivation_index: u32,
    ) -> Result<(), DbError> {
        self.inner
            .add_p2pk_key(pubkey, derivation_path, derivation_index)
            .await
    }

    async fn get_p2pk_key(
        &self,
        pubkey: &PublicKey,
    ) -> Result<Option<wallet::P2PKSigningKey>, DbError> {
        self.inner.get_p2pk_key(pubkey).await
    }

    async fn list_p2pk_keys(&self) -> Result<Vec<wallet::P2PKSigningKey>, DbError> {
        self.inner.list_p2pk_keys().await
    }

    async fn latest_p2pk(&self) -> Result<Option<wallet::P2PKSigningKey>, DbError> {
        self.inner.latest_p2pk().await
    }
}
