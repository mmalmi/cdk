//! FFI Database bindings

#[cfg(test)]
mod issue_acquisition;
#[cfg(test)]
mod issue_retirement;
#[cfg(test)]
mod melt_acquisition;
#[cfg(test)]
mod melt_completion;
#[cfg(test)]
mod melt_swap;
#[cfg(test)]
mod mint_quote_pages;
#[cfg(test)]
mod proof_changes;
#[cfg(test)]
mod receive_checkpoint;

use std::collections::HashMap;
use std::sync::Arc;

use cdk_common::bitcoin::bip32::DerivationPath;
use cdk_common::database::wallet::SagaCursor;
use cdk_common::database::WalletDatabase as CdkWalletDatabase;
use cdk_common::wallet::WalletSaga;

use crate::error::FfiError;
#[cfg(feature = "postgres")]
use crate::postgres::WalletPostgresDatabase;
use crate::sqlite::WalletSqliteDatabase;
use crate::types::*;

/// Bounded pending-quote lookup, including the retryable stored-size limit.
#[derive(Debug, uniffi::Enum)]
pub enum MintQuotePage {
    /// Quotes for the requested mint and unit in ascending ID order.
    Rows { quotes: Vec<MintQuote> },
    /// The backend measured the stored payload before decoding any rows.
    TooLarge { bytes: u64, limit: u64 },
}

/// FFI-compatible wallet database trait with all read and write operations
/// This trait mirrors the CDK WalletDatabase trait structure
#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait WalletDatabase: Send + Sync {
    // ========== Read methods ==========

    /// Get mint from storage
    async fn get_mint(&self, mint_url: MintUrl) -> Result<Option<MintInfo>, FfiError>;

    /// Get all mints from storage
    async fn get_mints(&self) -> Result<HashMap<MintUrl, Option<MintInfo>>, FfiError>;

    /// Get mint keysets for mint url
    async fn get_mint_keysets(
        &self,
        mint_url: MintUrl,
    ) -> Result<Option<Vec<KeySetInfo>>, FfiError>;

    /// Get mint keyset by id
    async fn get_keyset_by_id(&self, keyset_id: Id) -> Result<Option<KeySetInfo>, FfiError>;

    /// Get mint quote from storage
    async fn get_mint_quote(&self, quote_id: String) -> Result<Option<MintQuote>, FfiError>;

    /// Get mint quotes from storage
    async fn get_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError>;

    /// Get unissued mint quotes from storage
    /// Returns bolt11 quotes where nothing has been issued yet (amount_issued = 0) and all bolt12 quotes.
    async fn get_unissued_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError>;

    /// Read pending quotes for one mint and unit after the exclusive ID cursor.
    /// Enforce the native row and stored-byte limits before loading records.
    async fn get_unissued_mint_quotes_page(
        &self,
        mint_url: MintUrl,
        unit: CurrencyUnit,
        after: Option<String>,
        limit: u16,
    ) -> Result<MintQuotePage, FfiError>;

    /// Get melt quote from storage
    async fn get_melt_quote(&self, quote_id: String) -> Result<Option<MeltQuote>, FfiError>;

    /// Get melt quotes from storage
    async fn get_melt_quotes(&self) -> Result<Vec<MeltQuote>, FfiError>;

    /// Get Keys from storage
    async fn get_keys(&self, id: Id) -> Result<Option<Keys>, FfiError>;

    /// Get proofs from storage
    async fn get_proofs(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<ProofState>>,
        spending_conditions: Option<Vec<SpendingConditions>>,
    ) -> Result<Vec<ProofInfo>, FfiError>;

    /// Get proofs by Y values
    async fn get_proofs_by_ys(&self, ys: Vec<PublicKey>) -> Result<Vec<ProofInfo>, FfiError>;

    /// Read the complete unspent inventory without exceeding either budget.
    /// Custom stores must fail if they cannot enforce the budgets before decoding.
    async fn get_unspent_proofs_bounded(
        &self,
        _mint_url: MintUrl,
        _unit: CurrencyUnit,
        _max_proofs: u32,
        _max_bytes: u32,
    ) -> Result<Vec<ProofInfo>, FfiError>;

    /// Get balance efficiently using SQL aggregation
    async fn get_balance(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<ProofState>>,
    ) -> Result<u64, FfiError>;

    /// Get transaction from storage
    async fn get_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<Transaction>, FfiError>;

    /// List transactions from storage
    async fn list_transactions(
        &self,
        mint_url: Option<MintUrl>,
        direction: Option<TransactionDirection>,
        unit: Option<CurrencyUnit>,
    ) -> Result<Vec<Transaction>, FfiError>;

    /// Read a value from the KV store
    async fn kv_read(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
    ) -> Result<Option<Vec<u8>>, FfiError>;

    /// Read a value with a storage-enforced byte limit; never fall back to an unbounded read.
    async fn kv_read_bounded(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
        max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, FfiError>;

    /// List keys in a namespace
    async fn kv_list(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
    ) -> Result<Vec<String>, FfiError>;

    /// Add P2PK signing key to storage
    async fn add_p2pk_key(
        &self,
        pubkey: PublicKey,
        derivation_path: String,
        derivation_index: u32,
    ) -> Result<(), FfiError>;

    /// Get P2PK signing key from storage
    async fn get_p2pk_key(&self, pubkey: PublicKey) -> Result<Option<P2PKSigningKey>, FfiError>;

    /// List all P2PK signing keys from storage
    async fn list_p2pk_keys(&self) -> Result<Vec<P2PKSigningKey>, FfiError>;

    /// Get the latest P2PK signing key (most recently created)
    async fn latest_p2pk(&self) -> Result<Option<P2PKSigningKey>, FfiError>;

    /// Write a value to the KV store
    async fn kv_write(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), FfiError>;

    /// Remove a value from the KV store
    async fn kv_remove(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
    ) -> Result<(), FfiError>;

    // ========== Write methods ==========

    /// Update the proofs in storage by adding new proofs or removing proofs by their Y value
    async fn update_proofs(
        &self,
        added: Vec<ProofInfo>,
        removed_ys: Vec<PublicKey>,
    ) -> Result<(), FfiError>;

    /// Compare complete original records and apply all changes atomically.
    /// If provided, the complete saved saga must match in the same transaction.
    /// Unsupported backends must fail without applying any changes.
    async fn compare_and_swap_proofs(
        &self,
        changes: Vec<ProofChange>,
        expected_saga_json: Option<String>,
    ) -> Result<bool, FfiError>;

    /// Update proofs state in storage
    async fn update_proofs_state(
        &self,
        ys: Vec<PublicKey>,
        state: ProofState,
    ) -> Result<(), FfiError>;

    /// Add transaction to storage
    async fn add_transaction(&self, transaction: Transaction) -> Result<(), FfiError>;

    /// Remove transaction from storage
    async fn remove_transaction(&self, transaction_id: TransactionId) -> Result<(), FfiError>;

    /// Update mint url
    async fn update_mint_url(
        &self,
        old_mint_url: MintUrl,
        new_mint_url: MintUrl,
    ) -> Result<(), FfiError>;

    /// Atomically increment Keyset counter and return new value
    async fn increment_keyset_counter(&self, keyset_id: Id, count: u32) -> Result<u32, FfiError>;

    /// Atomically increment a namespaced derivation counter and return its new value.
    async fn increment_derivation_counter(
        &self,
        namespace: String,
        count: u32,
    ) -> Result<u32, FfiError>;

    /// Add Mint to storage
    async fn add_mint(
        &self,
        mint_url: MintUrl,
        mint_info: Option<MintInfo>,
    ) -> Result<(), FfiError>;

    /// Remove Mint from storage
    async fn remove_mint(&self, mint_url: MintUrl) -> Result<(), FfiError>;

    /// Add mint keyset to storage
    async fn add_mint_keysets(
        &self,
        mint_url: MintUrl,
        keysets: Vec<KeySetInfo>,
    ) -> Result<(), FfiError>;

    /// Persist output keys and admit their storage before allocating secrets.
    async fn prepare_output_keyset(
        &self,
        operation_id: String,
        mint_url: MintUrl,
        keyset: KeySet,
    ) -> Result<(), FfiError>;

    /// Compare and atomically save an output proposal, its keys and counter range.
    async fn prepare_saga_outputs(
        &self,
        expected_json: Option<String>,
        next_json: String,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<Transaction>,
    ) -> Result<bool, FfiError>;

    /// Add mint quote to storage
    async fn add_mint_quote(&self, quote: MintQuote) -> Result<(), FfiError>;

    /// Remove mint quote from storage
    async fn remove_mint_quote(&self, quote_id: String) -> Result<(), FfiError>;

    /// Add melt quote to storage
    async fn add_melt_quote(&self, quote: MeltQuote) -> Result<(), FfiError>;

    /// Remove melt quote from storage
    async fn remove_melt_quote(&self, quote_id: String) -> Result<(), FfiError>;

    /// Add Keys to storage
    async fn add_keys(&self, keyset: KeySet) -> Result<(), FfiError>;

    /// Remove Keys from storage
    async fn remove_keys(&self, id: Id) -> Result<(), FfiError>;

    // ========== Saga management methods ==========
    // WalletSaga is serialized as JSON for FFI compatibility

    /// Add a wallet saga to storage (JSON serialized)
    async fn add_saga(&self, saga_json: String) -> Result<(), FfiError>;

    /// Get a wallet saga by ID (returns JSON serialized)
    async fn get_saga(&self, id: String) -> Result<Option<String>, FfiError>;

    /// Update a wallet saga (JSON serialized) with optimistic locking.
    ///
    /// Returns `true` if the update succeeded (version matched),
    /// `false` if another instance modified the saga first.
    async fn update_saga(&self, saga_json: String) -> Result<bool, FfiError>;

    /// Opt in only after implementing atomic paid receipt, owned quote and terminal saga publication.
    fn supports_atomic_melt_completion(&self) -> bool;

    /// Complete only an exact PaidReady parent, owned quote and canonical Pending receipt.
    /// Return an unsupported error if these three records cannot commit together.
    async fn complete_melt(
        &self,
        expected_json: String,
        next_json: String,
        quote: MeltQuote,
    ) -> Result<bool, FfiError>;

    /// Opt in only after implementing the full atomic receipt/saga contract.
    fn supports_atomic_saga_completion(&self) -> bool;

    /// Atomically publish a matching receipt and advance an exact parent saga.
    async fn complete_saga(
        &self,
        expected_json: String,
        next_json: String,
        receipt: Transaction,
    ) -> Result<bool, FfiError>;

    /// Delete a wallet saga
    async fn delete_saga(&self, id: String) -> Result<(), FfiError>;

    /// Get all incomplete sagas (returns JSON serialized sagas)
    async fn get_incomplete_sagas(&self) -> Result<Vec<String>, FfiError>;

    /// Read bounded unfinished operation headers as JSON `SagaCursor` values.
    async fn get_incomplete_saga_ids(
        &self,
        after_json: Option<String>,
        limit: u16,
    ) -> Result<Vec<String>, FfiError>;

    // ========== Proof reservation methods ==========

    /// Reserve proofs for an operation
    async fn reserve_proofs(
        &self,
        ys: Vec<PublicKey>,
        operation_id: String,
    ) -> Result<(), FfiError>;

    /// Release live proofs reserved by an operation.
    ///
    /// Implementations must only change Reserved or Pending proofs owned by
    /// `operation_id`; Spent proofs and proofs owned by another operation must
    /// be preserved.
    async fn release_proofs(&self, operation_id: String) -> Result<(), FfiError>;

    /// Get proofs reserved by an operation
    async fn get_reserved_proofs(&self, operation_id: String) -> Result<Vec<ProofInfo>, FfiError>;

    // ========== Quote reservation methods ==========

    /// Opt in only after implementing the entire atomic issue retirement contract.
    fn supports_atomic_issue_retirement(&self) -> bool;

    /// Atomically publish original outputs and completed receipts, update and release
    /// owned quotes, and delete the exact journal. Cancellation supplies no proofs.
    /// Preserve all evidence on conflict; sequential publication is not supported.
    async fn retire_issue(
        &self,
        saga_json: String,
        quotes: Vec<MintQuote>,
        proofs: Option<Vec<ProofChange>>,
    ) -> Result<bool, FfiError>;

    /// Atomically save the new issue saga, pending receipts and every exact quote reservation.
    /// Return false without writes on conflict, or an error if unsupported.
    async fn acquire_issue(
        &self,
        saga_json: String,
        quotes: Vec<MintQuote>,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, FfiError>;

    /// Atomically acquire the exact saved proof plan, complete unlocked quote and new saga.
    /// Return false without writes if any original record or the absent saga does not match.
    /// Success sets the quote owner and increments its version; reload before further use.
    /// Return an unsupported error if the backend cannot commit all three parts together.
    async fn acquire_melt(&self, saga_json: String, quote: MeltQuote) -> Result<bool, FfiError>;

    /// Reserve a melt quote for an operation
    async fn reserve_melt_quote(
        &self,
        quote_id: String,
        operation_id: String,
    ) -> Result<(), FfiError>;

    /// Release a melt quote reserved by an operation
    async fn release_melt_quote(&self, operation_id: String) -> Result<(), FfiError>;

    /// Reserve a mint quote for an operation
    async fn reserve_mint_quote(
        &self,
        quote_id: String,
        operation_id: String,
    ) -> Result<(), FfiError>;

    /// Release a mint quote reserved by an operation
    async fn release_mint_quote(&self, operation_id: String) -> Result<(), FfiError>;
}

/// Internal bridge trait to convert from the FFI trait to the CDK database trait
/// This allows us to bridge between the UniFFI trait and the CDK's internal database trait
struct WalletDatabaseBridge {
    ffi_db: Arc<dyn WalletDatabase>,
}

impl WalletDatabaseBridge {
    fn new(ffi_db: Arc<dyn WalletDatabase>) -> Self {
        Self { ffi_db }
    }
}

impl std::fmt::Debug for WalletDatabaseBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalletDatabaseBridge")
    }
}

#[async_trait::async_trait]
impl CdkWalletDatabase<cdk::cdk_database::Error> for WalletDatabaseBridge {
    async fn kv_read(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, cdk::cdk_database::Error> {
        self.ffi_db
            .kv_read(
                primary_namespace.to_string(),
                secondary_namespace.to_string(),
                key.to_string(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn kv_read_bounded(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, cdk::cdk_database::Error> {
        self.ffi_db
            .kv_read_bounded(
                primary_namespace.to_string(),
                secondary_namespace.to_string(),
                key.to_string(),
                max_bytes,
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn kv_list(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
    ) -> Result<Vec<String>, cdk::cdk_database::Error> {
        self.ffi_db
            .kv_list(
                primary_namespace.to_string(),
                secondary_namespace.to_string(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    // Mint Management
    async fn get_mint(
        &self,
        mint_url: cdk::mint_url::MintUrl,
    ) -> Result<Option<cdk::nuts::MintInfo>, cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.into();
        let result = self
            .ffi_db
            .get_mint(ffi_mint_url)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        result
            .map(TryInto::try_into)
            .transpose()
            .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn get_mints(
        &self,
    ) -> Result<
        HashMap<cdk::mint_url::MintUrl, Option<cdk::nuts::MintInfo>>,
        cdk::cdk_database::Error,
    > {
        let result = self
            .ffi_db
            .get_mints()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        let mut cdk_result = HashMap::new();
        for (ffi_mint_url, mint_info_opt) in result {
            let cdk_url = ffi_mint_url
                .try_into()
                .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))?;
            let cdk_mint_info = mint_info_opt
                .map(TryInto::try_into)
                .transpose()
                .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))?;
            cdk_result.insert(cdk_url, cdk_mint_info);
        }
        Ok(cdk_result)
    }

    // Keyset Management
    async fn get_mint_keysets(
        &self,
        mint_url: cdk::mint_url::MintUrl,
    ) -> Result<Option<Vec<cdk::nuts::KeySetInfo>>, cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.into();
        let result = self
            .ffi_db
            .get_mint_keysets(ffi_mint_url)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        let cdk_keysets = result
            .map(|keysets| {
                keysets
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        Ok(cdk_keysets)
    }

    async fn get_keyset_by_id(
        &self,
        keyset_id: &cdk::nuts::Id,
    ) -> Result<Option<cdk::nuts::KeySetInfo>, cdk::cdk_database::Error> {
        let ffi_id = (*keyset_id).into();
        let result = self
            .ffi_db
            .get_keyset_by_id(ffi_id)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        let cdk_keyset = result
            .map(TryInto::try_into)
            .transpose()
            .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        Ok(cdk_keyset)
    }

    // Mint Quote Management
    async fn get_mint_quote(
        &self,
        quote_id: &str,
    ) -> Result<Option<cdk::wallet::MintQuote>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_mint_quote(quote_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .map(|q| {
                q.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .transpose()?)
    }

    async fn get_mint_quotes(
        &self,
    ) -> Result<Vec<cdk::wallet::MintQuote>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_mint_quotes()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .into_iter()
            .map(|q| {
                q.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn get_unissued_mint_quotes(
        &self,
    ) -> Result<Vec<cdk::wallet::MintQuote>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_unissued_mint_quotes()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .into_iter()
            .map(|q| {
                q.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn get_unissued_mint_quotes_page(
        &self,
        mint_url: cdk::mint_url::MintUrl,
        unit: cdk::nuts::CurrencyUnit,
        after: Option<String>,
        limit: u16,
    ) -> Result<Vec<cdk::wallet::MintQuote>, cdk::cdk_database::Error> {
        use cdk_common::database::{wallet::MAX_MINT_QUOTE_PAGE_SIZE, Error};
        if !(1..=MAX_MINT_QUOTE_PAGE_SIZE).contains(&limit) {
            return Err(Error::Internal("Invalid mint quote page limit".into()));
        }
        let page = self
            .ffi_db
            .get_unissued_mint_quotes_page(
                mint_url.clone().into(),
                unit.clone().into(),
                after.clone(),
                limit,
            )
            .await
            .map_err(|err| Error::Database(err.to_string().into()))?;
        let quotes = match page {
            MintQuotePage::Rows { quotes } => quotes,
            MintQuotePage::TooLarge { bytes, limit } => {
                return Err(Error::MintQuotePageTooLarge { bytes, limit })
            }
        };
        if quotes.len() > usize::from(limit) {
            return Err(Error::Internal(
                "Mint quote page exceeds requested limit".into(),
            ));
        }
        let mut previous = after;
        quotes
            .into_iter()
            .map(|quote| {
                let native: cdk::wallet::MintQuote = quote
                    .try_into()
                    .map_err(|err: FfiError| Error::Database(err.to_string().into()))?;
                if native.mint_url != mint_url
                    || native.unit != unit
                    || previous.as_ref().is_some_and(|id| &native.id <= id)
                {
                    return Err(Error::Internal(
                        "Mint quote page violates its scope or cursor".into(),
                    ));
                }
                previous = Some(native.id.clone());
                Ok(native)
            })
            .collect()
    }

    // Melt Quote Management
    async fn get_melt_quote(
        &self,
        quote_id: &str,
    ) -> Result<Option<cdk::wallet::MeltQuote>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_melt_quote(quote_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .map(|q| {
                q.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .transpose()?)
    }

    async fn get_melt_quotes(
        &self,
    ) -> Result<Vec<cdk::wallet::MeltQuote>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_melt_quotes()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .into_iter()
            .map(|q| {
                q.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect::<Result<Vec<_>, _>>()?)
    }

    // Keys Management
    async fn get_keys(
        &self,
        id: &cdk::nuts::Id,
    ) -> Result<Option<cdk::nuts::Keys>, cdk::cdk_database::Error> {
        let ffi_id: Id = (*id).into();
        let result = self
            .ffi_db
            .get_keys(ffi_id)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        // Convert FFI Keys back to CDK Keys using TryFrom
        result
            .map(|ffi_keys| {
                ffi_keys
                    .try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .transpose()
    }

    // Proof Management
    async fn get_proofs(
        &self,
        mint_url: Option<cdk::mint_url::MintUrl>,
        unit: Option<cdk::nuts::CurrencyUnit>,
        state: Option<Vec<cdk::nuts::State>>,
        spending_conditions: Option<Vec<cdk::nuts::SpendingConditions>>,
    ) -> Result<Vec<cdk::types::ProofInfo>, cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.map(Into::into);
        let ffi_unit = unit.map(Into::into);
        let ffi_state = state.map(|s| s.into_iter().map(Into::into).collect());
        let ffi_spending_conditions =
            spending_conditions.map(|sc| sc.into_iter().map(Into::into).collect());

        let result = self
            .ffi_db
            .get_proofs(ffi_mint_url, ffi_unit, ffi_state, ffi_spending_conditions)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        result
            .into_iter()
            .map(|info| {
                info.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect()
    }

    async fn get_proofs_by_ys(
        &self,
        ys: Vec<cdk::nuts::PublicKey>,
    ) -> Result<Vec<cdk::types::ProofInfo>, cdk::cdk_database::Error> {
        let ffi_ys: Vec<PublicKey> = ys.into_iter().map(Into::into).collect();

        let result = self
            .ffi_db
            .get_proofs_by_ys(ffi_ys)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        result
            .into_iter()
            .map(|info| {
                info.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect()
    }

    async fn get_unspent_proofs_bounded(
        &self,
        mint_url: cdk::mint_url::MintUrl,
        unit: cdk::nuts::CurrencyUnit,
        max_proofs: u32,
        max_bytes: u32,
    ) -> Result<Vec<cdk::types::ProofInfo>, cdk::cdk_database::Error> {
        self.ffi_db
            .get_unspent_proofs_bounded(mint_url.into(), unit.into(), max_proofs, max_bytes)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?
            .into_iter()
            .map(|info| {
                info.try_into()
                    .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect()
    }

    async fn get_balance(
        &self,
        mint_url: Option<cdk::mint_url::MintUrl>,
        unit: Option<cdk::nuts::CurrencyUnit>,
        state: Option<Vec<cdk::nuts::State>>,
    ) -> Result<u64, cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.map(Into::into);
        let ffi_unit = unit.map(Into::into);
        let ffi_state = state.map(|s| s.into_iter().map(Into::into).collect());

        self.ffi_db
            .get_balance(ffi_mint_url, ffi_unit, ffi_state)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    // Transaction Management
    async fn get_transaction(
        &self,
        transaction_id: cdk::wallet::types::TransactionId,
    ) -> Result<Option<cdk::wallet::types::Transaction>, cdk::cdk_database::Error> {
        let ffi_id = transaction_id.into();
        let result = self
            .ffi_db
            .get_transaction(ffi_id)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        result
            .map(|tx| tx.try_into())
            .transpose()
            .map_err(|e: FfiError| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn list_transactions(
        &self,
        mint_url: Option<cdk::mint_url::MintUrl>,
        direction: Option<cdk::wallet::types::TransactionDirection>,
        unit: Option<cdk::nuts::CurrencyUnit>,
    ) -> Result<Vec<cdk::wallet::types::Transaction>, cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.map(Into::into);
        let ffi_direction = direction.map(Into::into);
        let ffi_unit = unit.map(Into::into);

        let result = self
            .ffi_db
            .list_transactions(ffi_mint_url, ffi_direction, ffi_unit)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        result
            .into_iter()
            .map(|tx| tx.try_into())
            .collect::<Result<Vec<_>, FfiError>>()
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    // P2PK methods

    async fn add_p2pk_key(
        &self,
        pubkey: &cdk::nuts::PublicKey,
        derivation_path: DerivationPath,
        derivation_index: u32,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_pubkey: PublicKey = (*pubkey).into();
        let ffi_derivation_path = derivation_path.to_string();
        self.ffi_db
            .add_p2pk_key(ffi_pubkey, ffi_derivation_path, derivation_index)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn list_p2pk_keys(
        &self,
    ) -> Result<Vec<cdk_common::wallet::P2PKSigningKey>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .list_p2pk_keys()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .into_iter()
            .map(|k| {
                k.try_into().map_err(|e: crate::error::FfiError| {
                    cdk::cdk_database::Error::Database(e.to_string().into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn latest_p2pk(
        &self,
    ) -> Result<Option<cdk_common::wallet::P2PKSigningKey>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .latest_p2pk()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .map(|k| {
                k.try_into().map_err(|e: crate::error::FfiError| {
                    cdk::cdk_database::Error::Database(e.to_string().into())
                })
            })
            .transpose()?)
    }

    async fn get_p2pk_key(
        &self,
        pubkey: &cdk::nuts::PublicKey,
    ) -> Result<Option<cdk_common::wallet::P2PKSigningKey>, cdk::cdk_database::Error> {
        let ffi_pubkey: PublicKey = (*pubkey).into();
        let result = self
            .ffi_db
            .get_p2pk_key(ffi_pubkey)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        Ok(result
            .map(|k| {
                k.try_into().map_err(|e: crate::error::FfiError| {
                    cdk::cdk_database::Error::Database(e.to_string().into())
                })
            })
            .transpose()?)
    }

    // Write methods (non-transactional)

    async fn update_proofs(
        &self,
        added: Vec<cdk::types::ProofInfo>,
        removed_ys: Vec<cdk::nuts::PublicKey>,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_added: Vec<ProofInfo> = added.into_iter().map(Into::into).collect();
        let ffi_removed_ys: Vec<PublicKey> = removed_ys.into_iter().map(Into::into).collect();
        self.ffi_db
            .update_proofs(ffi_added, ffi_removed_ys)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn compare_and_swap_proofs(
        &self,
        changes: Vec<cdk_common::database::wallet::ProofChange>,
        expected_saga: Option<&WalletSaga>,
    ) -> Result<bool, cdk::cdk_database::Error> {
        let saga = expected_saga.map(serde_json::to_string).transpose()?;
        self.ffi_db
            .compare_and_swap_proofs(changes.into_iter().map(Into::into).collect(), saga)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn update_proofs_state(
        &self,
        ys: Vec<cdk::nuts::PublicKey>,
        state: cdk::nuts::State,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_ys: Vec<PublicKey> = ys.into_iter().map(Into::into).collect();
        let ffi_state = state.into();
        self.ffi_db
            .update_proofs_state(ffi_ys, ffi_state)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_transaction(
        &self,
        transaction: cdk::wallet::types::Transaction,
    ) -> Result<(), cdk::cdk_database::Error> {
        transaction
            .validate_role()
            .map_err(|e| cdk::cdk_database::Error::Internal(e.to_string()))?;
        let ffi_transaction = transaction.into();
        self.ffi_db
            .add_transaction(ffi_transaction)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn update_mint_url(
        &self,
        old_mint_url: cdk::mint_url::MintUrl,
        new_mint_url: cdk::mint_url::MintUrl,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_old = old_mint_url.into();
        let ffi_new = new_mint_url.into();
        self.ffi_db
            .update_mint_url(ffi_old, ffi_new)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn increment_keyset_counter(
        &self,
        keyset_id: &cdk::nuts::Id,
        count: u32,
    ) -> Result<u32, cdk::cdk_database::Error> {
        let ffi_id = (*keyset_id).into();
        self.ffi_db
            .increment_keyset_counter(ffi_id, count)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn increment_derivation_counter(
        &self,
        namespace: &str,
        count: u32,
    ) -> Result<u32, cdk::cdk_database::Error> {
        self.ffi_db
            .increment_derivation_counter(namespace.to_owned(), count)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_mint(
        &self,
        mint_url: cdk::mint_url::MintUrl,
        mint_info: Option<cdk::nuts::MintInfo>,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.into();
        let ffi_mint_info = mint_info.map(Into::into);
        self.ffi_db
            .add_mint(ffi_mint_url, ffi_mint_info)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn remove_mint(
        &self,
        mint_url: cdk::mint_url::MintUrl,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.into();
        self.ffi_db
            .remove_mint(ffi_mint_url)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_mint_keysets(
        &self,
        mint_url: cdk::mint_url::MintUrl,
        keysets: Vec<cdk::nuts::KeySetInfo>,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_mint_url = mint_url.into();
        let ffi_keysets: Vec<KeySetInfo> = keysets.into_iter().map(Into::into).collect();
        self.ffi_db
            .add_mint_keysets(ffi_mint_url, ffi_keysets)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn prepare_output_keyset(
        &self,
        operation_id: uuid::Uuid,
        mint_url: cdk::mint_url::MintUrl,
        keyset: cdk::nuts::KeySet,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .prepare_output_keyset(operation_id.to_string(), mint_url.into(), keyset.into())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn prepare_saga_outputs(
        &self,
        expected: Option<&WalletSaga>,
        next: WalletSaga,
        keyset: cdk::nuts::KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<cdk_common::wallet::Transaction>,
    ) -> Result<bool, cdk::cdk_database::Error> {
        self.ffi_db
            .prepare_saga_outputs(
                expected.map(serde_json::to_string).transpose()?,
                serde_json::to_string(&next)?,
                keyset.into(),
                counter_start,
                counter_count,
                receipt.map(Into::into),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_mint_quote(
        &self,
        quote: cdk::wallet::MintQuote,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_quote = quote.into();
        self.ffi_db
            .add_mint_quote(ffi_quote)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn remove_mint_quote(&self, quote_id: &str) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .remove_mint_quote(quote_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_melt_quote(
        &self,
        quote: cdk::wallet::MeltQuote,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_quote = quote.into();
        self.ffi_db
            .add_melt_quote(ffi_quote)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn remove_melt_quote(&self, quote_id: &str) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .remove_melt_quote(quote_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_keys(&self, keyset: cdk::nuts::KeySet) -> Result<(), cdk::cdk_database::Error> {
        let ffi_keyset: KeySet = keyset.into();
        self.ffi_db
            .add_keys(ffi_keyset)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn remove_keys(&self, id: &cdk::nuts::Id) -> Result<(), cdk::cdk_database::Error> {
        let ffi_id = (*id).into();
        self.ffi_db
            .remove_keys(ffi_id)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn remove_transaction(
        &self,
        transaction_id: cdk::wallet::types::TransactionId,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_id = transaction_id.into();
        self.ffi_db
            .remove_transaction(ffi_id)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn add_saga(&self, saga: WalletSaga) -> Result<(), cdk::cdk_database::Error> {
        let json = serde_json::to_string(&saga)
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        self.ffi_db
            .add_saga(json)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn get_saga(
        &self,
        id: &uuid::Uuid,
    ) -> Result<Option<WalletSaga>, cdk::cdk_database::Error> {
        let json_opt = self
            .ffi_db
            .get_saga(id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        match json_opt {
            Some(json) => {
                let saga: WalletSaga = serde_json::from_str(&json)
                    .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
                Ok(Some(saga))
            }
            None => Ok(None),
        }
    }

    async fn update_saga(&self, saga: WalletSaga) -> Result<bool, cdk::cdk_database::Error> {
        let json = serde_json::to_string(&saga)
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;
        self.ffi_db
            .update_saga(json)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    fn supports_atomic_issue_retirement(&self) -> bool {
        self.ffi_db.supports_atomic_issue_retirement()
    }

    fn supports_atomic_melt_completion(&self) -> bool {
        self.ffi_db.supports_atomic_melt_completion()
    }

    async fn complete_melt(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        quote: &cdk_common::wallet::MeltQuote,
    ) -> Result<bool, cdk::cdk_database::Error> {
        cdk_common::database::wallet::validate_melt_completion(expected, &next, quote)?;
        self.ffi_db
            .complete_melt(
                serde_json::to_string(expected)?,
                serde_json::to_string(&next)?,
                quote.clone().into(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    fn supports_atomic_saga_completion(&self) -> bool {
        self.ffi_db.supports_atomic_saga_completion()
    }

    async fn complete_saga(
        &self,
        expected: &WalletSaga,
        next: WalletSaga,
        receipt: cdk_common::wallet::Transaction,
    ) -> Result<bool, cdk::cdk_database::Error> {
        self.ffi_db
            .complete_saga(
                serde_json::to_string(expected)?,
                serde_json::to_string(&next)?,
                receipt.into(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn delete_saga(&self, id: &uuid::Uuid) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .delete_saga(id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn get_incomplete_saga_ids(
        &self,
        after: Option<SagaCursor>,
        limit: u16,
    ) -> Result<Vec<SagaCursor>, cdk::cdk_database::Error> {
        cdk_common::database::wallet::validate_saga_page_request(after, limit)?;
        let after = after
            .map(|cursor| serde_json::to_string(&cursor))
            .transpose()?;
        self.ffi_db
            .get_incomplete_saga_ids(after, limit)
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?
            .into_iter()
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .collect()
    }

    async fn get_incomplete_sagas(&self) -> Result<Vec<WalletSaga>, cdk::cdk_database::Error> {
        let json_vec = self
            .ffi_db
            .get_incomplete_sagas()
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        json_vec
            .into_iter()
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
            })
            .collect()
    }

    async fn reserve_proofs(
        &self,
        ys: Vec<cdk::nuts::PublicKey>,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        let ffi_ys: Vec<PublicKey> = ys.into_iter().map(Into::into).collect();
        self.ffi_db
            .reserve_proofs(ffi_ys, operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn release_proofs(
        &self,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .release_proofs(operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn get_reserved_proofs(
        &self,
        operation_id: &uuid::Uuid,
    ) -> Result<Vec<cdk::types::ProofInfo>, cdk::cdk_database::Error> {
        let result = self
            .ffi_db
            .get_reserved_proofs(operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?;

        result
            .into_iter()
            .map(|info| {
                Ok(cdk::types::ProofInfo {
                    proof: info.proof.try_into().map_err(|e: FfiError| {
                        cdk::cdk_database::Error::Database(e.to_string().into())
                    })?,
                    y: info.y.try_into().map_err(|e: FfiError| {
                        cdk::cdk_database::Error::Database(e.to_string().into())
                    })?,
                    mint_url: info.mint_url.try_into().map_err(|e: FfiError| {
                        cdk::cdk_database::Error::Database(e.to_string().into())
                    })?,
                    state: info.state.into(),
                    spending_condition: info
                        .spending_condition
                        .map(|sc| sc.try_into())
                        .transpose()
                        .map_err(|e: FfiError| {
                            cdk::cdk_database::Error::Database(e.to_string().into())
                        })?,
                    unit: info.unit.into(),
                    derivation_index: info.derivation_index,
                    used_by_operation: info
                        .used_by_operation
                        .map(|id| uuid::Uuid::parse_str(&id))
                        .transpose()
                        .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?,
                    created_by_operation: info
                        .created_by_operation
                        .map(|id| uuid::Uuid::parse_str(&id))
                        .transpose()
                        .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))?,
                })
            })
            .collect()
    }

    async fn retire_issue(
        &self,
        expected: &WalletSaga,
        quotes: &[cdk_common::wallet::MintQuote],
        proofs: Option<Vec<cdk_common::database::wallet::ProofChange>>,
    ) -> Result<bool, cdk::cdk_database::Error> {
        cdk_common::database::wallet::validate_issue_retirement(
            expected,
            quotes,
            proofs.as_deref(),
        )?;
        self.ffi_db
            .retire_issue(
                serde_json::to_string(expected)?,
                quotes.iter().cloned().map(Into::into).collect(),
                proofs.map(|changes| changes.into_iter().map(Into::into).collect()),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn acquire_issue(
        &self,
        saga: &WalletSaga,
        quotes: &[cdk_common::wallet::MintQuote],
        keyset: cdk::nuts::KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, cdk::cdk_database::Error> {
        cdk_common::database::wallet::validate_issue_acquisition(saga, quotes)?;
        self.ffi_db
            .acquire_issue(
                serde_json::to_string(saga)?,
                quotes.iter().cloned().map(Into::into).collect(),
                keyset.into(),
                counter_start,
                counter_count,
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn acquire_melt(
        &self,
        saga: &WalletSaga,
        quote: &cdk_common::wallet::MeltQuote,
    ) -> Result<bool, cdk::cdk_database::Error> {
        cdk_common::database::wallet::validate_melt_acquisition(saga, quote)?;
        self.ffi_db
            .acquire_melt(serde_json::to_string(saga)?, quote.clone().into())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn reserve_melt_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .reserve_melt_quote(quote_id.to_string(), operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn release_melt_quote(
        &self,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .release_melt_quote(operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn reserve_mint_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .reserve_mint_quote(quote_id.to_string(), operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn release_mint_quote(
        &self,
        operation_id: &uuid::Uuid,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .release_mint_quote(operation_id.to_string())
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn kv_write(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        value: &[u8],
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .kv_write(
                primary_namespace.to_string(),
                secondary_namespace.to_string(),
                key.to_string(),
                value.to_vec(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }

    async fn kv_remove(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<(), cdk::cdk_database::Error> {
        self.ffi_db
            .kv_remove(
                primary_namespace.to_string(),
                secondary_namespace.to_string(),
                key.to_string(),
            )
            .await
            .map_err(|e| cdk::cdk_database::Error::Database(e.to_string().into()))
    }
}

/// Generic FFI wrapper for any database implementing CDK's wallet database traits.
///
/// This wrapper converts between CDK types and FFI types, allowing any database
/// backend (SQLite, Postgres, Supabase, etc.) to be exposed through the FFI layer.
pub(crate) struct FfiWalletDatabaseWrapper<T, E>
where
    T: CdkWalletDatabase<E> + Send + Sync + 'static,
    E: std::error::Error
        + Send
        + Sync
        + Into<cdk_common::database::Error>
        + From<cdk_common::database::Error>
        + 'static,
{
    inner: T,
    _phantom: std::marker::PhantomData<E>,
}

impl<T, E> std::fmt::Debug for FfiWalletDatabaseWrapper<T, E>
where
    T: CdkWalletDatabase<E> + std::fmt::Debug + Send + Sync + 'static,
    E: std::error::Error
        + Send
        + Sync
        + Into<cdk_common::database::Error>
        + From<cdk_common::database::Error>
        + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FfiWalletDatabaseWrapper")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, E> FfiWalletDatabaseWrapper<T, E>
where
    T: CdkWalletDatabase<E> + Send + Sync + 'static,
    E: std::error::Error
        + Send
        + Sync
        + Into<cdk_common::database::Error>
        + From<cdk_common::database::Error>
        + 'static,
{
    /// Creates a new instance wrapping the given database
    pub fn new(inner: T) -> Arc<Self> {
        Arc::new(Self {
            inner,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Returns a reference to the inner database
    ///
    /// This is useful for accessing database-specific methods that are not part
    /// of the standard WalletDatabase trait (e.g., Supabase JWT token management).
    #[cfg(feature = "supabase")]
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

// Implement WalletDatabase trait - all read and write methods
#[async_trait::async_trait]
impl<T, E> WalletDatabase for FfiWalletDatabaseWrapper<T, E>
where
    T: CdkWalletDatabase<E> + Send + Sync + 'static,
    E: std::error::Error
        + Send
        + Sync
        + Into<cdk_common::database::Error>
        + From<cdk_common::database::Error>
        + 'static,
{
    // ========== Read methods ==========

    async fn get_unspent_proofs_bounded(
        &self,
        mint_url: MintUrl,
        unit: CurrencyUnit,
        max_proofs: u32,
        max_bytes: u32,
    ) -> Result<Vec<ProofInfo>, FfiError> {
        Ok(self
            .inner
            .get_unspent_proofs_bounded(mint_url.try_into()?, unit.into(), max_proofs, max_bytes)
            .await
            .map_err(FfiError::internal)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn get_proofs_by_ys(&self, ys: Vec<PublicKey>) -> Result<Vec<ProofInfo>, FfiError> {
        let cdk_ys: Vec<cdk::nuts::PublicKey> = ys
            .into_iter()
            .map(|y| y.try_into())
            .collect::<Result<Vec<_>, FfiError>>()?;

        let result = self
            .inner
            .get_proofs_by_ys(cdk_ys)
            .await
            .map_err(FfiError::internal)?;

        Ok(result.into_iter().map(Into::into).collect())
    }

    async fn get_mint(&self, mint_url: MintUrl) -> Result<Option<MintInfo>, FfiError> {
        let cdk_mint_url = mint_url.try_into()?;
        let result = self
            .inner
            .get_mint(cdk_mint_url)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(Into::into))
    }

    async fn get_mints(&self) -> Result<HashMap<MintUrl, Option<MintInfo>>, FfiError> {
        let result = self.inner.get_mints().await.map_err(FfiError::internal)?;
        Ok(result
            .into_iter()
            .map(|(k, v)| (k.into(), v.map(Into::into)))
            .collect())
    }

    async fn get_mint_keysets(
        &self,
        mint_url: MintUrl,
    ) -> Result<Option<Vec<KeySetInfo>>, FfiError> {
        let cdk_mint_url = mint_url.try_into()?;
        let result = self
            .inner
            .get_mint_keysets(cdk_mint_url)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(|keysets| keysets.into_iter().map(Into::into).collect()))
    }

    async fn get_keyset_by_id(&self, keyset_id: Id) -> Result<Option<KeySetInfo>, FfiError> {
        let cdk_id = keyset_id.try_into()?;
        let result = self
            .inner
            .get_keyset_by_id(&cdk_id)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(Into::into))
    }

    async fn get_mint_quote(&self, quote_id: String) -> Result<Option<MintQuote>, FfiError> {
        let result = self
            .inner
            .get_mint_quote(&quote_id)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(|q| q.into()))
    }

    async fn get_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError> {
        let result = self
            .inner
            .get_mint_quotes()
            .await
            .map_err(FfiError::internal)?;
        Ok(result.into_iter().map(|q| q.into()).collect())
    }

    async fn get_unissued_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError> {
        let result = self
            .inner
            .get_unissued_mint_quotes()
            .await
            .map_err(FfiError::internal)?;
        Ok(result.into_iter().map(|q| q.into()).collect())
    }

    async fn get_unissued_mint_quotes_page(
        &self,
        mint_url: MintUrl,
        unit: CurrencyUnit,
        after: Option<String>,
        limit: u16,
    ) -> Result<MintQuotePage, FfiError> {
        match self
            .inner
            .get_unissued_mint_quotes_page(mint_url.try_into()?, unit.into(), after, limit)
            .await
        {
            Ok(quotes) => Ok(MintQuotePage::Rows {
                quotes: quotes.into_iter().map(Into::into).collect(),
            }),
            Err(err) => match err.into() {
                cdk_common::database::Error::MintQuotePageTooLarge { bytes, limit } => {
                    Ok(MintQuotePage::TooLarge { bytes, limit })
                }
                other => Err(FfiError::database(other)),
            },
        }
    }

    async fn get_melt_quote(&self, quote_id: String) -> Result<Option<MeltQuote>, FfiError> {
        let result = self
            .inner
            .get_melt_quote(&quote_id)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(|q| q.into()))
    }

    async fn get_melt_quotes(&self) -> Result<Vec<MeltQuote>, FfiError> {
        let result = self
            .inner
            .get_melt_quotes()
            .await
            .map_err(FfiError::internal)?;
        Ok(result.into_iter().map(|q| q.into()).collect())
    }

    async fn get_keys(&self, id: Id) -> Result<Option<Keys>, FfiError> {
        let cdk_id = id.try_into()?;
        let result = self
            .inner
            .get_keys(&cdk_id)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(Into::into))
    }

    async fn get_proofs(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<ProofState>>,
        spending_conditions: Option<Vec<SpendingConditions>>,
    ) -> Result<Vec<ProofInfo>, FfiError> {
        let cdk_mint_url = mint_url.map(|u| u.try_into()).transpose()?;
        let cdk_unit = unit.map(Into::into);
        let cdk_state = state.map(|s| s.into_iter().map(Into::into).collect());
        let cdk_spending_conditions: Option<Vec<cdk::nuts::SpendingConditions>> =
            spending_conditions
                .map(|sc| {
                    sc.into_iter()
                        .map(|c| c.try_into())
                        .collect::<Result<Vec<_>, FfiError>>()
                })
                .transpose()?;

        let result = self
            .inner
            .get_proofs(cdk_mint_url, cdk_unit, cdk_state, cdk_spending_conditions)
            .await
            .map_err(FfiError::internal)?;

        Ok(result.into_iter().map(Into::into).collect())
    }

    async fn get_balance(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<ProofState>>,
    ) -> Result<u64, FfiError> {
        let cdk_mint_url = mint_url.map(|u| u.try_into()).transpose()?;
        let cdk_unit = unit.map(Into::into);
        let cdk_state = state.map(|s| s.into_iter().map(Into::into).collect());

        self.inner
            .get_balance(cdk_mint_url, cdk_unit, cdk_state)
            .await
            .map_err(FfiError::internal)
    }

    async fn get_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<Transaction>, FfiError> {
        let cdk_id = transaction_id.try_into()?;
        let result = self
            .inner
            .get_transaction(cdk_id)
            .await
            .map_err(FfiError::internal)?;
        Ok(result.map(Into::into))
    }

    async fn list_transactions(
        &self,
        mint_url: Option<MintUrl>,
        direction: Option<TransactionDirection>,
        unit: Option<CurrencyUnit>,
    ) -> Result<Vec<Transaction>, FfiError> {
        let cdk_mint_url = mint_url.map(|u| u.try_into()).transpose()?;
        let cdk_direction = direction.map(Into::into);
        let cdk_unit = unit.map(Into::into);

        let result = self
            .inner
            .list_transactions(cdk_mint_url, cdk_direction, cdk_unit)
            .await
            .map_err(FfiError::internal)?;

        Ok(result.into_iter().map(Into::into).collect())
    }

    async fn kv_read(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        self.inner
            .kv_read(&primary_namespace, &secondary_namespace, &key)
            .await
            .map_err(FfiError::internal)
    }

    async fn kv_read_bounded(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
        max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        self.inner
            .kv_read_bounded(&primary_namespace, &secondary_namespace, &key, max_bytes)
            .await
            .map_err(FfiError::internal)
    }

    async fn kv_list(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
    ) -> Result<Vec<String>, FfiError> {
        self.inner
            .kv_list(&primary_namespace, &secondary_namespace)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_p2pk_key(
        &self,
        pubkey: PublicKey,
        derivation_path: String,
        derivation_index: u32,
    ) -> Result<(), FfiError> {
        use std::str::FromStr;

        use cdk_common::bitcoin::bip32::DerivationPath;

        let cdk_pubkey: cdk::nuts::PublicKey = pubkey.try_into()?;
        let cdk_derivation_path =
            DerivationPath::from_str(&derivation_path).map_err(FfiError::internal)?;

        self.inner
            .add_p2pk_key(&cdk_pubkey, cdk_derivation_path, derivation_index)
            .await
            .map_err(FfiError::database)
    }

    async fn get_p2pk_key(&self, pubkey: PublicKey) -> Result<Option<P2PKSigningKey>, FfiError> {
        let cdk_pubkey: cdk::nuts::PublicKey = pubkey.try_into()?;
        let result = self
            .inner
            .get_p2pk_key(&cdk_pubkey)
            .await
            .map_err(FfiError::database)?;
        Ok(result.map(Into::into))
    }

    async fn list_p2pk_keys(&self) -> Result<Vec<P2PKSigningKey>, FfiError> {
        let result = self
            .inner
            .list_p2pk_keys()
            .await
            .map_err(FfiError::database)?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    async fn latest_p2pk(&self) -> Result<Option<P2PKSigningKey>, FfiError> {
        let result = self.inner.latest_p2pk().await.map_err(FfiError::database)?;
        Ok(result.map(Into::into))
    }

    async fn kv_write(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), FfiError> {
        self.inner
            .kv_write(&primary_namespace, &secondary_namespace, &key, &value)
            .await
            .map_err(FfiError::internal)
    }

    async fn kv_remove(
        &self,
        primary_namespace: String,
        secondary_namespace: String,
        key: String,
    ) -> Result<(), FfiError> {
        self.inner
            .kv_remove(&primary_namespace, &secondary_namespace, &key)
            .await
            .map_err(FfiError::internal)
    }

    // ========== Write methods ==========

    async fn update_proofs(
        &self,
        added: Vec<ProofInfo>,
        removed_ys: Vec<PublicKey>,
    ) -> Result<(), FfiError> {
        let cdk_added: Result<Vec<cdk::types::ProofInfo>, FfiError> =
            added.into_iter().map(TryInto::try_into).collect();
        let cdk_added = cdk_added?;

        let cdk_removed_ys: Result<Vec<cdk::nuts::PublicKey>, FfiError> =
            removed_ys.into_iter().map(|pk| pk.try_into()).collect();
        let cdk_removed_ys = cdk_removed_ys?;

        self.inner
            .update_proofs(cdk_added, cdk_removed_ys)
            .await
            .map_err(FfiError::internal)
    }

    async fn compare_and_swap_proofs(
        &self,
        changes: Vec<ProofChange>,
        expected_saga_json: Option<String>,
    ) -> Result<bool, FfiError> {
        let changes = changes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, FfiError>>()?;
        let saga: Option<WalletSaga> = expected_saga_json
            .map(|json| serde_json::from_str(&json))
            .transpose()?;
        self.inner
            .compare_and_swap_proofs(changes, saga.as_ref())
            .await
            .map_err(FfiError::internal)
    }

    async fn update_proofs_state(
        &self,
        ys: Vec<PublicKey>,
        state: ProofState,
    ) -> Result<(), FfiError> {
        let cdk_ys: Result<Vec<cdk::nuts::PublicKey>, FfiError> =
            ys.into_iter().map(|pk| pk.try_into()).collect();
        let cdk_ys = cdk_ys?;
        let cdk_state = state.into();

        self.inner
            .update_proofs_state(cdk_ys, cdk_state)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_transaction(&self, transaction: Transaction) -> Result<(), FfiError> {
        let cdk_transaction: cdk::wallet::types::Transaction = transaction.try_into()?;
        self.inner
            .add_transaction(cdk_transaction)
            .await
            .map_err(FfiError::internal)
    }

    async fn remove_transaction(&self, transaction_id: TransactionId) -> Result<(), FfiError> {
        let cdk_id = transaction_id.try_into()?;
        self.inner
            .remove_transaction(cdk_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn update_mint_url(
        &self,
        old_mint_url: MintUrl,
        new_mint_url: MintUrl,
    ) -> Result<(), FfiError> {
        let cdk_old = old_mint_url.try_into()?;
        let cdk_new = new_mint_url.try_into()?;
        self.inner
            .update_mint_url(cdk_old, cdk_new)
            .await
            .map_err(FfiError::internal)
    }

    async fn increment_keyset_counter(&self, keyset_id: Id, count: u32) -> Result<u32, FfiError> {
        let cdk_id = keyset_id.try_into()?;
        self.inner
            .increment_keyset_counter(&cdk_id, count)
            .await
            .map_err(FfiError::internal)
    }

    async fn increment_derivation_counter(
        &self,
        namespace: String,
        count: u32,
    ) -> Result<u32, FfiError> {
        self.inner
            .increment_derivation_counter(&namespace, count)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_mint(
        &self,
        mint_url: MintUrl,
        mint_info: Option<MintInfo>,
    ) -> Result<(), FfiError> {
        let cdk_mint_url = mint_url.try_into()?;
        let cdk_mint_info = mint_info.map(TryInto::try_into).transpose()?;
        self.inner
            .add_mint(cdk_mint_url, cdk_mint_info)
            .await
            .map_err(FfiError::internal)
    }

    async fn remove_mint(&self, mint_url: MintUrl) -> Result<(), FfiError> {
        let cdk_mint_url = mint_url.try_into()?;
        self.inner
            .remove_mint(cdk_mint_url)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_mint_keysets(
        &self,
        mint_url: MintUrl,
        keysets: Vec<KeySetInfo>,
    ) -> Result<(), FfiError> {
        let cdk_mint_url = mint_url.try_into()?;
        let cdk_keysets: Vec<cdk::nuts::KeySetInfo> = keysets
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        self.inner
            .add_mint_keysets(cdk_mint_url, cdk_keysets)
            .await
            .map_err(FfiError::internal)
    }

    async fn prepare_output_keyset(
        &self,
        operation_id: String,
        mint_url: MintUrl,
        keyset: KeySet,
    ) -> Result<(), FfiError> {
        self.inner
            .prepare_output_keyset(
                uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?,
                mint_url.try_into()?,
                keyset.try_into()?,
            )
            .await
            .map_err(FfiError::internal)
    }

    async fn prepare_saga_outputs(
        &self,
        expected_json: Option<String>,
        next_json: String,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
        receipt: Option<Transaction>,
    ) -> Result<bool, FfiError> {
        let expected = expected_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(FfiError::internal)?;
        let next = serde_json::from_str(&next_json).map_err(FfiError::internal)?;
        self.inner
            .prepare_saga_outputs(
                expected.as_ref(),
                next,
                keyset.try_into()?,
                counter_start,
                counter_count,
                receipt.map(TryInto::try_into).transpose()?,
            )
            .await
            .map_err(FfiError::internal)
    }

    async fn add_mint_quote(&self, quote: MintQuote) -> Result<(), FfiError> {
        let cdk_quote = quote.try_into()?;
        self.inner
            .add_mint_quote(cdk_quote)
            .await
            .map_err(FfiError::internal)
    }

    async fn remove_mint_quote(&self, quote_id: String) -> Result<(), FfiError> {
        self.inner
            .remove_mint_quote(&quote_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_melt_quote(&self, quote: MeltQuote) -> Result<(), FfiError> {
        let cdk_quote = quote.try_into()?;
        self.inner
            .add_melt_quote(cdk_quote)
            .await
            .map_err(FfiError::internal)
    }

    async fn remove_melt_quote(&self, quote_id: String) -> Result<(), FfiError> {
        self.inner
            .remove_melt_quote(&quote_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn add_keys(&self, keyset: KeySet) -> Result<(), FfiError> {
        let cdk_keyset: cdk::nuts::KeySet = keyset.try_into()?;
        self.inner
            .add_keys(cdk_keyset)
            .await
            .map_err(FfiError::internal)
    }

    async fn remove_keys(&self, id: Id) -> Result<(), FfiError> {
        let cdk_id = id.try_into()?;
        self.inner
            .remove_keys(&cdk_id)
            .await
            .map_err(FfiError::internal)
    }

    // ========== Saga management methods ==========

    async fn add_saga(&self, saga_json: String) -> Result<(), FfiError> {
        let saga: WalletSaga = serde_json::from_str(&saga_json).map_err(FfiError::internal)?;
        self.inner.add_saga(saga).await.map_err(FfiError::internal)
    }

    async fn get_saga(&self, id: String) -> Result<Option<String>, FfiError> {
        let id = uuid::Uuid::parse_str(&id).map_err(FfiError::internal)?;
        let result = self.inner.get_saga(&id).await.map_err(FfiError::internal)?;

        match result {
            Some(saga) => {
                let json = serde_json::to_string(&saga).map_err(FfiError::internal)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    async fn update_saga(&self, saga_json: String) -> Result<bool, FfiError> {
        let saga: WalletSaga = serde_json::from_str(&saga_json).map_err(FfiError::internal)?;
        self.inner
            .update_saga(saga)
            .await
            .map_err(FfiError::internal)
    }

    fn supports_atomic_issue_retirement(&self) -> bool {
        self.inner.supports_atomic_issue_retirement()
    }

    fn supports_atomic_melt_completion(&self) -> bool {
        self.inner.supports_atomic_melt_completion()
    }

    async fn complete_melt(
        &self,
        expected_json: String,
        next_json: String,
        quote: MeltQuote,
    ) -> Result<bool, FfiError> {
        let expected = serde_json::from_str(&expected_json).map_err(FfiError::internal)?;
        let next = serde_json::from_str(&next_json).map_err(FfiError::internal)?;
        let quote = quote.try_into()?;
        cdk_common::database::wallet::validate_melt_completion(&expected, &next, &quote)
            .map_err(FfiError::internal)?;
        self.inner
            .complete_melt(&expected, next, &quote)
            .await
            .map_err(FfiError::internal)
    }

    fn supports_atomic_saga_completion(&self) -> bool {
        self.inner.supports_atomic_saga_completion()
    }

    async fn complete_saga(
        &self,
        expected_json: String,
        next_json: String,
        receipt: Transaction,
    ) -> Result<bool, FfiError> {
        let expected = serde_json::from_str(&expected_json).map_err(FfiError::internal)?;
        let next = serde_json::from_str(&next_json).map_err(FfiError::internal)?;
        self.inner
            .complete_saga(&expected, next, receipt.try_into()?)
            .await
            .map_err(FfiError::internal)
    }

    async fn delete_saga(&self, id: String) -> Result<(), FfiError> {
        let id = uuid::Uuid::parse_str(&id).map_err(FfiError::internal)?;
        self.inner
            .delete_saga(&id)
            .await
            .map_err(FfiError::internal)
    }

    async fn get_incomplete_saga_ids(
        &self,
        after_json: Option<String>,
        limit: u16,
    ) -> Result<Vec<String>, FfiError> {
        let after = after_json
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(FfiError::internal)?;
        self.inner
            .get_incomplete_saga_ids(after, limit)
            .await
            .map_err(FfiError::internal)?
            .into_iter()
            .map(|cursor| serde_json::to_string(&cursor).map_err(FfiError::internal))
            .collect()
    }

    async fn get_incomplete_sagas(&self) -> Result<Vec<String>, FfiError> {
        let result = self
            .inner
            .get_incomplete_sagas()
            .await
            .map_err(FfiError::internal)?;

        result
            .into_iter()
            .map(|saga| serde_json::to_string(&saga).map_err(FfiError::internal))
            .collect()
    }

    // ========== Proof reservation methods ==========

    async fn reserve_proofs(
        &self,
        ys: Vec<PublicKey>,
        operation_id: String,
    ) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        let cdk_ys: Result<Vec<cdk::nuts::PublicKey>, FfiError> =
            ys.into_iter().map(|pk| pk.try_into()).collect();
        let cdk_ys = cdk_ys?;
        self.inner
            .reserve_proofs(cdk_ys, &operation_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn release_proofs(&self, operation_id: String) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        self.inner
            .release_proofs(&operation_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn get_reserved_proofs(&self, operation_id: String) -> Result<Vec<ProofInfo>, FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        let result = self
            .inner
            .get_reserved_proofs(&operation_id)
            .await
            .map_err(FfiError::internal)?;

        Ok(result.into_iter().map(Into::into).collect())
    }

    // ========== Quote reservation methods ==========

    async fn retire_issue(
        &self,
        saga_json: String,
        quotes: Vec<MintQuote>,
        proofs: Option<Vec<ProofChange>>,
    ) -> Result<bool, FfiError> {
        let proofs = proofs
            .map(|changes| {
                changes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, FfiError>>()
            })
            .transpose()?;
        let saga = serde_json::from_str(&saga_json).map_err(FfiError::internal)?;
        let quotes: Vec<cdk_common::wallet::MintQuote> = quotes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        cdk_common::database::wallet::validate_issue_retirement(&saga, &quotes, proofs.as_deref())
            .map_err(FfiError::internal)?;
        self.inner
            .retire_issue(&saga, &quotes, proofs)
            .await
            .map_err(FfiError::internal)
    }

    async fn acquire_issue(
        &self,
        saga_json: String,
        quotes: Vec<MintQuote>,
        keyset: KeySet,
        counter_start: u32,
        counter_count: u32,
    ) -> Result<bool, FfiError> {
        let saga = serde_json::from_str(&saga_json).map_err(FfiError::internal)?;
        let quotes: Vec<cdk_common::wallet::MintQuote> = quotes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        cdk_common::database::wallet::validate_issue_acquisition(&saga, &quotes)
            .map_err(FfiError::internal)?;
        self.inner
            .acquire_issue(
                &saga,
                &quotes,
                keyset.try_into()?,
                counter_start,
                counter_count,
            )
            .await
            .map_err(FfiError::internal)
    }

    async fn acquire_melt(&self, saga_json: String, quote: MeltQuote) -> Result<bool, FfiError> {
        let saga = serde_json::from_str(&saga_json).map_err(FfiError::internal)?;
        let quote = quote.try_into()?;
        cdk_common::database::wallet::validate_melt_acquisition(&saga, &quote)
            .map_err(FfiError::internal)?;
        self.inner
            .acquire_melt(&saga, &quote)
            .await
            .map_err(FfiError::internal)
    }

    async fn reserve_melt_quote(
        &self,
        quote_id: String,
        operation_id: String,
    ) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        self.inner
            .reserve_melt_quote(&quote_id, &operation_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn release_melt_quote(&self, operation_id: String) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        self.inner
            .release_melt_quote(&operation_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn reserve_mint_quote(
        &self,
        quote_id: String,
        operation_id: String,
    ) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        self.inner
            .reserve_mint_quote(&quote_id, &operation_id)
            .await
            .map_err(FfiError::internal)
    }

    async fn release_mint_quote(&self, operation_id: String) -> Result<(), FfiError> {
        let operation_id = uuid::Uuid::parse_str(&operation_id).map_err(FfiError::internal)?;
        self.inner
            .release_mint_quote(&operation_id)
            .await
            .map_err(FfiError::internal)
    }
}

/// Macro to implement WalletDatabase for wrapper types that delegate to an inner FfiWalletDatabaseWrapper.
/// This eliminates duplication between SQLite, Postgres, Supabase, and other FFI implementations.
///
/// Requirements: The following types must be in scope where this macro is invoked:
/// - WalletDatabase, FfiError, PublicKey, ProofInfo, MintUrl, MintInfo, KeySetInfo, Id,
///   MintQuote, MeltQuote, Keys, CurrencyUnit, ProofState, SpendingConditions, Transaction,
///   TransactionId, TransactionDirection, KeySet
/// - std::collections::HashMap
#[macro_export]
macro_rules! impl_ffi_wallet_database {
    ($wrapper_type:ty) => {
        #[uniffi::export(async_runtime = "tokio")]
        #[async_trait::async_trait]
        impl WalletDatabase for $wrapper_type {
            // ========== Read methods ==========

            async fn get_unspent_proofs_bounded(
                &self,
                mint_url: MintUrl,
                unit: CurrencyUnit,
                max_proofs: u32,
                max_bytes: u32,
            ) -> Result<Vec<ProofInfo>, FfiError> {
                self.inner
                    .get_unspent_proofs_bounded(mint_url, unit, max_proofs, max_bytes)
                    .await
            }

            async fn get_proofs_by_ys(
                &self,
                ys: Vec<PublicKey>,
            ) -> Result<Vec<ProofInfo>, FfiError> {
                self.inner.get_proofs_by_ys(ys).await
            }

            async fn get_mint(&self, mint_url: MintUrl) -> Result<Option<MintInfo>, FfiError> {
                self.inner.get_mint(mint_url).await
            }

            async fn get_mints(
                &self,
            ) -> Result<std::collections::HashMap<MintUrl, Option<MintInfo>>, FfiError> {
                self.inner.get_mints().await
            }

            async fn get_mint_keysets(
                &self,
                mint_url: MintUrl,
            ) -> Result<Option<Vec<KeySetInfo>>, FfiError> {
                self.inner.get_mint_keysets(mint_url).await
            }

            async fn get_keyset_by_id(
                &self,
                keyset_id: Id,
            ) -> Result<Option<KeySetInfo>, FfiError> {
                self.inner.get_keyset_by_id(keyset_id).await
            }

            async fn get_mint_quote(
                &self,
                quote_id: String,
            ) -> Result<Option<MintQuote>, FfiError> {
                self.inner.get_mint_quote(quote_id).await
            }

            async fn get_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError> {
                self.inner.get_mint_quotes().await
            }

            async fn get_unissued_mint_quotes(&self) -> Result<Vec<MintQuote>, FfiError> {
                self.inner.get_unissued_mint_quotes().await
            }

            async fn get_unissued_mint_quotes_page(
                &self,
                mint_url: MintUrl,
                unit: CurrencyUnit,
                after: Option<String>,
                limit: u16,
            ) -> Result<$crate::database::MintQuotePage, FfiError> {
                self.inner
                    .get_unissued_mint_quotes_page(mint_url, unit, after, limit)
                    .await
            }

            async fn get_melt_quote(
                &self,
                quote_id: String,
            ) -> Result<Option<MeltQuote>, FfiError> {
                self.inner.get_melt_quote(quote_id).await
            }

            async fn get_melt_quotes(&self) -> Result<Vec<MeltQuote>, FfiError> {
                self.inner.get_melt_quotes().await
            }

            async fn get_keys(&self, id: Id) -> Result<Option<Keys>, FfiError> {
                self.inner.get_keys(id).await
            }

            async fn get_proofs(
                &self,
                mint_url: Option<MintUrl>,
                unit: Option<CurrencyUnit>,
                state: Option<Vec<ProofState>>,
                spending_conditions: Option<Vec<SpendingConditions>>,
            ) -> Result<Vec<ProofInfo>, FfiError> {
                self.inner
                    .get_proofs(mint_url, unit, state, spending_conditions)
                    .await
            }

            async fn get_balance(
                &self,
                mint_url: Option<MintUrl>,
                unit: Option<CurrencyUnit>,
                state: Option<Vec<ProofState>>,
            ) -> Result<u64, FfiError> {
                self.inner.get_balance(mint_url, unit, state).await
            }

            async fn get_transaction(
                &self,
                transaction_id: TransactionId,
            ) -> Result<Option<Transaction>, FfiError> {
                self.inner.get_transaction(transaction_id).await
            }

            async fn list_transactions(
                &self,
                mint_url: Option<MintUrl>,
                direction: Option<TransactionDirection>,
                unit: Option<CurrencyUnit>,
            ) -> Result<Vec<Transaction>, FfiError> {
                self.inner
                    .list_transactions(mint_url, direction, unit)
                    .await
            }

            async fn kv_read(
                &self,
                primary_namespace: String,
                secondary_namespace: String,
                key: String,
            ) -> Result<Option<Vec<u8>>, FfiError> {
                self.inner
                    .kv_read(primary_namespace, secondary_namespace, key)
                    .await
            }

            async fn kv_read_bounded(
                &self,
                primary_namespace: String,
                secondary_namespace: String,
                key: String,
                max_bytes: u32,
            ) -> Result<Option<Vec<u8>>, FfiError> {
                self.inner
                    .kv_read_bounded(primary_namespace, secondary_namespace, key, max_bytes)
                    .await
            }

            async fn kv_list(
                &self,
                primary_namespace: String,
                secondary_namespace: String,
            ) -> Result<Vec<String>, FfiError> {
                self.inner
                    .kv_list(primary_namespace, secondary_namespace)
                    .await
            }

            async fn kv_write(
                &self,
                primary_namespace: String,
                secondary_namespace: String,
                key: String,
                value: Vec<u8>,
            ) -> Result<(), FfiError> {
                self.inner
                    .kv_write(primary_namespace, secondary_namespace, key, value)
                    .await
            }

            async fn kv_remove(
                &self,
                primary_namespace: String,
                secondary_namespace: String,
                key: String,
            ) -> Result<(), FfiError> {
                self.inner
                    .kv_remove(primary_namespace, secondary_namespace, key)
                    .await
            }

            // ========== Write methods ==========

            async fn update_proofs(
                &self,
                added: Vec<ProofInfo>,
                removed_ys: Vec<PublicKey>,
            ) -> Result<(), FfiError> {
                self.inner.update_proofs(added, removed_ys).await
            }

            async fn compare_and_swap_proofs(
                &self,
                changes: Vec<$crate::types::ProofChange>,
                expected_saga_json: Option<String>,
            ) -> Result<bool, FfiError> {
                self.inner
                    .compare_and_swap_proofs(changes, expected_saga_json)
                    .await
            }

            async fn update_proofs_state(
                &self,
                ys: Vec<PublicKey>,
                state: ProofState,
            ) -> Result<(), FfiError> {
                self.inner.update_proofs_state(ys, state).await
            }

            async fn add_transaction(&self, transaction: Transaction) -> Result<(), FfiError> {
                self.inner.add_transaction(transaction).await
            }

            async fn remove_transaction(
                &self,
                transaction_id: TransactionId,
            ) -> Result<(), FfiError> {
                self.inner.remove_transaction(transaction_id).await
            }

            async fn update_mint_url(
                &self,
                old_mint_url: MintUrl,
                new_mint_url: MintUrl,
            ) -> Result<(), FfiError> {
                self.inner.update_mint_url(old_mint_url, new_mint_url).await
            }

            async fn increment_keyset_counter(
                &self,
                keyset_id: Id,
                count: u32,
            ) -> Result<u32, FfiError> {
                self.inner.increment_keyset_counter(keyset_id, count).await
            }

            async fn increment_derivation_counter(
                &self,
                namespace: String,
                count: u32,
            ) -> Result<u32, FfiError> {
                self.inner
                    .increment_derivation_counter(namespace, count)
                    .await
            }

            async fn add_mint(
                &self,
                mint_url: MintUrl,
                mint_info: Option<MintInfo>,
            ) -> Result<(), FfiError> {
                self.inner.add_mint(mint_url, mint_info).await
            }

            async fn remove_mint(&self, mint_url: MintUrl) -> Result<(), FfiError> {
                self.inner.remove_mint(mint_url).await
            }

            async fn add_mint_keysets(
                &self,
                mint_url: MintUrl,
                keysets: Vec<KeySetInfo>,
            ) -> Result<(), FfiError> {
                self.inner.add_mint_keysets(mint_url, keysets).await
            }

            async fn prepare_output_keyset(
                &self,
                operation_id: String,
                mint_url: MintUrl,
                keyset: KeySet,
            ) -> Result<(), FfiError> {
                self.inner
                    .prepare_output_keyset(operation_id, mint_url, keyset)
                    .await
            }

            async fn prepare_saga_outputs(
                &self,
                expected_json: Option<String>,
                next_json: String,
                keyset: KeySet,
                counter_start: u32,
                counter_count: u32,
                receipt: Option<Transaction>,
            ) -> Result<bool, FfiError> {
                self.inner
                    .prepare_saga_outputs(
                        expected_json,
                        next_json,
                        keyset,
                        counter_start,
                        counter_count,
                        receipt,
                    )
                    .await
            }

            async fn add_mint_quote(&self, quote: MintQuote) -> Result<(), FfiError> {
                self.inner.add_mint_quote(quote).await
            }

            async fn remove_mint_quote(&self, quote_id: String) -> Result<(), FfiError> {
                self.inner.remove_mint_quote(quote_id).await
            }

            async fn add_melt_quote(&self, quote: MeltQuote) -> Result<(), FfiError> {
                self.inner.add_melt_quote(quote).await
            }

            async fn remove_melt_quote(&self, quote_id: String) -> Result<(), FfiError> {
                self.inner.remove_melt_quote(quote_id).await
            }

            async fn add_keys(&self, keyset: KeySet) -> Result<(), FfiError> {
                self.inner.add_keys(keyset).await
            }

            async fn remove_keys(&self, id: Id) -> Result<(), FfiError> {
                self.inner.remove_keys(id).await
            }

            // P2PK methods

            async fn add_p2pk_key(
                &self,
                pubkey: PublicKey,
                derivation_path: String,
                derivation_index: u32,
            ) -> Result<(), FfiError> {
                self.inner
                    .add_p2pk_key(pubkey, derivation_path, derivation_index)
                    .await
            }

            async fn get_p2pk_key(
                &self,
                pubkey: PublicKey,
            ) -> Result<Option<P2PKSigningKey>, FfiError> {
                self.inner.get_p2pk_key(pubkey).await
            }

            async fn list_p2pk_keys(&self) -> Result<Vec<P2PKSigningKey>, FfiError> {
                self.inner.list_p2pk_keys().await
            }

            async fn latest_p2pk(&self) -> Result<Option<P2PKSigningKey>, FfiError> {
                self.inner.latest_p2pk().await
            }

            // ========== Saga management methods ==========

            async fn add_saga(&self, saga_json: String) -> Result<(), FfiError> {
                self.inner.add_saga(saga_json).await
            }

            async fn get_saga(&self, id: String) -> Result<Option<String>, FfiError> {
                self.inner.get_saga(id).await
            }

            async fn update_saga(&self, saga_json: String) -> Result<bool, FfiError> {
                self.inner.update_saga(saga_json).await
            }

            fn supports_atomic_issue_retirement(&self) -> bool {
                self.inner.supports_atomic_issue_retirement()
            }

            fn supports_atomic_melt_completion(&self) -> bool {
                self.inner.supports_atomic_melt_completion()
            }

            async fn complete_melt(
                &self,
                expected_json: String,
                next_json: String,
                quote: MeltQuote,
            ) -> Result<bool, FfiError> {
                self.inner
                    .complete_melt(expected_json, next_json, quote)
                    .await
            }

            fn supports_atomic_saga_completion(&self) -> bool {
                self.inner.supports_atomic_saga_completion()
            }

            async fn complete_saga(
                &self,
                expected_json: String,
                next_json: String,
                receipt: Transaction,
            ) -> Result<bool, FfiError> {
                self.inner
                    .complete_saga(expected_json, next_json, receipt)
                    .await
            }

            async fn delete_saga(&self, id: String) -> Result<(), FfiError> {
                self.inner.delete_saga(id).await
            }

            async fn get_incomplete_saga_ids(
                &self,
                after_json: Option<String>,
                limit: u16,
            ) -> Result<Vec<String>, FfiError> {
                self.inner.get_incomplete_saga_ids(after_json, limit).await
            }

            async fn get_incomplete_sagas(&self) -> Result<Vec<String>, FfiError> {
                self.inner.get_incomplete_sagas().await
            }

            // ========== Proof reservation methods ==========

            async fn reserve_proofs(
                &self,
                ys: Vec<PublicKey>,
                operation_id: String,
            ) -> Result<(), FfiError> {
                self.inner.reserve_proofs(ys, operation_id).await
            }

            async fn release_proofs(&self, operation_id: String) -> Result<(), FfiError> {
                self.inner.release_proofs(operation_id).await
            }

            async fn get_reserved_proofs(
                &self,
                operation_id: String,
            ) -> Result<Vec<ProofInfo>, FfiError> {
                self.inner.get_reserved_proofs(operation_id).await
            }

            // ========== Quote reservation methods ==========

            async fn retire_issue(
                &self,
                saga_json: String,
                quotes: Vec<MintQuote>,
                proofs: Option<Vec<$crate::types::ProofChange>>,
            ) -> Result<bool, FfiError> {
                self.inner.retire_issue(saga_json, quotes, proofs).await
            }

            async fn acquire_issue(
                &self,
                saga_json: String,
                quotes: Vec<MintQuote>,
                keyset: KeySet,
                counter_start: u32,
                counter_count: u32,
            ) -> Result<bool, FfiError> {
                self.inner
                    .acquire_issue(saga_json, quotes, keyset, counter_start, counter_count)
                    .await
            }

            async fn acquire_melt(
                &self,
                saga_json: String,
                quote: MeltQuote,
            ) -> Result<bool, FfiError> {
                self.inner.acquire_melt(saga_json, quote).await
            }

            async fn reserve_melt_quote(
                &self,
                quote_id: String,
                operation_id: String,
            ) -> Result<(), FfiError> {
                self.inner.reserve_melt_quote(quote_id, operation_id).await
            }

            async fn release_melt_quote(&self, operation_id: String) -> Result<(), FfiError> {
                self.inner.release_melt_quote(operation_id).await
            }

            async fn reserve_mint_quote(
                &self,
                quote_id: String,
                operation_id: String,
            ) -> Result<(), FfiError> {
                self.inner.reserve_mint_quote(quote_id, operation_id).await
            }

            async fn release_mint_quote(&self, operation_id: String) -> Result<(), FfiError> {
                self.inner.release_mint_quote(operation_id).await
            }
        }
    };
}

/// FFI-safe database type enum
#[derive(uniffi::Enum, Clone)]
pub enum WalletDbBackend {
    Sqlite {
        path: String,
    },
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
    },
}

/// Unified wallet storage: either a built-in Rust backend or a custom
/// foreign-language implementation of the `WalletDatabase` callback interface.
///
/// Wallet methods can write to this store from FFI calls that mint, receive,
/// recover, subscribe, or check quote/proof state. Mobile host apps own platform
/// lifecycle handling for the chosen backend: use a durable app-owned location,
/// pause or cancel wallet work when backgrounding unless background storage
/// activity is intended, and use platform facilities such as iOS
/// `beginBackgroundTask` when writes must finish after a lifecycle transition.
///
/// This is an enum rather than accepting `WalletDatabase` directly because UniFFI
/// does not support trait objects as constructor parameters — only callback interfaces
/// wrapped in `Arc<dyn Trait>` inside an enum variant work across the FFI boundary.
#[derive(uniffi::Enum)]
pub enum WalletStore {
    Sqlite {
        path: String,
    },
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
    },
    Custom {
        db: Arc<dyn WalletDatabase>,
    },
}

/// Create a SQLite-backed wallet store.
///
/// Wallet operations may later write to this SQLite database. Mobile hosts are
/// responsible for choosing a durable file path and coordinating background
/// lifecycle transitions around wallet calls.
#[uniffi::export]
pub fn sqlite_wallet_store(path: String) -> WalletStore {
    WalletStore::Sqlite { path }
}

/// Create a PostgreSQL-backed wallet store.
///
/// Wallet operations may later write through this backend. Mobile hosts are
/// responsible for coordinating background lifecycle transitions around wallet
/// calls and any network/storage activity they trigger.
#[cfg(feature = "postgres")]
#[uniffi::export]
pub fn postgres_wallet_store(url: String) -> WalletStore {
    WalletStore::Postgres { url }
}

/// Create a wallet store backed by a custom foreign-language database implementation.
///
/// Wallet operations may later call write methods on the supplied database.
/// Mobile hosts are responsible for making those callbacks lifecycle-safe and
/// durable across app foreground/background transitions.
#[uniffi::export]
pub fn custom_wallet_store(db: Arc<dyn WalletDatabase>) -> WalletStore {
    WalletStore::Custom { db }
}

/// Resolve a `WalletStore` into an `Arc<dyn WalletDatabase>`.
pub fn resolve_wallet_store(store: WalletStore) -> Result<Arc<dyn WalletDatabase>, FfiError> {
    match store {
        WalletStore::Sqlite { path } => {
            let sqlite = WalletSqliteDatabase::new(path)?;
            Ok(sqlite as Arc<dyn WalletDatabase>)
        }
        #[cfg(feature = "postgres")]
        WalletStore::Postgres { url } => {
            let pg = WalletPostgresDatabase::new(url)?;
            Ok(pg as Arc<dyn WalletDatabase>)
        }
        WalletStore::Custom { db } => Ok(db),
    }
}

/// Factory helpers returning a CDK wallet database behind the FFI trait
#[uniffi::export]
pub fn create_wallet_db(backend: WalletDbBackend) -> Result<Arc<dyn WalletDatabase>, FfiError> {
    match backend {
        WalletDbBackend::Sqlite { path } => {
            let sqlite = WalletSqliteDatabase::new(path)?;
            Ok(sqlite as Arc<dyn WalletDatabase>)
        }
        #[cfg(feature = "postgres")]
        WalletDbBackend::Postgres { url } => {
            let pg = WalletPostgresDatabase::new(url)?;
            Ok(pg as Arc<dyn WalletDatabase>)
        }
    }
}

/// Helper function to create a CDK database from the FFI trait
pub fn create_cdk_database_from_ffi(
    ffi_db: Arc<dyn WalletDatabase>,
) -> Arc<dyn CdkWalletDatabase<cdk::cdk_database::Error> + Send + Sync> {
    Arc::new(WalletDatabaseBridge::new(ffi_db))
}
