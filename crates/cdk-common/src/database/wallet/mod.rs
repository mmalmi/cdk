//! CDK Database

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use bitcoin::bip32::DerivationPath;
use cashu::KeySet;

use super::Error;
use crate::mint_url::MintUrl;
use crate::nuts::{
    CurrencyUnit, Id, KeySetInfo, Keys, MintInfo, PublicKey, SpendingConditions, State,
};
use crate::wallet::{
    self, MintQuote as WalletMintQuote, ProofInfo, Transaction, TransactionDirection, TransactionId,
};

mod melt_completion;
pub use self::melt_completion::{validate_melt_completion, MeltCompletion};

mod issue_retirement;
pub use self::issue_retirement::{validate_issue_retirement, IssueRetirement};
mod issue_acquisition;
pub use self::issue_acquisition::validate_issue_acquisition;
mod issue_receipts;
pub use self::issue_receipts::issue_receipts;

mod melt_acquisition;
pub use self::melt_acquisition::{validate_melt_acquisition, validate_melt_inputs};

mod proof_change;
pub use proof_change::{validate_proof_changes, ProofChange};
mod saga_completion;
pub use saga_completion::{
    merge_saga_receipt, validate_output_preparation, validate_saga_completion,
    validate_saga_transition,
};

mod transaction_page;
pub use transaction_page::{
    TransactionCursor, MAX_TRANSACTION_PAGE_BYTES, MAX_TRANSACTION_PAGE_SIZE,
};

mod saga_page;
pub use saga_page::{
    validate_saga_page_request, IncompleteSagaReader, SagaCursor, MAX_SAGA_PAGE_SIZE,
};

mod send_history;
pub use send_history::{
    incoming_history_inputs_spent, send_history_inputs_spent, token_history_input_spent,
    token_history_inputs_spent, validate_incoming_history_retirement,
    validate_send_history_retirement, validate_token_history_retirement,
};

#[cfg(feature = "test")]
pub mod test;

/// Maximum number of proof records returned by an operation page.
pub const MAX_PROOF_PAGE_SIZE: u16 = 128;

/// Maximum aggregate stored payload returned by an operation proof page.
pub const MAX_PROOF_PAGE_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum UTF-8 bytes retained as a withdrawal payment proof, for every payment method.
/// Larger mint results retain the pending operation for recovery instead of publishing.
pub const MAX_MELT_PAYMENT_PROOF_BYTES: usize = 64 * 1024;

/// Maximum number of pending mint quotes read in one recovery page.
pub const MAX_MINT_QUOTE_PAGE_SIZE: u16 = 128;

/// Maximum aggregate stored payload returned by a pending mint quote page.
pub const MAX_MINT_QUOTE_PAGE_BYTES: u64 = 4 * 1024 * 1024;

/// Wallet Database trait
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Database<Err>: Debug
where
    Err: Into<Error> + From<Error>,
{
    /// Get mint from storage
    async fn get_mint(&self, mint_url: MintUrl) -> Result<Option<MintInfo>, Err>;

    /// Get all mints from storage
    async fn get_mints(&self) -> Result<HashMap<MintUrl, Option<MintInfo>>, Err>;

    /// Get mint keysets for mint url
    async fn get_mint_keysets(&self, mint_url: MintUrl) -> Result<Option<Vec<KeySetInfo>>, Err>;

    /// Get mint keyset by id
    async fn get_keyset_by_id(&self, keyset_id: &Id) -> Result<Option<KeySetInfo>, Err>;

    /// Get mint quote from storage
    async fn get_mint_quote(&self, quote_id: &str) -> Result<Option<WalletMintQuote>, Err>;

    /// Get mint quotes from storage
    async fn get_mint_quotes(&self) -> Result<Vec<WalletMintQuote>, Err>;
    /// Get unissued mint quotes from storage
    /// Returns bolt11 quotes where nothing has been issued yet (amount_issued = 0) and all bolt12 quotes.
    /// Includes unpaid bolt11 quotes to allow checking with the mint if they've been paid (wallet state may be outdated).
    async fn get_unissued_mint_quotes(&self) -> Result<Vec<WalletMintQuote>, Err>;

    /// Read pending mint quotes for one exact mint and unit, ordered by ID.
    ///
    /// Select at most `limit` rows after the exclusive ID cursor, including
    /// unpaid/expired quotes with no issued amount and all BOLT12 quotes. Require
    /// `1..=MAX_MINT_QUOTE_PAGE_SIZE`; reject an oversized page before loading its
    /// payload, using one snapshot for sizing and decoding. Corrupt selected
    /// records fail instead of being skipped. A deleted cursor remains valid.
    /// Pages are not a shared snapshot; callers coordinate writers. Unsupported
    /// backends fail without falling back to an unbounded read.
    async fn get_unissued_mint_quotes_page(
        &self,
        _mint_url: MintUrl,
        _unit: CurrencyUnit,
        _after: Option<String>,
        _limit: u16,
    ) -> Result<Vec<WalletMintQuote>, Err> {
        Err(
            Error::Internal("Mint quote pagination is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Get melt quote from storage
    async fn get_melt_quote(&self, quote_id: &str) -> Result<Option<wallet::MeltQuote>, Err>;

    /// Get melt quotes from storage
    async fn get_melt_quotes(&self) -> Result<Vec<wallet::MeltQuote>, Err>;

    /// Get [`Keys`] from storage
    async fn get_keys(&self, id: &Id) -> Result<Option<Keys>, Err>;

    /// Get proofs from storage
    async fn get_proofs(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<State>>,
        spending_conditions: Option<Vec<SpendingConditions>>,
    ) -> Result<Vec<ProofInfo>, Err>;

    /// Get proofs by Y values
    async fn get_proofs_by_ys(&self, ys: Vec<PublicKey>) -> Result<Vec<ProofInfo>, Err>;

    /// Read the complete unspent inventory in one exact mint/unit scope.
    ///
    /// Reject before loading variable-length payloads when either budget is
    /// exceeded; never truncate the inventory or skip corrupt selected rows.
    /// Size checks and payload reads must share one database snapshot. At most
    /// `max_proofs + 1` matching rows need inspection to reject excess inventory.
    /// Zero limits admit an empty inventory. Unsupported backends fail without
    /// an unbounded fallback. This grants no reservation or release rights.
    async fn get_unspent_proofs_bounded(
        &self,
        _mint_url: MintUrl,
        _unit: CurrencyUnit,
        _max_proofs: u32,
        _max_bytes: u32,
    ) -> Result<Vec<ProofInfo>, Err> {
        Err(
            Error::Internal("Bounded proof inventory is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Read proofs created by one exact wallet operation, across all wallet scopes.
    ///
    /// Returns at most `limit` records in ascending binary Y order, strictly after
    /// `after_y` when supplied. The cursor need not still exist in storage. Limits
    /// outside `1..=MAX_PROOF_PAGE_SIZE` are invalid. Selected rows must decode
    /// completely; corrupt evidence must not be skipped. An empty page is the end.
    ///
    /// Pages are separate reads, not a snapshot across calls. Callers must fence
    /// changes to the selected operation's proof identities while traversing it.
    /// Selected payloads must fit `MAX_PROOF_PAGE_BYTES` before crossing the storage
    /// driver boundary. Oversized pages fail without returning a partial page or
    /// changing evidence. This grants no release rights or total storage bound.
    /// Backends without an indexed implementation fail without scanning storage.
    async fn get_proofs_created_by_operation_page(
        &self,
        _operation: uuid::Uuid,
        _after_y: Option<PublicKey>,
        _limit: u16,
    ) -> Result<Vec<ProofInfo>, Err> {
        Err(Error::Internal(
            "Proof operation pagination is not supported by this backend".to_owned(),
        )
        .into())
    }

    /// Get balance
    async fn get_balance(
        &self,
        mint_url: Option<MintUrl>,
        unit: Option<CurrencyUnit>,
        state: Option<Vec<State>>,
    ) -> Result<u64, Err>;

    /// Get transaction from storage
    async fn get_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<Transaction>, Err>;

    /// List transactions from storage
    async fn list_transactions(
        &self,
        mint_url: Option<MintUrl>,
        direction: Option<TransactionDirection>,
        unit: Option<CurrencyUnit>,
    ) -> Result<Vec<Transaction>, Err>;

    /// Read one indexed page with optional mint, direction and unit filters.
    /// `None` includes every value of that field, including unregistered mints.
    ///
    /// Returns at most `limit` rows in ascending `(timestamp, binary id)` order,
    /// strictly after `after` when supplied, without looking up the cursor row.
    /// Limits outside `1..=MAX_TRANSACTION_PAGE_SIZE` and cursor timestamps above
    /// `i64::MAX` are invalid. Selected corrupt/noncanonical records fail the
    /// whole page. If the selected rows exceed [`MAX_TRANSACTION_PAGE_BYTES`],
    /// fail before loading their variable-length payloads into the row decoder;
    /// do not skip or truncate records. Callers may retry with a smaller limit.
    ///
    /// An empty page is the end. Reads across pages are not a snapshot: callers
    /// needing a stable traversal must fence changes to their scope. This read
    /// grants no history release rights. Unsupported backends fail without a
    /// full-table fallback.
    async fn get_transactions_page(
        &self,
        _mint_url: Option<MintUrl>,
        _direction: Option<TransactionDirection>,
        _unit: Option<CurrencyUnit>,
        _after: Option<TransactionCursor>,
        _limit: u16,
    ) -> Result<Vec<Transaction>, Err> {
        Err(
            Error::Internal("Transaction pagination is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Update the proofs in storage by adding new proofs or removing proofs by
    /// their Y value
    async fn update_proofs(
        &self,
        added: Vec<ProofInfo>,
        removed_ys: Vec<PublicKey>,
    ) -> Result<(), Err>;

    /// Compare every original proof record and atomically apply the whole batch.
    ///
    /// Returns `false` without changing any record if an original does not match.
    /// An optional saved saga must also match in the same transaction, including
    /// its version and data; even an empty proof batch checks this fence.
    /// Comparisons include state, witnesses and operation provenance, not just
    /// the coin identity. Concurrent callers must not both replace the same
    /// original. Invalid batches and storage failures return an error without
    /// committing a partial batch. This is not an authorization to erase history.
    /// A connection failure at commit can leave the full outcome uncertain;
    /// reconcile the saved intent and current records before retrying.
    ///
    /// Custom backends must implement the atomic contract before using recovery
    /// paths that require it; the default fails without touching storage.
    async fn compare_and_swap_proofs(
        &self,
        _changes: Vec<ProofChange>,
        _expected_saga: Option<&wallet::WalletSaga>,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic proof comparison is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Update proofs state in storage
    async fn update_proofs_state(&self, ys: Vec<PublicKey>, state: State) -> Result<(), Err>;

    /// Add transaction to storage.
    ///
    /// Implementations must reject noncanonical refund receipts using
    /// [`Transaction::validate_role`] before deriving the storage ID.
    async fn add_transaction(&self, transaction: Transaction) -> Result<(), Err>;

    /// Update mint url
    async fn update_mint_url(
        &self,
        old_mint_url: MintUrl,
        new_mint_url: MintUrl,
    ) -> Result<(), Err>;

    /// Atomically increment the keyset counter and return its new value.
    /// A zero count reads the current value (zero when absent) without writing.
    async fn increment_keyset_counter(&self, keyset_id: &Id, count: u32) -> Result<u32, Err>;

    /// Atomically increment a namespaced derivation counter and return its new value.
    async fn increment_derivation_counter(&self, namespace: &str, count: u32) -> Result<u32, Err>;

    /// Add Mint to storage
    async fn add_mint(&self, mint_url: MintUrl, mint_info: Option<MintInfo>) -> Result<(), Err>;

    /// Remove Mint from storage
    async fn remove_mint(&self, mint_url: MintUrl) -> Result<(), Err>;

    /// Add mint keyset to storage
    async fn add_mint_keysets(
        &self,
        mint_url: MintUrl,
        keysets: Vec<KeySetInfo>,
    ) -> Result<(), Err>;

    /// Persist output keys before allocating secrets. Capacity-limited stores
    /// must save metadata, counter storage and its allowance atomically. They may
    /// enlarge preparation allowances only while the operation can still cancel
    /// without contacting the mint. Recovery consumes its admitted allowance.
    /// The caller retains the operation's financial ownership and writer lock.
    async fn prepare_output_keyset(
        &self,
        _operation_id: uuid::Uuid,
        mint_url: MintUrl,
        keyset: KeySet,
    ) -> Result<(), Err> {
        let keys = self.get_keys(&keyset.id).await?;
        if keys.as_ref().is_some_and(|keys| *keys != keyset.keys) {
            return Err(Error::InvalidDbResponse.into());
        }
        let info = KeySetInfo {
            id: keyset.id,
            unit: keyset.unit.clone(),
            active: keyset.active.unwrap_or(false),
            input_fee_ppk: keyset.input_fee_ppk,
            final_expiry: keyset.final_expiry,
        };
        if self.get_keyset_by_id(&keyset.id).await?.as_ref() != Some(&info) {
            self.add_mint_keysets(mint_url, vec![info]).await?;
        }
        if keys.is_none() {
            self.add_keys(keyset).await?;
        }
        Ok(())
    }

    /// Atomically commit output keys, a proposed counter range and the next saga.
    ///
    /// Compare the complete expected saga and the current keyset counter with
    /// `counter_start`; return false without mutations if either changed. An
    /// absent counter is zero. Validate the keyset and advance by `counter_count`
    /// only in the same transaction as metadata, storage admission and the next
    /// single-version saga. An optional receipt follows the `complete_saga`
    /// identity and status rules and commits in the same transaction. With no
    /// expected parent, only a new initial standalone swap without a receipt is
    /// allowed; its operation ID must be unused. Any failure
    /// leaves all records unchanged.
    ///
    /// Proposed secrets must remain private until this call succeeds. The caller
    /// validates that the saved request uses exactly this keyset and counter range.
    async fn prepare_saga_outputs(
        &self,
        _expected: Option<&wallet::WalletSaga>,
        _next: wallet::WalletSaga,
        _keyset: KeySet,
        _counter_start: u32,
        _counter_count: u32,
        _receipt: Option<Transaction>,
    ) -> Result<bool, Err> {
        Err(Error::Internal("Atomic output preparation is required".to_owned()).into())
    }

    /// Add mint quote to storage
    async fn add_mint_quote(&self, quote: WalletMintQuote) -> Result<(), Err>;

    /// Remove mint quote from storage
    async fn remove_mint_quote(&self, quote_id: &str) -> Result<(), Err>;

    /// Add melt quote to storage
    async fn add_melt_quote(&self, quote: wallet::MeltQuote) -> Result<(), Err>;

    /// Remove melt quote from storage
    async fn remove_melt_quote(&self, quote_id: &str) -> Result<(), Err>;

    /// Add [`Keys`] to storage
    async fn add_keys(&self, keyset: KeySet) -> Result<(), Err>;

    /// Remove [`Keys`] from storage
    async fn remove_keys(&self, id: &Id) -> Result<(), Err>;

    /// Remove transaction from storage
    async fn remove_transaction(&self, transaction_id: TransactionId) -> Result<(), Err>;

    /// Check whether an exact incoming or outgoing token receipt can be retired.
    ///
    /// Check the receipt, inactive operation and complete original spent inputs
    /// using the same validation as atomic retirement. Bound receipt reads by
    /// [`MAX_TRANSACTION_PAGE_BYTES`] and each selected proof page by
    /// [`MAX_PROOF_PAGE_SIZE`] and [`MAX_PROOF_PAGE_BYTES`] before loading payloads.
    /// Oversized groups must retry smaller pages; individually oversized or
    /// malformed records return an error without skipping evidence. Checking
    /// operation existence must not load its recovery payload.
    ///
    /// Invalid, changed, absent or still-active evidence returns false. This is
    /// advisory, grants no release rights and changes nothing; atomic retirement
    /// must repeat all checks. Unsupported backends fail without unbounded reads.
    async fn token_transaction_retirable(&self, _expected: &Transaction) -> Result<bool, Err> {
        Err(Error::Internal(
            "Bounded token eligibility is not supported by this backend".to_owned(),
        )
        .into())
    }

    /// Atomically retire a bounded batch of exact outgoing token records after
    /// the caller durably releases retry rights and preserves cumulative costs.
    /// Requires terminal records, absent sagas and spent inputs owned by each send.
    /// A mismatch returns false without removing anything. Coins are never removed.
    async fn retire_send_transactions(&self, _expected: Vec<Transaction>) -> Result<bool, Err> {
        Err(
            Error::Internal("Token history retirement is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Atomically retire explicitly acknowledged incoming token receipts.
    /// Requires exact completed records, absent sagas and retained spent inputs
    /// whose total equals the receipt amount plus fees. Coins are never removed.
    /// Persist the caller's accounting and release result rights before calling.
    async fn retire_incoming_transactions(&self, _expected: Vec<Transaction>) -> Result<bool, Err> {
        Err(
            Error::Internal("Token history retirement is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Add a wallet saga to storage.
    ///
    /// The saga should be created with `WalletSaga::new()` which initializes
    /// `version = 0`. This is the starting point for optimistic locking.
    async fn add_saga(&self, saga: wallet::WalletSaga) -> Result<(), Err>;

    /// Get a wallet saga by ID.
    async fn get_saga(&self, id: &uuid::Uuid) -> Result<Option<wallet::WalletSaga>, Err>;

    /// Update a wallet saga with optimistic locking.
    ///
    /// Returns `Ok(true)` if the update succeeded (version match), or `Ok(false)`
    /// if another instance modified the saga first (version mismatch).
    async fn update_saga(&self, saga: wallet::WalletSaga) -> Result<bool, Err>;

    /// Whether this backend implements the atomic `complete_saga` contract.
    /// Custom backends must opt in only after implementing the entire contract.
    fn supports_atomic_saga_completion(&self) -> bool {
        false
    }

    /// Atomically publish a receipt and advance an exactly matching parent saga.
    /// Immutable receipt conflicts or a missing/changed parent return false with
    /// no writes. Matching terminal receipt statuses must never be downgraded.
    /// This does not change proofs; see `compare_and_swap_proofs` for that boundary.
    async fn complete_saga(
        &self,
        _expected: &wallet::WalletSaga,
        _next: wallet::WalletSaga,
        _receipt: Transaction,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic saga completion is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Delete a wallet saga.
    async fn delete_saga(&self, id: &uuid::Uuid) -> Result<(), Err>;

    /// Get all incomplete sagas.
    async fn get_incomplete_sagas(&self) -> Result<Vec<wallet::WalletSaga>, Err>;

    /// Read at most `limit` unfinished operation identities after the cursor.
    ///
    /// Return ascending `(created_at, id)` order across every mint and unit,
    /// without loading recovery payloads. IDs must be canonical, timestamps
    /// nonnegative, and `limit` in `1..=MAX_SAGA_PAGE_SIZE`. Corrupt selected
    /// headers fail the page instead of being skipped. A deleted cursor remains
    /// valid. This is not a snapshot across pages; callers coordinate writers.
    /// Custom backends must support this before using wallet-wide recovery.
    async fn get_incomplete_saga_ids(
        &self,
        _after: Option<SagaCursor>,
        _limit: u16,
    ) -> Result<Vec<SagaCursor>, Err> {
        Err(Error::Internal("Saga pagination is not supported by this backend".to_owned()).into())
    }

    /// Reserve proofs for an operation
    async fn reserve_proofs(
        &self,
        ys: Vec<PublicKey>,
        operation_id: &uuid::Uuid,
    ) -> Result<(), Err>;

    /// Release proofs reserved by an operation.
    ///
    /// Only proofs owned by `operation_id` in [`State::Reserved`] or
    /// [`State::Pending`] may be changed to [`State::Unspent`]. Implementations
    /// must preserve spent proofs and proofs owned by another operation.
    async fn release_proofs(&self, operation_id: &uuid::Uuid) -> Result<(), Err>;

    /// Get proofs reserved by an operation
    async fn get_reserved_proofs(&self, operation_id: &uuid::Uuid) -> Result<Vec<ProofInfo>, Err>;

    /// Whether this backend implements the full atomic paid-melt completion contract.
    fn supports_atomic_melt_completion(&self) -> bool {
        false
    }

    /// Atomically publish a paid receipt, unlock its quote and finish the exact parent.
    ///
    /// The expected PaidReady saga must retain verified payment evidence. Its exact
    /// Pending receipt and the complete owned quote must still exist. A mismatch
    /// returns false without writes, including missing or already-final receipts.
    /// The caller must finish proof custody before this boundary; it changes no proofs.
    async fn complete_melt(
        &self,
        _expected: &wallet::WalletSaga,
        _next: wallet::WalletSaga,
        _quote: &wallet::MeltQuote,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic melt completion is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Atomically reserve a complete quote and proof batch and insert a new melt saga.
    ///
    /// The saga must contain its exact acquisition plan. The stored quote must equal
    /// `quote`, the saga ID must be absent, and every proof must match its original
    /// record (including absence for external proofs). A mismatch returns `false`
    /// without writes; invalid intent or a storage failure returns an error.
    /// Success sets the quote owner and increments its version; callers must reload it.
    ///
    /// Implementations must commit all three parts together. An unknown commit
    /// outcome must be reconciled through the saved saga, never separate writes.
    async fn acquire_melt(
        &self,
        _saga: &wallet::WalletSaga,
        _quote: &wallet::MeltQuote,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic melt acquisition is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Atomically acquire every original mint quote and save the initial issue intent
    /// with its canonical pending receipts, output keys and derivation range.
    /// Compare the keyset counter with `counter_start` (absent means zero), then
    /// advance by `counter_count` in that same transaction. Proposed secrets
    /// remain private until success; callers validate the exact saved range.
    ///
    /// Requires an unused operation ID, exact unreserved quote snapshots and the
    /// complete valid initial intent with unused receipt identities. Returns false
    /// without changes on conflict.
    /// An uncertain commit must be reconciled through that same saved operation.
    /// Custom backends must implement the atomic contract before issuing coins.
    async fn acquire_issue(
        &self,
        _saga: &wallet::WalletSaga,
        _quotes: &[WalletMintQuote],
        _keyset: KeySet,
        _counter_start: u32,
        _counter_count: u32,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic issue acquisition is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Whether this backend implements the entire atomic issue retirement contract.
    fn supports_atomic_issue_retirement(&self) -> bool {
        false
    }

    /// Atomically publish original issue outputs and receipts, update and release
    /// every quote, and delete the unchanged journal. Submitted work supplies the
    /// complete original proof batch; existing output records must be preserved.
    /// Prepared cancellation supplies no proofs and marks pending receipts failed.
    /// Missing or changed authority, conflicting proofs or receipts, or a missing
    /// submitted receipt returns false without writes.
    /// Custom backends must implement the entire contract; sequential fallback is unsafe.
    async fn retire_issue(
        &self,
        _expected: &wallet::WalletSaga,
        _quotes: &[WalletMintQuote],
        _proofs: Option<Vec<ProofChange>>,
    ) -> Result<bool, Err> {
        Err(
            Error::Internal("Atomic issue retirement is not supported by this backend".to_owned())
                .into(),
        )
    }

    /// Reserve a melt quote for an operation.
    async fn reserve_melt_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), Err>;

    /// Release a melt quote reserved by an operation.
    async fn release_melt_quote(&self, operation_id: &uuid::Uuid) -> Result<(), Err>;

    /// Reserve a mint quote for an operation.
    async fn reserve_mint_quote(
        &self,
        quote_id: &str,
        operation_id: &uuid::Uuid,
    ) -> Result<(), Err>;

    /// Release a mint quote reserved by an operation.
    async fn release_mint_quote(&self, operation_id: &uuid::Uuid) -> Result<(), Err>;

    /// Read a value from the key-value store
    async fn kv_read(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Err>;

    /// Read a value without copying an oversized payload out of storage.
    ///
    /// The size check and read must share one snapshot. Missing keys return
    /// `None`; oversized or malformed stored values return an error without
    /// changing the record. Backends must enforce the bound before allocating
    /// the result or transferring it from the database, not after `kv_read`.
    async fn kv_read_bounded(
        &self,
        _primary_namespace: &str,
        _secondary_namespace: &str,
        _key: &str,
        _max_bytes: u32,
    ) -> Result<Option<Vec<u8>>, Err> {
        Err(
            Error::Internal("Bounded key-value reads are not supported by this backend".to_owned())
                .into(),
        )
    }

    /// List keys in a namespace
    async fn kv_list(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
    ) -> Result<Vec<String>, Err>;

    /// Write a value to the key-value store
    async fn kv_write(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
        value: &[u8],
    ) -> Result<(), Err>;

    /// Remove a value from the key-value store
    async fn kv_remove(
        &self,
        primary_namespace: &str,
        secondary_namespace: &str,
        key: &str,
    ) -> Result<(), Err>;

    // P2PK signing key methods

    /// Store a P2PK signing key for the wallet
    async fn add_p2pk_key(
        &self,
        pubkey: &PublicKey,
        derivation_path: DerivationPath,
        derivation_index: u32,
    ) -> Result<(), Err>;

    /// Get a stored P2PK signing key by pubkey.
    async fn get_p2pk_key(&self, pubkey: &PublicKey)
        -> Result<Option<wallet::P2PKSigningKey>, Err>;

    /// List all stored P2PK signing keys.
    async fn list_p2pk_keys(&self) -> Result<Vec<wallet::P2PKSigningKey>, Err>;

    /// Tries to get the latest p2pk key generated
    async fn latest_p2pk(&self) -> Result<Option<wallet::P2PKSigningKey>, Err>;
}
