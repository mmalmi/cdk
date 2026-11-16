# CDK Wallet

The CDK [`Wallet`] is a high level Cashu wallet. The [`Wallet`] is for a single mint and single unit. Multiple [`Wallet`]s can be created to support multi mints and multi units.


## Example

### Create and Initialize [`Wallet`]

```rust
use std::sync::Arc;
use cdk::nuts::CurrencyUnit;
use cdk::wallet::Wallet;
use cdk_sqlite::wallet::memory;
use rand::random;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let seed = random::<[u8; 64]>();
    let mint_url = "https://testnut.cashudevkit.org";
    let unit = CurrencyUnit::Sat;

    let localstore = memory::empty().await?;
    let wallet = Wallet::new(mint_url, unit, Arc::new(localstore), seed, None)?;

    // Required: Recover crashed operations (swap, send, receive, melt)
    // This prevents proofs from being stuck in reserved states.
    let report = wallet.recover_incomplete_sagas().await?;
    println!("Recovered: {}, Compensated: {}, Skipped: {}, Failed: {}",
        report.recovered, report.compensated, report.skipped, report.failed);

    // Optional: Check and mint pending mint quotes (makes network calls)
    let minted = wallet.mint_unissued_quotes().await?;
    println!("Minted {} from pending quotes", minted);

    Ok(())
}
```

## Read-only proof status

`Wallet::check_proofs_state` queries the mint without changing local proofs or
reservations. It validates the complete response and returns states in request
order, including for proofs absent from local storage. An `Unspent` answer does
not verify a bearer proof's signature or prevent a later spend. The common wallet
trait and FFI expose the same query; custom wallet-trait implementations must
provide it.

`check_proofs_spent` uses the same validated query, then marks known spent proofs
without deleting their ownership or derivation records. Unknown proofs are not
imported. The orphaned-pending check also retains this evidence. Applications must
explicitly release completed history after their own accounting is durable.

## Explicit history retirement

`retire_send_transactions` and `retire_incoming_transactions` remove at most 128
exact token-history records atomically. First persist the caller's cumulative
accounting and relinquish detailed-result retry rights. A lost commit reply must
be resolved by reading the records, never by applying another credit.

SQLite, PostgreSQL and redb check every expected row, terminal status, absence of
the operation's saga and retained spent-input evidence in one transaction. A
changed or missing row rejects the whole batch. Coin records, mint/melt records
and other wallet histories remain intact. Unsupported custom database backends
return an error. The common wallet trait and FFI expose the same methods.

The 0.18 implementation uses its native saga-based transaction IDs. Incoming
receipt retirement currently covers completed token receives; separate durable
refund receipts remain part of the later reclaim/recovery integration.

## Bound a send before spending

Set `SendOptions::max_total_amount` to cap the wallet debit, including token
value, redemption-fee reserve (when requested), denomination rounding and the
input fee of any required swap. Change returned to the wallet is excluded.
Preparation rejects an over-budget plan before reserving proofs; confirmation
rechecks the cap and the swap's approved terms before spending inputs.

`SendOptions::required_keyset_id` optionally requires all token proofs to use
the selected active keyset. Online sends convert selected older proofs in the
same priced swap instead of consolidating the whole wallet. Offline sends can
only reuse matching proofs. Keyset changes detected before a mint request reject without spending. Once
confirmation starts, recovery retains the original operation and its terms.

### Recover an interrupted send

Confirmation saves a `Confirming` journal entry before contacting the mint. It
contains the exact inputs, blinded outputs and secrets, memo, metadata, keyset
requirement and approved debit. Recovery restores those outputs or retries the
same request when none were issued; it never selects replacement inputs.
Incomplete or inconsistent responses leave the journal available for another
recovery attempt. Verified outputs are saved before import so interrupted local
writes can resume without another mint request. These records contain bearer
secrets and must remain private.

### Cancel a send before confirmation

Preparation first saves `PreparedAcquiring` with the exact intended reserved input
rows and original caller metadata. Only then may one atomic proof comparison
reserve the complete batch under that saved version. Promotion to `ProofsReserved`
must succeed before returning a usable preparation; confirmation rejects an
unpromoted acquisition. An uncertain save, acquisition or promotion leaves the
saved record responsible for recovery, without reinsertion or unconditional release.

Cancellation of an acquisition fences it before releasing any owned inputs. A
paused acquisition or promotion cannot later regain ownership. Unacquired inputs
and coins subsequently used by another operation stay untouched. Once promoted,
cancellation also requires the original reserved rows to remain intact.

Reconstructed confirmation must retain that metadata. An application can bind a
request ID and immutable terms hash before preparation, then identify its retained
operation even if interrupted before saving its own plan. This permits exact
cancellation recovery; metadata alone does not reconstruct a confirmation preview.
Cancellation competes with confirmation for the same saved version before releasing
those inputs. Once confirmation wins, cancellation cannot release the coins, erase recovery data or
mark the eventual send receipt failed. This also applies to direct sends that
create a token without contacting the mint. Explicit cancellation, definitive
pre-confirmation failures and generic recovery share this boundary.

Applications that durably track a send should select
`SendOptions::retain_cancellation` before preparation. Call
`cancel_prepared_send(operation_id, amount, inputs)` with the original requested
amount and combined prepared input proofs. Success means zero spend: no token,
wallet debit, refund or fee was created. Save that outcome durably before calling
`acknowledge_prepared_cancellation` with the same identity. Until acknowledged,
retries return the retained result even if another operation has reused the
released coins. An absent operation does not establish cancellation; only the
acknowledgment accepts absence to support retry after deletion.

Ordinary sends default to immediate cleanup of completed cancellation evidence.
A retained result is never silently expired; callers must acknowledge it to bound
history. Interrupted cleanup resumes the exact release without modifying later
coin ownership. Preparations missing their exact input snapshot remain unresolved
instead of guessing from current proof states. This is a fresh-profile API change;
no existing persisted preparation is migrated.
