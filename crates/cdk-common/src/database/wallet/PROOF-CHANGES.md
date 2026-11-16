# Conditional proof batches

`WalletDatabase::compare_and_swap_proofs` compares each complete original
`ProofInfo` and applies all replacements/deletions in one transaction. `None`
requires an absent original or requests deletion, depending on the field.

- `Ok(true)`: the whole batch committed. An empty batch still checks any supplied saga.
- `Ok(false)`: an original changed or was unexpectedly present/absent; nothing
  from this batch committed. Reload and reconcile the owning operation.
- `Err`: invalid input or storage failure. No partial batch commits. A lost
  response or a connection failure at commit may still leave the full outcome
  uncertain; inspect the records and recovery intent before retrying.

Comparison includes coin data, witnesses, state, derivation index and operation
provenance. An optional expected saga is compared in full, including its version,
in the same transaction. A missing or changed saga rejects the batch. PostgreSQL
locks the saga row until the proof write commits. Exact replacement can clear a
derivation index; ordinary `update_proofs` retains its existing index-preservation
behavior.
Duplicate identities and records whose secrets do not match their keys reject.
SQL decoding rejects malformed optional proof evidence instead of treating it
as absent, and rejects partial DLEQ data. SQL writes reject amounts outside the
database's signed integer range. PostgreSQL's existing INT4 columns, including
derivation indices and quote expiries, keep their existing range limits; values outside that range
reject instead of truncating. There is no automatic clamping or widening.
Existing malformed records require repair; the comparison API does not silently
normalize them.

SQLite uses its immediate write transaction; Redb uses one write transaction;
PostgreSQL uses a serializable transaction, including checks for absent records.
Serialization conflicts can return a storage error. Custom backends, including
the current Supabase implementation, inherit an explicit unsupported error.
Foreign callback implementations must implement the same atomic contract or
return an error without mutation; UniFFI requires an explicit callback method.
Callers adopting this capability must check support before beginning an operation
that depends on it. There is no fallback to unconditional proof updates.

This storage operation does not authorize replacing a coin or deleting financial
history. The owning wallet workflow must validate the mint, value, reservation
owner and allowed state transitions, save its recovery intent before mutation,
and reconcile ambiguous outcomes without resurrecting spent outputs. The optional
saga fence checks ownership of the saved intent; it does not change that intent.
Wallet consumers must explicitly adopt this API before relying on its guarantees.
