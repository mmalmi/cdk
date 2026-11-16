# Atomic send confirmation storage

Send confirmation uses two distinct native transactions:

1. `compare_and_swap_proofs(changes, Some(expected_saga))` compares the complete saved operation and every original proof. It marks swap inputs spent and imports all verified outputs together. An interruption cannot leave half of that batch applied. Retained proof provenance, derivation indices and witnesses remain intact; previously imported change may already belong to another operation and is preserved.
2. `complete_saga(expected, next, receipt)` compares that exact saved operation again, validates all immutable receipt fields, stores the receipt and advances the saga together. A matching terminal receipt stays terminal. A missing or changed parent rejects the write, including when acknowledged history has already been retired.

The completion callback preserves operation identity, amount, creation time and advances exactly one version. It validates the receipt role, mint, unit and real operation ID; the workflow must bind its result amount, fees and proof identities to its saved intent. Primary send completion also requires the original send amount and timestamp.

These are separate boundaries. The durable confirmation contains the verified outputs before the first transaction; recovery can repeat it after an interruption between the two transactions. A concurrent proof or parent mismatch returns false without mutation. Errors roll back the native transaction; a lost connection during commit may leave its entire outcome uncertain, requiring recovery. Neither boundary depends on an earlier read remaining current. Mint requests remain outside these local transactions; rejection cannot undo mint-side spending, so the exact saved intent must remain recoverable.

Rust custom backends default to unsupported. `supports_atomic_saga_completion()` must return true only after the complete contract is implemented. Foreign database callbacks must implement the same capability and completion methods. Sends check both capabilities before reserving coins or contacting the mint. No read/check/upsert fallback is permitted. The empty proof batch without a parent is a no-op capability check.

SQLite, PostgreSQL and redb implement both transactions. PostgreSQL uses serializable isolation and locks the expected parent row. No schema change is needed. This checkpoint changes send confirmation only; other workflows need their own consumer integration.
