# Durable receives

A receive owns its exact input acquisition and signed swap request until its
incoming receipt is durably published. Recovery never rebuilds requests from a
moving keyset counter or infers delivery from absent or spent input rows.

## Phases

- `InputsRecorded`: full original and acquired proof records are saved. The
  creator atomically acquires the whole set before allocating output secrets.
  Recovery cancels this phase; it cannot have contacted the mint.
- `Cancelling`: an exclusive journal version rolls back exact acquired records.
  Foreign owners survive unchanged. A mint-status observation of `Spent` must
  stay spent, without inventing an incoming receipt.
- `Redeeming`: the complete signed `PreSwap`, derivation indices, fee, and stable
  receipt are saved before submission. Any HTTP error leaves this intent intact.
  Inactive-keyset errors are returned unchanged; they cannot start a new receive.
  Recovery restores the exact outputs, or replays the same request only when the
  mint returns an empty restore result. Partial or changed results fail closed.
- `OutputsReady`: verified outputs are durable. One fenced proof batch marks
  originals spent and imports outputs. Later owners, spent state, and additional
  local evidence of an already imported output survive retries.
- `Completed`: an atomic receipt-and-saga operation published the incoming Primary
  receipt and this terminal phase. Only journal cleanup remains. Cleanup never
  rewrites proofs or recreates retired history.

A stored checkpoint without the exact request cannot enter `Redeeming` or later
phases. There is no legacy request reconstruction or migration path.

## Completed retries

Receives retain unique per-attempt saga IDs, so stale cancellation cannot target
a later attempt with the same coins. Completed receipts derive a separate ID from
a domain-separated fingerprint of the mint, unit and sorted immutable input coins.
The native receive stores this fingerprint as reserved `cdk_receive_fingerprint`
metadata; caller-supplied values are rejected. Ordering, memo and transport
witnesses do not create a new payment. Completed receives reject duplicate tokens
by default. Explicitly setting `ReceiveOptions.replay_completed` returns the
original net amount only from a matching completed Primary incoming receipt,
including its exact input set and gross amount. That amount is the original
payment, not new credit. The retry does not contact the mint or rely on spent
proof ownership. Retiring the receipt releases this original-result retry;
neither a retry nor terminal saga cleanup recreates retired history. Existing
interrupted operations still use the saved request through normal saga recovery.

## Custody and self-receive

An existing input is admissible only when freely unspent, or when it belongs to
our outstanding `TokenCreated` send. The acquired record retains every original
field except its temporary Pending state and receive owner. Completion restores
the original record with only its state changed to Spent. This preserves source
send ownership, provenance, witness, and derivation metadata.

Reclaim and self-receive contend for the same original PendingSpent record.
Reclaim admission changes it to Pending under the send owner; receive admission
changes it to Pending under the receive owner. Exact whole-set comparison admits
one winner before either path sends an irreversible mint request. A public mint
status check may additionally mark the winner's input Spent; this never replaces
the requirement to recover that operation's exact output evidence.

## Storage support

Both atomic proof changes and atomic receipt/saga completion are checked before
input acquisition, secret allocation, or mint submission. Custom databases that
do not implement these contracts fail closed. Proof handoff and final receipt
publication are separate atomic steps; the OutputsReady journal makes the gap
recoverable without reviving or duplicating funds.
