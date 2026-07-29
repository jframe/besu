# Skip redundant flat-DB reads during archive migration replay by trusting the trie log

## Motivation

Profiling the running `archive-migrator-0` thread on `dev-elc-bu-nb-mainnet-jason-bonsai-archive-proof-1`
(async-profiler, wall-clock event, 30s sample, ~600 samples) showed the migrator spending **79.3% of
wall time blocked inside `pread64`**, split:

- **~62%** — `PathBasedWorldStateUpdateAccumulator.rollAccountChange` → `loadAccountFromParent` →
  `BonsaiArchiveFlatDbStrategy.getFlatAccount` → `LayeredKeyValueStorage.getNearestBefore` →
  `RocksDBColumnarKeyValueStorage.nearestBefore` → RocksDB `seekForPrev` (cold, hits disk). No cache
  sits in front of this path during migration — `CacheManager.NO_OP_CACHE` is explicitly wired into
  `migrationKvStorage` in `BonsaiFlatDbToArchiveMigrator.initMigrationWorldState()`.
- **~38%** — `BonsaiWorldState.updateAccountStorageState` (trie walk, state-root computation) →
  `BonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode` → `RocksDB.get` (cold, hits disk). This is a
  structurally different mechanism (Merkle trie node fetch during state-root computation) and is
  **out of scope** for this change; see [Non-goals](#non-goals).

This spec covers only the ~62% account/storage-value hot path.

### Why the read exists

`rollAccountChange`/`rollStorageChange` replay a trie log's recorded account/storage changes onto the
migration accumulator. On first touch of an address/slot, if the accumulator has no tracked value yet,
it reads the "current" value from the underlying flat DB so it can (a) diff it against the trie log's
recorded `expectedValue` as a corruption/consistency check (`assertCloseEnoughForDiffing` for accounts,
`isSlotEquals` for storage slots), and (b) know what to mutate.

### Existing precedent

The proof-serving path (`BonsaiArchiveWorldStateProvider.rollArchiveProofWorldStateToBlockHash`) already
avoids this exact read via `PathBasedWorldStateUpdateAccumulator.archiveProofStorageRollFilter` /
`isArchiveProofRoll()`: when set, `rollAccountChange`/`rollStorageChange` seed the accumulator's
"current" value directly from the trie log's `expectedValue` instead of reading it back from storage.
Note this does **not** disable the validation — it makes it tautological, since the value now being
compared was seeded from the same `expectedValue` it's compared against. The code comment at
`PathBasedWorldStateUpdateAccumulator.java:751-752` states this explicitly ("The assertion below then
trivially holds.").

`archiveProofStorageRollFilter` cannot be reused directly for migration: it is a `Set<Address>` that also
restricts *which* accounts get their storage rolled at all (`shouldRollStorageFor`), whereas migration
must roll every touched account's storage. This is the same reasoning that led to `skipCodeRoll`
(commit `17116d27f8`) being added as an independent boolean rather than folded into
`isArchiveProofRoll()`.

## Accepted risk

Seeding from the trie log removes the only mechanism that has caught several subtle correctness bugs
already fixed in this codebase (CAS-dedup drops, append-only-history LRU-pinning fallthrough,
code-rolling on self-destructed contracts — see project memory). Once this flag is enabled for
migration, a flat-DB/trie-log divergence during replay will no longer throw and halt the migrator;
instead it will silently persist whatever the trie log says into the archive column families. Detection
of any such divergence would have to come from a downstream check (e.g. a future proof/state-root
verification), not from the migrator itself.

This trade-off has been explicitly discussed and accepted: this design implements **full trust, no
verification** (no sampled/async re-check), matching the existing proof-roll precedent. No sampling or
dry-run verification pattern exists anywhere in this codebase to build on; building one is out of scope
for this change.

## Design

### New flag: `trustTrieLogPriorValue`

Add to `PathBasedWorldStateUpdateAccumulator` (`ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/worldview/accumulator/PathBasedWorldStateUpdateAccumulator.java`):

- A new field `private boolean trustTrieLogPriorValue;` alongside the existing `skipCodeRoll` field.
- A new setter `public void setTrustTrieLogPriorValue(final boolean trustTrieLogPriorValue)`, following
  the same pattern as `setSkipCodeRoll`.
- A new helper method:
  ```java
  private boolean shouldSeedFromTrieLog() {
    return isArchiveProofRoll() || trustTrieLogPriorValue;
  }
  ```
  This avoids duplicating the OR condition at each of the two call sites below, and keeps
  `trustTrieLogPriorValue` independent from `archiveProofStorageRollFilter`/`isArchiveProofRoll()` per
  the reasoning above (migration needs full storage rolling for every account, unlike a proof roll).

### `rollAccountChange` changes (~line 741-798)

- Change the seeding guard from:
  ```java
  if (accountValue == null && isArchiveProofRoll() && expectedValue != null) {
  ```
  to:
  ```java
  if (accountValue == null && shouldSeedFromTrieLog() && expectedValue != null) {
  ```
- Change the flat-DB read guard from:
  ```java
  if (accountValue == null && !isArchiveProofRoll()) {
  ```
  to:
  ```java
  if (accountValue == null && !shouldSeedFromTrieLog()) {
  ```
- `assertCloseEnoughForDiffing` is left unconditional, unchanged — it becomes tautological when seeded,
  identical to the existing proof-roll behavior.

### `rollStorageChange` changes (~line 880-965)

- Change the seeding guard from:
  ```java
  if (slotValue == null && isArchiveProofRoll() && expectedValue != null && !expectedValue.isZero()) {
  ```
  to:
  ```java
  if (slotValue == null && shouldSeedFromTrieLog() && expectedValue != null && !expectedValue.isZero()) {
  ```
- Change the flat-DB read guard from:
  ```java
  if (slotValue == null && !isArchiveProofRoll()) {
  ```
  to:
  ```java
  if (slotValue == null && !shouldSeedFromTrieLog()) {
  ```
- The unconditional `isSlotEquals` check and the "expected to create slot, but slot exists" check are
  left unchanged — same tautological-once-seeded behavior as accounts.

### Wiring into the migrator

In `BonsaiFlatDbToArchiveMigrator.initMigrationWorldState()`
(`ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java`,
~line 502), immediately next to the existing:
```java
((PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater()).setSkipCodeRoll(true);
```
add:
```java
((PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater())
    .setTrustTrieLogPriorValue(true);
```

This is the **only** call site that ever sets `trustTrieLogPriorValue` to `true`. The flag defaults to
`false`, so normal block processing and the proof-serving path's accumulators are entirely unaffected.
This is a design invariant: `trustTrieLogPriorValue` must never be set `true` on any accumulator other
than the migrator's own `migrationWorldState` accumulator.

## Scope

Applies uniformly to both `rollAccountChange` and `rollStorageChange` — the "trust the trie log" argument
applies identically to account values and storage-slot values, and there is no separate profiling-driven
reason to fix one and not the other. Half-fixing (accounts only) would be an arbitrary carve-out with no
technical justification.

## Non-goals

- **Storage trie-node reads (hot path #2, ~38%)** — this is a separate mechanism (Merkle trie walk
  during state-root computation via `BonsaiCachedMerkleTrieLoader`/`StoredMerkleTrie.put`), unaffected
  by trusting the trie log's account/storage values. Not addressed by this change.
- **Sampled/async verification** — explicitly out of scope per the accepted-risk discussion above.
- **Runtime-tunable cache sizing or read-through caching improvements** — a separate, independent
  approach considered during brainstorming; not part of this change.

## Testing

1. New unit test (in the accumulator's test suite or a migrator-adjacent test) asserting that with
   `trustTrieLogPriorValue=true`, `rollAccountChange`/`rollStorageChange` never invoke
   `loadAccountFromParent`/`getStorageValueByStorageSlotKey` (i.e. never touch `wrappedWorldView()`) for
   a first-touch address/slot — verified via a mock/spy of the wrapped world view that fails the test if
   invoked.
2. Regression test mirroring the shape of the bug fixed in `17116d27f8`: migrate a block where the
   "prior" flat-DB row for a touched account/slot does not exist at all in the underlying storage,
   confirming migration completes successfully rather than throwing `IllegalStateException`.
3. The existing `BonsaiFlatDbToArchiveMigratorTest` suite must continue to pass unchanged — this change
   must not alter the actual migrated output, only the source of the "current" value read internally
   during replay.
4. Post-merge validation (manual, not part of the automated test suite): re-run the async-profiler
   capture described in [Motivation](#motivation) against a live migration to confirm the `seekForPrev`
   sample share in the migrator thread drops as expected.

## Files touched

- `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/worldview/accumulator/PathBasedWorldStateUpdateAccumulator.java`
- `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java`
- Corresponding test files for both classes above.
