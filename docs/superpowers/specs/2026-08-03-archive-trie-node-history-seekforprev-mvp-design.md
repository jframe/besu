# Design: Archive trie-node history via `getNearestBefore` (seekForPrev) — no index, MVP

Date: 2026-08-03

## Context

Bonsai archive nodes need to reconstruct historical trie-node versions to serve `eth_getProof`
and other historical-state reads once a node's live flat DB has moved past the requested block.
The current `design5-geth-lessons` branch built this via a purpose-built change-block index
(`TrieNodeChangeIndex` + `RangeRelativeOffsetList`, ~1500 lines) that tracks, per trie node per
1M-block range, a packed list of the blocks at which that node changed — plus a RocksDB
merge-operator to make the index's own writes cheap, plus migration prefetching, plus
depth-tiered checkpoint intervals. That is a lot of machinery (~19,000 lines across 87 files) to
land as a single first PR.

This spec defines a much smaller MVP that ships the same externally-visible capability —
reconstruct a trie node's RLP as of a given historical block — using only building blocks that
already exist and are already battle-tested on `upstream/main`, with the index and its dependents
deferred entirely to follow-up work.

**Key enabler**: `SegmentedKeyValueStorage.getNearestBefore(segment, key)` already exists on
`upstream/main` (`plugin-api/.../storage/SegmentedKeyValueStorage.java:52`), implemented via
`rocksIterator.seekForPrev(key)` in both `RocksDBColumnarKeyValueStorage` and
`RocksDBColumnarKeyValueSnapshot`, and is already used in production by
`BonsaiArchiveFlatDbStrategy` for the existing per-block-suffix account/storage archive scheme.
`TrieNodeHistoryStore`'s key layout (`naturalKey ‖ block(8 bytes BE)`, sorted lexicographically)
means `getNearestBefore(TRIE_NODE_HISTORY_ARCHIVE, naturalKey ‖ targetBlock)` finds "the latest
version of this node at or before targetBlock" directly, in one already-proven call — no index
required.

## Goals

- Reconstruct any archived trie node's RLP as of a historical block, correct and complete.
- Bound history storage growth via FULL/DIFF diffing (not FULL-only), using a single fixed
  checkpoint interval (no depth tiering).
- Reuse existing, already-tested code wherever it doesn't depend on the index:
  `TrieNodeDiffCodec` unchanged, `TrieNodeHistoryReader`'s existing bounded backward-walk logic
  promoted from fallback to primary path, `TrieNodeIndexProgress`'s coverage-gating logic
  unchanged.
- Ship a coherent, reviewable feature: schema + write path + read path + migration + CLI flag.

## Non-goals (explicitly deferred to follow-up PRs/specs)

1. **Change-block index** (`TrieNodeChangeIndex`, `RangeRelativeOffsetList`, their 3 CFs) — a pure
   perf layer that replaces the backward walk with O(1)-ish lookups. Not needed for correctness.
2. **RocksDB merge operator** — exists solely to make the index's own writes cheap; meaningless
   without the index, so it has no place in this MVP at all (not deferred, eliminated).
3. **Migration prefetching** (`MigrationPrefetcher`, `TrieNodePathEnumerator`) — cache warm-up;
   migration is simply slower without it, not incorrect.
4. **Depth-tiered checkpoint intervals** — refines the single fixed interval into root/shallow/deep
   tiers for better storage efficiency. Correctness-neutral.

Also out of scope for this spec (unrelated, independent, land as their own separate PRs):
`ConfirmedInSyncTrigger`, `SenderBalanceChecker`, `DebugTraceTransaction`, the
`TransactionSimulator`/`Blockchain.getBlockHeaderSafe` fallback family. And dropped entirely, not
carried forward in any form: the stats subcommand
(`TrieNodeHistoryStatsSubCommand`/`StorageSubCommand` registration), `TrieNodeHistoryComposition`
(diagnostics-only, no consumer without the stats subcommand), and the design docs previously
written for the index-based approach.

## Storage schema

One new column family (unchanged from the current branch): `TRIE_NODE_HISTORY_ARCHIVE`.

No `TRIE_NODE_INDEX_ARCHIVE`, `TRIE_NODE_INDEX_META_ARCHIVE`, or `TRIE_NODE_SUBBLOCK_ARCHIVE` —
those belong to the deferred index. `TRIE_BRANCH_FRONTIER` (the migrator's persistent frontier CF)
is still needed and is orthogonal to this spec's scope.

### Key layout

`ArchiveNodeKey` is trimmed to just what history storage needs:

```java
public static Bytes account(Bytes location)                      // = location
public static Bytes storage(Bytes accountHash, Bytes location)   // = accountHash ‖ location
public static Bytes historyKey(Bytes naturalKey, long block)      // = naturalKey ‖ block(8B BE)
public static long blockFromHistoryKey(Bytes historyKey)
public static Bytes naturalKeyFromHistoryKey(Bytes historyKey)
```

`RANGE_SIZE`, `rangeId()`, `rangeKey()`, `bloomKey()`, `subBlockKey()` are removed — they exist
only to support the deferred index and are dead code without it. The follow-up index spec
reintroduces them.

### Entry wire format

`TrieNodeDiffCodec` is reused **completely unchanged** — its FULL/DIFF/tombstone encoding has no
dependency on the index (confirmed: it only depends on Besu's RLP library) and is already covered
by a 752-line test suite.

One thing does need to travel with each entry that the index used to provide externally: how many
mutations have accumulated since the last FULL, so the write path can decide FULL vs. DIFF without
a backward scan on every write. `TrieNodeHistoryStore` therefore wraps each codec entry with a
1-byte counter:

```
TRIE_NODE_HISTORY_ARCHIVE[naturalKey ‖ block] = [distanceSinceFull: 1 byte] ‖ [TrieNodeDiffCodec entry]
```

This keeps `TrieNodeDiffCodec` itself untouched — the counter is a `TrieNodeHistoryStore`-level
concern, stripped/prepended on `get`/`put` and invisible to the codec and to callers that only
need the decoded node.

`TrieNodeHistoryStore` exposes this as a small decoded record, not raw `Bytes`, so both the write
path (checkpoint decision) and read path (reconstruction) work against the same typed shape:

```java
record HistoryEntry(int counter, TrieNodeDiffCodec.Decoded codecEntry) {}

Optional<HistoryEntry> getLatestBefore(Bytes naturalKey, long block); // getNearestBefore + unwrap
```

The pseudocode in the following two sections uses `HistoryEntry` accessors (`.counter()`,
`.codecEntry()`) on the `Optional` it returns.

### Checkpoint interval

A single fixed constant, e.g. `CHECKPOINT_INTERVAL = 16` (matching the current branch's
`DEEP_CHECKPOINT_INTERVAL`, already exercised in its test suite) — applied uniformly to every
node regardless of trie depth. No root/shallow/deep tiering (deferred, see Non-goals #4).

## Write path

`BonsaiArchiveTrieNodeStrategy` (or its stage-1 equivalent) already reads `priorNode` — the live
value about to be overwritten — from committed flat-DB storage before every write, purely to
compute the diff; that read is unrelated to and unaffected by this design. What changes is how it
decides FULL vs. DIFF:

```java
Optional<HistoryEntry> priorOpt = historyStore.getLatestBefore(naturalKey, block);

final Bytes entry;
final int newCounter;
if (priorNode == null) {
  // Creation: no prior node → always FULL | CREATION, counter resets.
  entry = TrieNodeDiffCodec.encodeDiff(null, newNode);
  newCounter = 0;
} else if (priorOpt.isEmpty() || priorOpt.get().codecEntry().isDeletion()) {
  // Re-creation after deletion (or first-ever history write for this key): FULL, counter resets.
  entry = TrieNodeDiffCodec.encodeFull(newNode);
  newCounter = 0;
} else {
  final int priorCounter = priorOpt.get().counter();
  if (priorCounter + 1 >= CHECKPOINT_INTERVAL) {
    entry = TrieNodeDiffCodec.encodeFull(newNode);
    newCounter = 0;
  } else {
    entry = TrieNodeDiffCodec.encodeDiff(priorNode, newNode);
    newCounter = priorCounter + 1;
  }
}
historyStore.put(tx, naturalKey, block, newCounter, entry);
```

Exactly **one extra read per write** (the `getNearestBefore` call for the prior entry's counter),
on top of the `priorNode` read the write path already performs for diffing. No buffered-batch
mode, no write-through cache, no Bloom filter — those were index-specific perf machinery; a plain
`getNearestBefore` per write is the MVP baseline, with a follow-up perf pass free to add caching
later if profiling shows it's warranted.

Deletion is unchanged: `entry = TrieNodeDiffCodec.encodeDiff(priorNode, null)` (tombstone); the
counter byte is irrelevant for tombstones since a walk always stops at one.

## Read path — `TrieNodeHistoryReader.nodeAt(naturalKey, targetBlock)`

```java
Optional<HistoryEntry> bStarOpt = historyStore.getLatestBefore(naturalKey, targetBlock);
if (bStarOpt.isEmpty()) return Optional.empty();          // never written before targetBlock

TrieNodeDiffCodec.Decoded bStarDecoded = bStarOpt.get().codecEntry();
if (bStarDecoded.isDeletion()) return Optional.empty();
if (bStarDecoded.isFull()) return Optional.of(bStarDecoded.fullNode());

// DIFF: walk backward via getNearestBefore, one step at a time, collecting diffs until a FULL.
List<Bytes> diffsDescending = new ArrayList<>();
diffsDescending.add(/* raw entry bytes for bStar */);
long walkBlock = /* block number of b* */;
Bytes fullEntry = null;
while (walkBlock > 0) {
  Optional<HistoryEntry> prevOpt = historyStore.getLatestBefore(naturalKey, walkBlock - 1);
  if (prevOpt.isEmpty()) break;
  TrieNodeDiffCodec.Decoded prevDecoded = prevOpt.get().codecEntry();
  if (prevDecoded.isDeletion()) return Optional.empty();  // tombstone in the chain: shouldn't happen
  if (prevDecoded.isFull()) { fullEntry = /* raw entry bytes */; break; }
  diffsDescending.add(/* raw entry bytes */);
  walkBlock = /* block number of prevOpt's entry */;
}
// fullEntry must be non-null: the checkpoint counter guarantees a FULL within CHECKPOINT_INTERVAL
// steps of any DIFF. Reverse diffsDescending to ascending order and reconstruct.
return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, ascending(diffsDescending)));
```

This is exactly the current branch's existing `backwardWalkFallback` method, promoted from "rare
fallback used when the index's window scan misses" to "the only reconstruction path," with its
`index.latestChangeBlock(naturalKey, walkBlock - 1)` calls replaced by
`historyStore.getLatestBefore(naturalKey, walkBlock - 1)` (`getNearestBefore` directly). The
windowed-multiGet index fast path and `MAX_BACKWARD_WALK_STEPS`/`RECONSTRUCT_WINDOW` machinery are
deleted along with `TrieNodeChangeIndex`.

### Termination guarantee

Unlike the current branch's now-removed formula-based positioning (which the `cf36b6a252`
perf fix had to patch because computed checkpoint positions didn't reliably land on a FULL), this
scheme's termination is airtight by construction: the write path only ever emits a DIFF when the
prior entry's counter is `< CHECKPOINT_INTERVAL - 1`, and increments it by exactly 1 each time. So
a DIFF chain starting at any `b*` reaches a FULL (or the start of history) in at most
`CHECKPOINT_INTERVAL` backward steps — no scanning, estimation, or defensive over-provisioning
required. A small bound (e.g. `MAX_BACKWARD_WALK_STEPS = CHECKPOINT_INTERVAL`) is retained purely
as a corrupt-data/defensive guard, not as a load-bearing correctness mechanism.

## `TrieNodeIndexProgress`

Reused essentially as-is — its `[indexStartBlock, lastIndexedBlock]` coverage window and
monotonic `setLastIndexedBlock`/`setIndexStartBlock` gating logic has no dependency on the index
(confirmed: it only persists two longs to `TRIE_BRANCH_STORAGE`). The one change: the `rangeSize`
constructor parameter and accessor are dropped — nothing in this MVP needs it. The follow-up index
spec reintroduces `rangeSize` if/when `TrieNodeChangeIndex` returns.

## `ArchiveProofNodeLoader`

Retains its hash-first fast path unchanged (read live `TRIE_BRANCH_STORAGE[naturalKey]`; if its
hash matches `expectedHash`, return directly — the common case for unchanged nodes, one read). On
miss, it now delegates straight to `TrieNodeHistoryReader.nodeAt(naturalKey, targetBlock)` — no
range-list preloading, no `readRangeList`, no in-memory binary search; those existed purely to
avoid a second index read; without the index there's nothing to preload. Still verifies
`keccak256(result) == expectedHash` and throws on mismatch (fail-closed), unchanged.

## Write-path / read-path / migration wiring

Unchanged in shape from the current branch's architecture (per the dependency analysis already
done for this feature):

- `BonsaiWorldStateKeyValueStorage` gets a pluggable `trieNodeStrategy` field and routes
  `getAccountStateTrieNode`/`putFlatAccountTrieNode`/etc. through it; `flushIndexIfEnabled()` calls
  `advanceIndexProgress` (renamed conceptually to `advanceHistoryProgress` if desired) before
  commit.
- `BonsaiArchiveWorldStateProvider` constructs `TrieNodeHistoryStore` + `TrieNodeHistoryReader` +
  `TrieNodeIndexProgress`, gated by `PathBasedExtraStorageConfiguration.getStateProofsEnabled()`,
  and routes `getAccountProof` through `ArchiveProofNodeLoader` when the target block is covered.
- `BonsaiArchiveMigrationTrieNodeStrategy` / `BonsaiFlatDbToArchiveMigrator` populate
  `TRIE_NODE_HISTORY_ARCHIVE` during backfill using the same write-path checkpoint logic as live
  import — no buffered-batch index mode (that existed to batch `multiGet`s against the index CFs);
  migration issues one `getNearestBefore` per node write, same as live import. This is the
  intentional MVP tradeoff — migration prefetching (Non-goal #3) exists specifically to speed this
  up later.
- `PathBasedExtraStorageOptions` adds the `--Xbonsai-archive-state-proofs-enabled` CLI flag;
  `BesuControllerBuilder` wires `BonsaiArchiveWorldStateProvider`'s history store/reader/progress
  into the migrator when the flag is set.
- `plugin-api MutableWorldState.isTrieDisabled()` default method and `PathBasedWorldState`'s
  override, needed for the trie-disabled archive world state used in proof serving, are unchanged
  from the current branch.

## Testing strategy

- `TrieNodeDiffCodec`: reuse the existing test suite unchanged (no code changes to this class).
- `TrieNodeHistoryStore`: unit tests for the counter-wrapping `put`/`get`, including the
  creation/re-creation-after-deletion counter-reset cases.
- `TrieNodeHistoryReader`: unit tests covering FULL-direct return, single-DIFF reconstruction,
  full-chain reconstruction up to `CHECKPOINT_INTERVAL - 1` diffs, and the tombstone/empty-history
  cases. Port the relevant existing `TrieNodeHistoryReaderTest` cases, adjusted to call
  `getNearestBefore` instead of the index.
- `ArchiveNodeKeyTest`: trim to cover only the retained methods.
- Integration: reuse `BonsaiArchiveProofsIntegrationTest` and
  `BonsaiArchiveWorldStateProviderTest` structure, adjusted for the simplified read path.
- Migration: reuse `BonsaiFlatDbToArchiveMigratorTest`'s structure for the non-prefetch,
  non-buffered-index code paths.

## Open questions

1. Exact name/location for the counter-wrapping logic — inside `TrieNodeHistoryStore` (proposed
   above) vs. a thin wrapper type. Proposed: inside `TrieNodeHistoryStore`, since it already owns
   the wire format for this CF.
2. Whether `CHECKPOINT_INTERVAL = 16` is the right single default before depth-tiering exists, or
   whether a different single value better balances storage growth vs. read amplification for
   this MVP. Should be validated against the same live-diagnostics approach used previously
   (per `cf36b6a252`'s commit message) once this is running against real history.
3. Confirm the real call site (if any) for `Blockchain.getBlockHeaderSafe(long)` before deciding
   whether that family of fixes needs to land alongside this feature or can genuinely stay a fully
   separate PR.

## Follow-up work (separate specs, not covered here)

1. Change-block index (`TrieNodeChangeIndex`/`RangeRelativeOffsetList`) to replace the backward
   walk with O(1) lookups.
2. RocksDB merge operator, to make the index's own writes cheap (depends on #1).
3. Migration prefetching (`MigrationPrefetcher`/`TrieNodePathEnumerator`).
4. Depth-tiered checkpoint intervals (root/shallow/deep), replacing the single fixed interval.
