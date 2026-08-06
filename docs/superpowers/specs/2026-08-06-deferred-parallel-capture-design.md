# Deferred Parallel Trie-Node Capture

**Date:** 2026-08-06
**Status:** Approved design, pending implementation plan
**Branch:** `bonsai-archive-proofs-trie-diff`

## Problem

A 60s wall-clock profile of live block import (local QBFT archive node, PID 23045)
shows `BonsaiArchiveTrieNodeStrategy.captureTrieNodeDiff` consuming **52% of the
import thread**:

| Component | % of import thread |
|---|---|
| `TrieNodeHistoryStore.getLatestBefore` → `RocksDBColumnarKeyValueStorage.getNearestBefore` | 36% |
| `TrieNodeHistoryStore.put` (`WriteBatchWithIndex` skiplist insert) | 9% |
| `ArchiveTrieNodeCodec.encodeDiff` (branch-child `Bytes.equals`) | 6.6% |

The dominant cost, `getLatestBefore`, exists only to fetch the **counter** of the
most-recent prior history entry so the FULL-vs-DIFF checkpoint decision can be
made. Each call builds a fresh `RocksIterator` (18% of the sub-cost), does one
`seekForPrev` (66%), and closes it (10%) — once per trie node written, thousands
of times per block, serially on the import thread.

Additional serial overhead per put: a `WORLD_BLOCK_NUMBER_KEY` read
(`currentBlockNumber`) that is constant within a block, and the prior-node flat-DB
read used as the diff base.

## Chosen approach

**Deferred parallel capture** ("L" from the brainstorm): keep capture semantics
byte-identical, but move all capture *reads and encoding* off the import thread
onto a small worker pool, overlapping them with the trie commit work that is
still running. Only the final `tx.put` of each history entry is applied serially,
at a single flush point before the block's transaction commits.

Alternatives considered and deferred (see brainstorm history): in-memory counter
cache (A), iterator reuse (B), probabilistic/content-hash checkpoint placement
(H — rejected against the current N-seek reader: with `MAX_BACKWARD_WALK_STEPS
= 16` a geometric tail gives a ~36% per-node reconstruction miss rate), async
background checkpointer (G — attractive end-state, larger change), RocksDB merge
operator (I), history-CF tuning (N — composes with this work, can follow
independently). L was chosen because it requires **no semantic, format, or
guarantee changes at all** — it is pure latency-hiding — and preserves the hard
`CHECKPOINT_INTERVAL` bound the reader depends on.

## Key enabling facts (verified in code)

1. **Capture reads target committed storage, not the transaction.**
   `BonsaiTrieNodeStrategy.getFlat*TrieNode` reads via `storage.get(...)`
   (committed DB); puts go to `transaction.put(...)`, invisible to reads until
   the transaction commits. Therefore the prior-node value, the history CF, and
   `WORLD_BLOCK_NUMBER_KEY` remain frozen at block N−1's state for the entire
   block. Capture computation may run at any point before tx commit and observes
   exactly what today's inline code observes.
2. **Reads are thread-safe and the view is stable.** RocksDB reads are
   thread-safe; the import thread is the only writer to the world-state and
   history CFs during import, and its writes sit in the uncommitted transaction.
   Worker threads never touch the transaction.
3. **A flush seam exists.** `BonsaiWorldStateKeyValueStorage.Updater.commit()`
   (`BonsaiWorldStateKeyValueStorage.java:471`) runs after all trie commits
   (storage tries, account trie, code) and before the underlying tx commit.
4. **Capture entry points are already centralized** in
   `BonsaiArchiveTrieNodeStrategy` (`putFlatAccountTrieNode`,
   `putFlatStorageTrieNode`, `removeFlatAccountStateTrieNode`), all invoked
   serially from `ParallelStoredMerklePatriciaTrie.CommitCache.flushTo` /
   `storeAndResetRoot` on the import thread.

## Design

### Data flow

```
import thread                          worker pool (trie-capture-%d)
─────────────                          ──────────────────────────────
putFlat*TrieNode:
  baseStrategy.put (live write, unchanged)
  if gate open:
    enqueue CaptureRequest ──chunk──▶  per chunk:
    (naturalKey, location,               priorNode = committed flat-DB read
     block, newNode|null)                counter   = historyStore.getLatestBefore
                                         entry     = captureTrieNodeDiff decision
...                                      → (historyKey, encodedValue)
Updater.commit():
  flushCaptures(tx):
    join all chunks         ◀──────────  results
    tx.put each (key, value)  [serial]
    historyProgress.save(tx)  [once]
    clear buffer
  tx.commit()                 [unchanged]
```

### Components

**`CaptureRequest`** (new, package-private record in
`ethereum/core/.../bonsai/storage/flat/` or `.../bonsai/archive/`):
`(Bytes naturalKey, Bytes location, long block, Bytes newNode /* null = removal */,
Hash accountHash /* null for account-trie nodes */)`. Carries everything a worker
needs; workers re-read the prior node themselves.

**`BonsaiArchiveTrieNodeStrategy`** (modified):
- `putFlat*TrieNode` / `removeFlatAccountStateTrieNode`: delegate the live
  write/remove immediately (unchanged), then — if the capture gate is open —
  append a `CaptureRequest` to the current block's buffer instead of computing
  the capture inline. No storage reads on the import thread.
- Block number is memoized per block alongside the existing `gatedBlockNumber`
  cache (one `WORLD_BLOCK_NUMBER_KEY` read per block instead of per put).
- Requests are chunked (~64 per task) and submitted to the executor **eagerly as
  they accumulate**, so capture reads overlap with the remaining trie-commit and
  hashing work.
- New methods, called from the `Updater` lifecycle:
  - `flushCaptures(SegmentedKeyValueStorageTransaction tx)` — submit any partial
    chunk, join all outstanding tasks, apply buffered `(key, value)` results to
    `tx` serially via `TrieNodeHistoryStore.put`-equivalent writes, persist the
    progress record once, clear the buffer. Worker exceptions propagate from
    here (fails block import, same as an inline capture failure today).
  - `discardCaptures()` — cancel/join outstanding tasks, drop results, clear the
    buffer. Called on rollback.

**Worker task** (pure function, no transaction access): for each request, read
the prior node from the committed flat DB (via `baseStrategy.getFlat*`), call
`historyStore.getLatestBefore` for the prior entry/counter, run the existing
FULL/DIFF/deletion decision (today's `captureTrieNodeDiff` logic, unchanged
including the `location.isEmpty()` root-always-FULL rule and the
`CHECKPOINT_INTERVAL` counter rollover), and emit `(historyKey, storedValue)`
pairs — where `storedValue` is the counter-prefixed codec entry exactly as
`TrieNodeHistoryStore.put` builds it today.

**Executor**: one dedicated fixed thread pool owned by the strategy,
`min(8, availableProcessors() / 2)` daemon threads named `trie-capture-%d`.
Not the trie's ForkJoinPool (saturated with hashing at exactly this time; these
tasks are read-latency-bound). Shut down when the owning storage closes.

**`BonsaiWorldStateKeyValueStorage.Updater`** (modified): `commit()` calls
`strategy.flushCaptures(getWorldStateTransaction())` before committing the
underlying transaction; `rollback()` (or the equivalent abort path) calls
`strategy.discardCaptures()`. `commitTrieLogOnly()` / `commitComposedOnly()`
paths are audited during implementation: any commit path that can carry trie-node
puts must flush; paths that cannot must not.

### Correctness argument

- **Same inputs.** Today the prior node and counter are read from committed
  storage between puts; the block's own writes are in the uncommitted tx and
  invisible. Deferring the reads to any point before tx commit changes nothing
  they observe.
- **Same outputs.** The decision logic is unchanged, so each request produces a
  byte-identical `(historyKey, value)` to the inline implementation.
- **Order independence.** History keys are distinct per `(naturalKey, block)`;
  entries within one block's batch do not read each other. Apply order within
  the flush is irrelevant.
- **Intra-block key collisions.** Within one trie, `CommitCache` is keyed by
  location, so each location stores at most once per commit; account-trie and
  storage-trie natural keys cannot collide (different key shapes). As a
  belt-and-braces guard the result buffer is keyed by `historyKey`, last write
  wins, matching what sequential `tx.put`s would produce.
- **Bound preserved.** FULL placement still follows the exact counter scheme;
  `TrieNodeHistoryReader`'s `MAX_BACKWARD_WALK_STEPS = CHECKPOINT_INTERVAL`
  guarantee is untouched.

### Lifecycle, threading, failure

- The capture buffer is per-open-block state in the strategy. Block-import
  persist is single-threaded (documented assumption in the existing gate cache);
  this is additionally asserted (throw if a new block's requests arrive while a
  previous block's buffer was neither flushed nor discarded).
- Worker failure → exception rethrown from `flushCaptures` → block import fails.
  No partial history entries are written because results are only applied to the
  tx after a successful join, and the tx itself rolls back on failure.
- `discardCaptures` on rollback prevents cross-block leakage.
- Executor rejection/saturation: unbounded queue on a fixed pool; requests are
  small records. Memory is bounded by nodes-per-block (thousands).

### Explicitly out of scope

- History-CF RocksDB tuning (prefix bloom, iterate bounds) — independent
  follow-up, composes with this work.
- Reader (`TrieNodeHistoryReader`) range-scan refactor — separate read-path work.
- Non-indexed `WriteBatch` for history puts — candidate follow-up once capture
  reads are off-thread and the serial `tx.put` cost (9%) is the next bottleneck.
- Any change to entry format, checkpoint placement, or the migrator.

## Testing

1. **Byte-equivalence (the test that matters):** drive the same sequence of
   puts/removes through the inline logic and the deferred implementation against
   identical fixture storage; assert the resulting history-CF contents are
   byte-identical (keys and values).
2. **Existing end-to-end test** (live capture from genesis,
   `d2a9250dde`) passes unchanged.
3. **Rollback:** buffered captures are discarded on rollback; nothing leaks into
   the next block's flush.
4. **Failure propagation:** a worker exception fails `flushCaptures` and the
   import; no history entries from the failed block are visible after rollback.
5. **Concurrency smoke:** a large synthetic block (thousands of nodes across
   many accounts) produces exactly the expected entry set — no losses, no
   duplicates.
6. **Perf validation:** re-profile the live QBFT node (same asprof wall-clock
   methodology) and compare `captureTrieNodeDiff`'s share of the import thread
   before/after.

## Expected outcome

Import thread sheds the capture read/encode work (~43% of wall: 36% lookup +
6.6% encode + prior-node reads) into overlap with existing commit work. The
remaining serial cost is the live flat-DB puts plus the history `tx.put`s
(~9–12%). Rough estimate: **block-import time −35–45%** on this workload,
with zero change to stored data or read-path guarantees.
