# Design: RocksDB merge operator for the archive trie-node index

Date: 2026-07-30

## Problem

`TrieNodeChangeIndex` (`ethereum/core/.../bonsai/storage/archiveindex/TrieNodeChangeIndex.java`)
maintains, per trie node (`naturalKey`) per 1M-block range, a packed list of block offsets at
which that node changed. Every mutation — `append()` and `appendAndGetPreviousCount()` — does a
full read-modify-write of the stored value:

1. `Get()` the current value from `TRIE_NODE_INDEX_ARCHIVE` (or the write-through `indexCache`).
2. Deserialize it into `[subCount][RangeRelativeOffsetList]`.
3. Append one entry (`Arrays.copyOf` reallocation inside `RangeRelativeOffsetList.append`).
4. Re-serialize the whole value and `put()` it back.
5. Occasionally (list > 4096 entries) slice the oldest 2048 entries into a new
   `TRIE_NODE_SUBBLOCK_ARCHIVE` key and shrink the tail.

This RMW is the dominant CPU/read cost identified in prior migration-bottleneck profiling (index
RMW ≈ 38% of migration reads). It also participates in `OptimisticTransactionDB` write-write
conflict detection, forcing the migrator's batch-retry loop (`MAX_BATCH_RETRIES`) on contention
with live block processing.

Goal: replace the append with a RocksDB merge operator so the common case is a blind write (no
read, no reallocation), while preserving exact correctness of:
- the mutation count used by the depth-tiered FULL/DIFF checkpoint decision
  (`BonsaiArchiveTrieNodeStrategy.captureTrieNodeDiff`, line 353), and
- the sub-block overflow split.

## Validated building block

RocksDB's `StringAppendOperator(String delim)` (available in rocksdbjni 10.6.2, the version this
repo uses) accepts a true empty-string delimiter and performs zero-separator concatenation. This
was verified directly against `rocksdbjni-10.6.2.jar`:

```
db.merge(key, [01 02 03]); db.merge(key, [04 05 06]); db.merge(key, [07 08 09]);
db.get(key) -> [01 02 03 04 05 06 07 08 09]   // before AND after flush + compactRange()
```

Merging on top of an existing `put()` value also concatenates correctly (`put([AA BB CC])` +
two merges → `[AA BB CC 01 02 03 04 05 06]`). This means the existing fixed-3-byte-per-entry
`RangeRelativeOffsetList` on-disk format can be produced by merge operands directly — **no
delimiter-stride parsing is needed on read**; the packed byte layout is unchanged.

## Design

### Split content from metadata

The single `[subCount][packed offsets]` value is split into two values, in two column families:

- **Content** — `TRIE_NODE_INDEX_ARCHIVE[naturalKey ‖ rangeId]`: just the packed 3-byte-per-entry
  offset list, nothing else. Written via `merge()` with `StringAppendOperator("")`. Every
  ordinary append is `tx.merge(TRIE_NODE_INDEX_ARCHIVE, key, threeByteOffset)` — no read.
- **Metadata** (new CF, `TRIE_NODE_INDEX_META_ARCHIVE[naturalKey ‖ rangeId]`): a small fixed-width
  value — `subCount (4 bytes) ‖ tailCount (4 bytes)`. Written via plain `put()`, read via plain
  `Get()`. This is what `appendAndGetPreviousCount` reads to compute
  `earlierCount + subCount * DEFAULT_SUBBLOCK_SPLIT_AT + tailCount` before deciding FULL vs DIFF.

Metadata **must not** be reconstructed via merge composition. `appendAndGetPreviousCount` reads
it on every single call; if it were merge-appended too, each `Get()` would force RocksDB to
replay every accumulated operand since the last compaction to rebuild the value — an O(n) read on
a key that's read n times, i.e. O(n²) overall. A tiny plain RMW (8 bytes, no array copy) avoids
this and is exactly the "cheap read" the sub-count/count bookkeeping needs.

### Common-case write (no split)

```
metadata = readMetadata(naturalKey, rangeId)        // small plain Get, no list deserialize
previousCount = earlierCount + metadata.subCount * SPLIT_AT + metadata.tailCount
tx.put(META, key, encode(metadata.subCount, metadata.tailCount + 1))
tx.merge(CONTENT, key, threeByteOffset(block))       // blind append, no read of content
return previousCount
```

`append()` (creation path) still does the same small metadata read — it needs `tailCount` to
decide whether this append crosses the sub-block split threshold, exactly like
`appendAndGetPreviousCount`. The only difference between the two methods is that `append()`
doesn't compute/return `earlierCount + subCount * SPLIT_AT + tailCount` for the caller. Neither
method skips the metadata read; what both eliminate is the expensive part — reading and
reallocating the (potentially thousands-of-bytes) packed content list on every call. Only the
rare split path (below) still touches content.

### Split case (tailCount would exceed `DEFAULT_SUBBLOCK_THRESHOLD`)

Triggered by the metadata read alone (no content read needed to detect it):

```
if (metadata.tailCount + 1 > SUBBLOCK_THRESHOLD) {
  content = Get(CONTENT, key)     // RocksDB resolves this by replaying accumulated merge operands
  (head, tail) = slice(content ++ newOffset, SPLIT_AT)
  tx.put(SUBBLOCK, subBlockKey(naturalKey, rangeId, metadata.subCount), head)
  tx.put(CONTENT, key, tail)                          // fresh base value; resets merge chain
  tx.put(META, key, encode(metadata.subCount + 1, tail.size()))
} else {
  ... common case above ...
}
```

This is the same 1-in-4096-appends cost the current code already pays; it's unaffected by this
change except that the trigger check no longer requires reading the content list.

### Buffered / migration path

`TrieNodeChangeIndex`'s buffered mode (`beginBuffered()` / `flushBuffer()`) already tracks
`baseSubCount`, `baseTail`, and `pending` offsets per key in memory, seeded by a prefetch of the
committed base value. Two changes:

1. Prefetch only needs to seed **metadata** (`baseSubCount`, `baseTail.size()`) per key, not the
   full content list — a much smaller prefetch payload.
2. At `flushBuffer(tx)`, instead of `Get → append every pending offset → put` per dirty key, issue
   **one `tx.merge(CONTENT, key, concatenatedPendingBytes)` per dirty key** (all pending 3-byte
   offsets for that key concatenated client-side into one operand — zero-delimiter concatenation
   makes this equivalent to N separate merges) plus one metadata `put()` reflecting the new
   `tailCount`. Splits inside a flush still fall back to a real content read, same as the live
   path.

### API surface changes

- `SegmentIdentifier` (`plugin-api`): add `default boolean usesAppendMergeOperator() { return
  false; }`, following the existing `isCacheIndexAndFilterBlocks()` pattern. `RocksDBColumnarKeyValueStorage.createColumnDescriptor`
  sets `.setMergeOperator(new StringAppendOperator(""))` on the column family options when true.
- `SegmentedKeyValueStorageTransaction` (`plugin-api`, `@Unstable`): add
  `void merge(SegmentIdentifier segmentIdentifier, byte[] key, byte[] value)`. This is a
  plugin-api change — update `knownHash` in `build.gradle`'s `checkAPIChanges` task
  (`./gradlew :plugin-api:check` to get the new hash), per this repo's existing pattern for
  public-method additions to storage interfaces.
- All five current implementors need a `merge()` method:
  - `RocksDBTransaction` → `innerTx.merge(cf, key, value)` (RocksJava `Transaction` exposes this).
  - `RocksDBWriteBatchTransaction` → `writeBatch.merge(cf, key, value)`.
  - `SegmentedInMemoryKeyValueStorage`'s transaction → no real merge operator available in-memory;
    simulate `StringAppendOperator("")` semantics directly (`get(key).orElse(empty) + value`) so
    unit tests observe the same end state as RocksDB.
  - `SegmentedKeyValueStorageTransactionValidatorDecorator` → pass-through, same pattern as `put`.
  - `BonsaiFlatDbToArchiveMigrator.MigrationTransaction` → **must** add
    `TRIE_NODE_INDEX_ARCHIVE` / new `TRIE_NODE_INDEX_META_ARCHIVE` to the existing segment
    allowlist in its `merge()` override, mirroring the allowlist already present in `put()`/`remove()`
    (lines ~1189–1221). This class defaults to dropping writes for segments not explicitly listed —
    exactly the pattern that silently dropped `TRIE_NODE_CAS_ARCHIVE` puts in a past incident
    (4.38M dangling HASH_REFs). Forgetting to extend this allowlist for the new metadata CF (or for
    merge on the content CF) would silently and undetectably corrupt the migrated index.
  - `BonsaiFlatDbToArchiveMigrator.FlatCapturingTx` → delegate, same as its `put()`.

### New column family

`TRIE_NODE_INDEX_META_ARCHIVE` registered in `KeyValueSegmentIdentifier.java` alongside
`TRIE_NODE_INDEX_ARCHIVE` and `TRIE_NODE_SUBBLOCK_ARCHIVE`.

### On-disk format change and migration

This changes the on-disk layout (content value loses its `subCount` prefix; a new CF holds
metadata). Per user direction, an on-disk format change is acceptable with a migration/rebuild
path. Existing `X_BONSAI_ARCHIVE` databases need one of:
- a one-time forward migration that reads every existing `[subCount][list]` value, writes the
  list bytes as the new content value (`put`, not `merge` — establishes a clean base) and the
  extracted `subCount`/`tailCount` as metadata, or
- treating this as gated behind the existing experimental/unstable `X_BONSAI_ARCHIVE` format such
  that it only ships with a documented reindex requirement.

The exact mechanism (migration tooling vs. reindex-on-upgrade) is left to the implementation plan;
this spec's scope is the write-path redesign, not the upgrade tooling. Whichever is chosen, it
must run before any node reads or writes the archive index with the new code, since the two
formats aren't wire-compatible.

## Correctness properties preserved

- `appendAndGetPreviousCount` returns the exact pre-write mutation count, computed from a cheap,
  always-fresh metadata read — the depth-tiered FULL/DIFF checkpoint decision
  (`checkpointIntervalForDepth`) is unaffected.
- Sub-block boundaries (`DEFAULT_SUBBLOCK_THRESHOLD` = 4096, `DEFAULT_SUBBLOCK_SPLIT_AT` = 2048)
  and `TRIE_NODE_SUBBLOCK_ARCHIVE` layout are unchanged; only the trigger check moves from
  "read content, check `list.size()`" to "read metadata, check `tailCount`".
- `RangeRelativeOffsetList`'s in-memory representation and binary-search lookup
  (`latestLeq`) are unchanged — the content CF's bytes are byte-identical to what today's packed
  format produces (minus the 4-byte `subCount` prefix, which moves to metadata).
- Read-side consumers (`TrieNodeHistoryReader`, proof assembly) need one change: read the content
  value plus the metadata value instead of one combined value, and drop the `subCount`-prefix
  parsing from `readIndexValue`.

## Out of scope

- Redesigning the depth-tiered checkpoint algorithm to avoid needing a synchronous previous-count
  read at all (would eliminate the metadata RMW too, but changes recently-shipped checkpoint
  semantics — bigger, riskier, and not needed to get the bulk of the win).
- Migration/reindex tooling implementation details (covered above at a design level only).
- Changing `OptimisticTransactionDB` vs `TransactionDB` choice, or other unrelated storage-layer
  behavior.

## Testing

- `RangeRelativeOffsetListTest`, `ArchiveNodeKeyTest`: unchanged (content format unchanged).
- `TrieNodeChangeIndexTest`: rewrite RMW-specific assertions (subCount+list combined read) to
  assert against the split content/metadata reads; sub-block split tests should still pass
  against the new trigger logic.
- New test: verify `SegmentedInMemoryKeyValueStorage`'s simulated merge matches real RocksDB
  `StringAppendOperator("")` behavior for the same operand sequence (a small parity test, ideally
  parameterized over both storage backends).
- `BonsaiFlatDbToArchiveMigratorTest`: add a case asserting the `MigrationTransaction` allowlist
  forwards `merge()` calls for `TRIE_NODE_INDEX_ARCHIVE`/`TRIE_NODE_INDEX_META_ARCHIVE` (regression
  test for the allowlist-drop failure mode described above).
- Existing `BonsaiArchiveTrieNodeIndexIntegrationTest` / `BonsaiArchiveProofsIntegrationTest`
  should pass unmodified if the read path change is transparent — these are the main correctness
  backstop for the redesign.
