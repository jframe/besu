# Design 5 — Geth-Lessons Index Improvements

**Date:** 2026-06-08
**Status:** Approved design, pending implementation plan
**Parent:** `phase3/2026-06-04-design5-trie-node-differential-index.md`
**Source analysis:** `phase3/2026-06-08-proof-cost-analysis.md`

---

## 1. Motivation

The proof cost analysis (PID 96687, 7880-block dev chain) shows Design 5's `LGetVisitor` at 1.00%
wall-clock vs seekForPrev's 0.57% — approximately 1.7–2× slower. The root cause is **index read
amplification**: every proof node consults three column families (bloom → range-marker → index list)
before reaching the history entry, totalling 4 reads per hot node and 2 reads per cold node.

Geth PBSS's `HistoricalNodeReader` avoids this by reading the current disk-layer node first and
comparing its stored hash against the expected hash from the parent branch node. A hash match means
the node is unchanged — no index consulted at all. A mismatch means the node was modified — go
directly to the index without any bloom or marker check.

Two changes implement this model for Design 5:

1. **Hash-stored fast path** — store `keccak256(nodeBytes) ‖ nodeBytes` in `TRIE_BRANCH_STORAGE`;
   compare the stored hash against `expectedHash` before any index read.
2. **Bloom and range-marker CF elimination** — with hash-first dispatch, `modifiedAfter` is
   superseded and the bloom CF has no remaining reader. Both bloom and range-marker CFs are removed
   entirely, along with all associated write machinery.

Chunk-based indexing (amortising index reads across the proof path, as in Geth's chunk-bitmap
scheme) is the natural next step but is a separate design.

---

## 2. Per-node I/O before and after

| Node type | Current reads | After this design | Delta |
|---|---|---|---|
| Cold node | 2 (bloom + live trie) | 1 (live trie, O(1) hash compare) | −1 |
| Hot node, same range | 4 (bloom + marker + list + history) | 3 (live trie + list + history) | −1 |
| Hot node, different range | 4 (bloom + marker + list + history) | 3 (live trie + list + history) | −1 |

A consistent −1 read across all node types. Additionally, the bloom write infrastructure
(`pendingBlooms`, `flushPendingBlooms`, `accumulateBloom`) is removed from the write path,
reducing per-block GC pressure.

No backward compatibility with existing databases is required.

---

## 3. Column-family changes

### 3.1 `TRIE_NODE_RANGE_MARKER_ARCHIVE` — removed

Removed from `KeyValueSegmentIdentifier`. All writes removed from
`TrieNodeChangeIndex.append()` and `appendListAndMarkerOnly()`. All reads removed.

The index list CF (`TRIE_NODE_INDEX_ARCHIVE`) key existence now serves as the implicit presence
check where needed.

### 3.2 `TRIE_NODE_BLOOM_ARCHIVE` — removed

With hash-first dispatch in `resolveNodeAt`, `modifiedAfter` is never called — hash match/mismatch
is definitive regardless of which range HEAD is in. The bloom CF has no remaining reader in the
proof path and is removed.

Removed from `KeyValueSegmentIdentifier`. The following are also deleted:

- `modifiedAfter()` in `TrieNodeChangeIndex`
- `accumulateBloom()` and `flushBloomAccumulator()` in `TrieNodeChangeIndex`
- `pendingBlooms` field, `flushPendingBlooms(SegmentedKeyValueStorageTransaction)` and
  `flushPendingBlooms(SegmentedKeyValueStorageTransaction, SegmentedKeyValueStorage)` in
  `BonsaiArchiveTrieNodeStrategy`
- Range-complete tracking (`markRangeComplete`) in `TrieNodeIndexProgress` — was only used to
  gate bloom writes and is no longer needed

`TrieNodeIndexProgress` retains `lastIndexedBlock` and `indexStartBlock`.

---

## 4. Hash-stored fast path

### 4.1 Storage format

`TRIE_BRANCH_STORAGE` values are always:

```
keccak256(nodeBytes)[32 bytes] ‖ nodeBytes
```

The first 32 bytes are the keccak256 hash of the remainder. No version prefix. No old-format
detection (no backward compat required).

### 4.2 Write path — `BonsaiTrieNodeStrategy`

`putFlatAccountTrieNode` and `putFlatStorageTrieNode` already receive `nodeHash` as a parameter
(type `Bytes32`). The stored value is `nodeHash ‖ node` — no keccak computation on the write
path, just a concatenation. Callers pass raw node bytes and are unaffected.

### 4.3 Read path — `BonsaiTrieNodeStrategy`

`getFlatAccountTrieNode` and `getFlatStorageTrieNode` return `value.slice(32)`. All existing
callers continue to receive raw node bytes.

### 4.4 Fast path in `ArchiveProofNodeLoader.resolveNodeAt`

Reads the raw value from `TRIE_BRANCH_STORAGE` directly (bypassing the strategy getter, which
strips the hash prefix). Extracts and compares the stored hash:

```
rawValue  ← TRIE_BRANCH_STORAGE.get(naturalKey)
storedHash ← rawValue[0..32]
nodeBytes  ← rawValue[32..]

if storedHash == expectedHash:
    return nodeBytes        // cold path — 1 read, 0 keccak, 0 index reads
else:
    // hot path — proceed to index lookup
```

Hash mismatch means the live trie node is not the T-version. The index is consulted directly
without bloom or marker check.

---

## 5. Combined `resolveNodeAt` algorithm

```
resolveNodeAt(naturalKey, expectedHash):

  // Step 1: live-trie read + hash check (cold fast path)
  rawValue   ← TRIE_BRANCH_STORAGE.get(naturalKey)               // 1 read
  storedHash ← rawValue[0..32]
  nodeBytes  ← rawValue[32..]
  if storedHash == expectedHash:
    return nodeBytes                                              // cold — done, 1 read

  // Step 2: index list read for targetBlock's range
  rangeId  ← targetBlock / rangeSize
  listOpt  ← TRIE_NODE_INDEX_ARCHIVE.get(naturalKey ‖ rangeId)   // 1 read

  // Step 3: find bStar (latest change ≤ targetBlock)
  if listOpt is empty:
    // No change recorded in this range — check earlier ranges
    bStar ← latestChangeBlock(naturalKey, targetBlock)
    if absent: return empty                                      // node absent at T

  else:
    bStarOffset ← listOpt.latestLeq(targetBlock % rangeSize)
    if absent:
      bStar ← latestChangeBlock(naturalKey, targetBlock)        // earlier-range fallback
      if absent: return empty
    else:
      bStar ← rangeId * rangeSize + bStarOffset

  // Step 4: history entry read + reconstruction
  entry        ← TRIE_NODE_HISTORY_ARCHIVE.get(naturalKey ‖ bStar)  // 1 read
  reconstructed ← reconstruct(entry)                            // FULL or diff chain

  // Step 5: fail-closed hash verification
  if keccak256(reconstructed) != expectedHash:
    throw IllegalStateException("hash mismatch — index/store inconsistency")
  return reconstructed                                          // hot — 3 reads
```

`modifiedAfter` is not called anywhere in this flow. The earlier-range fallback in step 3 uses
`latestChangeBlock` (index list scan over earlier ranges) which is unaffected by the bloom
removal.

---

## 6. Write path simplification

Per-block commit in `BonsaiArchiveTrieNodeStrategy` is simplified by removing all bloom
machinery. The per-mutation write sequence becomes:

1. Capture prior node from `TRIE_BRANCH_STORAGE` (if `trieNodeIndexEnabled`)
2. Write new node with hash prefix to `TRIE_BRANCH_STORAGE` via `baseStrategy`
3. Write diff/full entry to `TRIE_NODE_HISTORY_ARCHIVE`
4. Append offset to `TRIE_NODE_INDEX_ARCHIVE` list (no marker write)
5. Advance `TrieNodeIndexProgress` (`lastIndexedBlock`, `indexStartBlock`)

`flushPendingBlooms` calls are removed from the block-commit path in
`BonsaiArchiveWorldStateProvider` and any other callers.

---

## 7. Deferred optimization — reconstructed-node cache

Upper trie nodes (root, depth-1 branches) appear on every proof path and are reconstructed from
the same history entry for every proof targeting the same block. A future optimization: cache
`(naturalKey, bStar) → reconstructedBytes` scoped to a single proof session (one `eth_getProof`
call or a batched session). Estimated 50–80% hit rate for multi-slot proofs on the same block.
Not in scope for this design.

---

## 8. Non-goals

- **Chunk-based indexing** — grouping trie nodes into 3-level chunks with per-chunk bitmaps
  (as in Geth's `history_trienode_utils.go`). This amortises index reads across the proof path
  (O(depth/3) index reads instead of O(depth)) and is the next structural improvement, but
  requires rebuilding all existing index data and is a separate design.
- **List-first dispatch** — reading the index list before the live trie to eliminate the live-trie
  read for hot nodes (hot: 2 reads instead of 3, cold: 2 reads instead of 1). Beneficial on
  short chains where hot nodes dominate; hash-first is better at mainnet scale where cold nodes
  dominate.
- **CHECKPOINT_INTERVAL tuning** — reducing CI from 16 to 4 reduces diff application work but
  does not reduce I/O read count. Orthogonal and can be tuned independently.
