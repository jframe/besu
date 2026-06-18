# Bonsai→Forest Conversion: Cross-Block Node Cache + Resume

Date: 2026-06-18
Branch: worktree-bonsai-to-forest
Status: design (awaiting review)

## Problem

The `x-convert-to-forest` storage subcommand replays Bonsai trie logs block-by-block
to rebuild a Forest world state. Wall-clock profiling of a live mainnet conversion
(async-profiler 4.0, wall mode, PID 10336) showed the single conversion thread spends
**~95% of its time blocked in `pread64`** loading **account-state trie nodes** from
RocksDB:

```
ForestWorldStateKeyValueStorage.getAccountStateTrieNode
 → RocksDB.get → ... → PosixRandomAccessFile::Read → pread64   [BLOCKED ON DISK]
```

`iostat` corroborated: the data NVMe ran ~1000 random read IOPS at ~84% utilization,
r_await ~1.3 ms. Storage-trie reads and writes/commits were each <2% of wall time.

Root cause: `BonsaiTrieLogToForestConverter.applyTrieLog` builds a **fresh**
`StoredMerklePatriciaTrie` every block backed by a **raw, uncached** `NodeLoader`
(`forestStorage::getAccountStateTrieNode`). The upper account-trie nodes — touched on
every block — are re-fetched from RocksDB each block. As the Forest WORLD_STATE column
family grows (currently part of a 3.2 TB DB) past the ~8 GB OS page cache on a 15 GB
box, the cache-hit rate collapses and throughput degrades over the run.

## Goals

1. Eliminate the dominant cost: re-reading hot account-trie nodes from disk every block,
   by caching trie nodes across blocks in memory.
2. Make the conversion resumable: a stopped/restarted process must continue from the last
   committed block instead of restarting from block 1.
3. Be fully compatible with the existing on-disk database so a new build can be deployed
   and resume the already-running conversion with no migration.

## Non-goals

- No change to the on-disk Forest format or any persisted schema.
- No parallelism / prefetch in this change (possible follow-up).
- No WAL/commit-batching changes (writes are <2% of measured cost).

## Design

### 1. Persistent cross-block node cache

A single memory-bounded Caffeine cache, `Bytes32 (node hash) → Bytes (encoded node)`,
held as a field on `BonsaiTrieLogToForestConverter` so it lives across **all** blocks.
Forest keys both account and storage trie nodes by hash in one keyspace, so a single
cache is correct (no key collision; content-addressed).

- **Bounding:** `Caffeine.newBuilder().maximumWeight(maxBytes).weigher((hash, value) ->
  value.size() + 32)`, mirroring the `util/.../MemoryBoundCache` pattern. Default
  `maxBytes` ≈ 1 GiB (see flag below).
- **Read-through:** the account and storage `NodeLoader`s used by `applyTrieLog` /
  `rebuildStorageRoot` first consult the cache, then fall back to
  `forestStorage.getAccountStateTrieNode(hash)` / `getAccountStorageTrieNode(hash)`, and
  populate the cache on a miss.
- **Write-through (primary lever):** the `commit(...)` node callbacks that today call
  `updater.putAccountStateTrieNode(hash, value)` (and the storage equivalent) also do
  `cache.put(hash, value)`. The upper nodes rewritten on every block therefore stay
  resident and are served from RAM on the next block's traversal.
- The `MerkleTrie.EMPTY_TRIE_NODE_HASH` sentinel is handled before the cache (as today)
  and never cached.

Trie instances continue to be created per-block. `StoredMerkleTrie.commit()` resets its
root and releases its in-memory nodes, so a trie instance cannot be reused across blocks;
only the *node loads* are accelerated, via the cross-block cache.

#### Cache size flag

Add a hidden/experimental option to `ConvertToForestSubCommand`:

```
--Xx-convert-cache-size-mb=<MB>   (default: 1024)
```

Memory-bounded by bytes. A value of 0 disables the cache (falls back to the current raw
loader) for A/B comparison. Documented `-Xmx` guidance: the cache is on-heap, so with a
1 GiB cache set the JVM heap to at least ~4 GiB (`-Xmx4g`+) so the cache does not squeeze
the rest of the process. The cache competes with OS page cache for the box's 15 GB, but a
hash-keyed node cache has far better hit density for this workload than page cache.

#### Observability

Extend the existing throttled progress log (or add a second throttled line) to report
cache `hitRate()` and current weight, so the effect is visible live. Enable
`recordStats()` on the cache.

### 2. Resume via account-root presence (binary search)

In `ConvertToForestSubCommand.run()`, after `converter.seedGenesis(...)` (which is cheap
and idempotent), determine the resume point before entering the replay loop:

- Binary-search the largest block `K` in `[0, head]` for which
  `forestStorage.getAccountStateTrieNode(header(K).stateRoot)` is present. `K = 0`
  corresponds to genesis (already seeded).
- Set the converter's running root to `header(K).stateRoot` (new package-visible setter
  or a `resumeFrom(Hash root)` method on the converter) and start the replay loop at
  `K + 1`.
- Log `Resuming conversion from block {K} (root={...})`. If `K == head`, the conversion
  is already complete; flip metadata and exit.

**Correctness / safety:**
- The predicate "root node present" is monotonic in practice (each committed block 1..K
  has its root node written; uncommitted blocks do not). Boundary is confirmed by checking
  that `header(K+1).stateRoot` is absent; if present (coincidental root reuse), step
  forward until absent.
- The existing per-block state-root verification in `applyTrieLog` is a hard safety net:
  an incorrect resume point fails loudly on the first applied block rather than silently
  skipping state.
- Re-applying already-committed blocks is idempotent (content-addressed writes), so an
  over-conservative resume is harmless.

No persisted progress marker is introduced; resume relies solely on data already on disk,
which is what makes the current in-flight 22 h conversion recoverable by the new build.

## Affected code

- `ethereum/core/.../trie/forest/migration/BonsaiTrieLogToForestConverter.java`
  - Add cache field + constructor parameter (cache size / pre-built cache).
  - Route account + storage `NodeLoader`s through the cache (read-through).
  - Write-through in both `commit` callbacks.
  - Add `resumeFrom(Hash root)` (or setter) + a `cacheStats()` accessor.
- `app/.../cli/subcommands/storage/ConvertToForestSubCommand.java`
  - Add `--Xx-convert-cache-size-mb` option; pass to converter.
  - Binary-search resume point; start loop at `K+1`; handle already-complete case.
  - Add cache hit-rate to progress logging.

## Testing

- Unit (extend `BonsaiTrieLogToForestConverterTest`): converting with the cache produces
  the identical final root as without it; cache hits occur on repeated nodes; write-through
  populates the cache (loader not hit for a just-written node).
- Resume test: convert N blocks, construct a fresh converter over the same storage, verify
  the binary search resolves the resume block to N and replay continues to head with a
  matching root.
- Boundary test: resume when `K == head` (already complete) flips metadata and exits.
- Existing tests must pass unchanged (cache size 0 ⇒ current behaviour).

## Risks

- **Heap pressure** on the 15 GB box. Mitigated by configurable size + `-Xmx` guidance +
  cache-disable (0) escape hatch.
- **Resume false-positive** from coincidental state-root reuse. Mitigated by boundary
  check + per-block verification safety net.

## Rollout

1. Build the new version.
2. Stop PID 10336.
3. Start with the new build, e.g. `-Xmx4g` and `--Xx-convert-cache-size-mb=1024`.
4. Confirm the log shows `Resuming conversion from block {K}` near the prior progress and
   a rising cache hit-rate, with blocks/s improved over the pre-change baseline.
