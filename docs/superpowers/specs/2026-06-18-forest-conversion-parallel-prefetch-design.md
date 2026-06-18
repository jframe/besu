# Bonsai→Forest Conversion: Parallel Read-Ahead Prefetch

Date: 2026-06-18
Branch: worktree-bonsai-to-forest
Status: design (approved approach, awaiting plan)

## Problem

The `x-convert-to-forest` subcommand replays Bonsai trie logs block-by-block to
rebuild a Forest world state. After adding a cross-block node cache, wall-clock
profiling still showed **~91% of conversion-thread time blocked in `pread64`**
loading account/storage trie nodes from RocksDB. Increasing the cache from 1 GB
to 4 GB did not materially change throughput (~0.9 blocks/s).

`iostat -x` on the live node revealed the true cause:

```
Device      r/s     r_await  rareq-sz  aqu-sz  %util
nvme1n1   1334.80   0.67     35.38     0.89    89.82
```

**`aqu-sz` (average queue depth) = 0.89** — the disk services *less than one
request at a time*. `r/s` (1334) is exactly `1 / r_await` (1 / 0.00067 ≈ 1492),
the serial-IO latency ceiling. `%util` ~90% is misleading for an NVMe: it means
"≥1 request in flight 90% of the time," not bandwidth saturation. The device can
sustain tens of thousands of IOPS at queue depth 32–128; at QD ~0.9 we get
~1,300.

Root cause: the single conversion thread issues **one synchronous `pread` and
blocks** until it returns before issuing the next. The trie traversal is
inherently pointer-chasing (each node read reveals the next node's hash), so a
single thread can never have more than one read outstanding. The disk's
parallelism sits idle. Caching cannot fix this — it only removes *some* reads;
the remaining ones are still fully serialized. More RAM cannot fix it either.

## Goal

Exploit the idle disk parallelism by issuing trie-node reads concurrently,
raising the queue depth from ~1 to ~32, so read throughput scales toward the
NVMe's real IOPS ceiling. Expected 5–15× throughput improvement with no extra
RAM, no on-disk schema change, and **complete history preserved exactly**.

## Non-goals

- No change to the on-disk Forest format or persisted schema.
- No change to *what* nodes are written per block (history stays complete).

## Update — pipelining implemented

The initial windowed prefetch-then-apply (below) serialized the phases: thread
dumps of the running conversion showed all 32 prefetch threads parked during the
single-threaded apply, with disk queue depth collapsing back to ~1 and the apply
thread CPU-bound on trie mutation (`RemoveVisitor`/`BranchNode.accept`) plus
serial cache-miss reads. To keep the disk busy continuously, the warming of
window N+1 is now **pipelined** with the replay of window N:

- The converter gained `prefetchAsync(List<TrieLog>, Hash baseRoot)`, returning a
  `Future`. A single-thread coordinator (`forest-convert-prefetch-coord`) drives
  the parallel warming off the apply thread; the 32-thread pool does the reads.
- The caller passes an explicit `baseRoot` captured **before** mutating the
  running root, so background warming never races the apply thread on
  `currentRootHash`. That base is the root at the start of the window being
  applied — a window or two behind the warmed window's true pre-state root, which
  is harmless: warming is best-effort and write-through during the intervening
  apply keeps modified paths cached.
- The subcommand loop double-buffers: gather window N+1, kick off its async warm,
  replay window N, then await the warm before replaying N+1.

The first window is still warmed synchronously from the resume root.

## Design

### Windowed prefetch-then-apply

Replace the per-block loop with a per-window loop over `W` blocks. Each window:

1. **Gather (cheap):** deserialize the `W` upcoming trie logs and collect the
   union of all changed *account addresses* and, per address, the *storage slot
   keys*, across the whole window.
2. **Prefetch (parallel, IO-bound):** spread the changed accounts across `T`
   reader threads. Each thread, for each assigned account, traverses the account
   trie read-only from the **window base root** to that account's address-hash
   (warming the account-trie path nodes into the shared cache), decodes the
   account to find its base storage root, then traverses that account's storage
   trie to each of its changed slot hashes (warming storage-trie path nodes).
   With `T` threads each blocked on a `pread`, the disk queue depth rises to ~`T`.
3. **Apply (single thread, unchanged):** replay the `W` blocks one at a time via
   the existing `applyTrieLog`, which now finds nearly all nodes already resident
   in the cache. Per-block node writes, state-root verification, and commit are
   unchanged.

### Why prefetching from the *window base root* is correct and effective

The pre-state root of block `k` is `header(k-1).stateRoot`. Within a window
`[b+1, b+W]`, only `header(b).stateRoot` (the base) is guaranteed present in the
Forest DB at prefetch time — the intermediate roots are written as the window is
applied. So all prefetch traversals start from the **base root**, which is
materialized.

For a key `K` changed in block `k`:
- If no earlier block in the window modified `K`'s path, the path nodes under
  `root(k-1)` are byte-identical to those under `root(b)` (a trie only allocates
  new nodes along changed paths). Warming from the base root warms exactly the
  nodes the apply phase will read.
- If an earlier block `j` in the window did modify `K`'s path, then applying `j`
  already wrote those new path nodes, and the existing **write-through** keeps
  them in the cache. The apply of block `k` finds them in RAM.
- If `K` is created mid-window (absent at the base root), the base-root traversal
  terminates early, harmlessly warming the existing ancestor nodes; the new nodes
  are written + cached when `K` is created.

In every case the apply phase reads from RAM, not disk. **Prefetch can only
populate the cache; it can never change the output** (the cache is keyed by node
hash — a cached value for a hash is always the correct node). The existing
per-block state-root verification in `applyTrieLog` remains the hard correctness
net: any divergence fails loudly.

### Thread-safety

- `ForestWorldStateKeyValueStorage.getAccount{State,Storage}TrieNode` →
  `keyValueStorage.get(...)` → a RocksDB point read, which is thread-safe for
  concurrent readers.
- `MemoryBoundCache` is Caffeine-backed; concurrent `getIfPresent`/`put` are
  thread-safe.
- Each prefetch thread uses its **own** `StoredMerklePatriciaTrie` instances
  (account trie + transient per-account storage tries). Trie instances are not
  shared across threads. The traversal is read-only (`get`), never mutates, and
  never calls the `updater`.
- The prefetch phase does **not** touch the `forestStorage.updater()` or write
  anything; it only reads and populates the cache. Writes happen only in the
  single-threaded apply phase, exactly as today.

### Cache requirement

Prefetch is only useful with the cache enabled (it warms the cache). When the
cache is disabled (`--Xx-convert-cache-size-mb=0`), prefetch is skipped (it would
have nowhere to deposit warmed nodes and would just burn IO).

### New flags on `ConvertToForestSubCommand`

```
--Xx-convert-prefetch-threads=<N>   (default: 32; 0 disables prefetch)
--Xx-convert-prefetch-window=<W>    (default: 64 blocks)
```

`threads` is the queue-depth lever; tune live on the node to find the knee.
`window` controls how much work is queued for the threads per prefetch phase.
Both hidden/experimental.

### Converter API additions

- A thread pool owned by the converter (size `T`), created when prefetch is
  enabled, shut down when the converter is closed. The converter becomes
  `AutoCloseable` (or exposes a `close()`); the subcommand closes it in the
  existing `finally`.
- `void prefetch(List<TrieLog> layers)`: warms the cache for all changed keys in
  the given layers, traversing from the converter's current root (the window
  base). No-op when prefetch/cache disabled. Swallows per-key read errors (a
  prefetch miss is never fatal; apply will re-read authoritatively).
- Reuse the existing `cachingLoad` read-through so prefetch and apply share one
  cache and one code path.

## Affected code

- `ethereum/core/.../trie/forest/migration/BonsaiTrieLogToForestConverter.java`
  - Add optional `ExecutorService` (size `T`) + `prefetch(List<TrieLog>)`.
  - Implement `close()` to shut the pool down.
  - Extract changed account/storage keys from a `TrieLog` (same accessors
    `applyTrieLog` already uses: `getAccountChanges`, `getStorageChanges`).
- `app/.../cli/subcommands/storage/ConvertToForestSubCommand.java`
  - Add the two flags; pass thread count to the converter constructor.
  - Restructure the replay loop into windows of `W`: gather `W` layers, call
    `converter.prefetch(layers)`, then `applyTrieLog` each. Progress logging,
    resume, and the already-complete path are unchanged.
  - Close the converter in the existing `finally`.

## Testing

- Unit (extend `BonsaiTrieLogToForestConverterTest`): converting **with prefetch
  enabled** produces the identical final root as without it (oracle), over a
  multi-block fixture that touches both account and storage changes.
- Prefetch populates the cache: after `prefetch(layers)`, the nodes on a changed
  key's path are present in the cache (a subsequent load does not hit storage).
- Prefetch is a no-op and harmless when the cache is disabled.
- Windowed apply in the subcommand path yields the same head root as per-block
  apply (can be asserted at the converter level by applying the same layers in
  one window vs one-at-a-time).
- Existing tests pass unchanged (threads=0 / cache=0 ⇒ current behaviour).

## Risks

- **Wasted prefetch IO** for keys whose path changed earlier in the window.
  Bounded by `W`; small `W` keeps waste low. Mitigated by write-through serving
  those during apply anyway.
- **Memory:** prefetch warms more of the cache faster, but the cache is already
  memory-bounded — it just reaches steady state sooner. No new unbounded
  allocation (the per-window key set is `O(W × changes/block)`).
- **Thread-pool lifecycle:** must be shut down on completion/exception. Mitigated
  by `close()` in the subcommand's `finally`.

## Future work (not in this change)

- **Pipeline** prefetch of window `N+1` concurrently with apply of window `N`, so
  the disk stays busy during the (currently disk-idle) apply phase. Would lift
  average queue depth further.
- A larger-core / larger-RAM instance to also parallelize the CPU-side hashing
  once the disk wall is removed.

## Rollout

1. Build; stop the running conversion.
2. Restart via `~/convert.sh` with prefetch enabled (default 32 threads, window
   64), confirming `Resuming conversion from block {K}` near prior progress.
3. Watch `iostat -x`: `aqu-sz` should rise from ~1 toward ~`T` during prefetch
   phases, `r/s` should climb well above 1,300, and blocks/s should improve
   several-fold. Tune `--Xx-convert-prefetch-threads` to find the IOPS knee.
