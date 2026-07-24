# Forest conversion: JIT account-path pre-warm (design)

## Context

`BonsaiTrieLogToForestConverter.applyTrieLog` replays one block's trie log in two phases:

- **Phase 1** (parallel): rebuilds each changed account's storage trie across the
  `prefetchExecutor` pool (32 threads), collecting the resulting nodes and new storage
  roots via `invokeAll`.
- **Phase 2** (single-threaded): walks the shared account trie sequentially, one
  `put`/`remove` per changed account, then commits.

A wall-clock profile taken at ~14h of steady-state operation showed phase-2 account-trie
reads had largely resolved themselves via the cross-block node cache (apply-phase account
hit-rate ~99.8%, confirmed via the `applyAccountHits`/`applyAccountMisses` counters added
in commit `e972f71`). However, monitoring a live run also showed a **sustained high-churn
region** (~block 9.70M–9.71M+) where the account working set exceeds the 8GB node cache:
apply-account hit-rate collapsed to ~15-33% and cache eviction climbed sharply, dropping
throughput from ~6 b/s to ~1.5-2.6 b/s for over an hour. In that regime phase 2's account
reads are serial, synchronous disk reads (queue depth 1), while `iostat` in a comparable
(though not identical) window showed the disk well under saturation (%util 62-73%,
idle gaps of aqu-sz ~3 between phase-1 read bursts of aqu-sz ~24-34).

The existing window-level prefetch (`prefetchAsync`/`prefetchFrom`) already tries to warm
account paths ahead of replay, but from a root captured a window (or several, with deep
lookahead) *behind* the block currently being applied — a real staleness gap in high-churn
regions where paths diverge quickly. This design closes that gap with a **synchronous,
per-block, exact-root** warm, immediately before phase 2, reusing the same parallel pool
and pattern phase 1 already uses successfully.

## Goal

Reduce phase-2 serial disk reads in high-churn regions by pre-warming each changed
account's trie path from the *exact* root phase 2 is about to walk, in parallel, as part
of the same per-block burst phase 1 already performs.

## Non-goals

- Does not change replay semantics, the account-trie mutation logic, or the per-block
  state-root verification.
- Does not address compaction-wave-saturated disk (out of scope; a different regime).
- Does not attempt trie-level BFS/batched multiGet (larger, riskier change; not needed
  unless per-address traversal overhead proves to be the limiting factor after this ships).

## Design

### Where it hooks in

Inside `applyTrieLog`'s existing `if (prefetchExecutor != null)` block, the `tasks` list
already built for phase-1 storage-trie rebuild is extended with one additional task per
address in `accountChanges.keySet()` — the **full** changed-account set for this block,
not just the subset with storage changes (phase 2 mutates the account trie for every
entry in `accountChanges`, so every one benefits from warming).

Both task kinds — the existing storage-rebuild tasks and the new account-warm tasks — are
submitted to `prefetchExecutor` in a **single `invokeAll`**, so this is one parallel burst
per block, not two sequential barriers.

### The warm task

For a given address:

```java
private void warmAccountPath(final Bytes32 root, final Address address, final NodeLoader loader) {
  try {
    new StoredMerklePatriciaTrie<>(loader, root, b -> b, b -> b)
        .get(Bytes32.wrap(address.addressHash().getBytes()));
  } catch (final RuntimeException e) {
    // Best-effort warming; phase 2 re-reads authoritatively on a miss.
  }
}
```

This mirrors the existing `warmAccount` prefetch helper's account-side logic, but is
account-only (no storage-slot warming — that's already handled by phase 1's own rebuild)
and is invoked with `currentRootHash` **as captured at the start of this `applyTrieLog`
call**, i.e. the exact pre-mutation root phase 2 will traverse from. No staleness window.

Each task constructs its own `StoredMerklePatriciaTrie` instance (matching the existing
prefetch pattern) so there's no shared mutable trie state across threads; all threads read
through the same thread-safe Caffeine-backed node cache, so a shared upper-level branch
node is only fetched from disk once (by whichever task reaches it first) and is a cache hit
for the rest.

### Gating / kill switch

A new hidden CLI flag, `--Xx-convert-warm-account-paths` (boolean, default `true`),
gates just the account-warm task construction. When `false`, phase 1's storage-trie
parallelization and everything else is unaffected — only the new warm tasks are skipped.
This is independent of `--Xx-convert-prefetch-threads` (which, if set to 0, disables
*all* parallel work including the already-working storage rebuild) so the two behaviors
can be tuned/disabled independently.

Plumbing follows the existing pattern: a field on `ConvertToForestSubCommand`, threaded
into the `BonsaiTrieLogToForestConverter` constructor alongside `cacheMaxBytes` and
`prefetchThreads`.

### Correctness

Warming only populates the hash-keyed node cache; it is read-only and best-effort
(exceptions swallowed). It cannot change what phase 2 subsequently computes — the
account-trie mutation loop, `RLP` encoding, and the post-commit state-root comparison
against `expectedStateRoot` are byte-for-byte unchanged. The existing per-block
verification remains the correctness net exactly as it is today.

### Instrumentation

No new instrumentation needed. The `applyAccountHits`/`applyAccountMisses` counters
(added in `e972f71`) already measure precisely what this change should move: if it works,
apply-account hit-rate in a high-churn region should rise from ~15-33% toward the ~99%
seen in easier regions, and `blocks/s` should recover correspondingly. This is the
before/after signal to watch post-deploy.

### Testing

Extend `BonsaiTrieLogToForestConverterTest` with a case that changes multiple distinct
accounts in a single block with prefetch enabled (warm path active) and asserts the
reconstructed root matches an oracle-computed root, in the same style as the existing
`prefetchAsyncWarmsFromCurrentRootAndReplayMatches` / whole-window prefetch tests. Existing
tests already cover prefetch-disabled and cache-disabled paths (which continue to skip the
new tasks entirely via the same `prefetchExecutor != null` gate).

### Known risk (explicitly not resolved by this design)

Disk headroom (%util 62-73%, idle gaps between phase-1 bursts) was measured in a
compaction-quiet, *not-yet-high-churn* window. Whether the disk still has headroom
**during** a high-churn region (many more concurrent warm tasks per block) is unverified.
If the disk is closer to saturated there, adding account-warm tasks to the same burst
could increase per-block barrier time rather than shrinking it. This is why the fix ships
with a dedicated kill switch and is judged purely by the `applyAcct` hit-rate/throughput
signal after deploy, not assumed to work from this design alone.
