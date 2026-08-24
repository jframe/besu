# Self-tuning root checkpoints for archive trie-node history

**Date:** 2026-08-24
**Branch:** `bonsai-archive-proofs-diff-encoding`
**Goal:** Reduce archive `TRIE_BRANCH_STORAGE_ARCHIVE` storage in the **shallow-trie regime**
(enterprise / private chains) by letting trie *roots* be checkpoint+diff instead of forced FULL
every block — while remaining byte-identical to today on mainnet-shaped chains.
**Status:** design — awaiting review before implementation plan.

---

## 1. Problem

On a shallow trie (few accounts, activity concentrated on a handful of contracts — the typical
enterprise / private-network shape), the dominant archive-history cost is **forced-FULL entries
re-stored every block**. Modelling a ~10k-account chain with ~20 hot contracts, the per-block
archive write is roughly:

| Per-block stored bytes | source | forced FULL today? |
|------------------------|--------|--------------------|
| ~500 B                 | account-trie **root** | **yes** |
| ~5 × 500 B             | hot-contract **storage roots** | **yes** |
| ~10 × 40 B             | depth-1 branch diffs | no (size-guard → DIFF) |
| ~10 × 80 B             | leaves | full, but small |

**~40–60% of every block's archive write is forced-FULL root nodes.** The write path forces every
`location.size() == 0` node (account root and every storage-trie root) to a FULL entry on every
block it changes, via the `!request.location().isEmpty()` guard in
`ArchiveTrieNodeCapture.computeCapture`.

That rule is correct for **mainnet**, where the root changes ~all 16 children every block (a root
diff ≈ a root full, so diffing buys nothing). But it is the wrong rule for the **shallow regime**,
where activity is concentrated under a few address prefixes, so the root changes only a *few* of its
16 children per block — a root *diff* would be ~100 B, not ~500 B. The always-FULL rule is a
pessimization there.

### 1.1 Why the fix is safe (the self-tuning insight)

`ArchiveTrieNodeCodec.encodeDiff` already falls back to a FULL entry whenever the computed patch is
`>= newNode.size()` (codec lines 102–106). So if we simply let the root flow through the normal
checkpoint+diff path:

- **Mainnet:** the root changes densely → the patch is ≥ the node → `encodeDiff` returns a FULL →
  **byte-identical to today**, every block.
- **Enterprise:** the root changes sparsely → the patch is small → a DIFF is stored → the win.

The FULL-vs-DIFF decision is thus made **per block, per node, by the actual data** — no regime
detection, no config flag, no heuristic that can be mistuned.

---

## 2. Prior art / relationship to earlier designs

The mainnet-oriented `2026-07-20-depth-tiered-checkpoint-storage-design.md` set the root to
`interval = 1` (always FULL) for two reasons: (a) on mainnet a root diff ≈ a root full, and (b) the
root is the hottest read (every proof starts there), so a FULL means one fetch and zero replay. It
also **deferred** a separate "skip-root indexing" change because, in *that* codebase, the account
root's natural key was empty, creating a `seekForPrev` ambiguity in the index read path.

Two facts make the present change materially simpler and safe on **this** branch:

1. **No empty-key ambiguity.** `ArchiveNodeKey.account(location)` here prepends a 1-byte length
   prefix, so the account root's natural key is `[0x00]` (not empty) and a storage root's is
   `accountHash(32) ‖ [0x00]`. Roots are lexicographically disambiguated from depth-1 nodes by that
   prefix. The deferred "skip-root" hazard does not exist here.
2. **No separate index.** This branch's reader (`ArchiveHistoryReader`) resolves nodes with a
   seek-based `getLatestBefore` (`getNearestBefore` + exact natural-key guard) and reconstructs DIFF
   chains generically via `reconstructFromDiffChain`. Roots already traverse this exact path today
   (as FULLs).

Reason (a) for always-FULL is handled by the codec's size-guard (§1.1). Reason (b) — read cost — is
also self-neutralising: on mainnet, roots stay FULL (dense change → fallback), so `getLatestBefore`
lands on a FULL and does **zero** backward-walk; on enterprise, the DB is small and cache-hot, so a
bounded (≤ interval) walk is cheap.

---

## 3. Design

### 3.1 Write path — the single change

In `ArchiveTrieNodeCapture.computeCapture`, drop the root exclusion from the diff-eligibility guard:

```java
// before
if (!request.location().isEmpty() && request.block() != 0L && chainContiguous) {
// after
if (request.block() != 0L && chainContiguous) {
```

The root then uses `interval = checkpointIntervalForDepth(request.location().size())`. Block-0
creation (`priorNode == null` → FULL|CREATION) and non-contiguous blocks (`chainContiguous == false`
→ FULL) are unchanged and continue to force a FULL for roots exactly as for any other node.

### 3.2 Root checkpoint interval

Introduce a named constant and route depth 0 to it:

```java
/**
 * Trie roots (empty location, depth 0) checkpoint every ROOT_CHECKPOINT_INTERVAL mutations.
 * Unlike the mainnet depth-tier design (root = always FULL), roots here participate in
 * checkpoint+diff: encodeDiff's size-guard stores a FULL whenever a root diff is not smaller
 * (the mainnet dense-churn case), so this is byte-identical to always-FULL on mainnet-shaped
 * chains and only diverges — favourably — when a root changes few children per block.
 */
static final int ROOT_CHECKPOINT_INTERVAL = 32;

static int checkpointIntervalForDepth(final int locationSizeBytes) {
  if (locationSizeBytes == 0) {
    return ROOT_CHECKPOINT_INTERVAL;
  }
  return locationSizeBytes <= 2 ? SHALLOW_CHECKPOINT_INTERVAL : DEEP_CHECKPOINT_INTERVAL;
}
```

`ROOT_CHECKPOINT_INTERVAL = 32` matches the shallow tier (so behaviour is unchanged from what the
`<= 2` branch already returns) but is named separately so it can be tuned independently of shallow
non-root nodes. It **must** satisfy the read-window invariant (§3.4).

### 3.3 Read path — unchanged

No read-side code changes. `ArchiveHistoryReader.nodeAt` already:

- resolves the nearest entry at/before the target via `historyStore.getLatestBefore` (seek + exact
  natural-key match — correct for the length-prefixed root keys), and
- if that entry is a DIFF, walks backward collecting diffs until a FULL, then applies them forward
  (`reconstructFromDiffChain`), bounded by `MAX_BACKWARD_WALK_STEPS`.

This is identical for account roots (`[0x00]‖block`), storage roots (`accountHash‖[0x00]‖block`),
and any deeper node. The only new runtime possibility is that a *root* lookup lands on a DIFF and
triggers the walk — which is exactly the deep-node behaviour that already works. Correctness is
still fail-closed via the proof provider's state-root/hash verification above this layer.

### 3.4 Coupling — read-side reconstruction window

The invariant from the depth-tier work still holds and now includes the root:

> `max(ROOT_CHECKPOINT_INTERVAL, SHALLOW_CHECKPOINT_INTERVAL, DEEP_CHECKPOINT_INTERVAL)
> ≤ ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS`

Currently `max(32, 32, 16) = 32 ≤ 32`. Raising the root interval above 32 requires raising
`MAX_BACKWARD_WALK_STEPS` in lockstep. This is asserted by a unit test so a future bump cannot
silently break root reconstruction.

---

## 4. Scope

**In scope:**
- `ArchiveTrieNodeCapture`: the guard change (§3.1) and `ROOT_CHECKPOINT_INTERVAL` +
  `checkpointIntervalForDepth` update (§3.2). Covers **both** account roots and storage-trie roots
  (both are `location.size() == 0`).
- Tests: new write-behaviour, reconstruction, and invariant tests (§5); updates to the two existing
  "root always FULL" tests and any integration test that encodes that assumption.

**Out of scope (separate follow-ups):**
- CF-level ZSTD dictionary compression of `TRIE_BRANCH_STORAGE_ARCHIVE` (Lever 2).
- Adaptive per-node interval from measured child-change rate (Lever 3).
- Structure-aware branch diff / larger interval tuning (Levers 3–4).
- The mainnet "skip-root indexing" change — not applicable (this branch has no index).

**Migration:** write-format-only change. Existing archive DBs are re-migrated from block 0 (same
policy as the depth-tier change); no versioning or backward-compat path.

---

## 5. Testing

All in `ethereum/core/src/test/.../archive/trienode/`.

**Write behaviour (`ArchiveTrieNodeCaptureTest`):**
- *Sparse root change → DIFF:* a root node whose per-block delta is small (few bytes / few child
  hashes change) now produces a DIFF at non-checkpoint blocks (previously always FULL), and a FULL
  at block 0 (creation), every `ROOT_CHECKPOINT_INTERVAL`-th mutation, and after a non-contiguous
  gap.
- *Drastic root change → FULL (no regression):* a root node whose per-block delta rewrites most of
  the node stays FULL every block via the codec size-guard.
- Same two cases for a **storage** root (`ArchiveNodeKey.storage(accountHash, EMPTY)`).

**Reconstruction (`ArchiveHistoryReaderTest`) — proves the read path:**
- Build a root DIFF chain (a FULL then up to `ROOT_CHECKPOINT_INTERVAL − 1` DIFFs) and reconstruct
  at a mid-chain block; assert it equals the value written there.
- Repeat for a storage root.

**Invariant (`ArchiveTrieNodeCaptureTest`):**
- `max(ROOT, SHALLOW, DEEP)_CHECKPOINT_INTERVAL ≤ ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS`.

**Regression / existing-test updates:**
- Update `ArchiveTrieNodeCaptureTest.rootLocationAlwaysStoredFull` and
  `ArchiveTrieNodeStrategyTest.rootNodeIsAlwaysWrittenFull` to the new tiered behaviour (sparse →
  DIFF, drastic → FULL). Search test sources for other "root always FULL" assumptions:
  `grep -rn "root.*[Ff]ull\|always.*[Ff]ull" ethereum/core/src/test` and reconcile.
- Run the full archive test surface (`storage.flat.*`, `archive.trienode.*`, `BonsaiArchive*`
  integration) and spotless + `:ethereum:core:build -x test` per repo conventions.

---

## 6. Measurement & validation (after implementation)

1. On a representative **shallow / enterprise-shaped** chain (small account set, few hot contracts —
   e.g. the local QBFT net, or a seeded private chain), re-migrate/re-sync and compare
   `besu ... storage rocksdb x-stats` **`live-blob-file-size` + `live-sst-files-size`** for
   `TRIE_BRANCH_STORAGE_ARCHIVE` before/after, at the **same block height**, node stopped. Do not
   use directory `du` — the total-vs-live blob gap is GC lag, not logical size.
2. Confirm on a **mainnet/Hoodi** slice that the archive CF size and `eth_getProof` latency are
   unchanged (roots should remain FULL every block via the size-guard, so both should match the
   pre-change baseline within noise).

---

## 7. Open questions / risks

- **`ROOT_CHECKPOINT_INTERVAL` value.** 32 maximises shallow-regime saving and is cheap to
  reconstruct on a small enterprise DB. If a mixed-regime deployment (large DB *and* sparse root
  churn, so roots diff *and* the walk touches cold storage) proves latency-sensitive, lower it
  (e.g. 16); the invariant test bounds it above. Default 32 pending the §6 item 1 measurement.
- **Read-path assumption.** The design asserts the seek-based reader resolves root DIFF chains with
  no code change. This is the crux and is validated directly by the §5 reconstruction tests; if a
  root lookup were to mis-resolve, those tests fail rather than a silent wrong proof (the proof
  provider's hash verification is the outer backstop).
