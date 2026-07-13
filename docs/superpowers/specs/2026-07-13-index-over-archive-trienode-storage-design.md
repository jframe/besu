# Index over archive trie-node storage — design

**Date:** 2026-07-13
**Status:** Design / brainstorming output (not yet planned or implemented)
**Author:** Jason Frame

## Problem

Serving historical `eth_getProof` (and historical state generally) on a bonsai
archive node is read-latency bound. Two existing branches take different shapes:

- **`bonsai-archive-proofs`** stores each changed trie node's whole value keyed
  by the *window start* (`naturalKey ‖ windowStart`) and resolves a proof by (a)
  a RocksDB `seekForPrev` (`getNearestBeforeMatchLength`) per node to find the
  newest version ≤ the pinned window, then (b) **rolling trie logs** forward or
  backward from the nearest checkpoint to the exact target block. Rolling is
  required precisely because a window key is only accurate at the window
  boundary.
- **`design5-geth-lessons`** adds an explicit per-node **change index** (packed
  sorted block-offsets per node) plus a history store with a structural
  FULL/DIFF codec and a bounded backward reconstruct scan.

**Goal:** faster proof reads, and a design that is **simpler and closer to
Bonsai** than either branch — i.e. the stored value should be an ordinary Bonsai
trie node, and the read should be "find the version, fetch it, verify it."

**Key structural fact (from the proof-storage spike, `V_avg = 1.89` versions per
location):** trie nodes almost never change more than once or twice in their
lifetime. Diff schemes (structural or window-collapsed) therefore save little
space, and their complexity is hard to justify.

## Two designs

This spec documents two approaches. **Approach A is recommended.** Approach B is
the low-risk incremental fallback.

---

## Approach A — Index + per-block whole-node values (no rolling) — RECOMMENDED

**Core idea:** store every changed node's whole RLP at an exact-block key; a
per-node change index gives `latestLeq(X)`; a proof is served by resolving each
proof-path node independently. No rolling, no `seekForPrev`, no structural diffs.

### Storage layout (column families)

- **`TRIE_BRANCH_STORAGE_ARCHIVE`** (reuse the proofs CF, redefine the suffix):
  - key: `naturalKey ‖ block(8B big-endian)`
  - value: **whole node RLP**, byte-identical in form to a live Bonsai node —
    no codec, no FULL/DIFF flag, no tombstone envelope (deletions are recorded
    as an empty/absent value at that block; see Open questions).
  - `naturalKey` = `location` (account node) or `accountHash(32) ‖ location`
    (storage node), per `ArchiveNodeKey`.
- **`TRIE_NODE_INDEX_ARCHIVE`** + **`TRIE_NODE_SUBBLOCK_ARCHIVE`** (from design5):
  - per `(naturalKey, rangeId = block / 1_000_000)`, a packed sorted list of
    3-byte within-range offsets of the blocks where the node changed, with
    sub-block spilling for hot nodes. This is design5's index **minus** the
    FULL/DIFF bookkeeping.
- **`TRIE_BRANCH_STORAGE`** (live HEAD trie): unchanged.
- **`TRIE_BRANCH_MIGRATION`** (frontier CF): kept so migration's own reads stay
  point-lookups.

### Index (simplified vs design5)

Drop `CHECKPOINT_INTERVAL`, `FULL_ABOVE_DEPTH`, and `appendAndGetPreviousCount`.
The index API collapses to:

- `append(naturalKey, block)` — record that the node changed at `block`.
- `latestLeq(naturalKey, X) → Optional<block>` — newest change ≤ X (in-memory
  binary search over the packed offsets; sub-block walk only when the tail
  misses).

No running counts are needed, because there is no FULL/DIFF decision to make.
The batched in-memory write buffer and per-batch flush from
`docs/superpowers/plans/2026-06-23-batch-migration-index-writes.md` are reused
verbatim.

### Read / proof-serving path

Replace `BonsaiArchiveWorldStateProvider.rollArchiveProofWorldStateToBlockHash`
with per-node resolution (the `ArchiveProofNodeLoader` shape from design5). For
each `(location, expectedHash)` reached during proof-path traversal:

1. **Hash-first fast path:** read live `TRIE_BRANCH_STORAGE[naturalKey]`; if
   `keccak256(node) == expectedHash`, return it (node unchanged since — 1 read).
2. Else `b* = index.latestLeq(naturalKey, X)`;
   `get(TRIE_BRANCH_STORAGE_ARCHIVE, naturalKey ‖ b*)`.
3. **Fail-closed verification:** recompute `keccak256(node)`; if it does not
   equal `expectedHash`, throw (never serve an unverified node).

**Deleted from the proofs branch:** bidirectional trie-log rolling, checkpoint
selection, `ARCHIVE_PROOF_BLOCK_NUMBER_KEY` window-pinning,
`getNearestBeforeMatchLength`. **Deleted from design5:** `TrieNodeDiffCodec`,
the `RECONSTRUCT_WINDOW` backward scan, the backward-walk fallback.

### Write / migration path

Per changed node per block: write the whole RLP at `naturalKey ‖ block` and
`index.append(naturalKey, block)`. **No prior-node read** — because there is no
structural diff to compute, the migrator no longer reads the pre-overwrite node
value. This removes the ~34% "redundant prior-node reads" measured in migration
profiling, on top of the ~38% index read-modify-write that the batch-buffer plan
already amortizes.

Reuse the per-batch transaction and buffered index flush from the batch-migration
plan unchanged (archive + index + flat + `MIGRATION_PROGRESS_KEY` committed
atomically at the batch boundary).

### Crash safety

Unchanged from the batch plan: a partial (uncommitted) batch leaves nothing on
disk; resume reads `MIGRATION_PROGRESS_KEY` at the last batch boundary and
re-replays the next batch cleanly. The in-memory index buffer and any overlay are
discarded on crash — correct, because they were never committed.

### Costs / risks

- **Storage:** whole-value-per-version is ~24% larger than window-diffs
  (spike: SA 2638 MB vs SB_4 2131 MB ZSTD, at that dataset's scale; ratios hold).
  Acceptable price for the simplicity and read speed; content-addressed dedup
  (Approach C, out of scope here) can reclaim it later.
- **Format change:** the archive CF suffix changes meaning (block, not window),
  so this requires a re-migration from block 0. So does design5 today, so this
  is not a new burden.
- **Index absence/corruption:** without the index there is no version lookup.
  See Open questions for the optional `seekForPrev` fallback.

---

## Approach B — Index-selected window + minimal roll (fallback)

**Core idea:** keep the proofs branch's storage and rolling exactly as they are;
add a window-granularity index so the per-node archive read *during rolling* is a
point-get instead of a `seekForPrev`.

### Storage layout

- **`TRIE_BRANCH_STORAGE_ARCHIVE`**: **unchanged** — key `naturalKey ‖
  windowStart(8B BE)`, whole node value, one write per window the node changed in.
- **New `TRIE_NODE_WINDOW_INDEX_ARCHIVE`**: per `(naturalKey, rangeId)`, packed
  sorted offsets of *window-starts* at which the node was written. Same packed
  format as design5's index; entries are windows, so there are even fewer of them.
- `TRIE_BRANCH_STORAGE`, `TRIE_BRANCH_MIGRATION`: unchanged.

### Index

- `append(naturalKey, windowStart)` at each checkpoint persist.
- `latestWindowLeq(naturalKey, pinnedWindow) → Optional<windowStart>`.

### Read / proof-serving path

`BonsaiArchiveWorldStateProvider` rolling is **kept unchanged** (checkpoint
selection, bidirectional roll, `ARCHIVE_PROOF_BLOCK_NUMBER_KEY` pinning). The only
change is inside `BonsaiArchiveTrieNodeStrategy.getFlatAccountTrieNode` /
`getFlatStorageTrieNode`: instead of `getNearestBeforeMatchLength` (seekForPrev),
do `w = index.latestWindowLeq(naturalKey, pinnedWindow)` →
`get(TRIE_BRANCH_STORAGE_ARCHIVE, naturalKey ‖ w)`, with the existing
`baseStrategy` fallback to live HEAD when the index has no entry.

### Write / migration path

At each checkpoint persist, in addition to writing the changed node at
`naturalKey ‖ windowStart`, `index.append(naturalKey, windowStart)`. No
prior-node read needed (proofs already stores whole values). Index writes happen
once per window, so write-amp is lighter than A.

### Crash safety

Unchanged from the proofs branch's checkpoint-persist model; the index write is
inside the same persist transaction.

### Costs / risks

- Read latency win is **partial**: it removes the per-node `seekForPrev` but
  keeps O(interval) trie-log rolling — which dominates.
- Net moving parts **increase** vs today (rolling + window-pinning are retained,
  and an index is added).
- Smallest on disk (storage unchanged from proofs).

---

## Comparison

| Dimension | A (per-block + index) | B (window index + roll) |
|---|---|---|
| Read latency | best — point-gets, no roll | partial — point-gets, still rolls |
| Simplicity | deletes rolling + pinning + codec | keeps rolling + pinning; adds index |
| Closeness to Bonsai | values = raw Bonsai nodes; read = get + verify | keeps proofs' extra machinery |
| Storage size | +24% vs today | unchanged (smallest) |
| Migration reads dropped | prior-node **and** seek | seek only |
| Net moving parts | fewest | more than today |
| Re-migration required | yes (suffix semantics change) | no (storage format unchanged) |

## Recommendation

**Approach A.** It is the only option that fully delivers the stated goal
(rolling-free reads) *and* the steer toward simplicity / Bonsai-closeness — it
ends up with fewer moving parts than the branch it starts from, stores ordinary
Bonsai nodes, and reduces both migration read classes. Approach B is documented
as the low-risk fallback should the +24% storage or the re-migration ever be
unacceptable.

## Resolved decisions

1. **Deletion encoding (A): empty value at `naturalKey ‖ block`.** At the block
   a node is deleted, write a zero-length value. Read resolves
   `index.latestLeq(naturalKey, X) → b*`, `get(... naturalKey ‖ b*)`; an empty
   result means the node was deleted at `b*` → return absent. Live values remain
   raw node RLP (no codec); the index stays authoritative.
2. **No `seekForPrev` fallback (A): the index is authoritative.** Reads trust the
   index exclusively. If `index.latestLeq` returns a block but the value CF has no
   entry at `naturalKey ‖ b*` (other than the empty deletion sentinel), that is a
   hard error, not a silent seek fallback. Keeps the read path simple; migration
   correctness (index and value written in the same batch tx) is the guarantee.

## Open questions

1. **Hash-first fast path scope.** Applies cleanly to account/branch nodes;
   confirm it holds for storage-trie nodes under account deletion/recreation.
2. **Index granularity for A** — confirm `rangeId = block / 1_000_000` is right
   for the target chain length, or whether a different range size packs better.
