# Live trie-node history capture during initial sync

**Date:** 2026-08-06
**Status:** Design — approved for planning
**Scope:** Bonsai archive trie-node history population (proof support)

## Problem

The Bonsai archive trie-node history archive (`TRIE_NODE_HISTORY_ARCHIVE`, consumed by the
`eth_getProof` history path via `ArchiveProofNodeLoader`) is currently populated by
`TrieNodeHistoryWalker`. The walker is a trailing process that, for every historical block,
replays the block's trie log through an **isolated** `BonsaiWorldState`
(`TrieNodeHistoryWalkerWorldState` + `HistoryOnlyWriteStorage`) and calls `persist()` to
recompute the state root and re-derive every trie node.

This is too slow: it duplicates the entire trie computation the node already performed once
during block import. On real chain history the replay cost is prohibitive.

## Goal

Populate the trie-node history archive by **capturing trie nodes live during initial block
import**, when `persist()` has already computed every node — eliminating the separate replay
pass. Remove the walker.

- **In scope:** initial sync of a fresh archive node from genesis. Capture runs from genesis
  and trails the head by `maxLayersToLoad`, exactly like `BonsaiFlatDbToArchiveMigrator`.
- **Out of scope (followup PR):** ongoing population at head, including reorg-safe capture of
  blocks as they age past the reorg window, and any critical-path performance tuning.

## Target scenario

A fresh archive node running `--data-storage-format=X_BONSAI_ARCHIVE` with
`--Xbonsai-trie-node-history-enabled`, full-syncing from genesis. Every block is executed and
persisted, so every block's trie nodes are computed and available at `persist()` time. (Archive
nodes full-sync from genesis with trie-log retention; this is the same precondition the existing
flat-DB migrator relies on.)

The flat-DB side (`BonsaiFlatDbToArchiveMigrator`) is unchanged and continues to build the flat
archive via its post-sync replay. The two archives are independent; only the trie-node side
changes.

## Background: why the current strategy cannot be used live

`BonsaiArchiveTrieNodeStrategy` today is **walker-shaped**: its read methods
(`getFlatAccountTrieNode` / `getFlatStorageTrieNode`) return the *prior-block history* version of
a node, and its writes capture history only. That is correct for the walker's isolated world
state, whose whole job is to reconstruct history.

It is **wrong for the live import path**. During live import the world state must read the *live
current* trie node to compute the next state root; returning a historical version would corrupt
block import.

The pre-`92057a70854` version of this class had the shape we need: reads/writes **delegate** to a
base `BonsaiTrieNodeStrategy`, and writes **additionally** capture a history entry. This design
restores that delegating shape.

## Design

### 1. Restore the delegating live-capture strategy

Change `BonsaiArchiveTrieNodeStrategy` to:

- **reads** → delegate to an injected base `BonsaiTrieNodeStrategy` (live current node).
- **writes** (`putFlatAccountTrieNode`, `putFlatStorageTrieNode`, `removeFlatAccountStateTrieNode`)
  → delegate to the base strategy (so the live flat DB is written normally) **and** capture a
  FULL/DIFF history entry via `TrieNodeHistoryStore`, then advance `TrieNodeHistoryProgress`.
- **diff base** → the prior-node bytes for the DIFF are read from the **base strategy (live flat
  DB) before the put**, not from `historyReader.nodeAt`. This is the crux of live-capture
  correctness: during sequential import the live flat DB still holds block `N-1`'s value at the
  moment block `N` is persisted, so the live read *is* the correct previous-block diff base. After
  the put it holds block `N`'s value, serving as the base for block `N+1`. (The FULL-vs-DIFF and
  checkpoint-counter decision still consults `TrieNodeHistoryStore.getLatestBefore`, as today.)
- **block number** → derived at write time from `WORLD_BLOCK_NUMBER_KEY + 1`, mirroring
  `BonsaiArchiveFlatDbStrategy.getStateArchiveContextForWrite`. `WORLD_BLOCK_NUMBER_KEY` is written
  on every `persist()` in `PathBasedWorldState` (all bonsai modes), and committed storage lags the
  in-flight block by one, so `+1` yields the block currently being persisted. Absent key ⇒ block 0
  (genesis).

The diff/checkpoint logic (`captureTrieNodeDiff`, `CHECKPOINT_INTERVAL`, FULL vs DIFF selection)
is preserved unchanged.

### 2. Reorg-window capture gate ("trail head by maxLayers")

Live `persist()` runs when a block is at depth 0 (the new local head). To match the migrator's
coverage boundary and avoid ever writing reorg-window blocks, capture is gated:

> Capture block `N` **only if** `N == 0` **or** `N <= networkHead - maxLayersToLoad`.

The `N == 0` exception is required for genesis. Genesis is persisted exactly once at node
startup, **before** peers connect, so `bestChainHeight ≈ 0` and the plain gate would be closed.
Untouched genesis accounts' trie nodes are never re-persisted, so without this exception their
history would be permanently missing and proofs for genesis-allocated static accounts would fail.
Block 0 is always final, so capturing it unconditionally is safe. This subsumes the walker's
explicit `bootstrapGenesis`, provided the strategy is installed before genesis state is written
(see §3).

- `networkHead` comes from `syncState.bestChainHeight()` (max of local head and best-peer height
  estimate; the engine-API payload head post-merge).
- `maxLayersToLoad` comes from `TrieLogManager.getMaxLayersToLoad()`.

Both are supplied to the strategy via a lightweight accessor (e.g. a `LongSupplier` for the
"highest safe block to capture" = `bestChainHeight - maxLayersToLoad`) set by
`BesuControllerBuilder`, which owns `syncState`. The strategy already knows `N` from
`WORLD_BLOCK_NUMBER_KEY + 1`.

Behaviour:

- **During catch-up:** `networkHead` is far ahead of the block being imported, so the gate is open
  and every catch-up block is captured. Safe by construction — a block is captured only once it is
  already `maxLayers`-buried relative to the network head, i.e. network-final.
- **Approaching the tip:** once `N > networkHead - maxLayers` the gate closes; the last
  `maxLayers` blocks are not captured. Coverage ends exactly at `head - maxLayers`.
- **At head / restart-at-head:** every new block `N ≈ head` fails the gate, so capture stays off
  automatically. The initial→ongoing handoff sits at the reorg edge, matching the migrator; the
  followup's ongoing population takes over from `head - maxLayers`.

The gate is stateless and self-regulating: no "initial sync complete" marker or gap-detection is
required. Swapping the strategy back to a plain `BonsaiTrieNodeStrategy` at first in-sync is an
optional micro-optimisation to drop the per-`persist` comparison — not a correctness requirement,
and may be omitted for simplicity.

### 3. Install on the real storage

Reinstate `maybeInstallTrieNodeHistoryStrategy` in `KeyValueStorageProvider` (removed by
`e5fa297`): when the format is `X_BONSAI_ARCHIVE` and `--Xbonsai-trie-node-history-enabled`,
install the delegating strategy on the real `BonsaiWorldStateKeyValueStorage` via
`setTrieNodeStrategy`, wired to the shared `TrieNodeHistoryStore` / `TrieNodeHistoryReader` /
`TrieNodeHistoryProgress` instances that the proof read path also uses.

The strategy is installed at storage construction, before genesis state is written, so genesis
trie nodes are captured under the same path (subject to the gate).

### 4. Remove the walker

Delete:

- `TrieNodeHistoryWalker`
- `TrieNodeHistoryWalkerWorldState`
- `HistoryOnlyWriteStorage`
- Their tests (`TrieNodeHistoryWalkerTest`, `TrieNodeHistoryWalkerIntegrationTest`,
  `HistoryOnlyWriteStorageTest`)
- The walker wiring block in `BesuControllerBuilder` (the `getTrieNodeHistoryEnabled()` block that
  constructs and starts the walker).

Keep unchanged: `TrieNodeHistoryStore`, `TrieNodeHistoryReader`, `TrieNodeHistoryProgress`,
`ArchiveTrieNodeCodec`, `ArchiveNodeKey`, `ArchiveTrieNodeEntry`, `ArchiveProofNodeLoader`, and the
proof read path with its depth gate (`chainHead - block >= maxLayersToLoad`).

## Interfaces / boundaries

- **`BonsaiArchiveTrieNodeStrategy`** — what: delegates trie-node reads/writes to a base strategy
  and captures history on write, gated by the reorg window. Depends on: base `TrieNodeStrategy`,
  `TrieNodeHistoryStore`, `TrieNodeHistoryProgress`, a "highest safe block" `LongSupplier`, and
  `WORLD_BLOCK_NUMBER_KEY` in storage.
- **`KeyValueStorageProvider.maybeInstallTrieNodeHistoryStrategy`** — what: installs the strategy
  on real storage when archive + history are enabled. Depends on: `DataStorageConfiguration`,
  the shared history components.
- **`BesuControllerBuilder`** — what: constructs the shared history components (already does) and
  supplies the `bestChainHeight - maxLayers` accessor from `syncState`. Depends on: `syncState`,
  `TrieLogManager`.

## Read path (unchanged, confirms safety)

`eth_getProof` engages the history path only when `chainHead - block >= maxLayersToLoad`; within
the reorg window it uses the live trie-log path (`3420f268a2`). This exactly complements the
write-side gate: the archive covers `[0, head - maxLayers]`, reads consult it only for that same
range, and near-head reads never touch it. The post-in-sync gap this PR leaves (blocks newer than
`head - maxLayers`, owned by the followup) is served by the trie-log path.

## Risks and test focus

- **Genesis / block-1 boundary.** The original live implementation carried a "block-1 regression"
  note around `WORLD_BLOCK_NUMBER_KEY` derivation at genesis. Requires an explicit end-to-end test
  from genesis asserting block 0 (captured via the `N == 0` gate exception, with `bestChainHeight`
  still ≈ 0) and block 1 entries are correct, including a genesis-allocated account that is never
  touched again.
- **Gate correctness at the tip.** Test that coverage ends at exactly `head - maxLayers` and that
  no reorg-window block is captured.
- **Read delegation.** Verify live reads return the current node (not a historical version) so
  block import is unaffected.
- **Critical-path cost.** Capture adds archive writes inside the import transaction. Acceptable
  during initial sync; at head the gate disables capture, so there is no `newPayload` impact in
  this PR. Steady-state cost is a followup concern.
- **State-root validation.** The walker halted on `StateRootMismatchException` as a compensating
  control; live import already validates the state root, so this is obtained for free.

## Testing

- **Unit — strategy:** read delegation returns the live base value; DIFF base taken from the live
  read (not history); DIFF vs FULL capture selection; genesis suffix (block 0); `remove` capture;
  gate open/closed by block number vs. supplied threshold; `N == 0` captured even when the
  threshold gate is closed.
- **Integration — end to end:** restore/adapt the deleted live-capture integration test. Import
  genesis→N with capture enabled; assert history entries and `ArchiveProofNodeLoader`
  reconstruction match a directly-derived proof; assert coverage trails head by `maxLayers`.
- **Wiring:** strategy installed only when archive + history flag set; walker no longer started.
- **Regression:** existing archive-proof and flat-migrator tests still pass.

## Followup PR (not this work)

Ongoing trie-node population at head with reorg-safe capture of blocks as they age past
`maxLayers`, plus critical-path performance tuning (batching, contention mitigation). Begins
coverage at `head - maxLayers`, where this PR's initial capture ends.
