# Approach B: Window index over archive-proof storage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Target branch/worktree:** `bonsai-archive-proofs` at `/Users/jframe/code/besu/.worktrees/bonsai-archive-proofs` (NOT the design5 worktree these docs live in). All paths below are relative to that worktree.

**Goal:** Replace the per-node `seekForPrev` (`getNearestBeforeMatchLength`) in the archive proof read path with an explicit per-node window index — a point-get — while keeping the existing window-suffixed storage and the bidirectional trie-log rolling unchanged.

**Architecture:** The proofs branch stores each changed node's whole value at `naturalKey ‖ windowStart(8B)` in `TRIE_BRANCH_STORAGE_ARCHIVE`, and finds the right version by reverse-seeking to the newest suffix ≤ the pinned window. This plan adds a new CF, `TRIE_NODE_WINDOW_INDEX_ARCHIVE`, holding per node the ascending list of window-starts at which it was written. The read path then computes `latestWindowLeq(naturalKey, pinnedWindow)` in memory and point-gets `naturalKey ‖ thatWindow`, falling back to the live-HEAD base strategy when the index has no entry. Rolling (`BonsaiArchiveWorldStateProvider`) is untouched — this is purely a per-node read-resolution swap.

**Tech Stack:** Java 21, Gradle, RocksDB (via `plugins/rocksdb`), JUnit 5 + Mockito + AssertJ, Apache Tuweni `Bytes`.

## Global Constraints

- **On-disk additive change only:** a new CF is added; `TRIE_BRANCH_STORAGE_ARCHIVE` key/value format is unchanged. Existing archive data stays valid, but the new index is only populated for nodes written after the change — so a **re-migration from block 0 is required** to populate the index for historical windows (the read path falls back to `seekForPrev`-free base-strategy live reads only for HEAD, not for history, so without a rebuilt index historical reads would miss). See Task 3 note on the transitional fallback.
- The window index is **authoritative for archive reads** once populated; when it has no entry ≤ the pinned window the read falls through to the base (live-HEAD) strategy, exactly as the current `.or(baseStrategy)` does.
- Checkpoint interval is a single configured value (`getArchiveTrieNodeCheckpointInterval()`), persisted under `ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY`; do not change it.
- The window value written as a node's suffix is the `BonsaiContext` block number computed by `getWriteContext` / `getStateTrieArchiveContextForWrite` (the window start). The index must append that same value.
- Run `./gradlew :ethereum:core:spotlessApply` before every commit; fix LSP/compile diagnostics before moving on.
- Test command base: `./gradlew :ethereum:core:test --tests "<ClassName>"`.
- Preserve the Apache license header at the top of every touched/created `.java` file.

---

## Background: current flow (proofs branch)

- **Write** — both `BonsaiArchiveTrieNodeStrategy.putFlat{Account,Storage}TrieNode` (lines 114-164) and `BonsaiArchiveMigrationTrieNodeStrategy.putFlat{Account,Storage}TrieNode` (lines 91-139) compute `ctx = getWriteContext/getStateTrieArchiveContextForWrite` (the window start) and `transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, calculateArchiveKeyWithMinSuffix(ctx, naturalKey), node)`.
- **Read** — `BonsaiArchiveTrieNodeStrategy.getFlatAccountTrieNode`/`getFlatStorageTrieNode` (lines 74-112) build a max-suffix key from `getStateTrieArchiveContextForRead` and call `storage.getNearestBeforeMatchLength(TRIE_BRANCH_STORAGE_ARCHIVE, keyNearest)` with size + common-prefix filters, then `.or(baseStrategy...)`.
- **Read context** — `getStateTrieArchiveContextForRead` (lines 183-191) returns a `BonsaiContext` whose block number is the pinned window (from `ARCHIVE_PROOF_BLOCK_NUMBER_KEY`, set by the rolling layer) or `WORLD_BLOCK_NUMBER_KEY`.

## File Structure

- `.../bonsai/storage/flat/WindowChangeIndex.java` — **new**: per-node ascending window-start list; `append` (RMW) + `latestWindowLeq` (in-memory max-≤).
- `.../ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java` — **new CF** `TRIE_NODE_WINDOW_INDEX_ARCHIVE`.
- `.../bonsai/storage/flat/BonsaiArchiveKeyUtil.java` — add `archiveKeyForWindow(naturalKey, windowStart)` helper.
- `.../bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java` — append to index on write; read via index + point-get.
- `.../bonsai/storage/flat/BonsaiArchiveMigrationTrieNodeStrategy.java` — append to index on write.
- Tests: new `WindowChangeIndexTest`; extend the existing archive-proof integration test (see Task 4).

---

### Task 1: `WindowChangeIndex` + its column family

**Files:**
- Create: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/WindowChangeIndex.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/WindowChangeIndexTest.java`

**Interfaces:**
- Produces on `WindowChangeIndex`:
  - `void append(SegmentedKeyValueStorageTransaction tx, SegmentedKeyValueStorage storage, Bytes naturalKey, long windowStart)` — read-modify-write: reads the committed list, and if its last entry is `< windowStart` appends the 8-byte BE `windowStart` and writes it back on `tx`. Idempotent when `windowStart` is already the last committed entry.
  - `Optional<Long> latestWindowLeq(SegmentedKeyValueStorage storage, Bytes naturalKey, long window)` — returns the largest stored window-start `≤ window`, or empty. Linear max-scan over the packed 8-byte entries (lists are tiny — one entry per window a node changed in, and V_avg ≈ 1.9).
- Consumes: `KeyValueSegmentIdentifier.TRIE_NODE_WINDOW_INDEX_ARCHIVE`.

- [ ] **Step 1: Add the column family**

In `KeyValueSegmentIdentifier.java`, after `TRIE_BRANCH_MIGRATION` (lines 68-74), add a new constant using the **same flag arguments as `TRIE_BRANCH_MIGRATION`** (mutable, no static/blob):

```java
  TRIE_NODE_WINDOW_INDEX_ARCHIVE(
      "TRIE_NODE_WINDOW_INDEX_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      false,
      true,
      false,
      true),
```

- [ ] **Step 2: Write the failing test**

Create `WindowChangeIndexTest.java`:

```java
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WindowChangeIndexTest {

  private static final Bytes KEY = Bytes.fromHexString("0xdeadbeef");
  private static final Bytes OTHER = Bytes.fromHexString("0xcafebabe");

  private SegmentedInMemoryKeyValueStorage kv;
  private WindowChangeIndex index;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    index = new WindowChangeIndex();
  }

  private void append(final Bytes key, final long window) {
    var tx = kv.startTransaction();
    index.append(tx, kv, key, window);
    tx.commit();
  }

  @Test
  void latestWindowLeqFindsNewestAtOrBeforeTarget() {
    append(KEY, 0);
    append(KEY, 32);
    append(KEY, 96);

    assertThat(index.latestWindowLeq(kv, KEY, 100)).contains(96L);
    assertThat(index.latestWindowLeq(kv, KEY, 96)).contains(96L);
    assertThat(index.latestWindowLeq(kv, KEY, 95)).contains(32L);
    assertThat(index.latestWindowLeq(kv, KEY, 0)).contains(0L);
  }

  @Test
  void returnsEmptyWhenNoWindowAtOrBeforeTarget() {
    append(KEY, 64);
    assertThat(index.latestWindowLeq(kv, KEY, 63)).isEmpty();
  }

  @Test
  void returnsEmptyForUnknownKey() {
    append(KEY, 0);
    assertThat(index.latestWindowLeq(kv, OTHER, 100)).isEmpty();
  }

  @Test
  void appendIsIdempotentForRepeatedWindow() {
    append(KEY, 32);
    append(KEY, 32); // same window again — must not grow the list
    assertThat(index.latestWindowLeq(kv, KEY, 32)).contains(32L);
    assertThat(index.latestWindowLeq(kv, KEY, 40)).contains(32L);
  }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "WindowChangeIndexTest"`
Expected: FAIL — `WindowChangeIndex` does not exist.

- [ ] **Step 4: Implement `WindowChangeIndex`**

Create `WindowChangeIndex.java`:

```java
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_WINDOW_INDEX_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Per-node index of the window-starts at which a trie node was written to {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE}. Replaces the reverse {@code seekForPrev} in the archive proof read
 * path with an in-memory {@code latestWindowLeq} + a point-get.
 *
 * <p>Key: the node's natural key ({@code location} for account nodes, {@code accountHash ‖ location}
 * for storage nodes). Value: ascending 8-byte big-endian window-starts. Lists are tiny (one entry
 * per window a node changed in), so reads scan linearly and writes read-modify-write.
 */
public final class WindowChangeIndex {

  private static final int ENTRY_BYTES = 8;

  public void append(
      final SegmentedKeyValueStorageTransaction tx,
      final SegmentedKeyValueStorage storage,
      final Bytes naturalKey,
      final long windowStart) {
    Objects.requireNonNull(tx, "tx must not be null");
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    final byte[] keyBytes = naturalKey.toArrayUnsafe();
    final Optional<byte[]> current =
        storage.get(TRIE_NODE_WINDOW_INDEX_ARCHIVE, keyBytes);
    if (current.isPresent()) {
      final Bytes list = Bytes.wrap(current.get());
      final int n = list.size() / ENTRY_BYTES;
      if (n > 0) {
        final long last = list.slice((n - 1) * ENTRY_BYTES, ENTRY_BYTES).toLong();
        if (last >= windowStart) {
          return; // already recorded this window (or a later one) — nothing to do
        }
      }
      final Bytes updated =
          Bytes.concatenate(list, Bytes.ofUnsignedLong(windowStart));
      tx.put(TRIE_NODE_WINDOW_INDEX_ARCHIVE, keyBytes, updated.toArrayUnsafe());
    } else {
      tx.put(
          TRIE_NODE_WINDOW_INDEX_ARCHIVE,
          keyBytes,
          Bytes.ofUnsignedLong(windowStart).toArrayUnsafe());
    }
  }

  public Optional<Long> latestWindowLeq(
      final SegmentedKeyValueStorage storage, final Bytes naturalKey, final long window) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    final Optional<byte[]> current =
        storage.get(TRIE_NODE_WINDOW_INDEX_ARCHIVE, naturalKey.toArrayUnsafe());
    if (current.isEmpty()) {
      return Optional.empty();
    }
    final Bytes list = Bytes.wrap(current.get());
    final int n = list.size() / ENTRY_BYTES;
    long best = -1L;
    boolean found = false;
    for (int i = 0; i < n; i++) {
      final long w = list.slice(i * ENTRY_BYTES, ENTRY_BYTES).toLong();
      if (w <= window && w > best) {
        best = w;
        found = true;
      }
    }
    return found ? Optional.of(best) : Optional.empty();
  }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "WindowChangeIndexTest"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/WindowChangeIndex.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/WindowChangeIndexTest.java
git commit -m "feat(bonsai-archive): window change index + column family"
```

---

### Task 2: Append to the window index on every archive write

**Files:**
- Modify: `.../bonsai/storage/flat/BonsaiArchiveKeyUtil.java` (add key helper)
- Modify: `.../bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java` (live/catch-up write path)
- Modify: `.../bonsai/storage/flat/BonsaiArchiveMigrationTrieNodeStrategy.java` (bulk-migration write path)
- Test: `.../test/.../bonsai/storage/flat/WindowChangeIndexTest.java` (already created) — no new test here; coverage is via the read swap in Task 3 and the integration test in Task 4.

**Interfaces:**
- Produces on `BonsaiArchiveKeyUtil`:
  - `static byte[] archiveKeyForWindow(byte[] naturalKey, long windowStart)` — `naturalKey ‖ windowStart(8B BE)`.
- Consumes: `WindowChangeIndex.append` (Task 1); `BonsaiContext.getBlockNumber()`.

- [ ] **Step 1: Add the key helper**

In `BonsaiArchiveKeyUtil.java`, add:

```java
  public static byte[] archiveKeyForWindow(final byte[] naturalKey, final long windowStart) {
    return Arrays.concatenate(naturalKey, Bytes.ofUnsignedLong(windowStart).toArrayUnsafe());
  }
```

- [ ] **Step 2: Give both strategies a `WindowChangeIndex`**

In `BonsaiArchiveTrieNodeStrategy.java`, add a field `private final WindowChangeIndex windowIndex = new WindowChangeIndex();` (a stateless helper; a single instance is safe). Do the same in `BonsaiArchiveMigrationTrieNodeStrategy.java`.

- [ ] **Step 3: Append in `BonsaiArchiveTrieNodeStrategy` writes**

In `putFlatAccountTrieNode` (lines 114-136), inside the `if (trieNodeCheckpointInterval != null)` block, after the `transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, keySuffixed, ...)` call, add:

```java
      windowIndex.append(
          transaction, storage, location, ctx.getBlockNumber().orElse(0L));
```

In `putFlatStorageTrieNode` (lines 138-164), likewise after its archive put, using the storage natural key:

```java
      windowIndex.append(
          transaction,
          storage,
          Bytes.concatenate(accountHash.getBytes(), location),
          ctx.getBlockNumber().orElse(0L));
```

- [ ] **Step 4: Append in `BonsaiArchiveMigrationTrieNodeStrategy` writes**

In `putFlatAccountTrieNode` (lines 91-112), inside the `if (trieNodeCheckpointInterval != null)` block, after the archive put, add:

```java
      windowIndex.append(transaction, storage, location, ctx.getBlockNumber().orElse(0L));
```

In `putFlatStorageTrieNode` (lines 114-139), after the archive put:

```java
      windowIndex.append(
          transaction, storage, accountHashLocation, ctx.getBlockNumber().orElse(0L));
```

(Note: `accountHashLocation` is already computed at line 129; reuse it. `ctx` is already in scope from `getWriteContext`.)

- [ ] **Step 5: Compile**

Run: `./gradlew :ethereum:core:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveKeyUtil.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveMigrationTrieNodeStrategy.java
git commit -m "feat(bonsai-archive): append window-index entries on archive trie-node writes"
```

---

### Task 3: Swap the archive read path from seekForPrev to index point-get

**Files:**
- Modify: `.../bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java` (`getFlatAccountTrieNode`, `getFlatStorageTrieNode`)
- Test: `.../test/.../bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyReadTest.java` (new focused unit test)

**Interfaces:**
- Consumes: `WindowChangeIndex.latestWindowLeq`, `BonsaiArchiveKeyUtil.archiveKeyForWindow`, existing `getStateTrieArchiveContextForRead`.

- [ ] **Step 1: Write the failing read test**

Create `BonsaiArchiveTrieNodeStrategyReadTest.java`. It seeds the archive CF and the window index directly, pins a window via `ARCHIVE_PROOF_BLOCK_NUMBER_KEY`, and asserts the strategy returns the node written at the latest window ≤ the pinned window:

```java
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveTrieNodeStrategyReadTest {

  private static final Bytes LOCATION = Bytes.fromHexString("0x0102");
  private SegmentedInMemoryKeyValueStorage kv;
  private WindowChangeIndex index;
  private BonsaiArchiveTrieNodeStrategy strategy;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    index = new WindowChangeIndex();
    strategy = new BonsaiArchiveTrieNodeStrategy(16L); // interval unused on the read path
  }

  private void pinWindow(final long window) {
    var tx = kv.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(window).toArrayUnsafe());
    tx.commit();
  }

  private void seedNodeAtWindow(final Bytes location, final long window, final Bytes node) {
    var tx = kv.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        BonsaiArchiveKeyUtil.archiveKeyForWindow(location.toArrayUnsafe(), window),
        node.toArrayUnsafe());
    index.append(tx, kv, location, window);
    tx.commit();
  }

  @Test
  void resolvesNodeAtLatestWindowAtOrBeforePinnedWindow() {
    final Bytes v0 = Bytes.fromHexString("0xaa");
    final Bytes v32 = Bytes.fromHexString("0xbb");
    seedNodeAtWindow(LOCATION, 0, v0);
    seedNodeAtWindow(LOCATION, 32, v32);

    pinWindow(16); // between window 0 and 32
    assertThat(strategy.getFlatAccountTrieNode(LOCATION, hash(v0), kv)).contains(v0);

    pinWindow(48); // at/after window 32
    assertThat(strategy.getFlatAccountTrieNode(LOCATION, hash(v32), kv)).contains(v32);
  }

  @Test
  void fallsBackWhenNoIndexEntryAtOrBeforePinnedWindow() {
    // Node first written at window 32, but pinned window is 16 → no archive entry.
    // With no live-HEAD value either, the base strategy returns empty.
    final Bytes v32 = Bytes.fromHexString("0xbb");
    seedNodeAtWindow(LOCATION, 32, v32);
    pinWindow(16);
    assertThat(strategy.getFlatAccountTrieNode(LOCATION, hash(v32), kv)).isEmpty();
  }

  private static Bytes32 hash(final Bytes node) {
    return Hash.keccak256(node);
  }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyReadTest"`
Expected: FAIL — the current read path uses `getNearestBeforeMatchLength` and has no `windowIndex` wired into reads; the test asserts index-driven resolution (it will pass by luck via seek OR fail on the fallback case — run it and confirm at least one assertion fails before implementing).

- [ ] **Step 3: Rewrite `getFlatAccountTrieNode`**

Replace lines 74-87 with:

```java
  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    final Optional<Long> pinned =
        getStateTrieArchiveContextForRead(storage).flatMap(BonsaiContext::getBlockNumber);
    if (pinned.isPresent()) {
      final Optional<Long> window = windowIndex.latestWindowLeq(storage, location, pinned.get());
      if (window.isPresent()) {
        final Optional<Bytes> node =
            storage
                .get(
                    TRIE_BRANCH_STORAGE_ARCHIVE,
                    BonsaiArchiveKeyUtil.archiveKeyForWindow(
                        location.toArrayUnsafe(), window.get()))
                .map(Bytes::wrap);
        if (node.isPresent()) {
          return node;
        }
      }
    }
    return baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage);
  }
```

- [ ] **Step 4: Rewrite `getFlatStorageTrieNode`**

Replace lines 89-112 with the analogous body using the storage natural key:

```java
  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    final Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
    final Optional<Long> pinned =
        getStateTrieArchiveContextForRead(storage).flatMap(BonsaiContext::getBlockNumber);
    if (pinned.isPresent()) {
      final Optional<Long> window =
          windowIndex.latestWindowLeq(storage, accountHashLocation, pinned.get());
      if (window.isPresent()) {
        final Optional<Bytes> node =
            storage
                .get(
                    TRIE_BRANCH_STORAGE_ARCHIVE,
                    BonsaiArchiveKeyUtil.archiveKeyForWindow(
                        accountHashLocation.toArrayUnsafe(), window.get()))
                .map(Bytes::wrap);
        if (node.isPresent()) {
          return node;
        }
      }
    }
    return baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }
```

Remove the now-unused `KEY_SUFFIX_LENGTH` filter imports only if the compiler flags them (the `NearestKeyValue`/`getNearestBeforeMatchLength` calls are gone; leave `BonsaiArchiveKeyUtil` imported).

- [ ] **Step 5: Run the read test + full compile**

Run: `./gradlew :ethereum:core:spotlessApply && ./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyReadTest"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyReadTest.java
git commit -m "feat(bonsai-archive): resolve archive trie-node reads via window index point-get"
```

---

### Task 4: Integration guardrail + full build

**Files:**
- Test: the existing archive-proof integration/migrator test on the branch (locate via `grep -rl "getFlatAccountTrieNode\|ARCHIVE_PROOF_BLOCK_NUMBER_KEY\|rollArchiveProof" ethereum/core/src/test`).

- [ ] **Step 1: Locate the existing archive proof integration test**

Run: `grep -rl "ARCHIVE_PROOF_BLOCK_NUMBER_KEY\|BonsaiArchiveWorldStateProvider\|getArchiveTrieNodeCheckpointInterval" ethereum/core/src/test`
Read the matching test(s) to find the end-to-end "roll to block X, serve proof, verify against state root" case.

- [ ] **Step 2: Confirm the end-to-end proof path still verifies after the read swap**

Run that test class:
Run: `./gradlew :ethereum:core:test --tests "<ArchiveProofIntegrationTestClass>"`
Expected: PASS. Because rolling is unchanged and the read swap resolves the same node bytes the seek path did (both point at `naturalKey ‖ latestWindowLeq(pinnedWindow)`), the reconstructed trie and its root hash are byte-identical. If the test fails because it was migrated before the index existed, re-run its migration setup so the index is populated (see Global Constraints).

- [ ] **Step 3: Full module build + broad archive test sweep**

Run: `./gradlew :ethereum:core:build -x test`
Run: `./gradlew :ethereum:core:test --tests "WindowChangeIndexTest" --tests "BonsaiArchiveTrieNodeStrategyReadTest" --tests "BonsaiFlatDbToArchiveMigratorTest" --tests "<ArchiveProofIntegrationTestClass>"`
Expected: all green.

- [ ] **Step 4: Commit any spotless changes**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A && git commit -m "chore(bonsai-archive): spotless after window-index read swap" || echo "nothing to commit"
```

---

## Validation on a node (after merge)

1. Deploy the branch to a fresh node and re-migrate the archive from block 0 (to populate the window index for all historical windows).
2. Serve `eth_getProof` at several historical blocks and compare against a reference archive node — results must be identical (rolling is unchanged; only per-node version resolution changed).
3. Profile a proof-heavy workload and confirm the `getNearestBeforeMatchLength` reverse-seek time has moved into cheap point-gets. Note that **trie-log rolling still dominates** (this design does not remove it) — the expected win is the elimination of the per-node reverse seek, not rolling.

## Known limitations (by design — this is the conservative fallback vs Approach A)

- **Rolling is retained:** proof latency is still O(interval) trie-log replays; only the per-node seek is removed. Approach A removes rolling entirely.
- **Index write is a per-node RMW** at each window a node changes in (lighter than a per-block index, but still a read-modify-write). The `append` reads committed storage; within an uncommitted multi-checkpoint batch a node changing in two windows may append a duplicate — harmless, because `latestWindowLeq` is a max-≤ scan tolerant of duplicates and order.
- **Transitional reads:** until a full re-migration populates the index, historical reads for un-indexed windows fall through to the base (live-HEAD) strategy and will not find historical nodes. A re-migration is required (Global Constraints).

## Self-review notes (coverage vs spec)

- Spec B "new `TRIE_NODE_WINDOW_INDEX_ARCHIVE` CF, packed window-starts per node" → Task 1.
- Spec B "append windowStart at each checkpoint persist" → Task 2 (both write paths).
- Spec B "read: `latestWindowLeq(pinnedWindow)` → point-get `naturalKey‖window`, base fallback" → Task 3.
- Spec B "storage & rolling unchanged" → `BonsaiArchiveWorldStateProvider` and the archive CF format are not touched; Task 4 verifies the end-to-end proof still matches.
