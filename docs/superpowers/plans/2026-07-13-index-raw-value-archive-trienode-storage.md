# Approach A: Index + raw-value archive trie-node storage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve historical trie nodes (for `eth_getProof` / historical state) by storing each changed node's **whole RLP** keyed by exact block and resolving versions through the existing change index — deleting the structural diff codec and its reconstruction machinery.

**Architecture:** The current `design5-geth-lessons` branch already resolves nodes per-node through `TrieNodeChangeIndex` (no trie-log rolling). This plan simplifies the *value* layer: the history store holds raw node RLP instead of `TrieNodeDiffCodec` FULL/DIFF/tombstone entries; a deletion is a zero-length value. A read becomes `index.latestChangeBlock(key, X)` → point-get → (empty ⇒ absent, else the node). This removes the FULL/DIFF write decision, the prior-node read, the `RECONSTRUCT_WINDOW` backward scan, and the diff codec entirely. The change index is authoritative: if it names a block, the value store must have that block (both are written in the same batch transaction).

**Tech Stack:** Java 21, Gradle, RocksDB (via `plugins/rocksdb`), JUnit 5 + Mockito + AssertJ, Apache Tuweni `Bytes`.

## Global Constraints

- Single-threaded migration (`archive-migrator-0`); non-concurrent collections are safe in the migration world state.
- **On-disk format changes** (history CF values become raw RLP instead of codec entries). This requires a re-migration from block 0; the current branch already requires that, so it is not a new burden.
- The change index is **authoritative**: reads never fall back to `seekForPrev`. An index entry with no matching value is a hard data-inconsistency (logged; read returns empty so the fail-closed hash check in `ArchiveProofNodeLoader` surfaces it).
- Deletion encoding: a node absent at block `b` is stored as a **zero-length value** at `naturalKey ‖ b` (not a key removal, not a tombstone byte).
- Run `./gradlew :ethereum:core:spotlessApply` before every commit; fix LSP/compile diagnostics before moving on.
- Test command base: `./gradlew :ethereum:core:test --tests "<ClassName>"`.
- Preserve the Apache license header at the top of every touched/created `.java` file.

---

## Background: current value flow (to be simplified)

- **Write** (`BonsaiArchiveTrieNodeStrategy.captureTrieNodeDiff`, lines 311-352): reads the prior node, chooses `FULL` (creation / `location.size() ≤ FULL_ABOVE_DEPTH` / every `CHECKPOINT_INTERVAL`-th mutation) vs structural `DIFF`, encodes via `TrieNodeDiffCodec`, and stores the codec entry. `putFlatAccountTrieNode`/`putFlatStorageTrieNode` (lines 171-227) each read the prior node from `TRIE_BRANCH_STORAGE` before the base write.
- **Read** (`TrieNodeHistoryReader.nodeAt`, lines 131-189 and the preloaded-list overload 223-289): resolves `b*` via the index, fetches the codec entry, and if it is a DIFF runs `reconstructFromChangeBlocks` (a `RECONSTRUCT_WINDOW=64` backward multiGet scan) or `backwardWalkFallback` to find the nearest FULL and apply diffs.
- **Loader** (`ArchiveProofNodeLoader.resolveNodeAt`, lines 158-193): hash-first fast path, then `index.readRangeList` + `latestLeq` → `b*` → `historyReader.nodeAt(key, b*, list, rangeId)` → fail-closed hash verify.

`TrieNodeDiffCodec` (937 lines) and `TrieNodeHistoryComposition` (277 lines, already unreferenced in `main`) exist only to support DIFF reconstruction.

## File Structure

- `.../archiveindex/TrieNodeHistoryReader.java` — rewrite to raw-value point read; add package-private `nodeAtExactChangeBlock`; delete reconstruction/backward-walk/constants.
- `.../archiveindex/TrieNodeHistoryStore.java` — Javadoc only: values are raw node RLP or zero-length (deletion). No API change.
- `.../flat/BonsaiArchiveTrieNodeStrategy.java` — replace `captureTrieNodeDiff` with `captureTrieNode` (store raw RLP + `append`); drop prior-node reads, `FULL_ABOVE_DEPTH`, `CHECKPOINT_INTERVAL`, codec import; wire `removeFlatAccountStateTrieNode` to write a deletion sentinel.
- `.../archiveindex/ArchiveProofNodeLoader.java` — call the new `nodeAtExactChangeBlock(key, b*)`.
- `.../archiveindex/TrieNodeDiffCodec.java` + `TrieNodeHistoryComposition.java` — **delete** (+ their tests).
- `.../archiveindex/TrieNodeChangeIndex.java` — remove the now-unused FULL/DIFF-support methods (`appendAndGetPreviousCount`, `getChangeBlocksUpTo`, `countMutationsUpTo`, `countMutationsInEarlierRanges`) + their tests.
- Tests: `TrieNodeHistoryReaderTest`, `ArchiveProofNodeLoaderTest`, `TrieNodeChangeIndexTest`, `BonsaiFlatDbToArchiveMigratorTest` (integration guardrail).

---

### Task 1: Flip the history value format to raw node RLP (writer + reader + loader)

This is the cohesive core change: the reader and writer are two sides of one on-disk format and must flip together to keep the system consistent.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryReader.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/ArchiveProofNodeLoader.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryStore.java` (Javadoc only)
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryReaderTest.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/ArchiveProofNodeLoaderTest.java`

**Interfaces:**
- Produces on `TrieNodeHistoryReader`:
  - `Optional<Bytes> nodeAt(Bytes naturalKey, long targetBlock)` — unchanged signature; new body: `index.latestChangeBlock` → `nodeAtExactChangeBlock`.
  - `Optional<Bytes> nodeAtExactChangeBlock(Bytes naturalKey, long bStar)` — package-private; point-get at `bStar`; returns empty if the store has no entry (index/store mismatch, logged) **or** the stored value is zero-length (deletion sentinel); otherwise the raw node RLP.
  - **Removed:** the `nodeAt(Bytes, long, RangeRelativeOffsetList, long)` overload, `reconstructFromChangeBlocks`, `backwardWalkFallback`, and the `CHECKPOINT_INTERVAL` / `MAX_BACKWARD_WALK_STEPS` / `RECONSTRUCT_WINDOW` constants.
- Produces on `BonsaiArchiveTrieNodeStrategy`:
  - `void captureTrieNode(SegmentedKeyValueStorageTransaction tx, Bytes naturalKey, long block, Bytes newNode)` — package-private; `historyStore.put(tx, naturalKey, block, newNode)` then `changeIndex.append(tx, naturalKey, block)`. Replaces `captureTrieNodeDiff`.
- Consumes: `TrieNodeChangeIndex.latestChangeBlock`, `.append`, `.readRangeList`, `RangeRelativeOffsetList.latestLeq`; `TrieNodeHistoryStore.put`/`.get`.

- [ ] **Step 1: Write the failing reader tests for raw-value semantics**

Replace the body of `TrieNodeHistoryReaderTest` (keep the license header, package, imports, `KEY`/`OTHER_KEY` constants, `setUp`, and the `branchWith`/`putEntry`/`appendIndex` helpers shown at lines 44-101). Delete every existing `@Test` (they seed `TrieNodeDiffCodec` entries and assert diff reconstruction, which no longer exists) and add these:

```java
  @Test
  void returnsRawNodeAtLatestChangeAtOrBeforeTarget() {
    final Bytes v100 = branchWith(3, 100);
    final Bytes v102 = branchWith(7, 102);
    putEntry(KEY, 100, v100);
    appendIndex(KEY, 100);
    putEntry(KEY, 102, v102);
    appendIndex(KEY, 102);

    assertThat(reader.nodeAt(KEY, 103)).contains(v102);
    assertThat(reader.nodeAt(KEY, 102)).contains(v102);
    assertThat(reader.nodeAt(KEY, 101)).contains(v100);
    assertThat(reader.nodeAt(KEY, 100)).contains(v100);
  }

  @Test
  void returnsEmptyWhenNoChangeAtOrBeforeTarget() {
    putEntry(KEY, 100, branchWith(3, 100));
    appendIndex(KEY, 100);

    assertThat(reader.nodeAt(KEY, 99)).isEmpty();
  }

  @Test
  void zeroLengthValueIsADeletionSentinel() {
    putEntry(KEY, 100, branchWith(3, 100));
    appendIndex(KEY, 100);
    putEntry(KEY, 105, Bytes.EMPTY); // deletion at block 105
    appendIndex(KEY, 105);

    assertThat(reader.nodeAt(KEY, 104)).contains(branchWith(3, 100));
    assertThat(reader.nodeAt(KEY, 105)).isEmpty(); // deleted
    assertThat(reader.nodeAt(KEY, 999)).isEmpty(); // still deleted
  }

  @Test
  void indexReferencesBlockWithNoStoreEntryReturnsEmpty() {
    // Index says the node changed at 100, but the store has no entry (corruption).
    appendIndex(KEY, 100);
    assertThat(reader.nodeAt(KEY, 100)).isEmpty();
  }

  @Test
  void keysAreIsolated() {
    putEntry(KEY, 100, branchWith(3, 100));
    appendIndex(KEY, 100);
    assertThat(reader.nodeAt(OTHER_KEY, 100)).isEmpty();
  }

  @Test
  void nodeAtExactChangeBlockReturnsThatVersion() {
    final Bytes v100 = branchWith(3, 100);
    final Bytes v102 = branchWith(7, 102);
    putEntry(KEY, 100, v100);
    putEntry(KEY, 102, v102);

    assertThat(reader.nodeAtExactChangeBlock(KEY, 100)).contains(v100);
    assertThat(reader.nodeAtExactChangeBlock(KEY, 102)).contains(v102);
    assertThat(reader.nodeAtExactChangeBlock(KEY, 101)).isEmpty(); // no entry at 101
  }
```

- [ ] **Step 2: Run the reader tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "TrieNodeHistoryReaderTest"`
Expected: FAIL — `nodeAtExactChangeBlock` does not exist / old reconstruction behavior differs.

- [ ] **Step 3: Rewrite `TrieNodeHistoryReader` to raw-value point reads**

Replace the entire class body below the license header + package with:

```java
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the historical value of a trie node at a given block. Values are stored as raw node RLP
 * (Approach A): a read finds the latest change block ≤ target via {@link TrieNodeChangeIndex} and
 * point-reads it from {@link TrieNodeHistoryStore}. A zero-length stored value is a deletion
 * sentinel. There is no diff reconstruction: the stored value IS the node.
 */
public final class TrieNodeHistoryReader {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryReader.class);

  private final TrieNodeHistoryStore store;
  private final TrieNodeChangeIndex index;

  public TrieNodeHistoryReader(final TrieNodeHistoryStore store, final TrieNodeChangeIndex index) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.index = Objects.requireNonNull(index, "index must not be null");
  }

  /**
   * Returns the raw trie-node RLP for {@code naturalKey} at {@code targetBlock}, or empty if the
   * node did not exist at that block (never written, or deleted at/before it and not re-created).
   */
  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }
    final Optional<Long> latestOpt = index.latestChangeBlock(naturalKey, targetBlock);
    if (latestOpt.isEmpty()) {
      return Optional.empty();
    }
    return nodeAtExactChangeBlock(naturalKey, latestOpt.get());
  }

  /**
   * Point-reads the node value stored at the exact change block {@code bStar}. Returns empty if the
   * store has no entry there (index/store mismatch — logged) or the stored value is the zero-length
   * deletion sentinel; otherwise the raw node RLP.
   */
  Optional<Bytes> nodeAtExactChangeBlock(final Bytes naturalKey, final long bStar) {
    final Optional<Bytes> stored = store.get(naturalKey, bStar);
    if (stored.isEmpty()) {
      LOG.warn(
          "TrieNodeHistoryReader: index references block {} for key {} but store has no entry;"
              + " index/store mismatch — returning empty",
          bStar,
          naturalKey);
      return Optional.empty();
    }
    final Bytes value = stored.get();
    if (value.isEmpty()) {
      return Optional.empty(); // deletion sentinel
    }
    return Optional.of(value);
  }
}
```

- [ ] **Step 4: Update `ArchiveProofNodeLoader` to call the exact-block reader**

In `resolveNodeAt` (line 186), replace:

```java
        final Optional<Bytes> nodeOpt = historyReader.nodeAt(naturalKey, bStar, list, rangeId);
```

with:

```java
        final Optional<Bytes> nodeOpt = historyReader.nodeAtExactChangeBlock(naturalKey, bStar);
```

Leave the hash-first fast path, `readRangeList`/`latestLeq` logic, `resolveFromHistory`, and `verifyAndReturn` unchanged. Remove the now-unused `rangeId`/`list` references only if the compiler flags them (they are still used to compute `bStar`, so they stay).

- [ ] **Step 5: Rewrite the write path in `BonsaiArchiveTrieNodeStrategy`**

Delete the `CHECKPOINT_INTERVAL` (lines 51-57) and `FULL_ABOVE_DEPTH` (lines 59-67) constants and the `TrieNodeDiffCodec` import (line 25).

In `putFlatAccountTrieNode` (lines 171-196) remove the prior-node read (lines 178-182) and change the capture call:

```java
  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);

    if (trieLoader != null) {
      trieLoader.putAccountNode(nodeHash, node);
    }

    if (trieNodeIndexEnabled) {
      final long block = getCurrentBlockNumber(storage);
      final Bytes naturalKey = ArchiveNodeKey.account(location);
      captureTrieNode(transaction, naturalKey, block, node);
    }
  }
```

Apply the same shape to `putFlatStorageTrieNode` (lines 198-227): remove the prior-node read (lines 209-212), keep `accountHashLocation`/`naturalKey` derivation, and call `captureTrieNode(transaction, naturalKey, block, node)`.

Replace `captureTrieNodeDiff` (lines 279-352) with:

```java
  /**
   * Records the whole node RLP for {@code (naturalKey, block)} and appends the change to the index.
   * Values are stored verbatim (raw node RLP, identical in form to a live Bonsai trie node); a
   * zero-length {@code newNode} is the deletion sentinel. No prior-node read and no FULL/DIFF
   * decision are needed.
   */
  void captureTrieNode(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long block,
      final Bytes newNode) {
    historyStore.put(tx, naturalKey, block, newNode);
    changeIndex.append(tx, naturalKey, block);
  }
```

- [ ] **Step 6: Update `TrieNodeHistoryStore` Javadoc (no code change)**

In the class and `put`/`get` Javadoc (lines 28-52, 72-98, 122-136), replace references to "diff-codec entry / FULL / DIFF / tombstone" with: "the value is the raw trie-node RLP, or a zero-length value denoting deletion." Rename the `put` parameter `entry` to `nodeRlp` for clarity (update the parameter name and its `@param`).

- [ ] **Step 7: Update `ArchiveProofNodeLoaderTest` seeding to raw RLP**

Open `ArchiveProofNodeLoaderTest.java`. Wherever a test seeds the history store with `TrieNodeDiffCodec.encodeFull(x)` / `encodeDiff(...)`, replace the seeded value with the raw node RLP `x` itself (the loader now returns the stored bytes directly). Delete any test asserting DIFF-chain reconstruction across multiple blocks; keep the hash-first fast-path test, the index-path test (assert the exact seeded node is returned and hash-verified), the fail-closed mismatch test, and the absent-node test. Where a test needs a node whose keccak matches an `expectedHash`, compute `expectedHash = keccak256(rawNode)` from the seeded raw node.

- [ ] **Step 8: Run the affected suites**

Run: `./gradlew :ethereum:core:spotlessApply && ./gradlew :ethereum:core:test --tests "TrieNodeHistoryReaderTest" --tests "ArchiveProofNodeLoaderTest"`
Expected: PASS.

- [ ] **Step 9: Run the migrator integration guardrail**

Run: `./gradlew :ethereum:core:test --tests "BonsaiFlatDbToArchiveMigratorTest"`
Expected: PASS. If a test asserts a specific stored codec byte (e.g. checks `entry.get(0)` metadata bits), update it to assert the stored value equals the raw node RLP that was written. The end-to-end assertions (reader round-trips reconstructing a node whose keccak matches a historical state root) must still hold.

- [ ] **Step 10: Commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryReader.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/ArchiveProofNodeLoader.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryStore.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryReaderTest.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/ArchiveProofNodeLoaderTest.java
git commit -m "feat(bonsai-archive): store raw node RLP per version, index-authoritative reads"
```

---

### Task 2: Delete `TrieNodeDiffCodec` and `TrieNodeHistoryComposition`

After Task 1 nothing in `main` references the codec except `TrieNodeHistoryComposition`, which is itself unreferenced dead code.

**Files:**
- Delete: `.../archiveindex/TrieNodeDiffCodec.java`
- Delete: `.../archiveindex/TrieNodeHistoryComposition.java`
- Delete: `.../test/.../archiveindex/TrieNodeDiffCodecTest.java`
- Delete: `.../test/.../archiveindex/TrieNodeHistoryCompositionTest.java`

- [ ] **Step 1: Confirm no remaining references**

Run: `grep -rn "TrieNodeDiffCodec\|TrieNodeHistoryComposition" ethereum/core/src/main`
Expected: no output. If any remain (e.g. a stray import in `TrieNodeHistoryStore`), remove it first.

- [ ] **Step 2: Delete the four files**

```bash
git rm ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeDiffCodec.java \
       ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryComposition.java \
       ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeDiffCodecTest.java \
       ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryCompositionTest.java
```

- [ ] **Step 3: Compile the module**

Run: `./gradlew :ethereum:core:compileJava :ethereum:core:compileTestJava`
Expected: BUILD SUCCESSFUL. Any compile error means a missed reference — fix it, then re-run.

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor(bonsai-archive): delete diff codec and unused history composition"
```

---

### Task 3: Record node deletions as zero-length values

Wire `removeFlatAccountStateTrieNode` (the only node-removal hook on `TrieNodeStrategy`) to write the deletion sentinel so a direct `nodeAt` query for a removed location returns absent. (Storage-trie nodes have no removal hook; proof traversal only requests live nodes, so this is the belt-and-suspenders case.)

**Files:**
- Modify: `.../flat/BonsaiArchiveTrieNodeStrategy.java` (`removeFlatAccountStateTrieNode`, lines 229-235)
- Test: `.../test/.../archiveindex/` — add a focused test in `TrieNodeHistoryReaderTest` that already exists for the read side; add the write-side assertion in a new small test class or extend an existing strategy-level test if present. If no strategy unit test exists, add the test to `BonsaiFlatDbToArchiveMigratorTest`-adjacent coverage as noted below.

**Interfaces:**
- Consumes: `captureTrieNode(tx, naturalKey, block, Bytes.EMPTY)` from Task 1.

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeHistoryReaderTest` (it already has the store+index+reader wired, so it exercises the read consequence of a written sentinel):

```java
  @Test
  void deletionSentinelRoundTripsThroughStore() {
    putEntry(KEY, 100, branchWith(3, 100));
    appendIndex(KEY, 100);
    // Simulate what removeFlatAccountStateTrieNode writes: an empty value + index append.
    putEntry(KEY, 110, Bytes.EMPTY);
    appendIndex(KEY, 110);

    assertThat(reader.nodeAt(KEY, 109)).contains(branchWith(3, 100));
    assertThat(reader.nodeAt(KEY, 110)).isEmpty();
  }
```

(The read-side sentinel handling is already covered by Task 1's `zeroLengthValueIsADeletionSentinel`; this test documents the exact bytes the write path must emit.)

- [ ] **Step 2: Run it**

Run: `./gradlew :ethereum:core:test --tests "TrieNodeHistoryReaderTest.deletionSentinelRoundTripsThroughStore"`
Expected: PASS (this is a read-side test; it should pass on Task 1 code and pins the contract for Step 3).

- [ ] **Step 3: Wire the removal hook**

Replace `removeFlatAccountStateTrieNode` (lines 229-235) with:

```java
  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);

    if (trieNodeIndexEnabled) {
      final long block = getCurrentBlockNumber(storage);
      final Bytes naturalKey = ArchiveNodeKey.account(location);
      captureTrieNode(transaction, naturalKey, block, Bytes.EMPTY);
    }
  }
```

- [ ] **Step 4: Run the strategy/migrator suites**

Run: `./gradlew :ethereum:core:spotlessApply && ./gradlew :ethereum:core:test --tests "BonsaiFlatDbToArchiveMigratorTest" --tests "TrieNodeHistoryReaderTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeHistoryReaderTest.java
git commit -m "feat(bonsai-archive): record account trie-node deletions as empty-value entries"
```

---

### Task 4: Remove the now-unused FULL/DIFF-support index methods

`appendAndGetPreviousCount`, `getChangeBlocksUpTo`, `countMutationsUpTo`, and `countMutationsInEarlierRanges` existed only to drive the FULL/DIFF decision and diff reconstruction, both now gone. Remove them and their tests. Keep `append`, `latestChangeBlock`, `readRangeList`, `modifiedAfter`, and the buffered-flush methods — they are still used.

**Files:**
- Modify: `.../archiveindex/TrieNodeChangeIndex.java`
- Test: `.../test/.../archiveindex/TrieNodeChangeIndexTest.java`

- [ ] **Step 1: Confirm the four methods are unreferenced outside the index and its test**

Run: `grep -rn "appendAndGetPreviousCount\|getChangeBlocksUpTo\|countMutationsUpTo\|countMutationsInEarlierRanges" ethereum/core/src/main`
Expected: only `TrieNodeChangeIndex.java` itself appears. (Task 1 removed the `BonsaiArchiveTrieNodeStrategy` and `TrieNodeHistoryReader` references.)

- [ ] **Step 2: Delete the four public methods**

In `TrieNodeChangeIndex.java` remove the method bodies for `appendAndGetPreviousCount` (from line 502), `countMutationsUpTo` (from line 644), `getChangeBlocksUpTo` (from line 858), and `countMutationsInEarlierRanges` (from line 909). If a private helper (e.g. `earlierRangeCount` / `earlierRangeCountCache`) is now referenced only by the deleted methods, remove it too; if it is also used by `append`/`latestChangeBlock`, leave it. Let the compiler in Step 4 be the arbiter — do not remove anything still referenced.

- [ ] **Step 3: Delete their tests**

In `TrieNodeChangeIndexTest.java` delete every `@Test` whose body calls one of the four removed methods. Leave tests for `append`, `latestChangeBlock`, `readRangeList`, `modifiedAfter`, and the buffered flush.

- [ ] **Step 4: Compile, then run the index suite**

Run: `./gradlew :ethereum:core:compileJava :ethereum:core:compileTestJava`
Expected: BUILD SUCCESSFUL (fix any "still referenced" errors by restoring only the referenced helper).
Run: `./gradlew :ethereum:core:test --tests "TrieNodeChangeIndexTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "refactor(bonsai-archive): drop FULL/DIFF-support index methods"
```

---

### Task 5: Full build and archive-index regression sweep

**Files:** none (verification only).

- [ ] **Step 1: Full module build without tests**

Run: `./gradlew :ethereum:core:build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 2: Run the full archive-index + migrator test set**

Run: `./gradlew :ethereum:core:test --tests "TrieNodeChangeIndexTest" --tests "TrieNodeHistoryReaderTest" --tests "TrieNodeHistoryStoreTest" --tests "ArchiveProofNodeLoaderTest" --tests "ArchiveNodeKeyTest" --tests "RangeRelativeOffsetListTest" --tests "TrieNodeIndexDropperTest" --tests "TrieNodeIndexProgressTest" --tests "BonsaiFlatDbToArchiveMigratorTest"`
Expected: all green.

- [ ] **Step 3: Grep for dangling references to removed types/methods**

Run: `grep -rn "TrieNodeDiffCodec\|TrieNodeHistoryComposition\|RECONSTRUCT_WINDOW\|appendAndGetPreviousCount\|getChangeBlocksUpTo" ethereum/core/src`
Expected: no output.

- [ ] **Step 4: Commit (if spotless produced changes)**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A && git commit -m "chore(bonsai-archive): spotless after raw-value archive storage refactor" || echo "nothing to commit"
```

---

## Validation on a node (after merge)

1. Deploy this branch to a fresh node and re-migrate the archive from block 0 (the history CF value format changed).
2. Serve `eth_getProof` at several historical blocks and confirm results match a reference archive node.
3. Compare against the pre-change profiling (memory: `bonsai-archive-migration-bottleneck.md`):
   - Migration RocksDB `get` calls: expect the ~34% prior-node reads to disappear entirely (no prior-node read on the write path) and the index RMW to be near-zero under the batched-buffer plan.
   - Proof read latency: expect the `RECONSTRUCT_WINDOW` multiGet and any backward walk to be gone — each proof-path node is one index read (LRU-cached) + one point-get.

## Self-review notes (coverage vs spec)

- Spec "whole node RLP at `naturalKey ‖ block`, no codec" → Task 1 (store raw, delete codec in Task 2).
- Spec "index simplified to append + latestLeq" → Task 1 (reader uses only `latestChangeBlock`) + Task 4 (remove FULL/DIFF-support methods).
- Spec "read = latestLeq → point-get, no rolling/seek/reconstruct" → Task 1 reader rewrite.
- Spec "no prior-node read on write" → Task 1 Step 5.
- Spec decision "deletion = empty value" → Task 1 read semantics + Task 3 write wiring.
- Spec decision "index authoritative, no seekForPrev fallback" → Task 1 reader returns empty (logged) on index/store mismatch; no seek path added.
- Spec open question "hash-first fast path scope for storage nodes" → left as-is (unchanged from current branch); flagged for node validation in Step 2 above.
