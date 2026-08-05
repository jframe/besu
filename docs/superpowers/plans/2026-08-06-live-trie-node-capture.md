# Live trie-node history capture during initial sync — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate the Bonsai archive trie-node history archive by capturing trie nodes live during initial block import (trailing the head by `maxLayersToLoad`), and delete the trailing `TrieNodeHistoryWalker`.

**Architecture:** Restore `BonsaiArchiveTrieNodeStrategy` to a *delegating* shape — reads/writes pass through to a base `BonsaiTrieNodeStrategy`, and writes additionally capture a FULL/DIFF history entry. Capture is gated so a block `N` is only recorded when `N == 0` (genesis) or `N <= bestChainHeight - maxLayersToLoad`, giving migrator-parity coverage `[0, head - maxLayers]`. The strategy is installed on the real world-state storage before genesis is written; its network-head threshold is wired from `syncState` once `syncState` exists.

**Tech Stack:** Java 21, Gradle, JUnit 5 (Jupiter), AssertJ, Mockito. Bonsai path-based world state in `ethereum/core`.

## Global Constraints

- License header: every new `.java` file starts with the standard Apache-2.0 "Copyright contributors to Hyperledger Besu." header block (copy verbatim from any sibling file in the same package).
- Formatting/import order is enforced by Spotless. Run `./gradlew :ethereum:core:spotlessApply :app:spotlessApply` before each commit.
- Feature gate: all behaviour is off unless `--data-storage-format=X_BONSAI_ARCHIVE` **and** `--Xbonsai-trie-node-history-enabled` (`getPathBasedExtraStorageConfiguration().getUnstable().getTrieNodeHistoryEnabled()`). Do not change the flag's name or default.
- Never change the on-disk key format of `TRIE_NODE_HISTORY_ARCHIVE` (owned by `TrieNodeHistoryStore` / `ArchiveNodeKey`) — this plan reuses it unchanged.
- Block-import hot path: `TrieNodeHistoryProgress` stays `volatile`-based, no locks. Persist the 16-byte progress record at most once per block, not once per node.
- Commit after each task with a message body ending:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

---

## File Structure

- `ethereum/core/.../bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java` — **rewrite** to the delegating + gated shape (Task 2).
- `ethereum/core/.../bonsai/archive/TrieNodeHistoryWalker.java` — **delete** (Task 1).
- `ethereum/core/.../bonsai/archive/TrieNodeHistoryWalkerWorldState.java` — **delete** (Task 1).
- `ethereum/core/.../bonsai/archive/HistoryOnlyWriteStorage.java` — **delete** (Task 1).
- `app/.../controller/BesuControllerBuilder.java` — **modify**: remove the walker wiring block (Task 1); install the strategy in `createWorldStateArchive` and wire the threshold supplier in `build()` (Task 3).
- Tests: delete three walker tests (Task 1); rewrite `BonsaiArchiveTrieNodeStrategyTest` (Task 2); adapt `BonsaiArchiveWorldStateProviderTrieHistoryTest` (Task 2); add a genesis end-to-end test (Task 4).

Unchanged (do not touch): `TrieNodeHistoryStore`, `TrieNodeHistoryReader`, `TrieNodeHistoryProgress`, `ArchiveTrieNodeCodec`, `ArchiveNodeKey`, `ArchiveProofNodeLoader`, `BonsaiArchiveWorldStateProvider`, `BonsaiArchiveFlatDbStrategy`, `BonsaiFlatDbToArchiveMigrator`.

---

## Task 1: Remove the trailing walker

Removes the slow walker and its wiring first, so the tree stays green while later tasks introduce live capture. After this task the archive is simply not populated (read path falls back), which existing tests already tolerate.

**Files:**
- Delete: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalker.java`
- Delete: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerWorldState.java`
- Delete: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/HistoryOnlyWriteStorage.java`
- Delete: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerTest.java`
- Delete: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerIntegrationTest.java`
- Delete: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/HistoryOnlyWriteStorageTest.java`
- Modify: `app/src/main/java/org/hyperledger/besu/controller/BesuControllerBuilder.java` (remove the walker block, currently the `if (X_BONSAI_ARCHIVE && getTrieNodeHistoryEnabled())` block spanning the `final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage = ...` through `closeables.addFirst(walker);` at approx. lines 991-1024, and the now-unused walker imports at approx. lines 91-95 and 1001/1007/1009 helper imports if unreferenced elsewhere).

**Interfaces:**
- Consumes: nothing.
- Produces: a tree with no trie-node history writer. `trieNodeHistoryStore` / `trieNodeHistoryReader` / `trieNodeHistoryProgress` fields in `BesuControllerBuilder` remain (still created in `createWorldStateArchive`, still passed to `BonsaiArchiveWorldStateProvider`).

- [ ] **Step 1: Delete the three main-source walker classes and their three tests**

```bash
cd /Users/jframe/code/besu/.claude/worktrees/bonsai-archive-proofs-trie-diff
git rm \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalker.java \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerWorldState.java \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/HistoryOnlyWriteStorage.java \
  ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerTest.java \
  ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryWalkerIntegrationTest.java \
  ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/HistoryOnlyWriteStorageTest.java
```

- [ ] **Step 2: Remove the walker wiring block in `BesuControllerBuilder.build()`**

Delete the entire block that begins with the archive+history guard and constructs/starts the walker. It currently reads (verify exact bounds by searching for `Starting trie-node history walker`):

```java
    if (DataStorageFormat.X_BONSAI_ARCHIVE.equals(dataStorageConfiguration.getDataStorageFormat())
        && dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getTrieNodeHistoryEnabled()) {
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
          worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);
      final SegmentedKeyValueStorage composedWorldStateStorage =
          worldStateKeyValueStorage.getComposedWorldStateStorage();
      // trieNodeHistoryStore/Reader/Progress already initialised by createWorldStateArchive().
      final TrieNodeHistoryWalkerWorldState walkerWorldState =
          new TrieNodeHistoryWalkerWorldState(
              worldStateKeyValueStorage.getFlatDbStrategyProvider(),
              composedWorldStateStorage,
              trieNodeHistoryReader,
              trieNodeHistoryStore);
      final TrieLogManager walkerTrieLogManager =
          ((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager();
      final ScheduledExecutorService walkerExecutor =
          MonitoredExecutors.newScheduledThreadPool("trie-node-history-walker", 1, metricsSystem);
      final TrieNodeHistoryWalker walker =
          new TrieNodeHistoryWalker(
              walkerWorldState,
              walkerTrieLogManager,
              blockchain,
              trieNodeHistoryProgress,
              composedWorldStateStorage,
              walkerExecutor,
              genesisState);
      LOG.info("Starting trie-node history walker");
      walker.start();
      // Close the walker before storageProvider so the catch-up task finishes before RocksDB closes
      closeables.addFirst(walker);
    }
```

Remove the whole `if (...) { ... }`. Then remove the three walker-class imports (`TrieNodeHistoryWalker`, `TrieNodeHistoryWalkerWorldState`, and — only if no longer referenced anywhere in the file — `HistoryOnlyWriteStorage`). Leave `TrieNodeHistoryStore`/`TrieNodeHistoryReader`/`TrieNodeHistoryProgress` imports (still used).

- [ ] **Step 3: Verify compilation and that no dangling references remain**

Run: `./gradlew :ethereum:core:compileJava :app:compileJava -q`
Expected: BUILD SUCCESSFUL. Then:
Run: `grep -rn "TrieNodeHistoryWalker\|HistoryOnlyWriteStorage" --include="*.java" .`
Expected: no matches.

- [ ] **Step 4: Run the affected module tests**

Run: `./gradlew :ethereum:core:test --tests "*archive*" -q`
Expected: PASS (walker tests gone; remaining archive tests unaffected).

- [ ] **Step 5: Commit**

```bash
./gradlew :ethereum:core:spotlessApply :app:spotlessApply -q
git add -A
git commit -m "refactor(bonsai-archive): remove trailing trie-node history walker

Delete TrieNodeHistoryWalker, TrieNodeHistoryWalkerWorldState,
HistoryOnlyWriteStorage and their tests, and the walker wiring in
BesuControllerBuilder. Live capture replaces it in a following change.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Rewrite `BonsaiArchiveTrieNodeStrategy` to the delegating + gated shape

**Files:**
- Modify (full rewrite): `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java`
- Test (rewrite): `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyTest.java`
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/BonsaiArchiveWorldStateProviderTrieHistoryTest.java` (two construction sites)

**Interfaces:**
- Consumes: `TrieNodeStrategy` (base, `BonsaiTrieNodeStrategy`), `TrieNodeHistoryStore`, `TrieNodeHistoryProgress`, `TrieNodeHistoryReader.CHECKPOINT_INTERVAL` (static), `ArchiveTrieNodeCodec`, `ArchiveNodeKey`, `PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY`.
- Produces: `class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy` with:
  - constructor `BonsaiArchiveTrieNodeStrategy(TrieNodeStrategy baseStrategy, TrieNodeHistoryStore historyStore, TrieNodeHistoryProgress historyProgress, java.util.function.LongSupplier highestSafeBlockSupplier)`
  - `void setHighestSafeBlockSupplier(java.util.function.LongSupplier supplier)`
  - reads delegate to `baseStrategy`; writes delegate to `baseStrategy` and, when `shouldCapture(block)`, capture history and advance progress.
  - `shouldCapture(long block) == (block == 0L || block <= highestSafeBlockSupplier.getAsLong())`.

- [ ] **Step 1: Write the failing unit tests**

Replace the entire contents of `BonsaiArchiveTrieNodeStrategyTest.java` with:

```java
/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;
  private TrieNodeHistoryProgress progress;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
    progress = new TrieNodeHistoryProgress();
  }

  /** Distinct valid 2-item short-node RLP so ArchiveTrieNodeCodec's arity check accepts it. */
  private static Bytes shortNodeRlp(final int i) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.of(0x01));
    out.writeBytes(Bytes.of(i));
    out.endList();
    return out.encoded();
  }

  /** Set the committed world block number so the strategy derives currentBlock = n + 1. */
  private void setWorldBlockNumber(final long n) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(n).toArrayUnsafe());
    tx.commit();
  }

  private BonsaiArchiveTrieNodeStrategy strategy(final LongSupplier highestSafeBlock) {
    return new BonsaiArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), historyStore, progress, highestSafeBlock);
  }

  private void putAccount(
      final BonsaiArchiveTrieNodeStrategy strategy, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    tx.commit();
  }

  @Test
  void readDelegatesToLiveBaseValueNotHistory() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = Bytes.fromHexString("0xaa");
    // Write straight into the live segment via the base strategy.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    new BonsaiTrieNodeStrategy().putFlatAccountTrieNode(storage, tx, location, null, node);
    tx.commit();

    assertThat(strategy(() -> Long.MAX_VALUE).getFlatAccountTrieNode(location, null, storage))
        .contains(node);
  }

  @Test
  void creationWritesFullEntryWithCounterZeroAtGenesis() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(0);
    // No WORLD_BLOCK_NUMBER_KEY => block 0.
    putAccount(strategy(() -> Long.MAX_VALUE), location, node);

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.get(ArchiveNodeKey.account(location), 0L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.codecEntry().isCreation()).isTrue();
    assertThat(entry.counter()).isEqualTo(0);
  }

  @Test
  void diffBaseComesFromLiveValueAndChecksInFullEveryCheckpointInterval() {
    final Bytes location = Bytes.fromHexString("0x030405"); // depth 3, non-root
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    // Block 0 creation (FULL).
    putAccount(strategy, location, shortNodeRlp(0));
    // Blocks 1..CHECKPOINT_INTERVAL: each reads prior live value as diff base.
    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      setWorldBlockNumber(i - 1L);
      putAccount(strategy, location, shortNodeRlp(i));
    }
    assertThat(
            historyStore
                .get(ArchiveNodeKey.account(location), 1L)
                .orElseThrow()
                .codecEntry()
                .isFull())
        .isFalse();
    assertThat(
            historyStore
                .get(ArchiveNodeKey.account(location), (long) TrieNodeHistoryReader.CHECKPOINT_INTERVAL)
                .orElseThrow()
                .codecEntry()
                .isFull())
        .isTrue();
    // Reconstruction at every block matches the node written at that block.
    for (int i = 0; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      assertThat(reader.nodeAt(ArchiveNodeKey.account(location), i)).contains(shortNodeRlp(i));
    }
  }

  @Test
  void gateSkipsCaptureButStillWritesLiveNodeWhenBlockAboveThreshold() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(7);
    setWorldBlockNumber(9L); // currentBlock = 10
    putAccount(strategy(() -> 5L), location, node); // 10 > 5 => gated out

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 10L)).isEmpty();
    // But the live node was still written (block import must not be blocked).
    assertThat(new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, null, storage))
        .contains(node);
  }

  @Test
  void genesisCapturedEvenWhenThresholdGateIsClosed() {
    final Bytes location = Bytes.fromHexString("0x0102");
    // No WORLD_BLOCK_NUMBER_KEY => block 0; supplier far below 0.
    putAccount(strategy(() -> Long.MIN_VALUE), location, shortNodeRlp(0));

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isPresent();
  }

  @Test
  void removeCapturesTombstoneSoNodeAtReturnsEmpty() {
    final Bytes location = Bytes.fromHexString("0x0708");
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    putAccount(strategy, location, shortNodeRlp(1)); // block 0 creation

    setWorldBlockNumber(0L); // currentBlock = 1
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.commit();

    assertThat(reader.nodeAt(ArchiveNodeKey.account(location), 1L)).isEmpty();
    assertThat(new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, null, storage))
        .isEmpty();
  }

  @Test
  void progressAdvancesToCapturedBlockOncePerBlock() {
    final Bytes location = Bytes.fromHexString("0x0102");
    setWorldBlockNumber(2L); // currentBlock = 3
    putAccount(strategy(() -> Long.MAX_VALUE), location, shortNodeRlp(1));

    assertThat(progress.lastIndexedBlock()).isEqualTo(3L);
    assertThat(progress.indexStartBlock()).isLessThanOrEqualTo(3L);
  }
}
```

- [ ] **Step 2: Run tests to verify they fail to compile / fail**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" -q`
Expected: FAIL — the 4-arg constructor and `setHighestSafeBlockSupplier` don't exist yet (compilation error).

- [ ] **Step 3: Rewrite `BonsaiArchiveTrieNodeStrategy.java`**

Replace the entire file with:

```java
/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Archive-aware trie node strategy for the live block-import path. Reads and writes delegate to a
 * base {@link TrieNodeStrategy} (the live flat DB); writes additionally capture a FULL/DIFF history
 * entry and advance {@link TrieNodeHistoryProgress}.
 *
 * <p>Capture is gated so a block {@code N} is only recorded when {@code N == 0} (genesis, always
 * final) or {@code N <= highestSafeBlock}, where {@code highestSafeBlock = bestChainHeight -
 * maxLayersToLoad}. This trails the head by {@code maxLayersToLoad}, matching {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiFlatDbToArchiveMigrator}, and
 * never records a reorg-window block. The gate never suppresses the delegated live write — block
 * import must always proceed.
 *
 * <p>The diff base is the value read from the base strategy <em>before</em> the put. During
 * sequential import the live flat DB still holds block {@code N-1}'s value at that moment, so the
 * live read is the correct previous-block diff base.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy baseStrategy;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeHistoryProgress historyProgress;
  private volatile LongSupplier highestSafeBlockSupplier;
  private volatile long lastSavedProgressBlock = Long.MIN_VALUE;

  public BonsaiArchiveTrieNodeStrategy(
      final TrieNodeStrategy baseStrategy,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeHistoryProgress historyProgress,
      final LongSupplier highestSafeBlockSupplier) {
    this.baseStrategy = Objects.requireNonNull(baseStrategy);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.highestSafeBlockSupplier = Objects.requireNonNull(highestSafeBlockSupplier);
  }

  /**
   * Replaces the "highest safe block to capture" supplier. Used during startup wiring once {@code
   * syncState} exists; before that a placeholder keeps the gate closed for all blocks except
   * genesis.
   */
  public void setHighestSafeBlockSupplier(final LongSupplier supplier) {
    this.highestSafeBlockSupplier = Objects.requireNonNull(supplier);
  }

  private boolean shouldCapture(final long block) {
    return block == 0L || block <= highestSafeBlockSupplier.getAsLong();
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCapture(block);
    final Bytes priorNode =
        capture ? baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null) : null;
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (capture) {
      captureTrieNodeDiff(
          transaction, ArchiveNodeKey.account(location), location, block, priorNode, node);
      advanceHistoryProgress(transaction, block);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCapture(block);
    final Bytes priorNode =
        capture
            ? baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage).orElse(null)
            : null;
    baseStrategy.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (capture) {
      captureTrieNodeDiff(
          transaction,
          ArchiveNodeKey.storage(accountHash.getBytes(), location),
          location,
          block,
          priorNode,
          node);
      advanceHistoryProgress(transaction, block);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCapture(block);
    // nodeHash is unknown at removal time; BonsaiTrieNodeStrategy ignores it (plain point lookup).
    final Bytes priorNode =
        capture ? baseStrategy.getFlatAccountTrieNode(location, null, storage).orElse(null) : null;
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (capture && priorNode != null) {
      historyStore.put(
          transaction,
          ArchiveNodeKey.account(location),
          block,
          0,
          ArchiveTrieNodeCodec.encodeDiff(priorNode, null));
      advanceHistoryProgress(transaction, block);
    }
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    // Established pattern, mirrored from BonsaiArchiveFlatDbStrategy.getStateArchiveContextForWrite:
    // current committed WORLD_BLOCK_NUMBER_KEY + 1, or 0 if absent (genesis).
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private void captureTrieNodeDiff(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Bytes priorNode,
      final Bytes newNode) {
    if (priorNode == null) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeDiff(null, newNode));
      return;
    }
    final Optional<TrieNodeHistoryStore.HistoryEntry> priorEntryOpt =
        historyStore.getLatestBefore(naturalKey, block);
    if (priorEntryOpt.isEmpty() || priorEntryOpt.get().codecEntry().isDeletion()) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
      return;
    }
    if (location.isEmpty()) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
      return;
    }
    final int priorCounter = priorEntryOpt.get().counter();
    if (priorCounter + 1 >= TrieNodeHistoryReader.CHECKPOINT_INTERVAL) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
    } else {
      historyStore.put(
          tx, naturalKey, block, priorCounter + 1, ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode));
    }
  }

  private void advanceHistoryProgress(
      final SegmentedKeyValueStorageTransaction tx, final long block) {
    historyProgress.setLastIndexedBlock(block);
    historyProgress.setIndexStartBlock(block);
    // A block writes thousands of trie nodes; persist the (16-byte, idempotent) progress record
    // once per block rather than once per node.
    if (block != lastSavedProgressBlock) {
      historyProgress.save(tx);
      lastSavedProgressBlock = block;
    }
  }
}
```

- [ ] **Step 4: Fix the two construction sites in `BonsaiArchiveWorldStateProviderTrieHistoryTest`**

Both currently read `new BonsaiArchiveTrieNodeStrategy(historyReader, historyStore, 50L);`. In each case, immediately before the strategy is constructed, seed the world block number so the derived block is 50, and construct with the new signature. Replace each occurrence of:

```java
    final BonsaiArchiveTrieNodeStrategy archiveStrategy =
        new BonsaiArchiveTrieNodeStrategy(historyReader, historyStore, 50L);
```

with:

```java
    final SegmentedKeyValueStorageTransaction blockNumberTx = composed.startTransaction();
    blockNumberTx.put(
        org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
        org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage
            .WORLD_BLOCK_NUMBER_KEY,
        org.apache.tuweni.bytes.Bytes.ofUnsignedLong(49L).toArrayUnsafe());
    blockNumberTx.commit();
    final BonsaiArchiveTrieNodeStrategy archiveStrategy =
        new BonsaiArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(),
            historyStore,
            new TrieNodeHistoryProgress(),
            () -> Long.MAX_VALUE);
```

Note: pass a throwaway `new TrieNodeHistoryProgress()` to the strategy at both sites — the strategy only advances its own progress, and each test keeps constructing its separate provider-facing `progress` object (with `setLastIndexedBlock(50L)` / `setIndexStartBlock(50L)`) exactly as today, so `covers(50)` stays true. Add these imports if not present: `import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;` and `import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" --tests "BonsaiArchiveWorldStateProviderTrieHistoryTest" -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
./gradlew :ethereum:core:spotlessApply -q
git add -A
git commit -m "feat(bonsai-archive): delegating, reorg-gated live trie-node capture strategy

Reads/writes delegate to the base strategy; writes capture a FULL/DIFF
history entry gated to N==0 or N<=bestChainHeight-maxLayers. Block number
derived from WORLD_BLOCK_NUMBER_KEY+1; diff base taken from the live read.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Wire live capture into `BesuControllerBuilder`

Install the strategy before genesis is written, and set its real threshold supplier once `syncState` exists.

**Files:**
- Modify: `app/src/main/java/org/hyperledger/besu/controller/BesuControllerBuilder.java`

**Interfaces:**
- Consumes: `BonsaiArchiveTrieNodeStrategy(baseStrategy, historyStore, historyProgress, supplier)` and `setHighestSafeBlockSupplier` (Task 2); `syncState.bestChainHeight()`; `((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager().getMaxLayersToLoad()`; `worldStateKeyValueStorage.setTrieNodeStrategy(...)`.
- Produces: a populated trie-node history archive during initial sync.

- [ ] **Step 1: Add a field to hold the installed strategy**

Next to the existing fields (search for `private TrieNodeHistoryProgress trieNodeHistoryProgress;`, approx. line 253), add:

```java
  private org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy
      trieNodeHistoryWriteStrategy;
```

- [ ] **Step 2: Install the strategy in `createWorldStateArchive` (before genesis write)**

In `createWorldStateArchive`, inside the `case X_BONSAI_ARCHIVE ->` branch, within the existing `if (... getTrieNodeHistoryEnabled())` block (after `trieNodeHistoryProgress = TrieNodeHistoryProgress.load(composedWorldStateStorage);`), add:

```java
          trieNodeHistoryWriteStrategy =
              new org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat
                  .BonsaiArchiveTrieNodeStrategy(
                  new org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat
                      .BonsaiTrieNodeStrategy(),
                  trieNodeHistoryStore,
                  trieNodeHistoryProgress,
                  // Placeholder until build() wires syncState: gate closed for all blocks except
                  // genesis (N==0), which must be captured while it is written just below.
                  () -> Long.MIN_VALUE);
          worldStateKeyValueStorage.setTrieNodeStrategy(trieNodeHistoryWriteStrategy);
```

(Import cleanup: you may add proper imports for `BonsaiArchiveTrieNodeStrategy` and `BonsaiTrieNodeStrategy` instead of fully-qualified names, then run Spotless.)

- [ ] **Step 3: Wire the real threshold supplier in `build()`**

Where the walker block used to be (the archive+history guard removed in Task 1), add a small block after `syncState` and `worldStateArchive` are both in scope:

```java
    if (DataStorageFormat.X_BONSAI_ARCHIVE.equals(dataStorageConfiguration.getDataStorageFormat())
        && dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getTrieNodeHistoryEnabled()
        && trieNodeHistoryWriteStrategy != null) {
      final long maxLayers =
          ((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager().getMaxLayersToLoad();
      final SyncState effectiveSyncState = syncState;
      trieNodeHistoryWriteStrategy.setHighestSafeBlockSupplier(
          () -> effectiveSyncState.bestChainHeight() - maxLayers);
      LOG.info(
          "Live trie-node history capture enabled (trailing head by {} blocks)", maxLayers);
    }
```

- [ ] **Step 4: Verify compilation**

Run: `./gradlew :app:compileJava -q`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Run the controller-builder / archive tests**

Run: `./gradlew :app:test --tests "*ControllerBuilder*" -q && ./gradlew :ethereum:core:test --tests "*archive*" -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
./gradlew :app:spotlessApply -q
git add -A
git commit -m "feat(bonsai-archive): wire live trie-node capture into controller builder

Install the archive trie-node strategy before genesis is written (so
genesis is captured), and set the bestChainHeight-maxLayers threshold
supplier from syncState once it exists.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: End-to-end genesis capture integration test

Proves live capture from genesis produces history entries and proofs that match direct derivation, and that coverage trails the head by `maxLayers`.

**Files:**
- Create: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/LiveTrieNodeCaptureIntegrationTest.java`

**Interfaces:**
- Consumes: `BonsaiArchiveTrieNodeStrategy` (Task 2), `TrieNodeHistoryStore`, `TrieNodeHistoryReader`, `TrieNodeHistoryProgress`, `BonsaiTrieNodeStrategy`, `SegmentedInMemoryKeyValueStorage`, `StoredMerklePatriciaTrie`, `ArchiveNodeKey`.
- Produces: none (test only).

- [ ] **Step 1: Write the failing integration test**

Model it on the account-trie mechanics already used by `BonsaiArchiveWorldStateProviderTrieHistoryTest` (drive a `StoredMerklePatriciaTrie` and route its `commit(NodeUpdater)` through the strategy). Create:

```java
/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LiveTrieNodeCaptureIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;
  private TrieNodeHistoryProgress progress;
  private BonsaiArchiveTrieNodeStrategy strategy;

  // Capture everything (network head effectively infinite) for the account-trie scenario;
  // the trailing-window behaviour is asserted separately in trailsHeadByMaxLayers().
  private long highestSafeBlock = Long.MAX_VALUE;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
    progress = new TrieNodeHistoryProgress();
    strategy =
        new BonsaiArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), historyStore, progress, () -> highestSafeBlock);
  }

  private void setWorldBlockNumber(final long n) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(n).toArrayUnsafe());
    tx.commit();
  }

  /** Applies {@code address -> value} to a fresh trie built on the current live nodes at block. */
  private Bytes32 importAccountBlock(final Address address, final PmtStateTrieAccountValue value) {
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, hash, storage),
            b -> b,
            b -> b);
    trie.put(address.addressHash().getBytes(), RLP.encode(value::writeTo));
    trie.commit(
        (location, nodeHash, nodeValue) -> {
          final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
          strategy.putFlatAccountTrieNode(storage, tx, location, nodeHash, nodeValue);
          tx.commit();
        });
    return trie.getRootHash();
  }

  @Test
  void capturesGenesisAndSubsequentBlocksReconstructableViaReader() {
    final Address a = Address.fromHexString("0x1111111111111111111111111111111111111111");
    // Block 0 (genesis): no WORLD_BLOCK_NUMBER_KEY.
    importAccountBlock(a, new PmtStateTrieAccountValue(0L, Wei.of(1L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    // Block 1.
    setWorldBlockNumber(0L);
    importAccountBlock(a, new PmtStateTrieAccountValue(1L, Wei.of(2L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));

    // The account-trie root node (location EMPTY) has a history entry at both blocks 0 and 1.
    assertThat(historyStore.get(ArchiveNodeKey.account(Bytes.EMPTY), 0L)).isPresent();
    assertThat(historyStore.get(ArchiveNodeKey.account(Bytes.EMPTY), 1L)).isPresent();
    // Reconstruction of the root at block 0 differs from block 1 (state changed).
    final Optional<Bytes> root0 = reader.nodeAt(ArchiveNodeKey.account(Bytes.EMPTY), 0L);
    final Optional<Bytes> root1 = reader.nodeAt(ArchiveNodeKey.account(Bytes.EMPTY), 1L);
    assertThat(root0).isPresent();
    assertThat(root1).isPresent();
    assertThat(root0).isNotEqualTo(root1);
    // Progress covers [0, 1].
    assertThat(progress.covers(0L)).isTrue();
    assertThat(progress.covers(1L)).isTrue();
  }

  @Test
  void trailsHeadByMaxLayersAndAlwaysCapturesGenesis() {
    // Simulate a 1000-block network head with maxLayers = 512: safe block = 1000 - 512 = 488.
    highestSafeBlock = 488L;
    final Address a = Address.fromHexString("0x2222222222222222222222222222222222222222");

    // Genesis (block 0) is captured even though 0 <= 488 would also pass — assert it directly.
    importAccountBlock(a, new PmtStateTrieAccountValue(0L, Wei.of(1L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    assertThat(historyStore.get(ArchiveNodeKey.account(Bytes.EMPTY), 0L)).isPresent();

    // A block at 488 (currentBlock = 488 => WORLD_BLOCK_NUMBER_KEY = 487) is captured.
    setWorldBlockNumber(487L);
    importAccountBlock(a, new PmtStateTrieAccountValue(2L, Wei.of(3L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    assertThat(historyStore.get(ArchiveNodeKey.account(Bytes.EMPTY), 488L)).isPresent();

    // A block at 489 (in the reorg window) is NOT captured, but the live node IS written.
    setWorldBlockNumber(488L);
    final Bytes32 root489 =
        importAccountBlock(
            a, new PmtStateTrieAccountValue(3L, Wei.of(4L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    assertThat(historyStore.get(ArchiveNodeKey.account(Bytes.EMPTY), 489L)).isEmpty();
    assertThat(
            new BonsaiTrieNodeStrategy()
                .getFlatAccountTrieNode(Bytes.EMPTY, Bytes32.wrap(root489), storage))
        .isPresent();
    // Coverage stops at 488.
    assertThat(progress.lastIndexedBlock()).isEqualTo(488L);
  }
}
```

- [ ] **Step 2: Run to verify it fails first (red), if implemented before Task 2/3 land**

If Tasks 2–3 are already merged, this should pass immediately; otherwise:
Run: `./gradlew :ethereum:core:test --tests "LiveTrieNodeCaptureIntegrationTest" -q`
Expected: PASS (Task 2 provides the strategy; this test does not depend on Task 3 wiring).

- [ ] **Step 3: Run the full archive test suite**

Run: `./gradlew :ethereum:core:test --tests "*archive*" --tests "*ArchiveTrieNode*" -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
./gradlew :ethereum:core:spotlessApply -q
git add -A
git commit -m "test(bonsai-archive): end-to-end live trie-node capture from genesis

Assert genesis + subsequent blocks are captured and reconstructable, and
that capture trails the head by maxLayers while genesis is always captured.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run: `./gradlew :ethereum:core:build :app:build -x test -q` — compiles and Spotless clean.
- [ ] Run: `./gradlew :ethereum:core:test --tests "*archive*" --tests "*ArchiveTrieNode*" --tests "*ProofNodeLoader*" -q` — all green.
- [ ] Run: `grep -rn "TrieNodeHistoryWalker\|HistoryOnlyWriteStorage" --include="*.java" .` — no matches.
- [ ] PR description note: the pre-existing `TrieNodeHistoryStore.getLatestBefore` variable-length-natural-key prefix limitation is unchanged and out of scope; ongoing at-head population is a followup PR beginning at `head - maxLayers`.

## Self-Review Notes (spec coverage)

- Spec §1 (delegating strategy) → Task 2. §2 (reorg-window gate + genesis exception) → Task 2 (`shouldCapture`) + Task 4 assertions. §3 (install before genesis) → Task 3 Step 2. §4 (remove walker) → Task 1. Read path / flat migrator untouched → not modified by any task. Diff-base-from-live-read invariant → Task 2 code + `diffBaseComesFromLiveValue...` test. Genesis timing → Task 3 places install in `createWorldStateArchive` (before line ~725 genesis write), supplier deferred to `build()`.
