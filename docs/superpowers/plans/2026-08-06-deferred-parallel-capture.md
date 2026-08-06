# Deferred Parallel Trie-Node Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move archive trie-node capture reads (counter seek, prior-node read, diff encode) off the block-import thread onto a worker pool, applying only the final history `tx.put`s serially at a flush point before transaction commit — byte-identical output, ~35–45% import-time reduction.

**Architecture:** `BonsaiArchiveTrieNodeStrategy` changes from compute-inline to enqueue-and-flush. Puts enqueue a `CaptureRequest` and eagerly submit 64-request chunks to a shared daemon pool; workers read only committed storage (prior node + `getLatestBefore` counter) and emit `(historyKey, storedValue)` pairs; `Updater.commit()` joins and applies them to the tx before committing. Rollback discards. See spec: `docs/superpowers/specs/2026-08-06-deferred-parallel-capture-design.md`.

**Tech Stack:** Java 21, JUnit 5 + AssertJ, `SegmentedInMemoryKeyValueStorage` fixtures, Gradle.

## Global Constraints

- **Byte-identical output:** the history CF contents (keys AND values, including counter bytes) produced for any put/remove sequence must equal what the current inline implementation produces. Existing `BonsaiArchiveTrieNodeStrategyTest` assertions are the oracle — they must pass with only the documented helper change (adding a flush call).
- **No format changes:** `ArchiveNodeKey`, `ArchiveTrieNodeCodec`, `TrieNodeHistoryReader`, `CHECKPOINT_INTERVAL = 16` are untouched.
- **No `plugin-api` changes** (avoids the API-hash gate). `TrieNodeStrategy` lives in `ethereum/core` — safe to extend.
- **Worker threads never touch the transaction.** All `tx.put`/`tx.remove` calls stay on the import thread.
- **Single-threaded persist assumption:** during persist the storage stream is sequential (`BonsaiWorldState.java:161`, `canParallelize = maybeStateUpdater.isEmpty()`); the strategy asserts one open block at a time.
- Format with `./gradlew :ethereum:core:spotlessApply` before each commit.
- All commits end with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- Run tests from the worktree root: `/Users/jframe/code/besu/.claude/worktrees/bonsai-archive-proofs-trie-diff`

## File Map

| File | Role |
|---|---|
| `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/TrieNodeStrategy.java` | Modify: add default `flushCaptures` / `discardCaptures` hooks |
| `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorage.java` | Modify: `Updater.commit/commitComposedOnly/commitTrieLogOnly/rollback` call the hooks |
| `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryStore.java` | Modify: expose `encodeStoredValue` + `putEncoded` |
| `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java` | Modify: enqueue-and-flush, worker computation, executor, block-number memoization |
| `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyTest.java` | Modify: helpers flush before commit; new deferred/parallel/failure tests |
| `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryStoreTest.java` | Modify: `putEncoded` equivalence test |
| `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorageUpdaterCaptureTest.java` | Create: Updater lifecycle → hook wiring tests |

No wiring changes in `BesuControllerBuilder` (strategy constructor unchanged) and no changes to `LiveTrieNodeCaptureIntegrationTest` (it exercises the real `Updater` lifecycle, which now flushes automatically).

---

### Task 1: Strategy lifecycle hooks + Updater wiring

Add no-op capture-lifecycle hooks to the `TrieNodeStrategy` interface and call them from every `Updater` commit/rollback path. Behavior is unchanged for all existing strategies (defaults are no-ops); this creates the seam Task 3 plugs into.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/TrieNodeStrategy.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorage.java:471-492`
- Create: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorageUpdaterCaptureTest.java`

**Interfaces:**
- Consumes: existing `TrieNodeStrategy`, `BonsaiWorldStateKeyValueStorage.Updater` (fields `trieNodeStrategy`, `worldStorage`, `composedWorldStateTransaction`).
- Produces: `default void flushCaptures(SegmentedKeyValueStorage storage, SegmentedKeyValueStorageTransaction tx)` and `default void discardCaptures()` on `TrieNodeStrategy`; guaranteed call ordering: `commit()` and `commitComposedOnly()` flush **before** the composed tx commits; `commitTrieLogOnly()` and `rollback()` discard. Task 3 overrides these in `BonsaiArchiveTrieNodeStrategy`.

- [ ] **Step 1: Write the failing test**

Create `BonsaiWorldStateKeyValueStorageUpdaterCaptureTest.java`:

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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the Updater lifecycle drives the TrieNodeStrategy capture hooks: flush before the
 * composed transaction commits on commit paths that commit it; discard on paths that don't.
 */
class BonsaiWorldStateKeyValueStorageUpdaterCaptureTest {

  /** Records hook invocations; delegates trie-node ops to the plain bonsai strategy. */
  private static final class RecordingStrategy extends BonsaiTrieNodeStrategy {
    final List<String> events = new ArrayList<>();

    @Override
    public void flushCaptures(
        final SegmentedKeyValueStorage storage, final SegmentedKeyValueStorageTransaction tx) {
      events.add("flush");
    }

    @Override
    public void discardCaptures() {
      events.add("discard");
    }
  }

  /** Composed tx wrapper that records its own commit so ordering vs. flush is observable. */
  private static final class RecordingTx implements SegmentedKeyValueStorageTransaction {
    final SegmentedKeyValueStorageTransaction delegate;
    final List<String> events;

    RecordingTx(final SegmentedKeyValueStorageTransaction delegate, final List<String> events) {
      this.delegate = delegate;
      this.events = events;
    }

    @Override
    public void put(
        final org.hyperledger.besu.plugin.services.storage.SegmentIdentifier segment,
        final byte[] key,
        final byte[] value) {
      delegate.put(segment, key, value);
    }

    @Override
    public void remove(
        final org.hyperledger.besu.plugin.services.storage.SegmentIdentifier segment,
        final byte[] key) {
      delegate.remove(segment, key);
    }

    @Override
    public void commit() {
      events.add("composed-commit");
      delegate.commit();
    }

    @Override
    public void rollback() {
      events.add("composed-rollback");
      delegate.rollback();
    }
  }

  private SegmentedKeyValueStorage worldStorage;
  private RecordingStrategy strategy;

  @BeforeEach
  void setUp() {
    worldStorage = new SegmentedInMemoryKeyValueStorage();
    strategy = new RecordingStrategy();
  }

  private BonsaiWorldStateKeyValueStorage.Updater updater() {
    final RecordingTx composedTx =
        new RecordingTx(worldStorage.startTransaction(), strategy.events);
    return new BonsaiWorldStateKeyValueStorage.Updater(
        composedTx,
        trieLogTx(),
        null, // flatDbStrategy unused by the lifecycle paths under test
        worldStorage,
        strategy);
  }

  /**
   * Any real, committable KeyValueStorageTransaction works here — the assertions only observe
   * hook ordering on the composed tx. Check how existing BonsaiWorldStateKeyValueStorage tests
   * construct their trieLogStorage transaction and copy that fixture; the adapter below is the
   * usual pattern (verify the constructor signature against services/kvstore).
   */
  private org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction trieLogTx() {
    return new org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter(
            org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                .TRIE_LOG_STORAGE,
            new SegmentedInMemoryKeyValueStorage())
        .startTransaction();
  }

  @Test
  void commitFlushesBeforeComposedCommit() {
    updater().commit();
    assertThat(strategy.events).containsExactly("flush", "composed-commit");
  }

  @Test
  void commitComposedOnlyFlushesBeforeComposedCommit() {
    updater().commitComposedOnly();
    assertThat(strategy.events).containsExactly("flush", "composed-commit");
  }

  @Test
  void commitTrieLogOnlyDiscardsAndNeverCommitsComposed() {
    updater().commitTrieLogOnly();
    assertThat(strategy.events).containsExactly("discard");
  }

  @Test
  void rollbackDiscardsBeforeComposedRollback() {
    updater().rollback();
    assertThat(strategy.events).containsExactly("discard", "composed-rollback");
  }
}
```

Note on the `updater()` helper: the trie-log transaction argument just needs to be a real, committable `KeyValueStorageTransaction`. If the `SegmentedKeyValueStorageAdapter` constructor above doesn't match the current signature, use whatever this repo's simplest in-memory `KeyValueStorage` is (check `services/kvstore` for `InMemoryKeyValueStorage` or how `BonsaiWorldStateKeyValueStorage` tests build `trieLogStorage`) — the assertion only cares about hook ordering on the composed tx. Simplify the helper accordingly; the `instanceof` dance above is a placeholder-avoidance artifact and should just be a direct construction.

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiWorldStateKeyValueStorageUpdaterCaptureTest" 2>&1 | tail -20`
Expected: COMPILE FAILURE — `flushCaptures`/`discardCaptures` don't exist on `TrieNodeStrategy`.

- [ ] **Step 3: Add the interface hooks**

In `TrieNodeStrategy.java`, after the existing `removeFlatAccountStateTrieNode` declaration, add:

```java
  /**
   * Applies any capture work buffered by put/remove calls to the given transaction. Called by the
   * Updater on every path that commits the composed world-state transaction, immediately before
   * that commit. Default: no-op (non-archive strategies buffer nothing).
   */
  default void flushCaptures(
      final SegmentedKeyValueStorage storage, final SegmentedKeyValueStorageTransaction transaction) {}

  /**
   * Drops any buffered capture work. Called by the Updater on rollback and on commit paths that do
   * not commit the composed world-state transaction. Default: no-op.
   */
  default void discardCaptures() {}
```

- [ ] **Step 4: Wire the Updater paths**

In `BonsaiWorldStateKeyValueStorage.Updater` (lines 470–492), replace the four lifecycle methods:

```java
    @Override
    public void commit() {
      trieNodeStrategy.flushCaptures(worldStorage, composedWorldStateTransaction);
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.commit();
    }

    @Override
    public void commitTrieLogOnly() {
      trieNodeStrategy.discardCaptures();
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.close();
    }

    @Override
    public void commitComposedOnly() {
      trieNodeStrategy.flushCaptures(worldStorage, composedWorldStateTransaction);
      composedWorldStateTransaction.commit();
      trieLogStorageTransaction.close();
    }

    @Override
    public void rollback() {
      trieNodeStrategy.discardCaptures();
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
```

Audit while here: `CachedUpdater` overrides `commit()`/`commitTrieLogOnly()`/`commitComposedOnly()` but calls `super.*` — confirm each override delegates to super (they do, per `:589-602`); no changes needed there.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "BonsaiWorldStateKeyValueStorageUpdaterCaptureTest" 2>&1 | tail -20`
Expected: 4 tests PASS.

- [ ] **Step 6: Run the neighboring suites to catch regressions**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" --tests "BonsaiTrieNodeStrategyTest" --tests "LiveTrieNodeCaptureIntegrationTest" 2>&1 | tail -10`
Expected: PASS (hooks are no-ops so nothing changes yet).

- [ ] **Step 7: Format and commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A ethereum/core
git commit -m "feat(bonsai-archive): capture lifecycle hooks on TrieNodeStrategy + Updater wiring

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `TrieNodeHistoryStore` raw-value API

Expose the stored-value encoding (`[counter byte] ‖ codecEntry`) and a raw put, so worker threads can pre-build values off-thread and the flush can apply them without re-encoding.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryStore.java:40-49`
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/archive/TrieNodeHistoryStoreTest.java`

**Interfaces:**
- Consumes: existing `TrieNodeHistoryStore.put(tx, naturalKey, block, counter, codecEntry)`.
- Produces:
  - `public static Bytes encodeStoredValue(final int counter, final Bytes codecEntry)` — returns `Bytes.concatenate(Bytes.of((byte) counter), codecEntry)`.
  - `public void putEncoded(final SegmentedKeyValueStorageTransaction tx, final Bytes historyKey, final Bytes storedValue)` — writes the pre-built key/value to `TRIE_NODE_HISTORY_ARCHIVE`.
  - `put(...)` refactored to delegate to both (single encoding site).

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeHistoryStoreTest.java` (match the file's existing fixture style — it already builds a `SegmentedInMemoryKeyValueStorage`-backed store; reuse its helper for a valid codec entry, or build one with `ArchiveTrieNodeCodec.encodeFull(shortNodeRlp)` the way its existing tests do):

```java
  @Test
  void putEncodedWritesByteIdenticalEntryToPut() {
    final Bytes naturalKey = ArchiveNodeKey.account(Bytes.fromHexString("0x0102"));
    final Bytes codecEntry = ArchiveTrieNodeCodec.encodeFull(validShortNodeRlp()); // reuse/extract the file's existing RLP helper
    final long block = 42L;
    final int counter = 7;

    // Reference: the existing put().
    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
    historyStore.put(tx1, naturalKey, block, counter, codecEntry);
    tx1.commit();
    final byte[] viaPut =
        storage
            .get(
                TRIE_NODE_HISTORY_ARCHIVE,
                ArchiveNodeKey.historyKey(naturalKey, block).toArrayUnsafe())
            .orElseThrow();

    // Same entry via encodeStoredValue + putEncoded at a different block.
    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
    historyStore.putEncoded(
        tx2,
        ArchiveNodeKey.historyKey(naturalKey, block + 1),
        TrieNodeHistoryStore.encodeStoredValue(counter, codecEntry));
    tx2.commit();
    final byte[] viaPutEncoded =
        storage
            .get(
                TRIE_NODE_HISTORY_ARCHIVE,
                ArchiveNodeKey.historyKey(naturalKey, block + 1).toArrayUnsafe())
            .orElseThrow();

    assertThat(viaPutEncoded).isEqualTo(viaPut);
    // And the typed read path decodes it identically.
    final TrieNodeHistoryStore.HistoryEntry decoded =
        historyStore.get(naturalKey, block + 1).orElseThrow();
    assertThat(decoded.counter()).isEqualTo(counter);
    assertThat(decoded.rawEntryBytes()).isEqualTo(codecEntry);
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "TrieNodeHistoryStoreTest" 2>&1 | tail -20`
Expected: COMPILE FAILURE — `encodeStoredValue`/`putEncoded` undefined.

- [ ] **Step 3: Implement**

In `TrieNodeHistoryStore.java`, replace `put` and add the two methods:

```java
  public void put(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long block,
      final int counter,
      final Bytes codecEntry) {
    putEncoded(
        tx, ArchiveNodeKey.historyKey(naturalKey, block), encodeStoredValue(counter, codecEntry));
  }

  /** Builds the stored wire value: {@code [counter: 1 byte] ‖ codecEntry}. */
  public static Bytes encodeStoredValue(final int counter, final Bytes codecEntry) {
    return Bytes.concatenate(Bytes.of((byte) counter), codecEntry);
  }

  /** Writes a pre-built history entry. Key must come from {@link ArchiveNodeKey#historyKey}. */
  public void putEncoded(
      final SegmentedKeyValueStorageTransaction tx, final Bytes historyKey, final Bytes storedValue) {
    tx.put(TRIE_NODE_HISTORY_ARCHIVE, historyKey.toArrayUnsafe(), storedValue.toArrayUnsafe());
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "TrieNodeHistoryStoreTest" 2>&1 | tail -10`
Expected: PASS (all tests in the class, old and new).

- [ ] **Step 5: Format and commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A ethereum/core
git commit -m "feat(bonsai-archive): expose raw stored-value encode/put on TrieNodeHistoryStore

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Deferred capture in `BonsaiArchiveTrieNodeStrategy` (serial, no executor yet)

Convert the strategy from compute-inline to enqueue-and-flush, computing captures serially at flush time. This isolates the semantic change (defer to flush) from the concurrency change (Task 4), and the existing test assertions prove byte-equivalence. Also memoizes the per-put `WORLD_BLOCK_NUMBER_KEY` read.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java`
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyTest.java`

**Interfaces:**
- Consumes: Task 1's `flushCaptures`/`discardCaptures` interface hooks; Task 2's `encodeStoredValue`/`putEncoded`.
- Produces (relied on by Task 4):
  - Nested `record CaptureRequest(Bytes naturalKey, Bytes location, long block, Hash accountHash, Bytes32 nodeHash, Bytes newNode)` — `accountHash == null` ⇒ account-trie node; `newNode == null` ⇒ removal.
  - Nested `record CaptureResult(Bytes historyKey, Bytes storedValue)`.
  - `private Optional<CaptureResult> computeCapture(CaptureRequest request, SegmentedKeyValueStorage storage)` — pure (reads committed storage only, no tx).
  - Buffer fields `pendingRequests` (List), `bufferedBlock` (long), and overridden `flushCaptures`/`discardCaptures`.
  - **Contract:** history entries land only at `flushCaptures`; callers that commit raw transactions must flush first.

- [ ] **Step 1: Update the test helpers and add the deferred-visibility test (failing first)**

In `BonsaiArchiveTrieNodeStrategyTest.java`:

(a) Every helper that writes through the strategy and then commits must flush in between. There are helpers like `putAccount` (line 75) plus storage-put/remove equivalents further down the file — find them all with `grep -n "tx.commit()" BonsaiArchiveTrieNodeStrategyTest.java` and insert the flush line in each helper that called a `strategy.put*`/`strategy.remove*` beforehand:

```java
  private void putAccount(
      final BonsaiArchiveTrieNodeStrategy strategy, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    strategy.flushCaptures(storage, tx);   // <-- inserted line, same in every write helper
    tx.commit();
  }
```

(b) Add tests pinning the new contract:

```java
  @Test
  void captureIsBufferedUntilFlush() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(0);
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    tx.commit(); // committed WITHOUT flush: live node visible, history entry absent

    assertThat(new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, null, storage))
        .contains(node);
    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isEmpty();

    // Flushing into a new transaction lands the buffered entry.
    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
    strategy.flushCaptures(storage, tx2);
    tx2.commit();
    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isPresent();
  }

  @Test
  void discardDropsBufferedCaptures() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(0);
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    strategy.discardCaptures();
    strategy.flushCaptures(storage, tx); // nothing buffered => writes nothing
    tx.commit();

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isEmpty();
  }

  @Test
  void progressAdvancesOnceAtFlush() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    putAccount(strategy, location, shortNodeRlp(0)); // helper flushes then commits
    assertThat(progress.lastIndexedBlock()).isEqualTo(0L);
    assertThat(TrieNodeHistoryProgress.load(storage).lastIndexedBlock()).isEqualTo(0L);
  }
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" 2>&1 | tail -25`
Expected: `captureIsBufferedUntilFlush` FAILS (history entry present immediately today — capture is still inline); `discardDropsBufferedCaptures` FAILS. Pre-existing tests still pass (the added flush is a no-op default until Step 3).

- [ ] **Step 3: Rewrite the strategy to enqueue-and-flush**

Replace the body of `BonsaiArchiveTrieNodeStrategy` (keep class javadoc, constructor, `setHighestSafeBlockSupplier`, `shouldCapture`/`shouldCaptureBlock`, and the read delegations unchanged; the diff below shows the full new write-path/flush section). New/changed members:

```java
  /** One buffered write awaiting capture computation. accountHash null => account trie; newNode null => removal. */
  record CaptureRequest(
      Bytes naturalKey, Bytes location, long block, Hash accountHash, Bytes32 nodeHash, Bytes newNode) {}

  /** A computed history entry ready to apply to the transaction. */
  record CaptureResult(Bytes historyKey, Bytes storedValue) {}

  private final List<CaptureRequest> pendingRequests = new ArrayList<>();
  private long bufferedBlock = Long.MIN_VALUE;

  // WORLD_BLOCK_NUMBER_KEY is constant within a block (only this import thread's uncommitted tx
  // changes it); cache it between flush/discard boundaries instead of reading it on every put.
  private long cachedBlockNumber = Long.MIN_VALUE;
  private boolean blockNumberCached = false;

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    if (!blockNumberCached) {
      cachedBlockNumber =
          storage
              .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
              .map(b -> Bytes.wrap(b).toLong() + 1L)
              .orElse(0L);
      blockNumberCached = true;
    }
    return cachedBlockNumber;
  }

  private void enqueue(final CaptureRequest request) {
    if (!pendingRequests.isEmpty() && bufferedBlock != request.block()) {
      throw new IllegalStateException(
          "trie-node capture buffer holds block "
              + bufferedBlock
              + " but received a write for block "
              + request.block()
              + " — previous block was neither flushed nor discarded");
    }
    bufferedBlock = request.block();
    pendingRequests.add(request);
  }
```

The three write methods become enqueue-only (live write unchanged, no reads):

```java
  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final long block = currentBlockNumber(storage);
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(ArchiveNodeKey.account(location), location, block, null, nodeHash, node));
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
    baseStrategy.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.storage(accountHash.getBytes(), location),
              location,
              block,
              accountHash,
              nodeHash,
              node));
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(ArchiveNodeKey.account(location), location, block, null, null, null));
    }
  }
```

The worker computation — today's inline decision logic verbatim, reshaped as a pure function. The only reorder: the `location.isEmpty()` root check is hoisted above the `getLatestBefore` seek (output identical — root always FULL counter 0 — but skips the seek for roots):

```java
  /**
   * Computes the history entry for one buffered write. Reads only committed storage (the block's
   * own writes sit in the uncommitted transaction), so during sequential import the flat DB still
   * holds block N-1's value — the correct diff base. Safe to call from any thread; never touches
   * the transaction. Returns empty for a removal of a node with no live prior (nothing to record).
   */
  private Optional<CaptureResult> computeCapture(
      final CaptureRequest request, final SegmentedKeyValueStorage storage) {
    final Bytes priorNode =
        request.accountHash() == null
            ? baseStrategy
                .getFlatAccountTrieNode(request.location(), request.nodeHash(), storage)
                .orElse(null)
            : baseStrategy
                .getFlatStorageTrieNode(
                    request.accountHash(), request.location(), request.nodeHash(), storage)
                .orElse(null);

    if (request.newNode() == null) { // removal
      if (priorNode == null) {
        return Optional.empty();
      }
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeDiff(priorNode, null)));
    }
    if (priorNode == null) {
      return Optional.of(
          result(request, 0, ArchiveTrieNodeCodec.encodeDiff(null, request.newNode())));
    }
    if (request.location().isEmpty()) { // roots are always FULL — no seek needed
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final Optional<TrieNodeHistoryStore.HistoryEntry> priorEntryOpt =
        historyStore.getLatestBefore(request.naturalKey(), request.block());
    if (priorEntryOpt.isEmpty() || priorEntryOpt.get().codecEntry().isDeletion()) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final int priorCounter = priorEntryOpt.get().counter();
    if (priorCounter + 1 >= TrieNodeHistoryReader.CHECKPOINT_INTERVAL) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    return Optional.of(
        result(
            request,
            priorCounter + 1,
            ArchiveTrieNodeCodec.encodeDiff(priorNode, request.newNode())));
  }

  private static CaptureResult result(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new CaptureResult(
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        TrieNodeHistoryStore.encodeStoredValue(counter, codecEntry));
  }
```

Flush and discard (serial compute in this task; Task 4 swaps the compute loop for joined futures):

```java
  @Override
  public void flushCaptures(
      final SegmentedKeyValueStorage storage, final SegmentedKeyValueStorageTransaction transaction) {
    blockNumberCached = false; // the commit this precedes will change WORLD_BLOCK_NUMBER_KEY
    if (pendingRequests.isEmpty()) {
      return;
    }
    final long block = bufferedBlock;
    try {
      // Belt-and-braces: keyed by historyKey, last write wins — matches sequential tx.put order.
      final Map<Bytes, Bytes> results = new LinkedHashMap<>();
      for (final CaptureRequest request : pendingRequests) {
        computeCapture(request, storage)
            .ifPresent(r -> results.put(r.historyKey(), r.storedValue()));
      }
      results.forEach((key, value) -> historyStore.putEncoded(transaction, key, value));
    } finally {
      pendingRequests.clear();
      bufferedBlock = Long.MIN_VALUE;
    }
    historyProgress.setLastIndexedBlock(block);
    historyProgress.setIndexStartBlock(block);
    historyProgress.save(transaction);
  }

  @Override
  public void discardCaptures() {
    pendingRequests.clear();
    bufferedBlock = Long.MIN_VALUE;
    blockNumberCached = false;
  }
```

Delete the now-dead members: `captureTrieNodeDiff`, `advanceHistoryProgress`, `lastSavedProgressBlock`, and the old `currentBlockNumber` body. Update the class javadoc: capture is buffered per block and applied at `flushCaptures` (invoked by the Updater before every composed-tx commit); the diff base is read from committed storage at flush time, which during sequential import is still block N−1's state — same value the inline implementation read.

New imports: `java.util.ArrayList`, `java.util.LinkedHashMap`, `java.util.List`, `java.util.Map`, `org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec`, `org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader`.

- [ ] **Step 4: Run the full strategy suite**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" 2>&1 | tail -25`
Expected: ALL tests pass — pre-existing ones (byte-equivalence oracle: FULL/DIFF cadence, counters, gate behavior, reconstruction) plus the three new ones.

- [ ] **Step 5: Run the integration test**

Run: `./gradlew :ethereum:core:test --tests "LiveTrieNodeCaptureIntegrationTest" 2>&1 | tail -10`
Expected: PASS unchanged — it drives the real `Updater` lifecycle, which flushes via Task 1's wiring.

- [ ] **Step 6: Format and commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A ethereum/core
git commit -m "refactor(bonsai-archive): defer trie-node capture to a per-block flush

Capture requests buffer during puts and compute at flushCaptures (still
serial); removes the per-put WORLD_BLOCK_NUMBER_KEY read. Output is
byte-identical — existing strategy tests are the oracle.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Parallelize capture computation

Swap the serial flush loop for eager chunked submission to a shared daemon pool, joined at flush. Failure propagation and discard semantics get explicit tests.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategy.java`
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveTrieNodeStrategyTest.java`

**Interfaces:**
- Consumes: Task 3's `CaptureRequest`/`CaptureResult`/`computeCapture`, buffer fields, flush/discard.
- Produces: same external contract as Task 3 (`flushCaptures` applies everything buffered, `discardCaptures` drops it); internally `pendingRequests` is chunked into `inFlight` futures as it grows. Failure contract: any worker exception rethrows from `flushCaptures` as `RuntimeException` with the worker failure as cause; the buffer is always cleared.

- [ ] **Step 1: Write the failing tests**

Add to `BonsaiArchiveTrieNodeStrategyTest.java`:

```java
  /**
   * Seeded multi-key workload across many blocks with eager chunk submission (chunk size 64 =>
   * 200 keys per block forces multiple in-flight chunks). Oracle: the reader must reconstruct
   * every key at every block, and every stored counter must respect the checkpoint bound.
   */
  @Test
  void parallelCaptureMatchesReaderOracleAcrossBlocks() {
    final int keys = 200;
    final int blocks = 40;
    final java.util.Random random = new java.util.Random(1234);
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    final Bytes[] locations = new Bytes[keys];
    for (int k = 0; k < keys; k++) {
      locations[k] = Bytes.concatenate(Bytes.of(0x01), Bytes.ofUnsignedShort(k)); // depth 3, non-root
    }
    // expected[k] = list of node value per block (null = key untouched that block, carries forward)
    final Bytes[][] written = new Bytes[keys][blocks];

    for (int b = 0; b < blocks; b++) {
      if (b > 0) {
        setWorldBlockNumber(b - 1L);
      }
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      for (int k = 0; k < keys; k++) {
        if (b == 0 || random.nextInt(3) == 0) { // every key at genesis, ~1/3 mutate per block
          final Bytes node = shortNodeRlp(b * keys + k);
          strategy.putFlatAccountTrieNode(
              storage, tx, locations[k], org.hyperledger.besu.crypto.Hash.keccak256(node), node);
          written[k][b] = node;
        }
      }
      strategy.flushCaptures(storage, tx);
      tx.commit();
    }

    for (int k = 0; k < keys; k++) {
      Bytes expected = null;
      for (int b = 0; b < blocks; b++) {
        if (written[k][b] != null) {
          expected = written[k][b];
        }
        assertThat(reader.nodeAt(ArchiveNodeKey.account(locations[k]), b))
            .as("key %d at block %d", k, b)
            .contains(expected);
      }
    }
  }

  @Test
  void workerFailurePropagatesFromFlushAndClearsBuffer() {
    final TrieNodeStrategy failingBase =
        new BonsaiTrieNodeStrategy() {
          @Override
          public Optional<Bytes> getFlatAccountTrieNode(
              final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
            throw new IllegalStateException("boom: simulated read failure");
          }
        };
    final BonsaiArchiveTrieNodeStrategy strategy =
        new BonsaiArchiveTrieNodeStrategy(failingBase, historyStore, progress, () -> Long.MAX_VALUE);

    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> strategy.flushCaptures(storage, tx))
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("boom: simulated read failure");
    tx.rollback();

    // Buffer cleared: a fresh flush on a new tx writes nothing.
    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
    strategy.flushCaptures(storage, tx2);
    tx2.commit();
    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isEmpty();
  }

  @Test
  void discardDropsInFlightChunks() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    for (int k = 0; k < 200; k++) { // > CHUNK_SIZE, so chunks are already submitted
      final Bytes location = Bytes.concatenate(Bytes.of(0x02), Bytes.ofUnsignedShort(k));
      final Bytes node = shortNodeRlp(k);
      strategy.putFlatAccountTrieNode(
          storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    }
    strategy.discardCaptures();
    strategy.flushCaptures(storage, tx);
    tx.commit();
    assertThat(
            storage.stream(
                    org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                        .TRIE_NODE_HISTORY_ARCHIVE))
        .isEmpty();
  }
```

(If `storage.stream(segment)` isn't available on `SegmentedKeyValueStorage`, assert emptiness by `historyStore.get(...)` on a sample of the 200 keys instead — check how existing tests in this file enumerate the history CF.)

- [ ] **Step 2: Run tests — new ones may pass serially; verify compile + baseline**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" 2>&1 | tail -25`
Expected: the three new tests PASS against Task 3's serial implementation (they pin behavior, not mechanism). That's fine — they exist to hold through the parallel swap. Confirm green before proceeding.

- [ ] **Step 3: Implement eager chunked submission**

In `BonsaiArchiveTrieNodeStrategy`, add the executor and in-flight tracking (following the static-shared-pool precedent of `ParallelStoredMerklePatriciaTrie.FORK_JOIN_POOL` — daemon threads, process lifetime, never shut down):

```java
  /** Chunk of requests handed to one worker task. */
  private static final int CAPTURE_CHUNK_SIZE = 64;

  /**
   * Shared capture pool, mirroring ParallelStoredMerklePatriciaTrie's static-pool precedent.
   * Deliberately NOT the trie ForkJoinPool: that pool is saturated with hashing exactly while
   * captures run, and capture tasks are read-latency-bound, not CPU-bound. Daemon threads —
   * process-lifetime, no shutdown needed.
   */
  private static final ExecutorService CAPTURE_POOL =
      Executors.newFixedThreadPool(
          Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() / 2)),
          runnable -> {
            final Thread thread = new Thread(runnable, "trie-capture");
            thread.setDaemon(true);
            return thread;
          });

  private final List<Future<List<CaptureResult>>> inFlight = new ArrayList<>();
```

Change `enqueue` to submit chunks eagerly (the emptiness check for the block assertion must now cover in-flight work too):

```java
  private void enqueue(final CaptureRequest request, final SegmentedKeyValueStorage storage) {
    if ((!pendingRequests.isEmpty() || !inFlight.isEmpty()) && bufferedBlock != request.block()) {
      throw new IllegalStateException(
          "trie-node capture buffer holds block "
              + bufferedBlock
              + " but received a write for block "
              + request.block()
              + " — previous block was neither flushed nor discarded");
    }
    bufferedBlock = request.block();
    pendingRequests.add(request);
    if (pendingRequests.size() >= CAPTURE_CHUNK_SIZE) {
      submitChunk(storage);
    }
  }

  private void submitChunk(final SegmentedKeyValueStorage storage) {
    final List<CaptureRequest> chunk = List.copyOf(pendingRequests);
    pendingRequests.clear();
    inFlight.add(
        CAPTURE_POOL.submit(
            () -> {
              final List<CaptureResult> results = new ArrayList<>(chunk.size());
              for (final CaptureRequest request : chunk) {
                computeCapture(request, storage).ifPresent(results::add);
              }
              return results;
            }));
  }
```

Update the three write methods' enqueue calls to pass `storage`: `enqueue(new CaptureRequest(...), storage)`.

Replace the flush compute loop with join-and-apply, and make discard cancel in-flight work:

```java
  @Override
  public void flushCaptures(
      final SegmentedKeyValueStorage storage, final SegmentedKeyValueStorageTransaction transaction) {
    blockNumberCached = false; // the commit this precedes will change WORLD_BLOCK_NUMBER_KEY
    if (pendingRequests.isEmpty() && inFlight.isEmpty()) {
      return;
    }
    final long block = bufferedBlock;
    if (!pendingRequests.isEmpty()) {
      submitChunk(storage);
    }
    try {
      // Belt-and-braces: keyed by historyKey, last write wins — matches sequential tx.put order
      // (chunks are joined in submission order, which is put order).
      final Map<Bytes, Bytes> results = new LinkedHashMap<>();
      for (final Future<List<CaptureResult>> future : inFlight) {
        for (final CaptureResult result : future.get()) {
          results.put(result.historyKey(), result.storedValue());
        }
      }
      results.forEach((key, value) -> historyStore.putEncoded(transaction, key, value));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted while flushing trie-node captures", e);
    } catch (final ExecutionException e) {
      throw new RuntimeException("trie-node capture failed", e.getCause());
    } finally {
      inFlight.clear();
      pendingRequests.clear();
      bufferedBlock = Long.MIN_VALUE;
    }
    historyProgress.setLastIndexedBlock(block);
    historyProgress.setIndexStartBlock(block);
    historyProgress.save(transaction);
  }

  @Override
  public void discardCaptures() {
    inFlight.forEach(future -> future.cancel(true));
    inFlight.clear();
    pendingRequests.clear();
    bufferedBlock = Long.MIN_VALUE;
    blockNumberCached = false;
  }
```

New imports: `java.util.concurrent.ExecutionException`, `java.util.concurrent.ExecutorService`, `java.util.concurrent.Executors`, `java.util.concurrent.Future`.

Threading note for the class javadoc: workers call `computeCapture`, which reads only committed storage via thread-safe RocksDB/in-memory reads; `historyStore.getLatestBefore` sees a stable view because the block's own writes are in the still-uncommitted transaction and the import thread is the CFs' only writer.

- [ ] **Step 4: Run the full strategy suite**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveTrieNodeStrategyTest" 2>&1 | tail -25`
Expected: ALL pass — the oracle tests, the Task 3 deferred-contract tests, and the Task 4 workload/failure/discard tests, now against the parallel implementation.

- [ ] **Step 5: Run integration + updater tests**

Run: `./gradlew :ethereum:core:test --tests "LiveTrieNodeCaptureIntegrationTest" --tests "BonsaiWorldStateKeyValueStorageUpdaterCaptureTest" --tests "TrieNodeHistoryStoreTest" --tests "TrieNodeHistoryReaderTest" 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 6: Format and commit**

```bash
./gradlew :ethereum:core:spotlessApply
git add -A ethereum/core
git commit -m "feat(bonsai-archive): parallel trie-node capture on a shared worker pool

Capture chunks submit eagerly during puts and join at flush, overlapping
counter seeks and diff encoding with trie commit work. Worker failure
fails the flush (and the block import); discard cancels in-flight work.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Full verification pass

**Files:** none created; runs the broader suites the change can touch.

**Interfaces:**
- Consumes: everything above.
- Produces: a green build; the branch ready for perf validation.

- [ ] **Step 1: Run the bonsai/pathbased test packages**

Run: `./gradlew :ethereum:core:test --tests "org.hyperledger.besu.ethereum.trie.pathbased.*" 2>&1 | tail -15`
Expected: PASS. Pay attention to any world-state persist tests that commit via `commitTrieLogOnly` (now discards buffered captures) — if one fails, check whether it legitimately expects history entries from a trie-log-only commit (it shouldn't: that path never commits the composed tx, so inline capture writes were discarded with it anyway).

- [ ] **Step 2: Build without tests to catch compile/spotless issues across modules**

Run: `./gradlew :ethereum:core:build -x test 2>&1 | tail -5`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Run the trie module tests (ParallelStoredMerklePatriciaTrie untouched, but cheap insurance)**

Run: `./gradlew :ethereum:trie:test 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 4: Commit anything spotless changed; otherwise no commit**

```bash
git status --short   # if clean, done; if spotless touched files:
git add -A && git commit -m "chore: spotless

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Live perf validation (manual, operator-driven)

Re-measure the profile that motivated this work. **This task needs the user's QBFT dev node** — coordinate before restarting anything.

**Files:** none.

**Interfaces:**
- Consumes: the built branch; the running QBFT network at `~/code/networks/qbft-1node/`.
- Produces: before/after numbers for the PR description.

- [ ] **Step 1: Build the distribution**

```bash
./gradlew installDist -x test 2>&1 | tail -3
```

- [ ] **Step 2: Restart the node on this build (ask the user first — it's their running network)**

```bash
cd ~/code/networks/qbft-1node
BESU_ROOT=/Users/jframe/code/besu/.claude/worktrees/bonsai-archive-proofs-trie-diff ./besu.sh
```

- [ ] **Step 3: Profile 60s of block import (same methodology as the baseline)**

```bash
asprof start -e wall -t -i 5ms <pid>
sleep 60
asprof stop -o collapsed -f /tmp/trie-diff-wall-after.collapsed <pid>
```

- [ ] **Step 4: Compare against baseline**

Baseline (this plan's motivating profile): `captureTrieNodeDiff` = 52% of the import thread (`getLatestBefore` 36%, `put` 9%, `encodeDiff` 6.6%). Compute the same shares from the new capture:

```bash
grep -c "importBlock" /tmp/trie-diff-wall-after.collapsed                       # thread total
grep -c "flushCaptures" /tmp/trie-diff-wall-after.collapsed                     # remaining serial flush cost
grep -c "computeCapture" /tmp/trie-diff-wall-after.collapsed                    # should now sit on trie-capture threads
grep "trie-capture" /tmp/trie-diff-wall-after.collapsed | grep -c computeCapture
```

Success criteria: `getLatestBefore`/`computeCapture` samples on the import thread ≈ 0; import-thread `flushCaptures` cost ≈ the history `tx.put` share (~9–12%); overall import-thread samples per block down materially. Record the numbers in the PR description.

---

## Self-Review Notes

- **Spec coverage:** lifecycle hooks + all four Updater paths (Task 1); pre-built value encoding (Task 2); enqueue-and-flush, block-number memoization, single-block assertion, progress-once-per-flush, root-seek skip (Task 3); eager chunked parallelism, dedup map, failure propagation, discard-cancels (Task 4); integration + broad suites (Task 5); perf validation (Task 6). Spec's "audit commitTrieLogOnly/commitComposedOnly" is Task 1 Steps 4/6 and Task 5 Step 1.
- **Deliberate deviation from spec:** executor is a static shared daemon pool (precedent: `ParallelStoredMerklePatriciaTrie.FORK_JOIN_POOL`) rather than per-strategy-with-shutdown — removes lifecycle wiring for equivalent behavior; noted in Task 4 javadoc.
- **Type consistency:** `CaptureRequest`/`CaptureResult`/`computeCapture`/`flushCaptures`/`discardCaptures`/`encodeStoredValue`/`putEncoded` are used with identical signatures across Tasks 1–4.
- **Known flexibility points (implementer judgment, not placeholders):** Task 1's trie-log-tx construction helper (match repo fixtures); Task 4's history-CF emptiness assertion if `storage.stream` is unavailable. Both state the fallback explicitly.
