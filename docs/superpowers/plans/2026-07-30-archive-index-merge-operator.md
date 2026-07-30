# Archive Index Merge Operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `TrieNodeChangeIndex`'s per-append read-modify-write of the packed trie-node
change-block offset list with a RocksDB `StringAppendOperator("")` merge for the common case,
splitting the value into a blind-mergeable content CF and a small separately-read metadata CF so
the depth-tiered checkpoint's exact previous-count requirement is preserved.

**Architecture:** `TRIE_NODE_INDEX_ARCHIVE` becomes merge-only (pure packed 3-byte-per-entry
offset bytes, no prefix). A new CF `TRIE_NODE_INDEX_META_ARCHIVE` holds an 8-byte
`[subCount][tailCount]` value, read/written via plain `Get`/`Put`. `SegmentedKeyValueStorageTransaction`
gains a `merge()` method (plugin-api change); all eight implementors (six named classes plus two
anonymous no-op sentinels found by a full-sweep grep — see Task 15) and `SegmentIdentifier` gain a
`usesAppendMergeOperator()` flag wired into RocksDB column family options.

**Tech Stack:** Java 21, RocksDB (`rocksdbjni:10.6.2`), JUnit 5, AssertJ, Mockito.

## Global Constraints

- `rocksdbjni` version is `10.6.2` (`platform/build.gradle:177`) — `StringAppendOperator(String delim)`
  with a true empty string is available and verified to do zero-separator concatenation (confirmed
  by direct test against this exact jar; see spec).
- `ArchiveNodeKey.RANGE_SIZE = 1_000_000L`; sub-block thresholds are
  `DEFAULT_SUBBLOCK_THRESHOLD = 4096`, `DEFAULT_SUBBLOCK_SPLIT_AT = 2048`
  (`TrieNodeChangeIndex.java:63,73`) — unchanged by this plan.
- `SegmentedKeyValueStorageTransaction` and `SegmentIdentifier` are `@Unstable` plugin-api
  interfaces (`plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/`). Any
  method addition requires regenerating `knownHash` in `plugin-api/build.gradle:74` via
  `./gradlew :plugin-api:checkAPIChanges` (it fails and prints the new "Calculated" hash to paste
  in).
- `BonsaiFlatDbToArchiveMigrator.MigrationTransaction` (`ethereum/core/.../bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java:1155`)
  drops writes for any `SegmentIdentifier` not explicitly listed in its `put()`/`remove()`
  allowlist. This exact pattern silently dropped `TRIE_NODE_CAS_ARCHIVE` writes in a past incident
  (4.38M dangling HASH_REFs) — every new segment/method this plan adds to that class's write paths
  must be added to the allowlist explicitly, never assumed.
- Do not change `OptimisticTransactionDB` vs `TransactionDB` selection, or unrelated storage-layer
  behavior — out of scope per the spec.
- Spec reference: `docs/superpowers/specs/2026-07-30-archive-index-merge-operator-design.md`.

## Execution Order

**Dispatch tasks in this order, not their numeric order:**
`1, 2, 3, 4, 5, 15, 16, 6, 7, 8, 13, 9, 10, 11, 12, 14`.

Reason: Gradle compiles a module's entire main+test source set before running any test in it, even
a single `--tests`-filtered class. `SegmentedKeyValueStorageTransaction.merge()` is abstract
(Task 1), so **every** implementor in a module must have a `merge()` override before that module
compiles at all. `ethereum/core` has three implementors (`MigrationTransaction`/`FlatCapturingTx`
in `BonsaiFlatDbToArchiveMigrator`, and `PathBasedWorldState`'s no-op sentinel) that this plan
doesn't reach in their natural numeric position (13 and 15) until deep into the sequence — meaning
Tasks 6 through 12 would otherwise be unable to compile or run `:ethereum:core:test` at all for
their own, unrelated changes. Tasks 15 and 16 exist specifically to add `merge()` to those three
implementors early (Task 16 is a split-out of what would otherwise be part of Task 13 — see Task
16's and Task 13's own execution notes for the detail). Do this reordering once, up front; do not
re-derive it per task.

---

### Task 1: Plugin-API — add `merge()` and `usesAppendMergeOperator()`

**Files:**
- Modify: `plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentedKeyValueStorageTransaction.java`
- Modify: `plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentIdentifier.java`
- Modify: `plugin-api/build.gradle:74` (`knownHash`)

**Interfaces:**
- Produces: `void merge(SegmentIdentifier segmentIdentifier, byte[] key, byte[] value)` on
  `SegmentedKeyValueStorageTransaction` — every implementor in Tasks 2–5 must implement this.
- Produces: `default boolean usesAppendMergeOperator() { return false; }` on `SegmentIdentifier` —
  consumed by Task 4 (RocksDB column family wiring) and Task 6 (`KeyValueSegmentIdentifier`).

This task only adds the interface methods (with `SegmentedKeyValueStorageTransaction.merge()` left
unimplemented anywhere yet) — the module won't compile again until Tasks 2–5 add implementations.
That's fine; the next tasks land immediately after.

- [ ] **Step 1: Add `merge()` to `SegmentedKeyValueStorageTransaction`**

Edit `plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentedKeyValueStorageTransaction.java`,
adding this method after `put()`:

```java
  /**
   * Merges the specified value into whatever is currently associated with the given key, using
   * the column family's configured merge operator (e.g. blind concatenation via {@code
   * StringAppendOperator}). Unlike {@link #put}, this does not require reading the current value
   * first — the underlying store resolves the final value lazily, either at read time or during
   * compaction.
   *
   * <p>Only meaningful for segments whose backing column family has a merge operator configured
   * ({@link SegmentIdentifier#usesAppendMergeOperator()}); for segments without one, callers
   * should use {@link #put} instead.
   *
   * @param segmentIdentifier the segment identifier
   * @param key the key to merge a value into
   * @param value the value to merge in
   */
  void merge(SegmentIdentifier segmentIdentifier, byte[] key, byte[] value);
```

- [ ] **Step 2: Add `usesAppendMergeOperator()` to `SegmentIdentifier`**

Edit `plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentIdentifier.java`,
adding this default method after `isCacheIndexAndFilterBlocks()`:

```java
  /**
   * Whether this segment's backing column family should be configured with an append-only merge
   * operator (zero-delimiter {@code StringAppendOperator}) instead of plain overwrite semantics.
   * When true, writers may use {@link SegmentedKeyValueStorageTransaction#merge} to append bytes
   * to the current value without reading it first.
   *
   * @return true if this segment uses an append merge operator
   */
  default boolean usesAppendMergeOperator() {
    return false;
  }
```

- [ ] **Step 3: Regenerate the plugin-api API hash**

Run: `./gradlew :plugin-api:checkAPIChanges`
Expected: FAILS with a message showing `Expected: <old hash>` and `Calculated: <new hash>`.

Copy the `Calculated` value into `plugin-api/build.gradle:74`, replacing the `knownHash` string
literal.

- [ ] **Step 4: Verify the hash check now passes**

Run: `./gradlew :plugin-api:checkAPIChanges`
Expected: BUILD SUCCESSFUL (task passes; no other tests run yet — the module has unimplemented
abstract methods elsewhere until Tasks 2–5 land, so don't run `:plugin-api:build` yet).

- [ ] **Step 5: Commit**

```bash
git add plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentedKeyValueStorageTransaction.java \
        plugin-api/src/main/java/org/hyperledger/besu/plugin/services/storage/SegmentIdentifier.java \
        plugin-api/build.gradle
git commit -m "feat(plugin-api): add merge() and usesAppendMergeOperator() to storage SPI"
```

---

### Task 2: `SegmentedInMemoryKeyValueStorage` — implement `merge()`

**Files:**
- Modify: `services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorage.java`
- Test: `services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorageTest.java`

**Interfaces:**
- Consumes: `SegmentedKeyValueStorageTransaction.merge(SegmentIdentifier, byte[], byte[])` from Task 1.
- Produces: working `merge()` on `SegmentedInMemoryTransaction`, simulating zero-delimiter
  `StringAppendOperator` concatenation semantics. This is the storage backing almost all existing
  `TrieNodeChangeIndex` unit tests (they construct `new SegmentedInMemoryKeyValueStorage()`
  directly), so this must behave identically to real RocksDB merge for later tasks' tests to pass.

- [ ] **Step 1: Write the failing test**

Create `services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorageTest.java`
with these test methods inside the class shown in the file skeleton below:

```java
  @Test
  void mergeConcatenatesOntoEmptyKey() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1, 2, 3});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {4, 5, 6});
    tx.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {1, 2, 3, 4, 5, 6});
  }

  @Test
  void mergeConcatenatesOntoExistingPutValue() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx1 = kv.startTransaction();
    tx1.put(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {(byte) 0xAA});
    tx1.commit();

    final SegmentedKeyValueStorageTransaction tx2 = kv.startTransaction();
    tx2.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1, 2, 3});
    tx2.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {(byte) 0xAA, 1, 2, 3});
  }

  @Test
  void mergeWithinSameUncommittedTransactionAccumulates() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {2});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {3});
    tx.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {1, 2, 3});
  }
```

No test file exists yet for this class in this module (confirmed:
`services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/` contains no
`SegmentIdentifier` test doubles). Create the file fresh with this content (test methods from
above go inside the class body, after `TestSegment`):

```java
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class SegmentedInMemoryKeyValueStorageTest {

  private enum TestSegment implements SegmentIdentifier {
    FOO(new byte[] {1});

    private final byte[] id;

    TestSegment(final byte[] id) {
      this.id = id;
    }

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }

  // ... test methods above ...
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :services:kvstore:test --tests "*SegmentedInMemoryKeyValueStorageTest*"`
Expected: FAIL — `merge` is not defined on `SegmentedInMemoryTransaction` (compile error, since
Task 1 declared it abstract with no implementation here yet).

- [ ] **Step 3: Implement `merge()` on `SegmentedInMemoryTransaction`**

Edit `services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorage.java`,
adding this method to the `SegmentedInMemoryTransaction` inner class (after `remove()`, before
`commit()`):

```java
    @Override
    public void merge(
        final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
      final Bytes k = Bytes.wrap(key);
      final byte[] base;
      final Optional<byte[]> pendingPut = updatedValues.getOrDefault(segmentIdentifier, Map.of()).get(k);
      if (pendingPut != null) {
        base = pendingPut.orElse(new byte[0]);
      } else if (removedKeys.getOrDefault(segmentIdentifier, Set.of()).contains(k)) {
        base = new byte[0];
      } else {
        base =
            hashValueStore
                .computeIfAbsent(segmentIdentifier, __ -> newSegmentMap())
                .getOrDefault(k, Optional.empty())
                .orElse(new byte[0]);
      }
      final byte[] merged = Bytes.concatenate(Bytes.wrap(base), Bytes.wrap(value)).toArrayUnsafe();
      updatedValues.computeIfAbsent(segmentIdentifier, __ -> new HashMap<>()).put(k, Optional.of(merged));
      removedKeys.computeIfAbsent(segmentIdentifier, __ -> new HashSet<>()).remove(k);
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :services:kvstore:test --tests "*SegmentedInMemoryKeyValueStorageTest*"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorage.java \
        services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/SegmentedInMemoryKeyValueStorageTest.java
git commit -m "feat(kvstore): simulate merge-operator concatenation in in-memory storage"
```

---

### Task 3: `SegmentedKeyValueStorageTransactionValidatorDecorator` — implement `merge()`

**Files:**
- Modify: `services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedKeyValueStorageTransactionValidatorDecorator.java`
- Test: `services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/SegmentedKeyValueStorageTransactionValidatorDecoratorTest.java`

**Interfaces:**
- Consumes: `merge()` from Task 1; delegates to a wrapped `SegmentedKeyValueStorageTransaction`.
- Produces: a `merge()` override with the same active/closed state checks as `put()`/`remove()`.

- [ ] **Step 1: Write the failing test**

No test file exists yet for this decorator (confirmed: `find services/kvstore/src/test -iname
"*Decorator*"` returns nothing). `services/kvstore/build.gradle` has no Mockito dependency, so this
test uses a small hand-written fake rather than adding one. Create the file fresh with this
content:

```java
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentedKeyValueStorageTransactionValidatorDecoratorTest {

  /** Records every call made to it; used in place of a mock since this module has no Mockito. */
  private static final class RecordingTransaction implements SegmentedKeyValueStorageTransaction {
    final List<String> calls = new ArrayList<>();

    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      calls.add("put");
    }

    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      calls.add("merge");
    }

    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      calls.add("remove");
    }

    @Override
    public void commit() throws StorageException {
      calls.add("commit");
    }

    @Override
    public void rollback() {
      calls.add("rollback");
    }

    @Override
    public void close() {
      calls.add("close");
    }
  }

  private static final SegmentIdentifier TEST_SEGMENT =
      new SegmentIdentifier() {
        @Override
        public String getName() {
          return "TEST";
        }

        @Override
        public byte[] getId() {
          return new byte[] {1};
        }

        @Override
        public boolean containsStaticData() {
          return false;
        }

        @Override
        public boolean isEligibleToHighSpecFlag() {
          return false;
        }
      };

  @Test
  void mergeDelegatesWhenActiveAndOpen() {
    final RecordingTransaction delegate = new RecordingTransaction();
    final SegmentedKeyValueStorageTransactionValidatorDecorator decorator =
        new SegmentedKeyValueStorageTransactionValidatorDecorator(delegate, () -> false);

    decorator.merge(TEST_SEGMENT, new byte[] {1}, new byte[] {2});

    assertThat(delegate.calls).containsExactly("merge");
  }

  @Test
  void mergeThrowsAfterCommit() {
    final RecordingTransaction delegate = new RecordingTransaction();
    final SegmentedKeyValueStorageTransactionValidatorDecorator decorator =
        new SegmentedKeyValueStorageTransactionValidatorDecorator(delegate, () -> false);

    decorator.commit();

    assertThatThrownBy(() -> decorator.merge(TEST_SEGMENT, new byte[] {1}, new byte[] {2}))
        .isInstanceOf(IllegalStateException.class);
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :services:kvstore:test --tests "*SegmentedKeyValueStorageTransactionValidatorDecoratorTest*"`
Expected: FAIL (compile error — `merge` not implemented on the decorator).

- [ ] **Step 3: Implement `merge()` on the decorator**

Edit `services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedKeyValueStorageTransactionValidatorDecorator.java`,
adding this method after `remove()`:

```java
  @Override
  public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    checkState(active, "Cannot invoke merge() on a completed transaction.");
    checkState(!isClosed.get(), "Cannot invoke merge() on a closed storage.");
    transaction.merge(segmentId, key, value);
  }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :services:kvstore:test --tests "*SegmentedKeyValueStorageTransactionValidatorDecoratorTest*"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/kvstore/src/main/java/org/hyperledger/besu/services/kvstore/SegmentedKeyValueStorageTransactionValidatorDecorator.java \
        services/kvstore/src/test/java/org/hyperledger/besu/services/kvstore/SegmentedKeyValueStorageTransactionValidatorDecoratorTest.java
git commit -m "feat(kvstore): implement merge() pass-through in transaction validator decorator"
```

---

### Task 4: RocksDB — `RocksDBTransaction.merge()` + column family merge-operator wiring

**Files:**
- Modify: `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBTransaction.java`
- Modify: `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorage.java`
- Test: `plugins/rocksdb/src/test/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorageMergeOperatorTest.java` (new)

**Interfaces:**
- Consumes: `merge()` from Task 1; `SegmentIdentifier.usesAppendMergeOperator()` from Task 1.
- Produces: real RocksDB blind-merge behavior for any segment with `usesAppendMergeOperator() == true`.

`Transaction.merge(ColumnFamilyHandle, byte[], byte[])` (tracked) participates in
`OptimisticTransactionDB`'s write-conflict validation just like `put()` — two concurrent
transactions merging the same key would still be flagged as conflicting even though merges are
commutative appends. `Transaction.mergeUntracked(ColumnFamilyHandle, byte[], byte[])` explicitly
skips that conflict check (verified against `rocksdbjni-10.6.2` source:
`org/rocksdb/Transaction.java` — `mergeUntracked`'s javadoc: "Unlike merge(...) no conflict
checking will be performed for this key"). Use `mergeUntracked` — this is exactly the retry-loop
contention this change is meant to reduce (see `Global Constraints` and the migrator's existing
`isOptimisticConflictError` retry logic).

- [ ] **Step 1: Write the failing test**

Create `plugins/rocksdb/src/test/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorageMergeOperatorTest.java`.
This uses the same real construction pattern as
`OptimisticTransactionDBRocksDBColumnarKeyValueStorageTest.createSegmentedStore(Path, List, List)`
(same package, `OptimisticRocksDBColumnarKeyValueStorage` + `RocksDBConfigurationBuilder` +
`NoOpMetricsSystem` + `RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS`) — the concrete engine
`X_BONSAI_ARCHIVE` actually uses:

```java
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.junit.jupiter.api.Test;

class RocksDBColumnarKeyValueStorageMergeOperatorTest {

  private enum MergeTestSegment implements SegmentIdentifier {
    MERGE_SEGMENT(new byte[] {1}, true),
    DEFAULT(new byte[] {0}, false);

    private final byte[] id;
    private final boolean usesAppendMergeOperator;

    MergeTestSegment(final byte[] id, final boolean usesAppendMergeOperator) {
      this.id = id;
      this.usesAppendMergeOperator = usesAppendMergeOperator;
    }

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }

    @Override
    public boolean usesAppendMergeOperator() {
      return usesAppendMergeOperator;
    }
  }

  @Test
  void mergeConcatenatesWithoutDelimiterAndSurvivesCompaction() throws Exception {
    try (SegmentedKeyValueStorage storage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder()
                .databaseDir(Files.createTempDirectory("mergeOperatorTest"))
                .build(),
            List.of(MergeTestSegment.DEFAULT, MergeTestSegment.MERGE_SEGMENT),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final byte[] key = "k".getBytes(StandardCharsets.UTF_8);
      try (SegmentedKeyValueStorageTransaction tx = storage.startTransaction()) {
        tx.merge(MergeTestSegment.MERGE_SEGMENT, key, new byte[] {1, 2, 3});
        tx.commit();
      }
      try (SegmentedKeyValueStorageTransaction tx = storage.startTransaction()) {
        tx.merge(MergeTestSegment.MERGE_SEGMENT, key, new byte[] {4, 5, 6});
        tx.commit();
      }

      assertThat(storage.get(MergeTestSegment.MERGE_SEGMENT, key))
          .contains(new byte[] {1, 2, 3, 4, 5, 6});
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :plugins:rocksdb:test --tests "*RocksDBColumnarKeyValueStorageMergeOperatorTest*"`
Expected: FAIL — `usesAppendMergeOperator()` returns false effectively (no merge operator
configured yet) or compile error on `merge()` not being implemented on `RocksDBTransaction`.

- [ ] **Step 3: Implement `merge()` on `RocksDBTransaction`**

Edit `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBTransaction.java`.
Add the import:

```java
import org.rocksdb.RocksDBException;
```

(already present) — add this method after `put()`:

```java
  @Override
  public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      // mergeUntracked: unlike put(), concurrent merges to the same key are commutative appends,
      // not conflicting writes — tracked merge() would still fail OptimisticTransactionDB's
      // write-conflict validation for two transactions merging the same key, exactly the
      // contention this change is meant to remove.
      innerTx.mergeUntracked(columnFamilyMapper.apply(segmentId), key, value);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        logger.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }
```

- [ ] **Step 4: Wire `usesAppendMergeOperator()` into column family options**

Edit `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorage.java`.
Add the import:

```java
import org.rocksdb.StringAppendOperator;
```

In `createColumnDescriptor` (around line 231), change:

```java
    final var cfOptions =
        new ColumnFamilyOptions()
            .setTtl(0)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setTableFormatConfig(basedTableConfig)
            .setLevelCompactionDynamicLevelBytes(dynamicLevelBytes);
    columnFamilyOptionsList.add(cfOptions);
```

to:

```java
    final var cfOptions =
        new ColumnFamilyOptions()
            .setTtl(0)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setTableFormatConfig(basedTableConfig)
            .setLevelCompactionDynamicLevelBytes(dynamicLevelBytes);
    if (segment.usesAppendMergeOperator()) {
      // Empty-string delimiter performs pure zero-separator concatenation (verified against
      // rocksdbjni 10.6.2), matching the packed fixed-width offset format written via merge().
      cfOptions.setMergeOperator(new StringAppendOperator(""));
    }
    columnFamilyOptionsList.add(cfOptions);
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :plugins:rocksdb:test --tests "*RocksDBColumnarKeyValueStorageMergeOperatorTest*"`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBTransaction.java \
        plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorage.java \
        plugins/rocksdb/src/test/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorageMergeOperatorTest.java
git commit -m "feat(rocksdb): implement untracked merge() and wire StringAppendOperator per segment"
```

---

### Task 5: `RocksDBWriteBatchTransaction` — implement `merge()`

**Files:**
- Modify: `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBWriteBatchTransaction.java`
- Test: `plugins/rocksdb/src/test/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorageMergeOperatorTest.java` (same file created in Task 4)

**Interfaces:**
- Consumes: `merge()` from Task 1; `WriteBatch.merge(ColumnFamilyHandle, byte[], byte[])`
  (confirmed present in `rocksdbjni-10.6.2` via `javap`).
- Produces: `merge()` support for the migration write-batch path (used by
  `BonsaiFlatDbToArchiveMigrator`, Task 13).

- [ ] **Step 1: Write the failing test**

There is no existing dedicated test file for this class today. `RocksDBWriteBatchTransaction` is
what `OptimisticRocksDBColumnarKeyValueStorage.startWriteBatchTransaction()`
(`plugins/rocksdb/src/main/java/.../segmented/OptimisticRocksDBColumnarKeyValueStorage.java:106-112`)
constructs internally, and `SegmentedKeyValueStorage.startWriteBatchTransaction()` is a `default`
interface method any `SegmentedKeyValueStorage` exposes — so the same real construction from
Task 4's test reaches this class via that method. Add this test to the same file created in
Task 4 (`RocksDBColumnarKeyValueStorageMergeOperatorTest.java`):

```java
  @Test
  void writeBatchTransactionMergeAppliesStringAppendConcatenationOnCommit() throws Exception {
    try (SegmentedKeyValueStorage storage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder()
                .databaseDir(Files.createTempDirectory("writeBatchMergeTest"))
                .build(),
            List.of(MergeTestSegment.DEFAULT, MergeTestSegment.MERGE_SEGMENT),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final byte[] key = "k".getBytes(StandardCharsets.UTF_8);
      try (SegmentedKeyValueStorageTransaction tx = storage.startWriteBatchTransaction()) {
        tx.merge(MergeTestSegment.MERGE_SEGMENT, key, new byte[] {1, 2, 3});
        tx.commit();
      }

      assertThat(storage.get(MergeTestSegment.MERGE_SEGMENT, key)).contains(new byte[] {1, 2, 3});
    }
  }
```

(This reuses the `MergeTestSegment` enum and imports already added to that file in Task 4 — no new
file is created for this task.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :plugins:rocksdb:test --tests "*RocksDBColumnarKeyValueStorageMergeOperatorTest.writeBatchTransactionMergeAppliesStringAppendConcatenationOnCommit*"`
Expected: FAIL (compile error — `merge` not implemented on `RocksDBWriteBatchTransaction`).

- [ ] **Step 3: Implement `merge()`**

Edit `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBWriteBatchTransaction.java`,
adding after `put()`:

```java
  @Override
  public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      writeBatch.merge(columnFamilyMapper.apply(segmentId), key, value);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        logger.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :plugins:rocksdb:test --tests "*RocksDBColumnarKeyValueStorageMergeOperatorTest*"`
Expected: PASS (both tests in this file, from Task 4 and this task)

- [ ] **Step 5: Commit**

```bash
git add plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/RocksDBWriteBatchTransaction.java \
        plugins/rocksdb/src/test/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueStorageMergeOperatorTest.java
git commit -m "feat(rocksdb): implement merge() on write-batch transaction"
```

---

### Task 6: New `TRIE_NODE_INDEX_META_ARCHIVE` segment

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java`
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifierTest.java`

**Interfaces:**
- Consumes: `usesAppendMergeOperator()` from Task 1.
- Produces: `KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE` — consumed by all remaining
  tasks in `TrieNodeChangeIndex`, `TrieNodeIndexDropper`, `BonsaiFlatDbToArchiveMigrator`.

- [ ] **Step 1: Add the 7-arg constructor overload and the new segment**

Edit `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java`.

Change the `TRIE_NODE_INDEX_ARCHIVE` entry (currently lines 67–73):

```java
  TRIE_NODE_INDEX_ARCHIVE(
      "TRIE_NODE_INDEX_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      false,
      false,
      false,
      true),
```

to:

```java
  TRIE_NODE_INDEX_ARCHIVE(
      "TRIE_NODE_INDEX_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      false,
      false,
      false,
      true,
      true),
  // Small fixed-width [4B subCount][4B tailCount] value read/written via plain put/get — kept
  // separate from TRIE_NODE_INDEX_ARCHIVE's merge-only packed offset content so the depth-tiered
  // checkpoint's exact previous-mutation-count read never has to resolve accumulated merge
  // operands over the (potentially large) offset list.
  TRIE_NODE_INDEX_META_ARCHIVE(
      "TRIE_NODE_INDEX_META_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      false,
      false,
      false,
      true),
```

Add the field, 7-arg constructor, and `usesAppendMergeOperator()` override. Change:

```java
  private final byte[] id;
  private final EnumSet<DataStorageFormat> formats;
  private final boolean containsStaticData;
  private final boolean eligibleToHighSpecFlag;
  private final boolean staticDataGarbageCollectionEnabled;
  private final boolean cacheIndexAndFilterBlocks;
```

to:

```java
  private final byte[] id;
  private final EnumSet<DataStorageFormat> formats;
  private final boolean containsStaticData;
  private final boolean eligibleToHighSpecFlag;
  private final boolean staticDataGarbageCollectionEnabled;
  private final boolean cacheIndexAndFilterBlocks;
  private final boolean usesAppendMergeOperator;
```

Change the 6-arg constructor:

```java
  KeyValueSegmentIdentifier(
      final byte[] id,
      final EnumSet<DataStorageFormat> formats,
      final boolean containsStaticData,
      final boolean eligibleToHighSpecFlag,
      final boolean staticDataGarbageCollectionEnabled,
      final boolean cacheIndexAndFilterBlocks) {
    this.id = id;
    this.formats = formats;
    this.containsStaticData = containsStaticData;
    this.eligibleToHighSpecFlag = eligibleToHighSpecFlag;
    this.staticDataGarbageCollectionEnabled = staticDataGarbageCollectionEnabled;
    this.cacheIndexAndFilterBlocks = cacheIndexAndFilterBlocks;
  }
```

to:

```java
  KeyValueSegmentIdentifier(
      final byte[] id,
      final EnumSet<DataStorageFormat> formats,
      final boolean containsStaticData,
      final boolean eligibleToHighSpecFlag,
      final boolean staticDataGarbageCollectionEnabled,
      final boolean cacheIndexAndFilterBlocks) {
    this(
        id,
        formats,
        containsStaticData,
        eligibleToHighSpecFlag,
        staticDataGarbageCollectionEnabled,
        cacheIndexAndFilterBlocks,
        false);
  }

  KeyValueSegmentIdentifier(
      final byte[] id,
      final EnumSet<DataStorageFormat> formats,
      final boolean containsStaticData,
      final boolean eligibleToHighSpecFlag,
      final boolean staticDataGarbageCollectionEnabled,
      final boolean cacheIndexAndFilterBlocks,
      final boolean usesAppendMergeOperator) {
    this.id = id;
    this.formats = formats;
    this.containsStaticData = containsStaticData;
    this.eligibleToHighSpecFlag = eligibleToHighSpecFlag;
    this.staticDataGarbageCollectionEnabled = staticDataGarbageCollectionEnabled;
    this.cacheIndexAndFilterBlocks = cacheIndexAndFilterBlocks;
    this.usesAppendMergeOperator = usesAppendMergeOperator;
  }
```

Add the override after `isCacheIndexAndFilterBlocks()`:

```java
  @Override
  public boolean usesAppendMergeOperator() {
    return usesAppendMergeOperator;
  }
```

- [ ] **Step 2: Extend the existing segment-registration test**

Edit `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifierTest.java`.
Change:

```java
  @Test
  void trieNodeArchiveSegmentsAreRegistered() {
    for (String name :
        List.of(
            "TRIE_NODE_HISTORY_ARCHIVE", "TRIE_NODE_INDEX_ARCHIVE", "TRIE_NODE_SUBBLOCK_ARCHIVE")) {
      assertThatCode(() -> KeyValueSegmentIdentifier.valueOf(name)).doesNotThrowAnyException();
    }
  }
```

to:

```java
  @Test
  void trieNodeArchiveSegmentsAreRegistered() {
    for (String name :
        List.of(
            "TRIE_NODE_HISTORY_ARCHIVE",
            "TRIE_NODE_INDEX_ARCHIVE",
            "TRIE_NODE_INDEX_META_ARCHIVE",
            "TRIE_NODE_SUBBLOCK_ARCHIVE")) {
      assertThatCode(() -> KeyValueSegmentIdentifier.valueOf(name)).doesNotThrowAnyException();
    }
  }

  @Test
  void onlyIndexContentSegmentUsesAppendMergeOperator() {
    assertThat(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE.usesAppendMergeOperator()).isTrue();
    assertThat(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE.usesAppendMergeOperator())
        .isFalse();
    assertThat(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE.usesAppendMergeOperator())
        .isFalse();
  }
```

Add the import: `import static org.assertj.core.api.Assertions.assertThat;` (alongside the existing
`assertThatCode` static import).

- [ ] **Step 3: Run the tests to verify they fail, then pass**

Run: `./gradlew :ethereum:core:test --tests "*KeyValueSegmentIdentifierTest*"`
Expected: FAILs first if run before Step 1's production edit is complete (unknown enum constant /
missing method); PASS once Steps 1–2 are both applied.

- [ ] **Step 4: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifier.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/storage/keyvalue/KeyValueSegmentIdentifierTest.java
git commit -m "feat(bonsai-archive): add TRIE_NODE_INDEX_META_ARCHIVE segment"
```

---

### Task 7: `TrieNodeChangeIndex` — metadata read/write helpers

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java`

**Interfaces:**
- Consumes: `TRIE_NODE_INDEX_META_ARCHIVE` from Task 6.
- Produces: package-private `record IndexMetadata(int subCount, int tailCount)` with `EMPTY`
  constant, package-private static `readMetadataValue(byte[])`/`writeMetadataValue(int, int)` —
  consumed by Tasks 8–11 (same class) and Task 12 (`TrieNodeIndexDropper`, same package).

This task only adds the new helpers (unused by production code yet) and direct unit tests for
their round-trip correctness — it doesn't touch `append()`/`appendAndGetPreviousCount()` or any
other method yet, keeping this step isolated and low-risk.

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeChangeIndexTest.java` (these call package-private/private static methods — since
the test class is in the same package, package-private works; for the `private static` methods
use a small reflective-free approach by testing indirectly is not possible for `private`, so make
`readMetadataValue`/`writeMetadataValue` package-private, matching the visibility style already
used for `sliceHead`/`sliceTail` in this file):

```java
  @Test
  void metadataRoundTripsThroughBytes() {
    final byte[] bytes = TrieNodeChangeIndex.writeMetadataValue(3, 517);
    final TrieNodeChangeIndex.IndexMetadata metadata = TrieNodeChangeIndex.readMetadataValue(bytes);
    assertThat(metadata.subCount()).isEqualTo(3);
    assertThat(metadata.tailCount()).isEqualTo(517);
  }

  @Test
  void metadataDefaultsToEmptyForShortBytes() {
    final TrieNodeChangeIndex.IndexMetadata metadata =
        TrieNodeChangeIndex.readMetadataValue(new byte[] {1, 2, 3});
    assertThat(metadata.subCount()).isZero();
    assertThat(metadata.tailCount()).isZero();
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: FAIL — compile error, `IndexMetadata`/`writeMetadataValue`/`readMetadataValue` don't exist.

- [ ] **Step 3: Add the metadata helpers, replacing `IndexValue`/`readIndexValue`/`writeIndexValue`**

Edit `TrieNodeChangeIndex.java`. Replace the entire block from the `// Index value helpers` comment
through the end of `writeIndexValue` (lines 970–1027) with:

```java
  // ---------------------------------------------------------------------------
  // Index metadata helpers (format: [4B subCount BE][4B tailCount BE])
  // ---------------------------------------------------------------------------

  /** Number of bytes in a serialised {@link IndexMetadata} value. */
  private static final int METADATA_BYTES = 8;

  /**
   * Parsed representation of a value stored in {@code TRIE_NODE_INDEX_META_ARCHIVE}: the number of
   * sub-blocks already stored in {@code TRIE_NODE_SUBBLOCK_ARCHIVE} for a {@code (naturalKey,
   * rangeId)} pair, and the number of entries currently in the tail (the packed offset list stored
   * in {@code TRIE_NODE_INDEX_ARCHIVE}, which no longer carries this count itself).
   */
  record IndexMetadata(int subCount, int tailCount) {
    static final IndexMetadata EMPTY = new IndexMetadata(0, 0);
  }

  /**
   * Parses an {@link IndexMetadata} from raw {@code TRIE_NODE_INDEX_META_ARCHIVE} bytes. Returns
   * {@link IndexMetadata#EMPTY} for missing or short (corrupt) values.
   *
   * @param raw the raw bytes from {@code TRIE_NODE_INDEX_META_ARCHIVE}
   * @return the parsed metadata, or {@link IndexMetadata#EMPTY} if {@code raw} is too short
   */
  static IndexMetadata readMetadataValue(final byte[] raw) {
    if (raw.length < METADATA_BYTES) {
      return IndexMetadata.EMPTY;
    }
    final int subCount =
        ((raw[0] & 0xFF) << 24) | ((raw[1] & 0xFF) << 16) | ((raw[2] & 0xFF) << 8) | (raw[3] & 0xFF);
    final int tailCount =
        ((raw[4] & 0xFF) << 24) | ((raw[5] & 0xFF) << 16) | ((raw[6] & 0xFF) << 8) | (raw[7] & 0xFF);
    return new IndexMetadata(subCount, tailCount);
  }

  /**
   * Serialises a sub-block count and tail entry count into the 8-byte {@code
   * TRIE_NODE_INDEX_META_ARCHIVE} value format.
   *
   * @param subCount the number of existing sub-blocks
   * @param tailCount the number of entries currently in the tail content value
   * @return the serialised 8-byte value
   */
  static byte[] writeMetadataValue(final int subCount, final int tailCount) {
    final byte[] result = new byte[METADATA_BYTES];
    result[0] = (byte) ((subCount >>> 24) & 0xFF);
    result[1] = (byte) ((subCount >>> 16) & 0xFF);
    result[2] = (byte) ((subCount >>> 8) & 0xFF);
    result[3] = (byte) (subCount & 0xFF);
    result[4] = (byte) ((tailCount >>> 24) & 0xFF);
    result[5] = (byte) ((tailCount >>> 16) & 0xFF);
    result[6] = (byte) ((tailCount >>> 8) & 0xFF);
    result[7] = (byte) (tailCount & 0xFF);
    return result;
  }

  /**
   * Reads the current {@link IndexMetadata} for {@code indexKey}, checking the write-through
   * {@link #indexCache} (which now caches metadata bytes, not full content) before falling back to
   * committed storage. Honours fresh-migration bloom short-circuiting like the old content read
   * did.
   *
   * @param indexKey the range key ({@link ArchiveNodeKey#rangeKey})
   * @param indexKeyBytes {@code indexKey.toArrayUnsafe()}, passed in to avoid re-deriving it
   * @return the current metadata, or {@link IndexMetadata#EMPTY} if absent
   */
  private IndexMetadata readMetadataForWrite(final Bytes indexKey, final byte[] indexKeyBytes) {
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      return readMetadataValue(cached);
    }
    if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
      return IndexMetadata.EMPTY;
    }
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes)
        .map(TrieNodeChangeIndex::readMetadataValue)
        .orElse(IndexMetadata.EMPTY);
  }

  /**
   * Returns the metadata for {@code (naturalKey, rangeId)} directly from committed storage,
   * bypassing {@link #indexCache}. Used by read-only query paths that must not be affected by
   * uncommitted write-path caching.
   *
   * @param indexKeyBytes the range key bytes ({@link ArchiveNodeKey#rangeKey})
   * @return the current metadata, or {@link IndexMetadata#EMPTY} if absent
   */
  private IndexMetadata readCommittedMetadata(final byte[] indexKeyBytes) {
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes)
        .map(TrieNodeChangeIndex::readMetadataValue)
        .orElse(IndexMetadata.EMPTY);
  }

  /** Packs a single within-range offset into its 3-byte big-endian merge-operand form. */
  private static byte[] threeByteOffset(final int offset) {
    return new byte[] {(byte) ((offset >> 16) & 0xFF), (byte) ((offset >> 8) & 0xFF), (byte) (offset & 0xFF)};
  }
```

Update the two call sites of `sliceHead`/`sliceTail` further down (they're unchanged, keep them as
they are — they don't reference `IndexValue`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: Some pre-existing tests in this file will now FAIL to compile because they reference the
now-deleted `IndexValue`/`readIndexValue`/`writeIndexValue` (search for these names:
`grep -n "IndexValue\|readIndexValue\|writeIndexValue" TrieNodeChangeIndexTest.java`). This is
expected at this point in the plan — Tasks 8–11 fix the production call sites that also reference
these; leave any still-broken test references as-is for now, they'll be fixed as part of those
tasks. If the new `metadataRoundTripsThroughBytes`/`metadataDefaultsToEmptyForShortBytes` tests
themselves pass in isolation, that confirms this task; run them specifically:

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest.metadataRoundTripsThroughBytes*" --tests "*TrieNodeChangeIndexTest.metadataDefaultsToEmptyForShortBytes*"`
Expected: PASS (the rest of the file's compile errors, if any, are addressed in Task 8).

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "feat(bonsai-archive): add split content/metadata helpers to TrieNodeChangeIndex"
```

---

### Task 8: `TrieNodeChangeIndex` — rewrite `append()` and `appendAndGetPreviousCount()`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java`

**Interfaces:**
- Consumes: `IndexMetadata`/`readMetadataForWrite`/`writeMetadataValue`/`threeByteOffset` from Task 7;
  `SegmentedKeyValueStorageTransaction.merge()` from Task 1.
- Produces: `append()`/`appendAndGetPreviousCount()` unchanged public signatures, new internal
  implementation via a shared `writeAndGetPreviousMetadata(...)` helper — Task 11 (buffered path)
  and Task 12 (`TrieNodeIndexDropper`) rely on the same content/metadata split but not on this
  specific helper.

This is the highest-risk task in the plan — it changes the semantics callers depend on
(`previousCount` correctness for the depth-tiered checkpoint decision) even though the public
signatures don't change. Existing tests in `TrieNodeChangeIndexTest` that assert on raw
`TRIE_NODE_INDEX_ARCHIVE` bytes including a subCount prefix must be updated (search
`grep -n "SUBCOUNT_BYTES\|readIndexValue\|IndexValue\b" TrieNodeChangeIndexTest.java` and fix each
hit to read `TRIE_NODE_INDEX_META_ARCHIVE` for subCount/tailCount and `TRIE_NODE_INDEX_ARCHIVE` for
raw packed content with no prefix, following the same transformation shown in Step 3 below).

- [ ] **Step 1: Write the failing tests**

Add to `TrieNodeChangeIndexTest.java`:

```java
  @Test
  void appendWritesContentAndMetadataSeparately() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);
    final var tx = kv.startTransaction();
    index.append(tx, KEY, 5);
    tx.commit();

    final Bytes indexKey = ArchiveNodeKey.rangeKey(KEY, 0);
    assertThat(kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe()))
        .contains(new byte[] {0, 0, 5}); // pure packed content, no subCount prefix
    assertThat(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe()))
        .contains(TrieNodeChangeIndex.writeMetadataValue(0, 1));
  }

  @Test
  void appendAndGetPreviousCountReturnsCountBeforeThisWrite() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);

    final var tx1 = kv.startTransaction();
    final long first = index.appendAndGetPreviousCount(tx1, KEY, 10);
    tx1.commit();
    assertThat(first).isZero();

    final var tx2 = kv.startTransaction();
    final long second = index.appendAndGetPreviousCount(tx2, KEY, 20);
    tx2.commit();
    assertThat(second).isEqualTo(1L);

    final var tx3 = kv.startTransaction();
    final long third = index.appendAndGetPreviousCount(tx3, KEY, 30);
    tx3.commit();
    assertThat(third).isEqualTo(2L);
  }

  @Test
  void appendTriggersSubBlockSplitAtThreshold() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    // subBlockThreshold=4, subBlockSplitAt=2: the 5th append (list size would become 5 > 4)
    // triggers a split moving the first 2 entries into a sub-block.
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000, 4, 2);

    for (int block = 1; block <= 5; block++) {
      final var tx = kv.startTransaction();
      index.append(tx, KEY, block);
      tx.commit();
    }

    final Bytes indexKey = ArchiveNodeKey.rangeKey(KEY, 0);
    final TrieNodeChangeIndex.IndexMetadata metadata =
        TrieNodeChangeIndex.readMetadataValue(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe())
                .orElseThrow());
    assertThat(metadata.subCount()).isEqualTo(1);
    assertThat(metadata.tailCount()).isEqualTo(3); // 5 entries - 2 split off

    final Bytes subKey = ArchiveNodeKey.subBlockKey(KEY, 0, 0);
    assertThat(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKey.toArrayUnsafe()))
        .contains(new byte[] {0, 0, 1, 0, 0, 2}); // blocks 1, 2 (the oldest)

    final Optional<RangeRelativeOffsetList> full = index.readRangeList(KEY, 0);
    assertThat(full).isPresent();
    assertThat(full.get().size()).isEqualTo(5);
  }
```

Add these imports to the test file if not already present:
`import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;` (already
imported per the existing file), and confirm `Optional` is imported (`java.util.Optional`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: FAIL — `appendWritesContentAndMetadataSeparately` fails because `append()` still writes
the combined `[subCount][content]` format to `TRIE_NODE_INDEX_ARCHIVE` and nothing to
`TRIE_NODE_INDEX_META_ARCHIVE`.

- [ ] **Step 3: Rewrite `append()` and `appendAndGetPreviousCount()`**

Replace the entire body of `append()` (lines 690–774) with:

```java
  public void append(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    final long rangeId = block / rangeSize;
    final int offset = (int) (block - rangeId * rangeSize);
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    if (buffer != null) {
      // Buffered path: accumulate offset in memory; no storage read or write.
      maybeDrainPeriodically();
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
        if (prefetchExecutor != null
            && !e.baseLoaded
            && (sessionWrittenKeys == null || sessionWrittenKeys.mightContain(indexKeyBytes))) {
          enqueueBasePrefetch(indexKey);
        }
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      e.pending.add(offset);
      return;
    }

    writeAndGetPreviousMetadata(tx, naturalKey, rangeId, indexKey, indexKeyBytes, offset);
  }

  /**
   * Core write shared by {@link #append} and {@link #appendAndGetPreviousCount}: reads the current
   * (cheap, fixed-width) metadata, blind-merges {@code offset} onto the content key in the common
   * case, and performs the (rare) sub-block split — which does require reading the actual content
   * bytes — when the new tail count would exceed {@link #subBlockThreshold}.
   *
   * @return the metadata as it was <em>before</em> this write (used by {@link
   *     #appendAndGetPreviousCount} to compute the previous mutation count; ignored by {@link
   *     #append})
   */
  private IndexMetadata writeAndGetPreviousMetadata(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long rangeId,
      final Bytes indexKey,
      final byte[] indexKeyBytes,
      final int offset) {
    final IndexMetadata before = readMetadataForWrite(indexKey, indexKeyBytes);
    final int newTailCount = before.tailCount() + 1;

    if (newTailCount > subBlockThreshold) {
      final byte[] rawContent =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes).orElse(new byte[0]);
      RangeRelativeOffsetList current =
          (rawContent.length == 0
                  ? RangeRelativeOffsetList.empty()
                  : RangeRelativeOffsetList.fromBytes(Bytes.wrap(rawContent)))
              .append(offset);
      final RangeRelativeOffsetList head = sliceHead(current, subBlockSplitAt);
      final RangeRelativeOffsetList tail = sliceTail(current, subBlockSplitAt);
      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, before.subCount());
      tx.put(
          KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE,
          subKey.toArrayUnsafe(),
          head.toBytes().toArrayUnsafe());
      // Fresh base value for content: resets the merge-operand chain for this key.
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, tail.toBytes().toArrayUnsafe());
      final byte[] newMetadata = writeMetadataValue(before.subCount() + 1, tail.size());
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes, newMetadata);
      indexCache.put(indexKey, newMetadata);
    } else {
      tx.merge(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, threeByteOffset(offset));
      final byte[] newMetadata = writeMetadataValue(before.subCount(), newTailCount);
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes, newMetadata);
      indexCache.put(indexKey, newMetadata);
    }
    if (sessionWrittenKeys != null) {
      sessionWrittenKeys.put(indexKeyBytes);
    }
    return before;
  }
```

Replace the entire body of `appendAndGetPreviousCount()` (lines 792–887) with:

```java
  public long appendAndGetPreviousCount(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    final long rangeId = block / rangeSize;
    final int offset = (int) (block - rangeId * rangeSize);
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    final long earlierCount = earlierRangeCount(naturalKey, rangeId, indexKey);

    if (buffer != null) {
      maybeDrainPeriodically();
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
        if (prefetchExecutor != null
            && !e.baseLoaded
            && (sessionWrittenKeys == null || sessionWrittenKeys.mightContain(indexKeyBytes))) {
          enqueueBasePrefetch(indexKey);
        }
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      final long previousCount =
          earlierCount
              + (long) e.baseSubCount * DEFAULT_SUBBLOCK_SPLIT_AT
              + e.baseTail.size() // unchanged field name for now — see note below
              + e.pending.size();
      e.pending.add(offset);
      return previousCount;
    }

    final IndexMetadata before =
        writeAndGetPreviousMetadata(tx, naturalKey, rangeId, indexKey, indexKeyBytes, offset);
    return earlierCount + (long) before.subCount() * DEFAULT_SUBBLOCK_SPLIT_AT + before.tailCount();
  }
```

Note: the buffered branches of both `append()` and `appendAndGetPreviousCount()` above are
untouched, copy-pasted verbatim from the original code (still referencing `e.baseTail`, which
still exists on `BufferedEntry` as of this task) — only the non-buffered branches change in this
task. Task 11 rewrites `BufferedEntry` to replace `baseTail` (a `RangeRelativeOffsetList`) with
`baseTailCount` (an `int`), and updates this exact `e.baseTail.size()` expression to
`e.baseTailCount` as part of that rewrite.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: The four new tests from Step 1 PASS. Other pre-existing tests that directly assert on
the old combined `TRIE_NODE_INDEX_ARCHIVE` format (subCount prefix included) will now FAIL — fix
each by applying the same transformation used in Step 1's tests: read
`TRIE_NODE_INDEX_META_ARCHIVE` via `TrieNodeChangeIndex.readMetadataValue(...)` for subCount/tail
count assertions, and read `TRIE_NODE_INDEX_ARCHIVE` directly (no prefix) for content assertions.
Do this for every failing test before moving on — do not skip or disable any.

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "feat(bonsai-archive): merge-append TrieNodeChangeIndex content, RMW only metadata"
```

---

### Task 9: `TrieNodeChangeIndex` — rewrite count/earlier-range helpers

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java`

**Interfaces:**
- Consumes: `IndexMetadata`/`readMetadataValue` from Task 7.
- Produces: `earlierRangeCount`, `countMutationsUpTo`, `countMutationsInEarlierRanges` unchanged
  public/package signatures and return values, reading `TRIE_NODE_INDEX_META_ARCHIVE` instead of
  manually parsing the old combined format.

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeChangeIndexTest.java`:

```java
  @Test
  void countMutationsUpToSpansMultipleRanges() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx = kv.startTransaction();
    index.append(tx, KEY, 500_000); // range 0
    index.append(tx, KEY, 1_500_000); // range 1
    index.append(tx, KEY, 1_600_000); // range 1
    tx.commit();

    assertThat(index.countMutationsUpTo(KEY, 400_000)).isZero();
    assertThat(index.countMutationsUpTo(KEY, 999_999)).isEqualTo(1L);
    assertThat(index.countMutationsUpTo(KEY, 1_600_000)).isEqualTo(3L);
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest.countMutationsUpToSpansMultipleRanges*"`
Expected: This particular test may already PASS if `countMutationsUpTo`'s old byte-parsing logic
happens to still work against the new (unprefixed) content format by coincidence — check first. If
it currently reads `TRIE_NODE_INDEX_ARCHIVE` and tries to strip a 4-byte subCount prefix that no
longer exists there, it will silently compute wrong (too-small) counts rather than throwing,
because the raw content bytes no longer have a subCount to strip — the first 4 content bytes would
be misread as subCount. Confirm this test FAILS (wrong count, not a compile error) before
proceeding; if it happens to pass, add a case with `subCount > 0` (many appends past the split
threshold) to force a real divergence, using the same `subBlockThreshold=4, subBlockSplitAt=2`
constructor pattern from Task 8's `appendTriggersSubBlockSplitAtThreshold` test.

- [ ] **Step 3: Rewrite the three counting methods**

Replace `earlierRangeCount` (lines 901–926) with:

```java
  private long earlierRangeCount(final Bytes naturalKey, final long rangeId, final Bytes cacheKey) {
    if (rangeId == 0) {
      return 0L;
    }
    final Long memoised = earlierRangeCountCache.get(cacheKey);
    if (memoised != null) {
      return memoised;
    }
    long earlierCount = 0L;
    for (long r = 0; r < rangeId; r++) {
      final Bytes rKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final IndexMetadata metadata = readCommittedMetadata(rKey.toArrayUnsafe());
      earlierCount += (long) metadata.subCount() * DEFAULT_SUBBLOCK_SPLIT_AT + metadata.tailCount();
    }
    earlierRangeCountCache.put(cacheKey, earlierCount);
    return earlierCount;
  }
```

Replace `countMutationsUpTo` (lines 943–968) with:

```java
  public long countMutationsUpTo(final Bytes naturalKey, final long block) {
    if (block < 0) {
      return 0L;
    }
    final long maxRangeId = block / rangeSize;
    long total = 0L;
    for (long r = 0; r <= maxRangeId; r++) {
      final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final IndexMetadata metadata = readCommittedMetadata(indexKey.toArrayUnsafe());
      total += (long) metadata.subCount() * DEFAULT_SUBBLOCK_SPLIT_AT + metadata.tailCount();
    }
    return total;
  }
```

Replace `countMutationsInEarlierRanges` (lines 1208–1230) with:

```java
  int countMutationsInEarlierRanges(final Bytes naturalKey, final long rangeId) {
    if (rangeId <= 0) {
      return 0;
    }
    int total = 0;
    for (long r = 0; r < rangeId; r++) {
      final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final IndexMetadata metadata = readCommittedMetadata(indexKey.toArrayUnsafe());
      total += metadata.subCount() * DEFAULT_SUBBLOCK_SPLIT_AT + metadata.tailCount();
    }
    return total;
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: PASS (including any other pre-existing tests of these three methods — fix any that still
assert the old prefixed-byte format directly).

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "feat(bonsai-archive): read mutation counts from split metadata CF"
```

---

### Task 10: `TrieNodeChangeIndex` — rewrite content-reading query methods

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java`

**Interfaces:**
- Consumes: `IndexMetadata`/`readCommittedMetadata` from Tasks 7/9.
- Produces: `assembleFullRangeList`, `hasChangeAboveFloor`, `latestChangeInRange` unchanged
  signatures/behavior, reading split content+metadata.

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeChangeIndexTest.java` (uses the same small-threshold constructor as Task 8 to force
a sub-block, exercising the slow path of `assembleFullRangeList`/`latestChangeInRange`):

```java
  @Test
  void latestChangeBlockFindsEntryAfterSubBlockSplit() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000, 4, 2);

    for (int block = 1; block <= 5; block++) {
      final var tx = kv.startTransaction();
      index.append(tx, KEY, block);
      tx.commit();
    }

    // Block 1 was split into the sub-block (oldest entries) — must still be found.
    assertThat(index.latestChangeBlock(KEY, 1)).contains(1L);
    assertThat(index.latestChangeBlock(KEY, 2)).contains(2L);
    assertThat(index.latestChangeBlock(KEY, 5)).contains(5L);
  }

  @Test
  void modifiedAfterDetectsChangeInTailOnly() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);
    final var tx = kv.startTransaction();
    index.append(tx, KEY, 100);
    tx.commit();

    assertThat(index.modifiedAfter(KEY, 50, 200)).isTrue();
    assertThat(index.modifiedAfter(KEY, 150, 200)).isFalse();
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest.latestChangeBlockFindsEntryAfterSubBlockSplit*" --tests "*TrieNodeChangeIndexTest.modifiedAfterDetectsChangeInTailOnly*"`
Expected: FAIL or wrong result — these methods still parse `TRIE_NODE_INDEX_ARCHIVE` bytes
assuming a 4-byte subCount prefix that Task 8 already stopped writing there.

- [ ] **Step 3: Rewrite the three methods**

Replace `assembleFullRangeList` (lines 1091–1136) with:

```java
  private Optional<RangeRelativeOffsetList> assembleFullRangeList(
      final Bytes naturalKey, final long rangeId) {
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();
    final Optional<byte[]> contentRaw =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes);
    if (contentRaw.isEmpty()) {
      return Optional.empty();
    }
    final RangeRelativeOffsetList tail =
        contentRaw.get().length == 0
            ? RangeRelativeOffsetList.empty()
            : RangeRelativeOffsetList.fromBytes(Bytes.wrap(contentRaw.get()));
    final int subCount = readCommittedMetadata(indexKeyBytes).subCount();

    if (subCount == 0) {
      return Optional.of(tail);
    }

    final List<byte[]> subKeys = new ArrayList<>(subCount);
    for (int subId = 0; subId < subCount; subId++) {
      subKeys.add(ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subId).toArrayUnsafe());
    }
    final List<Optional<byte[]>> subRaws =
        storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKeys);

    final List<Bytes> chunks = new ArrayList<>(subCount + 1);
    for (final Optional<byte[]> subRaw : subRaws) {
      if (subRaw.isEmpty()) {
        continue;
      }
      chunks.add(Bytes.wrap(subRaw.get()));
    }
    chunks.add(tail.toBytes());
    return Optional.of(RangeRelativeOffsetList.concat(chunks));
  }
```

Replace `hasChangeAboveFloor` (lines 1327–1349) with:

```java
  private boolean hasChangeAboveFloor(
      final Bytes naturalKey, final long rangeId, final int floor, final int maxOffset) {
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe())
        .map(
            bytes -> {
              final RangeRelativeOffsetList tail =
                  bytes.length == 0
                      ? RangeRelativeOffsetList.empty()
                      : RangeRelativeOffsetList.fromBytes(Bytes.wrap(bytes));
              return tail.latestLeq(maxOffset).stream().anyMatch(last -> last > floor);
            })
        .orElse(false);
  }
```

Replace `latestChangeInRange` (lines 1400–1446) with:

```java
  private Optional<Long> latestChangeInRange(
      final Bytes naturalKey, final long rangeId, final int withinRangeCeil) {
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();
    final Optional<byte[]> contentRaw =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes);
    if (contentRaw.isEmpty()) {
      return Optional.empty();
    }
    final RangeRelativeOffsetList tail =
        contentRaw.get().length == 0
            ? RangeRelativeOffsetList.empty()
            : RangeRelativeOffsetList.fromBytes(Bytes.wrap(contentRaw.get()));

    final OptionalInt tailHit = tail.latestLeq(withinRangeCeil);
    if (tailHit.isPresent()) {
      return Optional.of(rangeId * rangeSize + tailHit.getAsInt());
    }

    final int subCount = readCommittedMetadata(indexKeyBytes).subCount();
    for (int subId = subCount - 1; subId >= 0; subId--) {
      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subId);
      final Optional<byte[]> subRaw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKey.toArrayUnsafe());
      if (subRaw.isEmpty()) {
        continue;
      }
      final RangeRelativeOffsetList subList = RangeRelativeOffsetList.fromBytes(Bytes.wrap(subRaw.get()));
      final OptionalInt subHit = subList.latestLeq(withinRangeCeil);
      if (subHit.isPresent()) {
        return Optional.of(rangeId * rangeSize + subHit.getAsInt());
      }
    }
    return Optional.empty();
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "feat(bonsai-archive): read content/metadata separately in query paths"
```

---

### Task 11: `TrieNodeChangeIndex` — rewrite buffered/migration path

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java`

**Interfaces:**
- Consumes: `IndexMetadata`/`writeMetadataValue`/`readMetadataValue` from Task 7; `merge()` from Task 1.
- Produces: `BufferedEntry.baseTailCount` (replaces `baseTail`) — this is the field Task 8 already
  references in `appendAndGetPreviousCount`'s buffered branch's final form.

This is the second-highest-risk task: it changes `flushBuffer`'s write pattern to batch multiple
pending offsets into a single `merge()` call per dirty key, only falling back to a real content
read when a split boundary is actually crossed mid-batch.

- [ ] **Step 1: Write the failing test**

Add to `TrieNodeChangeIndexTest.java`:

```java
  @Test
  void bufferedFlushMergesAllPendingOffsetsInOneOperand() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);
    index.beginBuffered();
    final long p1 = index.appendAndGetPreviousCount(null, KEY, 1);
    final long p2 = index.appendAndGetPreviousCount(null, KEY, 2);
    final long p3 = index.appendAndGetPreviousCount(null, KEY, 3);
    assertThat(p1).isZero();
    assertThat(p2).isEqualTo(1L);
    assertThat(p3).isEqualTo(2L);

    final var tx = kv.startTransaction();
    index.flushBuffer(tx);
    tx.commit();

    final Bytes indexKey = ArchiveNodeKey.rangeKey(KEY, 0);
    assertThat(kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe()))
        .contains(new byte[] {0, 0, 1, 0, 0, 2, 0, 0, 3});
    assertThat(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe()))
        .contains(TrieNodeChangeIndex.writeMetadataValue(0, 3));
  }

  @Test
  void bufferedFlushSplitsMidBatchWhenThresholdCrossed() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    // threshold=4, splitAt=2: appending blocks 1..5 in one buffered batch crosses the threshold
    // on the 5th pending offset.
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000, 4, 2);
    index.beginBuffered();
    for (int block = 1; block <= 5; block++) {
      index.append(null, KEY, block);
    }
    final var tx = kv.startTransaction();
    index.flushBuffer(tx);
    tx.commit();

    final Bytes indexKey = ArchiveNodeKey.rangeKey(KEY, 0);
    final TrieNodeChangeIndex.IndexMetadata metadata =
        TrieNodeChangeIndex.readMetadataValue(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe())
                .orElseThrow());
    assertThat(metadata.subCount()).isEqualTo(1);
    assertThat(metadata.tailCount()).isEqualTo(3);

    final Bytes subKey = ArchiveNodeKey.subBlockKey(KEY, 0, 0);
    assertThat(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKey.toArrayUnsafe()))
        .contains(new byte[] {0, 0, 1, 0, 0, 2});

    assertThat(index.readRangeList(KEY, 0).orElseThrow().size()).isEqualTo(5);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest.bufferedFlushMergesAllPendingOffsetsInOneOperand*" --tests "*TrieNodeChangeIndexTest.bufferedFlushSplitsMidBatchWhenThresholdCrossed*"`
Expected: FAIL — `flushBuffer` still writes the old combined-prefix format via `tx.put`, one write
per offset consumed sequentially, and `BufferedEntry` doesn't have `baseTailCount` (compile error,
since Task 8 already introduced a reference to it in `appendAndGetPreviousCount`'s buffered branch
placeholder note — resolve that now).

- [ ] **Step 3: Rewrite `BufferedEntry`, `initBufferedEntry`, and `flushBuffer`**

Replace the `BufferedEntry` class (lines 152–174) with:

```java
  /** Accumulated index state for a single {@code (naturalKey, rangeId)} within a batch. */
  private static final class BufferedEntry {
    final Bytes naturalKey;
    final long rangeId;
    int baseSubCount;
    int baseTailCount;

    /**
     * {@code true} once {@link #baseSubCount} and {@link #baseTailCount} have been populated
     * (either from {@link #indexCache} on first touch or via the bulk {@link #flushBuffer}
     * multiGet). When {@code false} the fields hold zero defaults and must be loaded before the
     * batch is written.
     */
    boolean baseLoaded;

    final List<Integer> pending = new ArrayList<>();

    BufferedEntry(final Bytes naturalKey, final long rangeId) {
      this.naturalKey = naturalKey;
      this.rangeId = rangeId;
    }
  }
```

Now go back to `appendAndGetPreviousCount` (from Task 8) and change its buffered branch's
`e.baseTail.size() // unchanged field name for now — see note below` line to `e.baseTailCount`
(also delete the now-stale trailing comment):

```java
      final long previousCount =
          earlierCount
              + (long) e.baseSubCount * DEFAULT_SUBBLOCK_SPLIT_AT
              + e.baseTailCount
              + e.pending.size();
```

Replace `initBufferedEntry` (lines 573–585) with:

```java
  private BufferedEntry initBufferedEntry(
      final Bytes indexKey, final Bytes naturalKey, final long rangeId) {
    final BufferedEntry e = new BufferedEntry(naturalKey, rangeId);
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      final IndexMetadata metadata = readMetadataValue(cached);
      e.baseSubCount = metadata.subCount();
      e.baseTailCount = metadata.tailCount();
      e.baseLoaded = true;
    }
    return e;
  }
```

Replace `flushBuffer` (lines 370–459) with:

```java
  public void flushBuffer(final SegmentedKeyValueStorageTransaction tx) {
    if (buffer == null) {
      return;
    }

    drainPrefetch();

    // ── Phase 1: bulk-load metadata for entries not found in indexCache at first touch ────────
    final List<Bytes> missKeys = new ArrayList<>();
    final List<byte[]> missKeyBytes = new ArrayList<>();
    for (final Map.Entry<Bytes, BufferedEntry> entry : buffer.entrySet()) {
      final BufferedEntry be = entry.getValue();
      if (!be.baseLoaded) {
        final byte[] indexKeyBytes = entry.getKey().toArrayUnsafe();
        final Optional<byte[]> staged = prefetchedBase.get(entry.getKey());
        if (staged != null) {
          be.baseLoaded = true;
          prefetchBaseHits.incrementAndGet();
          staged.ifPresent(
              bytes -> {
                final IndexMetadata metadata = readMetadataValue(bytes);
                be.baseSubCount = metadata.subCount();
                be.baseTailCount = metadata.tailCount();
                indexCache.put(entry.getKey(), bytes);
              });
        } else if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
          be.baseLoaded = true;
        } else {
          missKeys.add(entry.getKey());
          missKeyBytes.add(indexKeyBytes);
        }
      }
    }
    if (!missKeys.isEmpty()) {
      final List<Optional<byte[]>> results =
          storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, missKeyBytes);
      for (int i = 0; i < missKeys.size(); i++) {
        final Bytes indexKey = missKeys.get(i);
        final Optional<byte[]> raw = results.get(i);
        final BufferedEntry be = buffer.get(indexKey);
        be.baseLoaded = true;
        raw.ifPresent(
            bytes -> {
              final IndexMetadata metadata = readMetadataValue(bytes);
              be.baseSubCount = metadata.subCount();
              be.baseTailCount = metadata.tailCount();
              indexCache.put(indexKey, bytes);
            });
      }
    }

    // ── Phase 2: merge pending offsets (one operand per dirty key in the common case) ─────────
    for (final Map.Entry<Bytes, BufferedEntry> entry : buffer.entrySet()) {
      final Bytes indexKey = entry.getKey();
      final byte[] indexKeyBytes = indexKey.toArrayUnsafe();
      final BufferedEntry be = entry.getValue();
      final int n = be.pending.size();
      if (n == 0) {
        continue;
      }

      int subCount = be.baseSubCount;
      int tailCount = be.baseTailCount;
      int flushedSoFar = 0;
      int pendingIdx = 0;
      RangeRelativeOffsetList currentTail = null;

      while (pendingIdx < n) {
        final int newTailCount = tailCount + 1;
        if (newTailCount > subBlockThreshold) {
          if (currentTail == null) {
            final byte[] existingContent =
                storage
                    .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes)
                    .orElse(new byte[0]);
            currentTail =
                existingContent.length == 0
                    ? RangeRelativeOffsetList.empty()
                    : RangeRelativeOffsetList.fromBytes(Bytes.wrap(existingContent));
            for (int i = flushedSoFar; i <= pendingIdx; i++) {
              currentTail = currentTail.append(be.pending.get(i));
            }
          } else {
            currentTail = currentTail.append(be.pending.get(pendingIdx));
          }
          final RangeRelativeOffsetList head = sliceHead(currentTail, subBlockSplitAt);
          final RangeRelativeOffsetList tail = sliceTail(currentTail, subBlockSplitAt);
          final Bytes subKey = ArchiveNodeKey.subBlockKey(be.naturalKey, be.rangeId, subCount);
          tx.put(
              KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE,
              subKey.toArrayUnsafe(),
              head.toBytes().toArrayUnsafe());
          tx.put(
              KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE,
              indexKeyBytes,
              tail.toBytes().toArrayUnsafe());
          subCount++;
          tailCount = tail.size();
          currentTail = tail;
          pendingIdx++;
          flushedSoFar = pendingIdx;
        } else {
          tailCount = newTailCount;
          pendingIdx++;
        }
      }

      if (flushedSoFar < n) {
        final byte[] operand = new byte[(n - flushedSoFar) * RangeRelativeOffsetList.ENTRY_BYTES];
        int pos = 0;
        for (int i = flushedSoFar; i < n; i++) {
          final int off = be.pending.get(i);
          operand[pos] = (byte) ((off >> 16) & 0xFF);
          operand[pos + 1] = (byte) ((off >> 8) & 0xFF);
          operand[pos + 2] = (byte) (off & 0xFF);
          pos += 3;
        }
        tx.merge(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, operand);
      }

      final byte[] newMetadata = writeMetadataValue(subCount, tailCount);
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes, newMetadata);
      indexCache.put(indexKey, newMetadata);
    }
    buffer = null;
    prefetchQueue.clear();
    prefetchedBase = new ConcurrentHashMap<>();
    callsSinceLastPeriodicDrain = 0;
  }
```

Replace the `storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, keyBytes)` call
inside `drainPrefetch()` (around line 542) with:

```java
              final List<Optional<byte[]>> res =
                  storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, keyBytes);
```

(prefetch now stages metadata, not full content — the rest of `drainPrefetch`'s body is unchanged;
only this one storage call changes its target CF).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: PASS. Also run the full test class once more without filters to catch any remaining
fallout from earlier tasks:

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeChangeIndexTest*"`
Expected: PASS, all tests green.

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndex.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeChangeIndexTest.java
git commit -m "feat(bonsai-archive): batch buffered index appends into one merge per dirty key"
```

---

### Task 12: `TrieNodeIndexDropper` — update for split format

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeIndexDropper.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeIndexDropperTest.java`

**Interfaces:**
- Consumes: `TrieNodeChangeIndex.readMetadataValue`/`writeMetadataValue` — these are currently
  package-private static methods on `TrieNodeChangeIndex` (Task 7), which `TrieNodeIndexDropper`
  can call directly since it's in the same package.

Offset removal is an arbitrary mid-list edit, which can never be a blind merge — this path stays a
real read-modify-write, same as today, just updated for the split format (content has no prefix
anymore; a removal also decrements `tailCount` in the separate metadata value).

- [ ] **Step 1: Write the failing test**

First read the existing `TrieNodeIndexDropperTest.java` to find its exact test setup pattern
(`grep -n "class TrieNodeIndexDropperTest" -A 40
ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeIndexDropperTest.java`)
and add a test following that pattern:

```java
  @Test
  void dropBlockRemovesOffsetAndDecrementsMetadataTailCount() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeChangeIndex index = new TrieNodeChangeIndex(kv, 1_000_000);
    final Bytes naturalKey = ArchiveNodeKey.account(Bytes.of(0x01));

    var tx = kv.startTransaction();
    index.append(tx, naturalKey, 10);
    index.append(tx, naturalKey, 20);
    tx.commit();

    // historyKey must exist for dropBlock's history-CF scan to find this natural key at block 20.
    tx = kv.startTransaction();
    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE,
        ArchiveNodeKey.historyKey(naturalKey, 20).toArrayUnsafe(),
        new byte[] {0x00});
    tx.commit();

    final TrieNodeIndexDropper dropper = new TrieNodeIndexDropper();
    tx = kv.startTransaction();
    dropper.dropBlock(20, kv, tx);
    tx.commit();

    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, 0);
    assertThat(kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe()))
        .contains(new byte[] {0, 0, 10}); // only block 10's offset remains
    final TrieNodeChangeIndex.IndexMetadata metadata =
        TrieNodeChangeIndex.readMetadataValue(
            kv.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe())
                .orElseThrow());
    assertThat(metadata.tailCount()).isEqualTo(1);
    assertThat(metadata.subCount()).isZero();
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeIndexDropperTest.dropBlockRemovesOffsetAndDecrementsMetadataTailCount*"`
Expected: FAIL — `dropOffsetFromIndex` still reads/writes the old combined
`[subCount][content]` format from `TRIE_NODE_INDEX_ARCHIVE` only, never touching
`TRIE_NODE_INDEX_META_ARCHIVE`, so the metadata assertion fails (absent value).

- [ ] **Step 3: Rewrite `dropOffsetFromIndex`**

Replace the entire body of `dropOffsetFromIndex` (lines 198–269) with:

```java
  private void dropOffsetFromIndex(
      final Bytes naturalKey,
      final long rangeId,
      final int offset,
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx) {

    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    final byte[] packedRaw =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes).orElse(null);
    if (packedRaw == null) {
      // No index entry — nothing to remove.
      return;
    }
    final int subCount =
        storage
            .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes)
            .map(TrieNodeChangeIndex::readMetadataValue)
            .orElse(TrieNodeChangeIndex.IndexMetadata.EMPTY)
            .subCount();
    final Bytes packed = Bytes.wrap(packedRaw);

    // Remove the target offset from the packed list by rebuilding without it.
    final int n = packed.size() / ENTRY_BYTES;
    int removed = 0;
    final MutableBytes result = MutableBytes.create(n * ENTRY_BYTES);
    int dst = 0;
    for (int i = 0; i < n; i++) {
      final int base = i * ENTRY_BYTES;
      final int entryOffset =
          ((packed.get(base) & 0xFF) << 16)
              | ((packed.get(base + 1) & 0xFF) << 8)
              | (packed.get(base + 2) & 0xFF);
      if (entryOffset == offset && removed == 0) {
        removed++;
        continue;
      }
      result.set(dst, packed.get(base));
      result.set(dst + 1, packed.get(base + 1));
      result.set(dst + 2, packed.get(base + 2));
      dst += ENTRY_BYTES;
    }

    if (removed == 0) {
      // Offset not found in the tail — it may be in a sub-block or was never there.
      return;
    }

    final int remainingEntries = n - removed;
    final Bytes newPacked = result.slice(0, remainingEntries * ENTRY_BYTES);

    if (remainingEntries == 0 && subCount == 0) {
      tx.remove(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes);
      tx.remove(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes);
    } else {
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, newPacked.toArrayUnsafe());
      tx.put(
          KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE,
          indexKeyBytes,
          TrieNodeChangeIndex.writeMetadataValue(subCount, remainingEntries));
    }
  }
```

Remove the now-unused `SUBCOUNT_BYTES` constant and its javadoc (lines 76–83) — it's no longer
referenced anywhere in this file.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*TrieNodeIndexDropperTest*"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeIndexDropper.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/archiveindex/TrieNodeIndexDropperTest.java
git commit -m "fix(bonsai-archive): update TrieNodeIndexDropper for split content/metadata format"
```

---

### Task 13: `BonsaiFlatDbToArchiveMigrator` — metadata-CF allowlist + regression test

**Execution note:** this task depends on Task 6 (`TRIE_NODE_INDEX_META_ARCHIVE` must exist to
allowlist it) and Task 8 (its regression test relies on `TrieNodeChangeIndex.append()` actually
issuing a `merge()` + metadata `put()`) — dispatch it after both, not in the plan's numeric
order. The `merge()` method itself on `MigrationTransaction`/`FlatCapturingTx` was already added in
Task 16 (dispatch Task 16 immediately after Task 1, before Task 6 — see that task's note on why:
`:ethereum:core` cannot compile at all, for any task, without `merge()` existing on every
`SegmentedKeyValueStorageTransaction` implementor in that module).

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigratorTest.java`

**Interfaces:**
- Consumes: `merge()` on `MigrationTransaction`/`FlatCapturingTx` from Task 16;
  `TRIE_NODE_INDEX_META_ARCHIVE` from Task 6; `TrieNodeChangeIndex.append()` issuing
  merge()+put() from Task 8.
- Produces: `MigrationTransaction`/`FlatCapturingTx` correctly forward `put()` calls for the new
  metadata CF too (they already forward `merge()` calls for the content CF, from Task 16),
  instead of silently dropping metadata writes.

This is the task most directly guarding against the allowlist-drop failure mode called out in
`Global Constraints` — get the allowlist wrong here and migration silently produces wrong
mutation counts (and thus wrong FULL/DIFF checkpoint decisions) with no error.

- [ ] **Step 1: Write the failing test**

`MigrationTransaction` is a `private static final class` — this file's existing tests exercise it
indirectly by running a real migration and asserting on the resulting storage, e.g.
`trieMigratorWithIndexEnabled_populatesDiffIndexAtCheckpoint` (around line 718): it builds a
`TrieNodeHistoryStore`/`TrieNodeChangeIndex`/`TrieNodeIndexProgress` backed by the same `storage`
field the migrator writes to, constructs a migrator via
`createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress)`, runs
`migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)`, then asserts directly on
`storage`/`historyStore`. That existing test already exercises `TrieNodeChangeIndex.append()` for
block 1's root-node creation (the `priorNode == null` path in `captureTrieNodeDiff`) — which,
after Task 8, issues one `merge()` to `TRIE_NODE_INDEX_ARCHIVE` and one `put()` to
`TRIE_NODE_INDEX_META_ARCHIVE`, both passing through `MigrationTransaction`. Add a new test
following that exact same setup, asserting directly on both CFs:

```java
  @Test
  public void migrationForwardsIndexMergeAndMetadataPutThroughAllowlist() throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Root node's natural key is Bytes.EMPTY (ArchiveNodeKey.account(Bytes.EMPTY) == Bytes.EMPTY);
    // it was created (not mutated) at block 1, so TrieNodeChangeIndex.append() ran, which merges
    // TRIE_NODE_INDEX_ARCHIVE and puts TRIE_NODE_INDEX_META_ARCHIVE — both must have reached
    // committed storage through MigrationTransaction's allowlist, not been silently dropped.
    final Bytes indexKey = ArchiveNodeKey.rangeKey(Bytes.EMPTY, 0);
    assertThat(storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe()))
        .withFailMessage("MigrationTransaction must forward merge() calls for TRIE_NODE_INDEX_ARCHIVE")
        .isPresent();
    assertThat(
            storage.get(
                KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKey.toArrayUnsafe()))
        .withFailMessage("MigrationTransaction must forward put() calls for TRIE_NODE_INDEX_META_ARCHIVE")
        .isPresent();
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "*BonsaiFlatDbToArchiveMigratorTest.migrationForwardsIndexMergeAndMetadataPutThroughAllowlist*"`
Expected: FAIL — the `TRIE_NODE_INDEX_META_ARCHIVE` put is silently dropped because it's not yet in
the `put()` allowlist (Task 16 already added `merge()` for the content CF, so that half of the
assertion passes; only the metadata-CF `isPresent()` assertion fails).

- [ ] **Step 3: Extend the `put()`/`remove()` allowlist for the metadata CF**

Edit `BonsaiFlatDbToArchiveMigrator.java`. In `MigrationTransaction.put()` (around line 1189),
change:

```java
      } else if (segmentId == TRIE_NODE_HISTORY_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_ARCHIVE
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.put(segmentId, key, value);
      }
```

to:

```java
      } else if (segmentId == TRIE_NODE_HISTORY_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_META_ARCHIVE
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.put(segmentId, key, value);
      }
```

In `MigrationTransaction.remove()` (around line 1207), apply the same addition for symmetry:

```java
      } else if (segmentId == TRIE_NODE_HISTORY_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_META_ARCHIVE
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.remove(segmentId, key);
      }
```

Ensure `TRIE_NODE_INDEX_META_ARCHIVE` is statically imported at the top of the file alongside the
other `KeyValueSegmentIdentifier` constants already imported there (check the existing `import
static ...KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE;`-style imports and add a matching one
for `TRIE_NODE_INDEX_META_ARCHIVE`).

`merge()` on both `MigrationTransaction` and `FlatCapturingTx` already exists (added in Task 16)
and needs no change here — it already forwards `TRIE_NODE_INDEX_ARCHIVE` merges correctly.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :ethereum:core:test --tests "*BonsaiFlatDbToArchiveMigratorTest*"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigratorTest.java
git commit -m "fix(bonsai-archive): forward index metadata put() through migration allowlist"
```

---

### Task 14: Full verification pass

**Files:** none (verification only — fix any fallout discovered in the files already touched above).

**Interfaces:**
- Consumes: everything from Tasks 1–13.
- Produces: a green build across every module this plan touched.

- [ ] **Step 1: Run the full affected test suites**

Run: `./gradlew :plugin-api:test :services:kvstore:test :plugins:rocksdb:test :ethereum:core:test --tests "*TrieNodeChangeIndex*" --tests "*TrieNodeIndexDropper*" --tests "*BonsaiFlatDbToArchiveMigrator*" --tests "*ArchiveNodeKey*" --tests "*RangeRelativeOffsetList*" --tests "*BonsaiArchiveTrieNodeIndex*" --tests "*BonsaiArchiveProofsIntegration*" --tests "*TrieNodeHistoryReader*" --tests "*TrieNodeHistoryComposition*" --tests "*ArchiveProofNodeLoader*"`
Expected: PASS. If any of the integration tests fail, they are the correctness backstop the spec
calls out — do not weaken their assertions; fix the production code path they're exercising by
re-checking it against the transformation pattern used for the method it corresponds to in Tasks
9–10 (metadata for counts/subCount, content for actual offsets, no prefix stripping).

- [ ] **Step 2: Run spotless and a full compile of every touched module**

Run: `./gradlew :plugin-api:spotlessApply :services:kvstore:spotlessApply :plugins:rocksdb:spotlessApply :ethereum:core:spotlessApply`
Run: `./gradlew :plugin-api:build :services:kvstore:build :plugins:rocksdb:build :ethereum:core:build -x test`
Expected: BUILD SUCCESSFUL for both commands.

- [ ] **Step 3: Re-run the full targeted test list once more post-spotless**

Run: (same command as Step 1)
Expected: PASS — spotless reformatting must not have changed behavior.

- [ ] **Step 4: Commit any spotless formatting fixes**

```bash
git add -A
git status
```

Review the diff — it should contain only whitespace/import-ordering changes from spotless (if any
files changed at all). If so:

```bash
git commit -m "style(bonsai-archive): spotless formatting for merge-operator index changes"
```

If `git status` shows no changes, skip the commit.

---

### Task 15: No-op transaction sentinels — add `merge()`

**Files:**
- Modify: `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueSnapshot.java`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/worldview/PathBasedWorldState.java`

**Interfaces:**
- Consumes: `merge()` from Task 1.
- Produces: no new interfaces — these are the two remaining `SegmentedKeyValueStorageTransaction`
  implementors not covered by any other task.

This task exists because the original research pass (grep for `implements
SegmentedKeyValueStorageTransaction`) missed two **anonymous** implementations — anonymous classes
don't have an `implements` keyword to grep for. A full sweep (`grep -rn "new
SegmentedKeyValueStorageTransaction()" --include="*.java" .`) found both:

- `RocksDBColumnarKeyValueSnapshot.noOpTx` (`plugins/rocksdb/.../segmented/RocksDBColumnarKeyValueSnapshot.java:291-315`)
  — returned by `getSnapshotTransaction()` for immutable snapshots; every method is already a no-op.
- `PathBasedWorldState.noOpSegmentedTx` (`ethereum/core/.../common/worldview/PathBasedWorldState.java:324-345`)
  — a similar no-op sentinel.

Neither has a dedicated unit test today (their `put()`/`remove()` no-ops aren't tested standalone
either — they're exercised only implicitly through the snapshot/world-state behavior that uses
them), so this task doesn't invent one for `merge()` either, matching that existing convention.

- [ ] **Step 1: Add the no-op `merge()` override to `RocksDBColumnarKeyValueSnapshot.noOpTx`**

Edit `plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueSnapshot.java`.
In the `noOpTx` field's anonymous class body, add after `remove()`:

```java
        @Override
        public void merge(
            final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
          // no-op
        }
```

- [ ] **Step 2: Add the no-op `merge()` override to `PathBasedWorldState.noOpSegmentedTx`**

Edit `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/worldview/PathBasedWorldState.java`.
In the `noOpSegmentedTx` field's anonymous class body, add after `remove()`:

```java
        @Override
        public void merge(
            final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
          // no-op
        }
```

- [ ] **Step 3: Compile-check both modules**

Run: `./gradlew :plugins:rocksdb:compileJava :ethereum:core:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add plugins/rocksdb/src/main/java/org/hyperledger/besu/plugin/services/storage/rocksdb/segmented/RocksDBColumnarKeyValueSnapshot.java \
        ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/worldview/PathBasedWorldState.java
git commit -m "feat(storage): add no-op merge() to the two anonymous transaction sentinels"
```

---

### Task 16: `MigrationTransaction`/`FlatCapturingTx` — add `merge()` (compile-unblocking)

**Execution note:** dispatch this task immediately after Task 1 (before Task 6), not in the plan's
numeric position. Reason: `ethereum/core`'s main source set will not compile at all —
for any task, including Tasks 6-12 which don't otherwise touch this file — until every
`SegmentedKeyValueStorageTransaction` implementor in that module has a `merge()` method (Java
requires ALL abstract methods implemented before a class compiles; Gradle always compiles a
module's whole source set before running any of its tests, even a `--tests`-filtered single-class
run). `MigrationTransaction` and `FlatCapturingTx` live in `ethereum/core` and are two of the
three implementors there (the third, `PathBasedWorldState`'s no-op sentinel, is Task 15). This task
adds ONLY the `merge()` method itself (which only needs to allowlist the pre-existing
`TRIE_NODE_INDEX_ARCHIVE` segment, not the not-yet-created `TRIE_NODE_INDEX_META_ARCHIVE`) so it has
no dependency on Task 6. Task 13 later extends `put()`/`remove()` for the new metadata CF and adds
the end-to-end regression test, once Task 6 and Task 8 exist.

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java`

**Interfaces:**
- Consumes: `merge()` from Task 1.
- Produces: `MigrationTransaction.merge()`/`FlatCapturingTx.merge()` — both fully correct and final
  as written here; Task 13 does not modify either method again, only the `put()`/`remove()`
  allowlists.

- [ ] **Step 1: Add `merge()` to `MigrationTransaction`**

Edit `BonsaiFlatDbToArchiveMigrator.java`. In the `MigrationTransaction` inner class, add after
`remove()` (before `commit()`):

```java
    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      // Only the trie-node index content CF is ever merged; everything else is dropped to match
      // the same allowlist discipline as put()/remove() above — see the CAS-dedup incident
      // referenced in this class's write-path javadoc for why silent, unlisted drops are
      // dangerous here.
      if (segmentId == TRIE_NODE_INDEX_ARCHIVE) {
        realTx.merge(segmentId, key, value);
      }
    }
```

- [ ] **Step 2: Add `merge()` to `FlatCapturingTx`**

In the `FlatCapturingTx` inner class, add after `remove()` (before `commit()`):

```java
    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      delegate.merge(segmentId, key, value);
    }
```

- [ ] **Step 3: Compile-check**

Run: `./gradlew :ethereum:core:compileJava`
Expected: BUILD SUCCESSFUL (assuming Task 15's `PathBasedWorldState` fix has also already landed —
if not, this will still show one remaining error there, which is that task's responsibility, not
this one's).

- [ ] **Step 4: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java
git commit -m "feat(bonsai-archive): forward index merge() through migration allowlist"
```

---

## Deferred (per spec's "Out of scope")

Not part of this plan — raise separately if needed:
- On-disk migration/reindex tooling for existing `X_BONSAI_ARCHIVE` databases written before this
  change (the spec leaves the exact mechanism — forward migration vs. documented reindex — as a
  follow-up decision).
- Redesigning the depth-tiered checkpoint algorithm to avoid needing a synchronous previous-count
  read at all.
