# Unified Deletion with Smart Detection - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Unify `removeFlatAccount()` and `deleteFlatAccountAtBlock()` using smart detection that automatically distinguishes SELFDESTRUCT (needs marker) from reorg cleanup (needs deletion based on historical data).

**Architecture:** Enhance `removeFlatAccount()` to check if data exists at the current block. If yes (reorg cleanup), check for historical data and either delete (reveal history) or write marker (no history). If no (SELFDESTRUCT), always write marker. Reuses existing `getFlatAccount()` for historical checks.

**Tech Stack:** Java 21, RocksDB (via SegmentedKeyValueStorage), JUnit 5, AssertJ

---

## Task 1: Add Historical Data Check Helper for Accounts

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java` (add after line 313)

**Step 1: Write the failing test**

Create test file and add test for historical data detection:

```java
// In BonsaiArchiveFlatDbStrategyTest.java after existing tests

@Test
public void hasHistoricalDataBeforeShouldReturnTrueWhenDataExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000100").addressHash();
  final Bytes accountValue = Bytes.fromHexString("0x112233");

  // Write data at block 10
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  Supplier<Optional<BonsaiContext>> context = () -> Optional.of(new BonsaiContext(10L));
  archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue, context);
  tx.commit();

  // Check for historical data before block 20
  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalAccountDataBefore(
      storage, accountHash, 20L);

  assertThat(hasHistory).isTrue();
}

@Test
public void hasHistoricalDataBeforeShouldReturnFalseWhenNoDataExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000101").addressHash();

  // Check for historical data before block 20 (no data exists)
  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalAccountDataBefore(
      storage, accountHash, 20L);

  assertThat(hasHistory).isFalse();
}

@Test
public void hasHistoricalDataBeforeShouldReturnFalseForGenesisBlock() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000102").addressHash();

  // Check for historical data before genesis (block 0)
  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalAccountDataBefore(
      storage, accountHash, 0L);

  assertThat(hasHistory).isFalse();
}

@Test
public void hasHistoricalDataBeforeShouldIgnoreDeletionMarkers() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000103").addressHash();

  // Write deletion marker at block 10
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  Supplier<Optional<BonsaiContext>> context = () -> Optional.of(new BonsaiContext(10L));
  archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context);
  tx.commit();

  // Check for historical data before block 20 (only marker exists)
  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalAccountDataBefore(
      storage, accountHash, 20L);

  assertThat(hasHistory).isFalse();
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.hasHistoricalData*"`

Expected: FAIL with "cannot find symbol: method hasHistoricalAccountDataBefore"

**Step 3: Add helper method to make it package-private for testing**

In `BonsaiArchiveFlatDbStrategy.java`, add after the `deleteFlatStorageAtBlock()` method:

```java
/**
 * Checks if legitimate (non-marker) account data exists before the given block number.
 * Used by smart detection to decide between deletion (reveal history) vs marker (barrier).
 *
 * <p>Package-private for testing.
 *
 * @param storage the key-value storage
 * @param accountHash the account hash to check
 * @param blockNumber the block number to search before
 * @return true if non-marker data exists at any block < blockNumber
 */
boolean hasHistoricalAccountDataBefore(
    final SegmentedKeyValueStorage storage,
    final Hash accountHash,
    final long blockNumber) {

  if (blockNumber == 0) {
    return false; // No blocks before genesis
  }

  // Reuse getFlatAccount with readContext = blockNumber - 1
  // This searches for the nearest non-marker entry before blockNumber
  Supplier<Optional<BonsaiContext>> readContext =
      () -> Optional.of(new BonsaiContext(blockNumber - 1));

  Optional<Bytes> historicalData =
      getFlatAccount(
          () -> Optional.empty(), // worldStateRootHash not needed for this check
          null, // nodeLoader not needed
          accountHash,
          storage,
          readContext);

  return historicalData.isPresent();
}
```

**Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.hasHistoricalData*"`

Expected: PASS (all 4 tests)

**Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategyTest.java
git commit -m "feat: add historical account data detection helper

Add hasHistoricalAccountDataBefore() to detect if legitimate data
exists before a given block. Reuses getFlatAccount() logic to avoid
duplication. Package-private for testing.

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Add Historical Data Check Helper for Storage Slots

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java` (add after hasHistoricalAccountDataBefore)

**Step 1: Write the failing test**

Add to `BonsaiArchiveFlatDbStrategyTest.java`:

```java
@Test
public void hasHistoricalStorageDataBeforeShouldReturnTrueWhenDataExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000200").addressHash();
  final Hash slotHash = Hash.hash(Bytes.of(1));
  final Bytes storageValue = Bytes.fromHexString("0x445566");

  // Write storage at block 10
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  Supplier<Optional<BonsaiContext>> context = () -> Optional.of(new BonsaiContext(10L));
  archiveFlatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
      storage, tx, accountHash, slotHash, storageValue, context);
  tx.commit();

  // Check for historical data before block 20
  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalStorageDataBefore(
      storage, accountHash, slotHash, 20L);

  assertThat(hasHistory).isTrue();
}

@Test
public void hasHistoricalStorageDataBeforeShouldReturnFalseWhenNoDataExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000201").addressHash();
  final Hash slotHash = Hash.hash(Bytes.of(1));

  boolean hasHistory = archiveFlatDbStrategy.hasHistoricalStorageDataBefore(
      storage, accountHash, slotHash, 20L);

  assertThat(hasHistory).isFalse();
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.hasHistoricalStorageData*"`

Expected: FAIL with "cannot find symbol: method hasHistoricalStorageDataBefore"

**Step 3: Add helper method**

In `BonsaiArchiveFlatDbStrategy.java`, add after `hasHistoricalAccountDataBefore()`:

```java
/**
 * Checks if legitimate (non-marker) storage data exists before the given block number.
 * Reuses existing getFlatAccountStorageValueByStorageSlotKey logic.
 *
 * <p>Package-private for testing.
 *
 * @param storage the key-value storage
 * @param accountHash the account hash
 * @param slotHash the storage slot hash
 * @param blockNumber the block number to search before
 * @return true if non-marker data exists at any block < blockNumber
 */
boolean hasHistoricalStorageDataBefore(
    final SegmentedKeyValueStorage storage,
    final Hash accountHash,
    final Hash slotHash,
    final long blockNumber) {

  if (blockNumber == 0) {
    return false;
  }

  Supplier<Optional<BonsaiContext>> readContext =
      () -> Optional.of(new BonsaiContext(blockNumber - 1));

  Optional<Bytes> historicalData =
      getFlatAccountStorageValueByStorageSlotKey(
          () -> Optional.empty(),
          null,
          storage,
          accountHash,
          slotHash,
          readContext);

  return historicalData.isPresent();
}
```

**Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.hasHistoricalStorageData*"`

Expected: PASS (all 2 tests)

**Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategyTest.java
git commit -m "feat: add historical storage data detection helper

Add hasHistoricalStorageDataBefore() for storage slot historical checks.
Reuses getFlatAccountStorageValueByStorageSlotKey() logic.

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Implement Smart Detection in removeFlatAccount()

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java:270-287`

**Step 1: Write the failing test**

Add to `BonsaiArchiveFlatDbStrategyTest.java`:

```java
@Test
public void removeFlatAccountShouldWriteMarkerForSelfDestruct() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000300").addressHash();

  // Simulate SELFDESTRUCT: no data at current block yet
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  Supplier<Optional<BonsaiContext>> context = () -> Optional.of(new BonsaiContext(20L));
  archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context);
  tx.commit();

  // Verify marker was written
  byte[] key =
      Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20L)).toArrayUnsafe();
  Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE, key);
  assertThat(value).isPresent();
  assertThat(Arrays.areEqual(value.get(), BonsaiArchiveFlatDbStrategy.DELETED_ACCOUNT_VALUE))
      .isTrue();
}

@Test
public void removeFlatAccountShouldDeleteOrphanedDataWhenHistoryExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000301").addressHash();
  final Bytes historicalData = Bytes.fromHexString("0x112233");
  final Bytes orphanedData = Bytes.fromHexString("0x445566");

  // Write legitimate historical data at block 10
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  archiveFlatDbStrategy.putFlatAccount(
      storage, tx, accountHash, historicalData, () -> Optional.of(new BonsaiContext(10L)));
  tx.commit();

  // Write orphaned data at block 20 (simulating reorg)
  tx = storage.startTransaction();
  archiveFlatDbStrategy.putFlatAccount(
      storage, tx, accountHash, orphanedData, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Remove orphaned data (smart detection should DELETE to reveal history)
  tx = storage.startTransaction();
  archiveFlatDbStrategy.removeFlatAccount(
      storage, tx, accountHash, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Verify block 20 entry was deleted (not just marked)
  byte[] key20 =
      Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20L)).toArrayUnsafe();
  Optional<byte[]> value20 = storage.get(ACCOUNT_INFO_STATE, key20);
  assertThat(value20).isEmpty();

  // Verify historical data at block 10 still exists
  byte[] key10 =
      Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(10L)).toArrayUnsafe();
  Optional<byte[]> value10 = storage.get(ACCOUNT_INFO_STATE, key10);
  assertThat(value10).isPresent();
  assertThat(Bytes.wrap(value10.get())).isEqualTo(historicalData);
}

@Test
public void removeFlatAccountShouldWriteMarkerWhenOrphanedDataHasNoHistory() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000302").addressHash();
  final Bytes orphanedData = Bytes.fromHexString("0x445566");

  // Write orphaned data at block 20 (no historical data before this)
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  archiveFlatDbStrategy.putFlatAccount(
      storage, tx, accountHash, orphanedData, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Remove orphaned data (smart detection should write MARKER since no history)
  tx = storage.startTransaction();
  archiveFlatDbStrategy.removeFlatAccount(
      storage, tx, accountHash, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Verify marker was written (overwrote orphaned data)
  byte[] key =
      Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20L)).toArrayUnsafe();
  Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE, key);
  assertThat(value).isPresent();
  assertThat(Arrays.areEqual(value.get(), BonsaiArchiveFlatDbStrategy.DELETED_ACCOUNT_VALUE))
      .isTrue();
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.removeFlatAccount*"`

Expected: FAIL (tests expect smart detection behavior, current code just writes markers)

**Step 3: Implement smart detection**

Replace the `removeFlatAccount()` method body in `BonsaiArchiveFlatDbStrategy.java`:

```java
@Override
public void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

  // Get write context or default to genesis
  BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

  // Calculate key suffixed with block context
  byte[] keySuffixed =
      calculateArchiveKeyWithMinSuffix(
          Optional.of(writeContext), accountHash.getBytes().toArrayUnsafe());

  LOG.info("removeFlatAccount: hash={}, writeContext={}", accountHash, writeContext);

  // SMART DETECTION: Check if data exists at this exact block
  Optional<byte[]> currentBlockData = storage.get(ACCOUNT_INFO_STATE, keySuffixed);

  if (currentBlockData.isPresent()
      && !Arrays.areEqual(DELETED_ACCOUNT_VALUE, currentBlockData.get())) {

    // CASE: Data exists at current block = ORPHANED DATA from reorg
    // This is reorg cleanup - check for historical data

    if (hasHistoricalAccountDataBefore(storage, accountHash, writeContext.getBlockNumber())) {
      // Historical data exists - DELETE orphaned entry to reveal it
      LOG.info(
          "removeFlatAccount: deleting orphaned data to reveal history at block {}",
          writeContext.getBlockNumber());
      transaction.remove(ACCOUNT_INFO_STATE, keySuffixed);
    } else {
      // No historical data - OVERWRITE orphaned data with marker
      LOG.info(
          "removeFlatAccount: overwriting orphaned data with marker at block {}",
          writeContext.getBlockNumber());
      transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
    }

  } else {
    // CASE: No data at current block = SELFDESTRUCT scenario
    // Account is being destroyed during block commit (data not written yet)
    // Always write marker to hide historical data
    LOG.info("removeFlatAccount: writing marker for SELFDESTRUCT at block {}", writeContext.getBlockNumber());
    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
  }
}
```

**Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.removeFlatAccount*"`

Expected: PASS (all 3 new tests)

**Step 5: Run all BonsaiArchiveFlatDbStrategyTest tests**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest"`

Expected: PASS (all existing tests + new tests)

**Step 6: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategyTest.java
git commit -m "feat: implement smart detection in removeFlatAccount

Automatically detect SELFDESTRUCT vs reorg cleanup scenarios:
- No current data → Write marker (SELFDESTRUCT)
- Orphaned data + history → Delete to reveal history
- Orphaned data + no history → Overwrite with marker

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Implement Smart Detection in removeFlatAccountStorageValueByStorageSlotHash()

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java:425-446`

**Step 1: Write the failing test**

Add to `BonsaiArchiveFlatDbStrategyTest.java`:

```java
@Test
public void removeFlatStorageShouldDeleteOrphanedDataWhenHistoryExists() {
  final Hash accountHash =
      Address.fromHexString("0x0000000000000000000000000000000000000400").addressHash();
  final Hash slotHash = Hash.hash(Bytes.of(1));
  final Bytes historicalValue = Bytes.fromHexString("0x112233");
  final Bytes orphanedValue = Bytes.fromHexString("0x445566");

  // Write legitimate historical storage at block 10
  SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
  archiveFlatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
      storage, tx, accountHash, slotHash, historicalValue, () -> Optional.of(new BonsaiContext(10L)));
  tx.commit();

  // Write orphaned storage at block 20
  tx = storage.startTransaction();
  archiveFlatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
      storage, tx, accountHash, slotHash, orphanedValue, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Remove orphaned storage (should DELETE to reveal history)
  tx = storage.startTransaction();
  archiveFlatDbStrategy.removeFlatAccountStorageValueByStorageSlotHash(
      storage, tx, accountHash, slotHash, () -> Optional.of(new BonsaiContext(20L)));
  tx.commit();

  // Verify block 20 entry was deleted
  byte[] naturalKey = BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey(accountHash, slotHash);
  byte[] key20 =
      Bytes.concatenate(Bytes.wrap(naturalKey), Bytes.ofUnsignedLong(20L)).toArrayUnsafe();
  Optional<byte[]> value20 = storage.get(
      KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE, key20);
  assertThat(value20).isEmpty();

  // Verify historical data at block 10 still exists
  byte[] key10 =
      Bytes.concatenate(Bytes.wrap(naturalKey), Bytes.ofUnsignedLong(10L)).toArrayUnsafe();
  Optional<byte[]> value10 = storage.get(
      KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE, key10);
  assertThat(value10).isPresent();
  assertThat(Bytes.wrap(value10.get())).isEqualTo(historicalValue);
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.removeFlatStorage*"`

Expected: FAIL (current code writes marker instead of deleting)

**Step 3: Implement smart detection for storage**

Replace the `removeFlatAccountStorageValueByStorageSlotHash()` method body in `BonsaiArchiveFlatDbStrategy.java`:

```java
@Override
public void removeFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

  // Get write context or default to genesis
  BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

  // Get natural key from account hash and slot key
  byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
  // Calculate key suffixed with block context
  byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(Optional.of(writeContext), naturalKey);

  LOG.info(
      "removeFlatAccountStorageValueByStorageSlotHash: hash={}, slotHash={}, writeContext={}",
      accountHash,
      slotHash,
      writeContext);

  // SMART DETECTION: Check if data exists at this exact block
  Optional<byte[]> currentBlockData = storage.get(ACCOUNT_STORAGE_STORAGE, keySuffixed);

  if (currentBlockData.isPresent()
      && !Arrays.areEqual(DELETED_STORAGE_VALUE, currentBlockData.get())) {

    // CASE: Data exists at current block = ORPHANED DATA from reorg
    // This is reorg cleanup - check for historical data

    if (hasHistoricalStorageDataBefore(
        storage, accountHash, slotHash, writeContext.getBlockNumber())) {
      // Historical data exists - DELETE orphaned entry to reveal it
      LOG.info(
          "removeFlatAccountStorageValueByStorageSlotHash: deleting orphaned storage to reveal history");
      transaction.remove(ACCOUNT_STORAGE_STORAGE, keySuffixed);
    } else {
      // No historical data - OVERWRITE orphaned data with marker
      LOG.info(
          "removeFlatAccountStorageValueByStorageSlotHash: overwriting orphaned storage with marker");
      transaction.put(ACCOUNT_STORAGE_STORAGE, keySuffixed, DELETED_STORAGE_VALUE);
    }

  } else {
    // CASE: No data at current block = storage deletion during block commit
    // Always write marker to hide historical data
    LOG.info(
        "removeFlatAccountStorageValueByStorageSlotHash: writing marker for storage deletion");
    transaction.put(ACCOUNT_STORAGE_STORAGE, keySuffixed, DELETED_STORAGE_VALUE);
  }
}
```

**Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest.removeFlatStorage*"`

Expected: PASS

**Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategyTest.java
git commit -m "feat: implement smart detection for storage slot removal

Apply same smart detection logic to storage slots:
- Detect orphaned storage and check for historical data
- Delete to reveal history or overwrite with marker

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Remove Deprecated Delete Methods

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java:301-313, 457-474`
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorage.java:304-310, 413-420`

**Step 1: Run tests before deletion to establish baseline**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveReorgTest"`

Expected: Some tests may still be failing (we'll fix in next task)

**Step 2: Remove deleteFlatAccountAtBlock() and deleteFlatStorageAtBlock()**

In `BonsaiArchiveFlatDbStrategy.java`, delete these two methods:
- `deleteFlatAccountAtBlock()` (lines 289-313)
- `deleteFlatStorageAtBlock()` (lines 448-474)

**Step 3: Remove deleteAccountInfoStateAtBlock() and deleteStorageValueBySlotHashAtBlock()**

In `BonsaiWorldStateKeyValueStorage.java` Updater class, delete these two methods:
- `deleteAccountInfoStateAtBlock()` (lines 296-310)
- `deleteStorageValueBySlotHashAtBlock()` (lines 404-420)

**Step 4: Run tests to verify no compilation errors**

Run: `./gradlew :ethereum:core:compileJava :ethereum:core:compileTestJava`

Expected: SUCCESS (no compilation errors)

**Step 5: Apply Spotless formatting**

Run: `./gradlew spotlessApply`

Expected: Code formatted

**Step 6: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorage.java
git commit -m "refactor: remove deprecated delete methods

Remove deleteFlatAccountAtBlock, deleteFlatStorageAtBlock,
deleteAccountInfoStateAtBlock, and deleteStorageValueBySlotHashAtBlock.

Smart detection in remove methods now handles all scenarios.

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Run Integration Tests

**Files:**
- None (testing only)

**Step 1: Run BonsaiArchiveReorgTest**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveReorgTest"`

Expected: ALL TESTS PASS (including `shouldMaintainCorrectStateForMultipleBlocksAfterReorg`)

**Step 2: If tests fail, debug**

If `shouldMaintainCorrectStateForMultipleBlocksAfterReorg` still fails:
1. Check logs for "removeFlatAccount" entries
2. Verify smart detection is choosing correct path
3. Check if writeRollbackDeletionMarkers is calling remove methods correctly

**Step 3: Run all archive strategy tests**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveFlatDbStrategyTest"`

Expected: ALL TESTS PASS

**Step 4: Run broader test suite**

Run: `./gradlew :ethereum:core:test`

Expected: No new failures (existing failures unrelated to this change)

**Step 5: Document test results**

If all tests pass, proceed to commit. If failures exist, investigate and fix before proceeding.

---

## Task 7: Clean Up Debug Logging

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java`

**Step 1: Review log levels**

Check that INFO-level logs in `removeFlatAccount()` and storage equivalent are appropriate for production. If they're too verbose, change to DEBUG or remove.

**Step 2: Update log statements if needed**

Change INFO logs to DEBUG if they're only useful during development:

```java
// Change from:
LOG.info("removeFlatAccount: writing marker for SELFDESTRUCT at block {}", ...);

// To:
LOG.debug("removeFlatAccount: writing marker for SELFDESTRUCT at block {}", ...);
```

**Step 3: Apply Spotless**

Run: `./gradlew spotlessApply`

**Step 4: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git commit -m "chore: adjust logging levels in smart detection

Change verbose INFO logs to DEBUG for production readiness.

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Update Documentation

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java` (method javadoc)

**Step 1: Update removeFlatAccount() javadoc**

Update the javadoc comment for `removeFlatAccount()`:

```java
/**
 * Removes account data using smart detection to automatically choose between deletion marker
 * (for SELFDESTRUCT) and actual deletion (for reorg cleanup with historical data).
 *
 * <p>Smart detection logic:
 * <ul>
 *   <li>If no data exists at current block → Write deletion marker (SELFDESTRUCT scenario)
 *   <li>If orphaned data exists + historical data → Delete entry to reveal history
 *   <li>If orphaned data exists + no historical data → Overwrite with marker
 * </ul>
 *
 * @param storage the key-value storage
 * @param transaction the storage transaction
 * @param accountHash the hash of the account to remove
 * @param writeContextSupplier supplier for the write context (block number)
 */
```

**Step 2: Update storage removal method javadoc**

Update `removeFlatAccountStorageValueByStorageSlotHash()` javadoc similarly.

**Step 3: Update design doc status**

Update `docs/plans/2026-02-16-unified-deletion-smart-detection-design.md`:
- Change status from "Approved" to "Implemented"
- Add implementation date

**Step 4: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java
git add docs/plans/2026-02-16-unified-deletion-smart-detection-design.md
git commit -m "docs: update javadoc and design doc for smart detection

Document the smart detection logic in method javadocs.
Mark design doc as implemented.

Co-Authored-By: Claude Sonnet 4.5 (1M context) <noreply@anthropic.com>"
```

---

## Success Criteria

✅ All `BonsaiArchiveFlatDbStrategyTest` tests pass
✅ All `BonsaiArchiveReorgTest` tests pass (including `shouldMaintainCorrectStateForMultipleBlocksAfterReorg`)
✅ No new test failures in `:ethereum:core:test`
✅ Deprecated `delete*()` methods removed
✅ Code formatted with Spotless
✅ Javadoc updated

## Rollback Plan

If critical issues are discovered:
1. Revert commits in reverse order
2. Temporarily restore deprecated methods if needed
3. Investigate root cause before re-attempting

## Next Steps After Implementation

1. Run reference tests: `./gradlew referenceTests`
2. Test on Hoodi testnet past block 2225170 (the problematic reorg)
3. Monitor mainnet sync for any state root mismatches
