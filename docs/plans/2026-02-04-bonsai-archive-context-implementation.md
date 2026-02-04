# Bonsai Archive Context Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Pass read/write context through method parameters instead of reading from the database, fixing reorg issues in archive mode.

**Architecture:** Create `BonsaiArchiveWorldState` to hold contexts. Add `Supplier<Optional<BonsaiContext>>` parameters to strategy methods. Provider sets contexts based on operation type.

**Tech Stack:** Java 21, Gradle, JUnit 5

---

## Task 1: Add Context Supplier Parameters to FlatDbStrategy

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/storage/flat/FlatDbStrategy.java`

**Step 1: Add import for BonsaiContext**

Add to imports:
```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
```

**Step 2: Update putFlatAccount signature**

Change:
```java
public abstract void putFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Bytes accountValue);
```

To:
```java
public abstract void putFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Bytes accountValue,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier);
```

**Step 3: Update removeFlatAccount signature**

Change:
```java
public abstract void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash);
```

To:
```java
public abstract void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier);
```

**Step 4: Update putFlatAccountStorageValueByStorageSlotHash signature**

Change:
```java
public abstract void putFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Bytes storageValue);
```

To:
```java
public abstract void putFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Bytes storageValue,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier);
```

**Step 5: Update removeFlatAccountStorageValueByStorageSlotHash signature**

Change:
```java
public abstract void removeFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash);
```

To:
```java
public abstract void removeFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier);
```

**Step 6: Verify compilation fails (expected)**

Run: `./gradlew :ethereum:core:compileJava 2>&1 | head -50`
Expected: Compilation errors in subclasses

---

## Task 2: Add Context Parameters to BonsaiFlatDbStrategy

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiFlatDbStrategy.java`

**Step 1: Add import for BonsaiContext**

Add to imports:
```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
```

**Step 2: Update getFlatAccount signature**

Change:
```java
public abstract Optional<Bytes> getFlatAccount(
    Supplier<Optional<Bytes>> worldStateRootHashSupplier,
    NodeLoader nodeLoader,
    Hash accountHash,
    SegmentedKeyValueStorage storage);
```

To:
```java
public abstract Optional<Bytes> getFlatAccount(
    Supplier<Optional<Bytes>> worldStateRootHashSupplier,
    NodeLoader nodeLoader,
    Hash accountHash,
    SegmentedKeyValueStorage storage,
    Supplier<Optional<BonsaiContext>> readContextSupplier);
```

**Step 3: Update getFlatStorageValueByStorageSlotKey signature**

Change:
```java
public abstract Optional<Bytes> getFlatStorageValueByStorageSlotKey(
    Supplier<Optional<Bytes>> worldStateRootHashSupplier,
    Supplier<Optional<Hash>> storageRootSupplier,
    NodeLoader nodeLoader,
    Hash accountHash,
    StorageSlotKey storageSlotKey,
    SegmentedKeyValueStorage storageStorage);
```

To:
```java
public abstract Optional<Bytes> getFlatStorageValueByStorageSlotKey(
    Supplier<Optional<Bytes>> worldStateRootHashSupplier,
    Supplier<Optional<Hash>> storageRootSupplier,
    NodeLoader nodeLoader,
    Hash accountHash,
    StorageSlotKey storageSlotKey,
    SegmentedKeyValueStorage storageStorage,
    Supplier<Optional<BonsaiContext>> readContextSupplier);
```

**Step 4: Update putFlatAccount to add context parameter (ignore it)**

Change:
```java
@Override
public void putFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Bytes accountValue) {
  transaction.put(
      ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe(), accountValue.toArrayUnsafe());
}
```

To:
```java
@Override
public void putFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Bytes accountValue,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  // Non-archive mode ignores context
  transaction.put(
      ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe(), accountValue.toArrayUnsafe());
}
```

**Step 5: Update removeFlatAccount to add context parameter (ignore it)**

Change:
```java
@Override
public void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash) {
  transaction.remove(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
}
```

To:
```java
@Override
public void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  // Non-archive mode ignores context
  transaction.remove(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
}
```

**Step 6: Update putFlatAccountStorageValueByStorageSlotHash to add context parameter (ignore it)**

Change:
```java
@Override
public void putFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Bytes storageValue) {
  transaction.put(
      ACCOUNT_STORAGE_STORAGE,
      Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe(),
      storageValue.toArrayUnsafe());
}
```

To:
```java
@Override
public void putFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Bytes storageValue,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  // Non-archive mode ignores context
  transaction.put(
      ACCOUNT_STORAGE_STORAGE,
      Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe(),
      storageValue.toArrayUnsafe());
}
```

**Step 7: Update removeFlatAccountStorageValueByStorageSlotHash to add context parameter (ignore it)**

Change:
```java
@Override
public void removeFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash) {
  transaction.remove(
      ACCOUNT_STORAGE_STORAGE,
      Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe());
}
```

To:
```java
@Override
public void removeFlatAccountStorageValueByStorageSlotHash(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Hash slotHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  // Non-archive mode ignores context
  transaction.remove(
      ACCOUNT_STORAGE_STORAGE,
      Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe());
}
```

---

## Task 3: Update BonsaiFullFlatDbStrategy

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiFullFlatDbStrategy.java`

**Step 1: Add import for BonsaiContext**

Add to imports:
```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
```

**Step 2: Update getFlatAccount signature (ignore context)**

Add `Supplier<Optional<BonsaiContext>> readContextSupplier` as final parameter to the method. The implementation stays the same - non-archive mode ignores context.

**Step 3: Update getFlatStorageValueByStorageSlotKey signature (ignore context)**

Add `Supplier<Optional<BonsaiContext>> readContextSupplier` as final parameter to the method. The implementation stays the same.

---

## Task 4: Update BonsaiPartialFlatDbStrategy

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiPartialFlatDbStrategy.java`

**Step 1: Add import for BonsaiContext**

Add to imports:
```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
```

**Step 2: Update getFlatAccount signature (ignore context)**

Add `Supplier<Optional<BonsaiContext>> readContextSupplier` as final parameter.

**Step 3: Update getFlatStorageValueByStorageSlotKey signature (ignore context)**

Add `Supplier<Optional<BonsaiContext>> readContextSupplier` as final parameter.

---

## Task 5: Update BonsaiArchiveFlatDbStrategy to Use Passed Context

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java`

**Step 1: Update getFlatAccount to use passed context**

Change method signature to accept `Supplier<Optional<BonsaiContext>> readContextSupplier`.

Replace:
```java
Bytes keyNearest =
    calculateArchiveKeyWithMaxSuffix(
        getStateArchiveContextForRead(storage), accountHash.getBytes().toArrayUnsafe());
```

With:
```java
BonsaiContext readContext = readContextSupplier.get()
    .orElseThrow(() -> new IllegalStateException("Read context required for archive flat DB"));

Bytes keyNearest =
    calculateArchiveKeyWithMaxSuffix(
        Optional.of(readContext), accountHash.getBytes().toArrayUnsafe());
```

**Step 2: Update getFlatStorageValueByStorageSlotKey to use passed context**

Change method signature to accept `Supplier<Optional<BonsaiContext>> readContextSupplier`.

Replace:
```java
Bytes keyNearest =
    calculateArchiveKeyWithMaxSuffix(getStateArchiveContextForRead(storage), naturalKey);
```

With:
```java
BonsaiContext readContext = readContextSupplier.get()
    .orElseThrow(() -> new IllegalStateException("Read context required for archive flat DB"));

Bytes keyNearest =
    calculateArchiveKeyWithMaxSuffix(Optional.of(readContext), naturalKey);
```

**Step 3: Update putFlatAccount to use passed context**

Change method signature to accept `Supplier<Optional<BonsaiContext>> writeContextSupplier`.

Replace:
```java
byte[] keySuffixed =
    calculateArchiveKeyWithMinSuffix(
        getStateArchiveContextForWrite(storage).get(), accountHash.getBytes().toArrayUnsafe());
```

With:
```java
BonsaiContext writeContext = writeContextSupplier.get()
    .orElseThrow(() -> new IllegalStateException("Write context required for archive flat DB"));

byte[] keySuffixed =
    calculateArchiveKeyWithMinSuffix(
        writeContext, accountHash.getBytes().toArrayUnsafe());
```

**Step 4: Update removeFlatAccount to use passed context**

Similar change - use passed `writeContextSupplier` instead of `getStateArchiveContextForWrite(storage)`.

**Step 5: Update putFlatAccountStorageValueByStorageSlotHash to use passed context**

Similar change - use passed `writeContextSupplier`.

**Step 6: Update removeFlatAccountStorageValueByStorageSlotHash to use passed context**

Similar change - use passed `writeContextSupplier`.

**Step 7: Remove or deprecate getStateArchiveContextForRead and getStateArchiveContextForWrite methods**

These are no longer needed since context is passed in. Remove the methods.

---

## Task 6: Update BonsaiWorldStateKeyValueStorage to Pass Context

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiWorldStateKeyValueStorage.java`

**Step 1: Add import for BonsaiContext**

Add to imports:
```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
```

**Step 2: Update getAccount method**

Change:
```java
public Optional<Bytes> getAccount(final Hash accountHash) {
  return getFlatDbStrategy()
      .getFlatAccount(
          this::getWorldStateRootHash,
          this::getAccountStateTrieNode,
          accountHash,
          composedWorldStateStorage);
}
```

To:
```java
public Optional<Bytes> getAccount(
    final Hash accountHash,
    final Supplier<Optional<BonsaiContext>> readContextSupplier) {
  return getFlatDbStrategy()
      .getFlatAccount(
          this::getWorldStateRootHash,
          this::getAccountStateTrieNode,
          accountHash,
          composedWorldStateStorage,
          readContextSupplier);
}
```

**Step 3: Update getStorageValueByStorageSlotKey methods**

Add `Supplier<Optional<BonsaiContext>> readContextSupplier` parameter and pass through.

**Step 4: Update Updater class to accept and store writeContextSupplier**

Add field and constructor parameter:
```java
private final Supplier<Optional<BonsaiContext>> writeContextSupplier;

public Updater(
    final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
    final KeyValueStorageTransaction trieLogStorageTransaction,
    final FlatDbStrategy flatDbStrategy,
    final SegmentedKeyValueStorage worldStorage,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  // ... existing assignments
  this.writeContextSupplier = writeContextSupplier;
}
```

**Step 5: Update Updater methods to pass context**

Update `putAccountInfoState`, `removeAccountInfoState`, `putStorageValueBySlotHash`, `removeStorageValueBySlotHash` to pass `writeContextSupplier` to flatDbStrategy methods.

**Step 6: Update updater() factory method**

Change:
```java
@Override
public Updater updater() {
  return new Updater(
      composedWorldStateStorage.startTransaction(),
      trieLogStorage.startTransaction(),
      getFlatDbStrategy(),
      composedWorldStateStorage);
}
```

To:
```java
public Updater updater(final Supplier<Optional<BonsaiContext>> writeContextSupplier) {
  return new Updater(
      composedWorldStateStorage.startTransaction(),
      trieLogStorage.startTransaction(),
      getFlatDbStrategy(),
      composedWorldStateStorage,
      writeContextSupplier);
}
```

---

## Task 7: Update BonsaiSnapshotWorldStateKeyValueStorage

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/BonsaiSnapshotWorldStateKeyValueStorage.java`

**Step 1: Add import for BonsaiContext**

**Step 2: Update getAccount override to accept and pass through context**

**Step 3: Update getStorageValueByStorageSlotKey overrides to accept and pass through context**

**Step 4: Update updater() override to accept and pass through context**

---

## Task 8: Create BonsaiArchiveWorldState Class

**Files:**
- Create: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/worldview/BonsaiArchiveWorldState.java`

**Step 1: Create new class**

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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.function.Supplier;

public class BonsaiArchiveWorldState extends BonsaiWorldState {

  private Optional<BonsaiContext> readContext = Optional.empty();
  private Optional<BonsaiContext> writeContext = Optional.empty();

  public BonsaiArchiveWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(archive, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache);
  }

  public Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
    return () -> readContext;
  }

  public Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
    return () -> writeContext;
  }

  public void setReadContext(final BonsaiContext context) {
    this.readContext = Optional.of(context);
  }

  public void setWriteContext(final BonsaiContext context) {
    this.writeContext = Optional.of(context);
  }

  public void clearReadContext() {
    this.readContext = Optional.empty();
  }

  public void clearWriteContext() {
    this.writeContext = Optional.empty();
  }
}
```

---

## Task 9: Update BonsaiWorldState to Use Context from Subclass

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/worldview/BonsaiWorldState.java`

**Step 1: Add default context supplier methods**

Add methods that can be overridden by `BonsaiArchiveWorldState`:
```java
protected Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
  return Optional::empty;
}

protected Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
  return Optional::empty;
}
```

**Step 2: Update get(Address) to pass read context**

Update calls to `getWorldStateStorage().getAccount()` to pass `getReadContextSupplier()`.

**Step 3: Update storage access methods to pass read context**

**Step 4: Update persist/updater calls to pass write context**

---

## Task 10: Update BonsaiArchiveWorldStateProvider to Set Contexts

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProvider.java`

**Step 1: Import BonsaiArchiveWorldState and BonsaiContext**

**Step 2: Create BonsaiArchiveWorldState instances instead of BonsaiWorldState**

**Step 3: Set read context for historical queries**

In `getWorldState()`, after creating world state:
```java
if (worldState instanceof BonsaiArchiveWorldState archiveWorldState) {
  archiveWorldState.setReadContext(new BonsaiContext(queryParams.getBlockHeader().getNumber()));
  if (queryParams.shouldWorldStateUpdateHead()) {
    archiveWorldState.setWriteContext(new BonsaiContext(queryParams.getBlockHeader().getNumber()));
  }
}
```

**Step 4: Set contexts appropriately during rollback/rollforward operations**

---

## Task 11: Run Tests and Verify Fix

**Step 1: Run the integration tests**

Run: `./gradlew :ethereum:core:test --tests "BonsaiArchiveReorgIntegrationTest"`
Expected: All 20 tests pass (0 failures)

**Step 2: Run broader test suite**

Run: `./gradlew :ethereum:core:test`
Expected: All tests pass

**Step 3: Commit changes**

```bash
git add -A
git commit -m "feat: pass archive context through parameters instead of reading from DB

This fixes reorg issues where the global WORLD_BLOCK_NUMBER_KEY in the
database caused archive mode to return values from orphaned chains
after a reorg.

Key changes:
- Create BonsaiArchiveWorldState to hold read/write contexts
- Add Supplier<Optional<BonsaiContext>> parameters to strategy methods
- BonsaiArchiveWorldStateProvider sets contexts based on operation
- Non-archive strategies ignore the context parameter

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```
