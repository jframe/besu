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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BonsaiFlatDbToArchiveMigratorTest {

  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private TrieLogManager trieLogManager;
  @Mock private Blockchain blockchain;

  private SegmentedKeyValueStorage storage;
  private ScheduledExecutorService executorService;
  private BonsaiFlatDbToArchiveMigrator migrator;
  private BonsaiArchiveFlatDbStrategy archiveStrategy;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    executorService = Executors.newSingleThreadScheduledExecutor();
    migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            trieLogManager,
            blockchain,
            executorService,
            new org.hyperledger.besu.metrics.noop.NoOpMetricsSystem());
    archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(
            new org.hyperledger.besu.metrics.noop.NoOpMetricsSystem(),
            new org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat
                .CodeHashCodeStorageStrategy());

    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(storage);
  }

  @AfterEach
  void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  void processesSingleBlock() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    PmtStateTrieAccountValue priorAccountValue =
        new PmtStateTrieAccountValue(
            1L, Wei.of(100), Hash.hash(Bytes.of(1, 2, 3)), Hash.hash(Bytes.of(4, 5, 6)));
    PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(
            2L, Wei.of(200), Hash.hash(Bytes.of(7, 8, 9)), Hash.hash(Bytes.of(10, 11, 12)));

    TrieLogLayer trieLog =
        createTrieLogWithAccountChange(testAddress, priorAccountValue, newAccountValue);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    verify(worldStateStorage, times(1)).upgradeToArchiveDbMode();

    BonsaiContext context = new BonsaiContext(0L);
    byte[] expectedKey =
        BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMaxSuffix(
                Optional.of(context), testAddress.addressHash().toArrayUnsafe())
            .toArrayUnsafe();
    Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    Bytes expectedAccountBytes = RLP.encode(newAccountValue::writeTo);
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(expectedAccountBytes);
  }

  @Test
  void processesMultipleBlocks_batchCommit() {
    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    StorageSlotKey slotKey = new StorageSlotKey(Hash.hash(Bytes.of(1, 2, 3)), Optional.empty());

    for (long i = 0; i < 15; i++) {
      BlockHeader header = createBlockHeader(i);
      when(blockchain.getBlockHeader(i)).thenReturn(Optional.of(header));

      PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(
              i, Wei.of(1000 + i), Hash.hash(Bytes.of((byte) i)), Hash.EMPTY);
      UInt256 storageValue = UInt256.valueOf(500 + i);

      TrieLogLayer trieLog = new TrieLogLayer();
      trieLog.addAccountChange(testAddress, null, accountValue);
      trieLog.addStorageChange(testAddress, slotKey, null, storageValue);

      when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));
    }

    CompletableFuture<Void> future = migrator.migrate(0L, 14L);
    Awaitility.await().until(future::isDone);

    verify(worldStateStorage, times(1)).upgradeToArchiveDbMode();

    for (long i = 0; i < 15; i++) {
      setWorldBlockNumber(i);

      Optional<Bytes> storedAccountValue = getAccountUsingStrategy(testAddress);

      assertThat(storedAccountValue).as("Block " + i + " account should be stored").isPresent();

      PmtStateTrieAccountValue expectedAccountValue =
          new PmtStateTrieAccountValue(
              i, Wei.of(1000 + i), Hash.hash(Bytes.of((byte) i)), Hash.EMPTY);
      Bytes expectedAccountBytes = RLP.encode(expectedAccountValue::writeTo);
      assertThat(storedAccountValue.get())
          .as("Block " + i + " account value should match")
          .isEqualTo(expectedAccountBytes);

      Optional<Bytes> storedStorageValue = getStorageValueUsingStrategy(testAddress, slotKey);

      assertThat(storedStorageValue).as("Block " + i + " storage should be stored").isPresent();

      UInt256 expectedStorageValue = UInt256.valueOf(500 + i);
      assertThat(storedStorageValue.get())
          .as("Block " + i + " storage value should match")
          .isEqualTo(expectedStorageValue.toBytes());
    }
  }

  @Test
  void migratesNewAccount() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(
            1L, Wei.of(100), Hash.hash(Bytes.of(1, 2, 3)), Hash.hash(Bytes.of(4, 5, 6)));

    TrieLogLayer trieLog = createTrieLogWithAccountChange(testAddress, null, newAccountValue);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getAccountUsingStrategy(testAddress);

    assertThat(storedValue).isPresent();
    Bytes expectedAccountBytes = RLP.encode(newAccountValue::writeTo);
    assertThat(storedValue.get()).isEqualTo(expectedAccountBytes);
  }

  @Test
  void migratesDeletedAccount() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    PmtStateTrieAccountValue priorAccountValue =
        new PmtStateTrieAccountValue(
            1L, Wei.of(100), Hash.hash(Bytes.of(1, 2, 3)), Hash.hash(Bytes.of(4, 5, 6)));

    TrieLogLayer trieLog = createTrieLogWithAccountChange(testAddress, priorAccountValue, null);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getAccountUsingStrategy(testAddress);

    assertThat(storedValue).isEmpty();
  }

  @Test
  void migratesUpdatedAccount() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    PmtStateTrieAccountValue priorAccountValue =
        new PmtStateTrieAccountValue(
            1L, Wei.of(100), Hash.hash(Bytes.of(1, 2, 3)), Hash.hash(Bytes.of(4, 5, 6)));
    PmtStateTrieAccountValue updatedAccountValue =
        new PmtStateTrieAccountValue(
            2L, Wei.of(200), Hash.hash(Bytes.of(7, 8, 9)), Hash.hash(Bytes.of(10, 11, 12)));

    TrieLogLayer trieLog =
        createTrieLogWithAccountChange(testAddress, priorAccountValue, updatedAccountValue);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getAccountUsingStrategy(testAddress);

    assertThat(storedValue).isPresent();
    Bytes expectedAccountBytes = RLP.encode(updatedAccountValue::writeTo);
    assertThat(storedValue.get()).isEqualTo(expectedAccountBytes);
  }

  @Test
  void migratesNewStorage() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    StorageSlotKey slotKey = new StorageSlotKey(Hash.hash(Bytes.of(1, 2, 3)), Optional.empty());
    UInt256 newStorageValue = UInt256.valueOf(123);

    TrieLogLayer trieLog =
        createTrieLogWithStorageChange(testAddress, slotKey, null, newStorageValue);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getStorageValueUsingStrategy(testAddress, slotKey);

    assertThat(storedValue).isPresent();
    assertThat(storedValue.get()).isEqualTo(newStorageValue.toBytes());
  }

  @Test
  void migratesDeletedStorage() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    StorageSlotKey slotKey = new StorageSlotKey(Hash.hash(Bytes.of(1, 2, 3)), Optional.empty());
    UInt256 priorStorageValue = UInt256.valueOf(123);

    TrieLogLayer trieLog =
        createTrieLogWithStorageChange(testAddress, slotKey, priorStorageValue, null);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getStorageValueUsingStrategy(testAddress, slotKey);

    assertThat(storedValue).isEmpty();
  }

  @Test
  void migratesUpdatedStorage() {
    BlockHeader header = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header));

    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    StorageSlotKey slotKey = new StorageSlotKey(Hash.hash(Bytes.of(1, 2, 3)), Optional.empty());
    UInt256 priorStorageValue = UInt256.valueOf(123);
    UInt256 updatedStorageValue = UInt256.valueOf(456);

    TrieLogLayer trieLog =
        createTrieLogWithStorageChange(
            testAddress, slotKey, priorStorageValue, updatedStorageValue);
    when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));

    CompletableFuture<Void> future = migrator.migrate(0L, 0L);
    Awaitility.await().until(future::isDone);

    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue = getStorageValueUsingStrategy(testAddress, slotKey);

    assertThat(storedValue).isPresent();
    assertThat(storedValue.get()).isEqualTo(updatedStorageValue.toBytes());
  }

  @Test
  void migratesAccount_createUpdateDelete() {
    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");

    // Block 0: Create account
    BlockHeader header0 = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header0));
    PmtStateTrieAccountValue value0 =
        new PmtStateTrieAccountValue(
            1L, Wei.of(100), Hash.hash(Bytes.of(1, 2, 3)), Hash.hash(Bytes.of(4, 5, 6)));
    TrieLogLayer trieLog0 = createTrieLogWithAccountChange(testAddress, null, value0);
    when(trieLogManager.getTrieLogLayer(header0.getHash())).thenReturn(Optional.of(trieLog0));

    // Block 1: Update account
    BlockHeader header1 = createBlockHeader(1L);
    when(blockchain.getBlockHeader(1L)).thenReturn(Optional.of(header1));
    PmtStateTrieAccountValue value1 =
        new PmtStateTrieAccountValue(
            2L, Wei.of(200), Hash.hash(Bytes.of(7, 8, 9)), Hash.hash(Bytes.of(10, 11, 12)));
    TrieLogLayer trieLog1 = createTrieLogWithAccountChange(testAddress, value0, value1);
    when(trieLogManager.getTrieLogLayer(header1.getHash())).thenReturn(Optional.of(trieLog1));

    // Block 2: Delete account
    BlockHeader header2 = createBlockHeader(2L);
    when(blockchain.getBlockHeader(2L)).thenReturn(Optional.of(header2));
    TrieLogLayer trieLog2 = createTrieLogWithAccountChange(testAddress, value1, null);
    when(trieLogManager.getTrieLogLayer(header2.getHash())).thenReturn(Optional.of(trieLog2));

    CompletableFuture<Void> future = migrator.migrate(0L, 2L);
    Awaitility.await().until(future::isDone);

    // Verify block 0 has value0
    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue0 = getAccountUsingStrategy(testAddress);
    assertThat(storedValue0).isPresent();
    assertThat(storedValue0.get()).isEqualTo(RLP.encode(value0::writeTo));

    // Verify block 1 has value1
    setWorldBlockNumber(1L);
    Optional<Bytes> storedValue1 = getAccountUsingStrategy(testAddress);
    assertThat(storedValue1).isPresent();
    assertThat(storedValue1.get()).isEqualTo(RLP.encode(value1::writeTo));

    // Verify block 2 has no value (deleted)
    setWorldBlockNumber(2L);
    Optional<Bytes> storedValue2 = getAccountUsingStrategy(testAddress);
    assertThat(storedValue2).isEmpty();
  }

  @Test
  void migratesStorage_createUpdateDelete() {
    Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
    StorageSlotKey slotKey = new StorageSlotKey(Hash.hash(Bytes.of(1, 2, 3)), Optional.empty());

    // Block 0: Create storage
    BlockHeader header0 = createBlockHeader(0L);
    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(header0));
    UInt256 value0 = UInt256.valueOf(100);
    TrieLogLayer trieLog0 = createTrieLogWithStorageChange(testAddress, slotKey, null, value0);
    when(trieLogManager.getTrieLogLayer(header0.getHash())).thenReturn(Optional.of(trieLog0));

    // Block 1: Update storage
    BlockHeader header1 = createBlockHeader(1L);
    when(blockchain.getBlockHeader(1L)).thenReturn(Optional.of(header1));
    UInt256 value1 = UInt256.valueOf(200);
    TrieLogLayer trieLog1 = createTrieLogWithStorageChange(testAddress, slotKey, value0, value1);
    when(trieLogManager.getTrieLogLayer(header1.getHash())).thenReturn(Optional.of(trieLog1));

    // Block 2: Delete storage
    BlockHeader header2 = createBlockHeader(2L);
    when(blockchain.getBlockHeader(2L)).thenReturn(Optional.of(header2));
    TrieLogLayer trieLog2 = createTrieLogWithStorageChange(testAddress, slotKey, value1, null);
    when(trieLogManager.getTrieLogLayer(header2.getHash())).thenReturn(Optional.of(trieLog2));

    CompletableFuture<Void> future = migrator.migrate(0L, 2L);
    Awaitility.await().until(future::isDone);

    // Verify block 0 has value0
    setWorldBlockNumber(0L);
    Optional<Bytes> storedValue0 = getStorageValueUsingStrategy(testAddress, slotKey);
    assertThat(storedValue0).isPresent();
    assertThat(storedValue0.get()).isEqualTo(value0.toBytes());

    // Verify block 1 has value1
    setWorldBlockNumber(1L);
    Optional<Bytes> storedValue1 = getStorageValueUsingStrategy(testAddress, slotKey);
    assertThat(storedValue1).isPresent();
    assertThat(storedValue1.get()).isEqualTo(value1.toBytes());

    // Verify block 2 has no value (deleted)
    setWorldBlockNumber(2L);
    Optional<Bytes> storedValue2 = getStorageValueUsingStrategy(testAddress, slotKey);
    assertThat(storedValue2).isEmpty();
  }

  @Test
  void progressIsSaved() {
    for (long i = 0; i <= 100; i++) {
      BlockHeader header = createBlockHeader(i);
      when(blockchain.getBlockHeader(i)).thenReturn(Optional.of(header));

      TrieLogLayer trieLog = createEmptyTrieLog();
      when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));
    }

    CompletableFuture<Void> future = migrator.migrate(0L, 100L);
    Awaitility.await().until(future::isDone);

    verify(worldStateStorage, times(1)).upgradeToArchiveDbMode();

    Optional<byte[]> progress = storage.get(TRIE_BRANCH_STORAGE, MIGRATION_PROGRESS_KEY);
    assertThat(progress).isPresent();
    assertThat(Bytes.wrap(progress.get()).toLong()).isEqualTo(100L);
  }

  @Test
  void upgradeToArchiveModeIsCalledBeforeBlockProcessing() {
    // Set up multiple blocks so we can verify the order
    for (long i = 0; i < 5; i++) {
      BlockHeader header = createBlockHeader(i);
      when(blockchain.getBlockHeader(i)).thenReturn(Optional.of(header));

      Address testAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
      PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(
              i, Wei.of(100 + i), Hash.hash(Bytes.of((byte) i)), Hash.EMPTY);
      TrieLogLayer trieLog = createTrieLogWithAccountChange(testAddress, null, accountValue);
      when(trieLogManager.getTrieLogLayer(header.getHash())).thenReturn(Optional.of(trieLog));
    }

    // Clear mock invocations from when() stub setup
    org.mockito.Mockito.clearInvocations(worldStateStorage, blockchain, trieLogManager);

    CompletableFuture<Void> future = migrator.migrate(0L, 4L);
    Awaitility.await().until(future::isDone);

    // Verify upgradeToArchiveDbMode is called before any block header is fetched
    // This ensures the archive mode is set before migration processing begins
    InOrder inOrder = inOrder(worldStateStorage, blockchain);
    inOrder.verify(worldStateStorage).upgradeToArchiveDbMode();
    inOrder.verify(blockchain, atLeastOnce()).getBlockHeader(anyLong());
  }

  private TrieLogLayer createEmptyTrieLog() {
    return new TrieLogLayer();
  }

  private TrieLogLayer createTrieLogWithAccountChange(
      final Address address, final AccountValue priorValue, final AccountValue newValue) {
    TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addAccountChange(address, priorValue, newValue);
    return trieLog;
  }

  private TrieLogLayer createTrieLogWithStorageChange(
      final Address address,
      final StorageSlotKey slotKey,
      final UInt256 priorValue,
      final UInt256 newValue) {
    TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addStorageChange(address, slotKey, priorValue, newValue);
    return trieLog;
  }

  private BlockHeader createBlockHeader(final long blockNumber) {
    return BlockHeaderBuilder.createDefault()
        .number(blockNumber)
        .timestamp(System.currentTimeMillis())
        .buildBlockHeader();
  }

  private void setWorldBlockNumber(final long blockNumber) {
    final org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction tx =
        storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        org.hyperledger.besu.ethereum.trie.pathbased.common.storage
            .PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY,
        org.apache.tuweni.bytes.Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  private Optional<Bytes> getStorageValueUsingStrategy(
      final Address accountAddress, final StorageSlotKey slotKey) {
    return archiveStrategy.getFlatStorageValueByStorageSlotKey(
        Optional::empty,
        Optional::empty,
        (location, hash) -> Optional.empty(),
        accountAddress.addressHash(),
        slotKey,
        storage);
  }

  private Optional<Bytes> getAccountUsingStrategy(final Address accountAddress) {
    return archiveStrategy.getFlatAccount(
        Optional::empty,
        (location, hash) -> Optional.empty(),
        accountAddress.addressHash(),
        storage);
  }
}
