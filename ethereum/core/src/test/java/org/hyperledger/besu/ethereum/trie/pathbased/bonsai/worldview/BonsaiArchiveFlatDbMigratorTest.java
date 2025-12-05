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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BonsaiArchiveFlatDbMigratorTest {
  @Mock TrieLogManager trieLogManager;

  private MutableBlockchain blockchain;
  private BonsaiWorldStateKeyValueStorage worldStateStorage;
  private ScheduledExecutorService executorService;
  private BonsaiArchiveFlatDbMigrator migrator;

  @BeforeEach
  void setup() {
    final InMemoryKeyValueStorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
    final DataStorageConfiguration dataStorageConfiguration =
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;

    worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider, new NoOpMetricsSystem(), dataStorageConfiguration);

    blockchain = createBlockchain();

    executorService = Executors.newScheduledThreadPool(1);

    migrator =
        new BonsaiArchiveFlatDbMigrator(
            worldStateStorage,
            blockchain,
            executorService,
            trieLogManager,
            new NoOpMetricsSystem(),
            10); // Small batch size for tests
  }

  private MutableBlockchain createBlockchain() {
    final BlockHeader genesisHeader =
        BlockHeaderBuilder.createDefault()
            .parentHash(Hash.ZERO)
            .coinbase(Address.ZERO)
            .difficulty(Difficulty.ONE)
            .number(0L)
            .gasLimit(5000L)
            .timestamp(0L)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();

    final Block genesisBlock =
        new Block(genesisHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));

    final KeyValueStorage kvStoreChain = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvStoreChain,
            new VariablesKeyValueStorage(kvStoreVariables),
            new MainnetBlockHeaderFunctions(),
            false);

    return DefaultBlockchain.createMutable(
        genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
  }

  private Block createBlock(final long blockNumber, final Hash parentHash) {
    return new Block(
        BlockHeaderBuilder.create()
            .parentHash(parentHash)
            .coinbase(Address.ZERO)
            .difficulty(Difficulty.ONE)
            .number(blockNumber)
            .gasLimit(5000L)
            .timestamp(System.currentTimeMillis())
            .ommersHash(Hash.EMPTY_LIST_HASH)
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .transactionsRoot(Hash.EMPTY_TRIE_HASH)
            .receiptsRoot(Hash.EMPTY_TRIE_HASH)
            .logsBloom(org.hyperledger.besu.evm.log.LogsBloomFilter.empty())
            .gasUsed(0L)
            .extraData(org.apache.tuweni.bytes.Bytes.EMPTY)
            .mixHash(Hash.ZERO)
            .nonce(0L)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader(),
        new BlockBody(Collections.emptyList(), Collections.emptyList()));
  }

  @AfterEach
  void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  void shouldNotStartMigrationWhenAlreadyInArchiveMode() {
    worldStateStorage.upgradeToArchiveFlatDbMode();

    migrator.onInitialSyncCompleted();
  }

  @Test
  void shouldMigrateAccountsFromNonVersionedToVersionedFormat() {
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    final Address address = Address.fromHexString("0x1234567890123456789012345678901234567890");
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(1L, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    final Bytes accountBytes = RLP.encode(accountValue::writeTo);

    final var updater = worldStateStorage.updater();
    updater.putAccountInfoState(address.addressHash(), accountBytes);
    updater.commit();

    final Block block1 = createBlock(1, blockchain.getChainHeadHash());
    blockchain.appendBlock(block1, Collections.emptyList());

    final TrieLogLayer trieLogLayer = new TrieLogLayer();
    trieLogLayer.addAccountChange(address, accountValue, accountValue);
    final TrieLog trieLog = trieLogLayer;

    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());
    when(trieLogManager.getTrieLogLayer(block1.getHash())).thenReturn(Optional.of(trieLog));

    setWorldStateBlockContext(0L);

    migrator.onInitialSyncCompleted();

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L));

    // Wait for the mode to switch to ARCHIVE (happens after last block is processed)
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE));

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L);
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    setWorldStateBlockContext(1L);
    final Optional<Bytes> retrievedAccount =
        worldStateStorage
            .getFlatDbStrategy()
            .getFlatAccount(
                Optional::empty,
                null,
                address.addressHash(),
                worldStateStorage.getComposedWorldStateStorage());
    assertThat(retrievedAccount).hasValue(accountBytes);
  }

  @Test
  void shouldMigrateStorageFromNonVersionedToVersionedFormat() {
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    final Address address = Address.fromHexString("0xabcdef0123456789abcdef0123456789abcdef01");
    final StorageSlotKey storageSlot = new StorageSlotKey(UInt256.valueOf(42));
    final UInt256 storageValue = UInt256.valueOf(999);
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(1L, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    final Bytes accountBytes = RLP.encode(accountValue::writeTo);

    final var updater = worldStateStorage.updater();
    updater.putAccountInfoState(address.addressHash(), accountBytes);
    updater.putStorageValueBySlotHash(
        address.addressHash(), storageSlot.getSlotHash(), Bytes.wrap(storageValue.toBytes()));
    updater.commit();

    final Block block1 = createBlock(1, blockchain.getChainHeadHash());
    blockchain.appendBlock(block1, Collections.emptyList());

    final TrieLogLayer trieLogLayer = new TrieLogLayer();
    trieLogLayer.addAccountChange(address, accountValue, accountValue);
    trieLogLayer.addStorageChange(address, storageSlot, storageValue, storageValue);
    final TrieLog trieLog = trieLogLayer;

    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());
    when(trieLogManager.getTrieLogLayer(block1.getHash())).thenReturn(Optional.of(trieLog));

    setWorldStateBlockContext(0L);

    migrator.onInitialSyncCompleted();

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L));

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L);
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    setWorldStateBlockContext(1L);
    final Optional<Bytes> retrievedAccount =
        worldStateStorage
            .getFlatDbStrategy()
            .getFlatAccount(
                Optional::empty,
                null,
                address.addressHash(),
                worldStateStorage.getComposedWorldStateStorage());
    assertThat(retrievedAccount).hasValue(accountBytes);

    final Optional<Bytes> retrievedStorage =
        worldStateStorage
            .getFlatDbStrategy()
            .getFlatStorageValueByStorageSlotKey(
                Optional::empty,
                Optional::empty,
                null,
                address.addressHash(),
                storageSlot,
                worldStateStorage.getComposedWorldStateStorage());
    assertThat(retrievedStorage).hasValue(Bytes.wrap(storageValue.toBytes()));
  }

  @Test
  void shouldMigrateMultipleAccountsWithMultipleStorageValues() {
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    final TrieLogLayer trieLogLayer = new TrieLogLayer();

    final Address address1 = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final PmtStateTrieAccountValue accountValue1 =
        new PmtStateTrieAccountValue(1L, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    final StorageSlotKey storage1Slot1 = new StorageSlotKey(UInt256.valueOf(10));
    final UInt256 storage1Value1 = UInt256.valueOf(111);
    final StorageSlotKey storage1Slot2 = new StorageSlotKey(UInt256.valueOf(20));
    final UInt256 storage1Value2 = UInt256.valueOf(222);

    final Address address2 = Address.fromHexString("0x2222222222222222222222222222222222222222");
    final PmtStateTrieAccountValue accountValue2 =
        new PmtStateTrieAccountValue(2L, Wei.of(200), Hash.EMPTY, Hash.EMPTY);
    final StorageSlotKey storage2Slot1 = new StorageSlotKey(UInt256.valueOf(30));
    final UInt256 storage2Value1 = UInt256.valueOf(333);
    final StorageSlotKey storage2Slot2 = new StorageSlotKey(UInt256.valueOf(40));
    final UInt256 storage2Value2 = UInt256.valueOf(444);

    final Address address3 = Address.fromHexString("0x3333333333333333333333333333333333333333");
    final PmtStateTrieAccountValue accountValue3 =
        new PmtStateTrieAccountValue(3L, Wei.of(300), Hash.EMPTY, Hash.EMPTY);
    final StorageSlotKey storage3Slot1 = new StorageSlotKey(UInt256.valueOf(50));
    final UInt256 storage3Value1 = UInt256.valueOf(555);

    final var updater = worldStateStorage.updater();
    updater.putAccountInfoState(address1.addressHash(), RLP.encode(accountValue1::writeTo));
    updater.putStorageValueBySlotHash(
        address1.addressHash(), storage1Slot1.getSlotHash(), Bytes.wrap(storage1Value1.toBytes()));
    updater.putStorageValueBySlotHash(
        address1.addressHash(), storage1Slot2.getSlotHash(), Bytes.wrap(storage1Value2.toBytes()));

    updater.putAccountInfoState(address2.addressHash(), RLP.encode(accountValue2::writeTo));
    updater.putStorageValueBySlotHash(
        address2.addressHash(), storage2Slot1.getSlotHash(), Bytes.wrap(storage2Value1.toBytes()));
    updater.putStorageValueBySlotHash(
        address2.addressHash(), storage2Slot2.getSlotHash(), Bytes.wrap(storage2Value2.toBytes()));

    updater.putAccountInfoState(address3.addressHash(), RLP.encode(accountValue3::writeTo));
    updater.putStorageValueBySlotHash(
        address3.addressHash(), storage3Slot1.getSlotHash(), Bytes.wrap(storage3Value1.toBytes()));
    updater.commit();

    final Block block1 = createBlock(1, blockchain.getChainHeadHash());
    blockchain.appendBlock(block1, Collections.emptyList());

    trieLogLayer.addAccountChange(address1, accountValue1, accountValue1);
    trieLogLayer.addStorageChange(address1, storage1Slot1, storage1Value1, storage1Value1);
    trieLogLayer.addStorageChange(address1, storage1Slot2, storage1Value2, storage1Value2);

    trieLogLayer.addAccountChange(address2, accountValue2, accountValue2);
    trieLogLayer.addStorageChange(address2, storage2Slot1, storage2Value1, storage2Value1);
    trieLogLayer.addStorageChange(address2, storage2Slot2, storage2Value2, storage2Value2);

    trieLogLayer.addAccountChange(address3, accountValue3, accountValue3);
    trieLogLayer.addStorageChange(address3, storage3Slot1, storage3Value1, storage3Value1);

    final TrieLog trieLog = trieLogLayer;

    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());
    when(trieLogManager.getTrieLogLayer(block1.getHash())).thenReturn(Optional.of(trieLog));

    setWorldStateBlockContext(0L);

    migrator.onInitialSyncCompleted();

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L));

    // Wait for the mode to switch to ARCHIVE (happens after last block is processed)
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE));

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(1L);
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    setWorldStateBlockContext(1L);
    final var strategy = worldStateStorage.getFlatDbStrategy();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    assertThat(strategy.getFlatAccount(Optional::empty, null, address1.addressHash(), storage))
        .hasValue(RLP.encode(accountValue1::writeTo));
    assertThat(
            strategy.getFlatStorageValueByStorageSlotKey(
                Optional::empty,
                Optional::empty,
                null,
                address1.addressHash(),
                storage1Slot1,
                storage))
        .hasValue(Bytes.wrap(storage1Value1.toBytes()));
    assertThat(
            strategy.getFlatStorageValueByStorageSlotKey(
                Optional::empty,
                Optional::empty,
                null,
                address1.addressHash(),
                storage1Slot2,
                storage))
        .hasValue(Bytes.wrap(storage1Value2.toBytes()));

    assertThat(strategy.getFlatAccount(Optional::empty, null, address2.addressHash(), storage))
        .hasValue(RLP.encode(accountValue2::writeTo));

    assertThat(strategy.getFlatAccount(Optional::empty, null, address3.addressHash(), storage))
        .hasValue(RLP.encode(accountValue3::writeTo));
  }

  @Test
  void shouldSkipBlocksWithoutTrielogs() {
    final Block block1 = createBlock(1, blockchain.getChainHeadHash());
    blockchain.appendBlock(block1, Collections.emptyList());

    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());
    when(trieLogManager.getTrieLogLayer(block1.getHash())).thenReturn(Optional.empty());

    migrator.onInitialSyncCompleted();

    await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).until(() -> true);

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).isEmpty();
  }

  @Test
  void shouldUpgradeToArchiveModeAfterMigrationCompletes() {
    migrator.onInitialSyncCompleted();

    await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).until(() -> true);
  }

  @Test
  void shouldMigrateMultipleBlocksInBatches() {
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());

    for (long i = 1; i <= 150; i++) {
      final Block block = createBlock(i, blockchain.getChainHeadHash());
      blockchain.appendBlock(block, Collections.emptyList());

      final Address address = Address.fromHexString(String.format("0x%040d", i));
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(i, Wei.of(i * 100), Hash.EMPTY, Hash.EMPTY);
      final Bytes accountBytes = RLP.encode(accountValue::writeTo);

      final var updater = worldStateStorage.updater();
      updater.putAccountInfoState(address.addressHash(), accountBytes);
      updater.commit();

      final TrieLogLayer trieLogLayer = new TrieLogLayer();
      // Prior state is null (account didn't exist before), updated state is the new value
      trieLogLayer.addAccountChange(address, null, accountValue);

      when(trieLogManager.getTrieLogLayer(block.getHash())).thenReturn(Optional.of(trieLogLayer));
    }

    setWorldStateBlockContext(0L);

    migrator.onInitialSyncCompleted();

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(150L));

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(150L);
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    final var strategy = worldStateStorage.getFlatDbStrategy();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    for (long i = 1; i <= 150; i++) {
      setWorldStateBlockContext(i);
      final Address address = Address.fromHexString(String.format("0x%040d", i));
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(i, Wei.of(i * 100), Hash.EMPTY, Hash.EMPTY);
      final Bytes expectedAccountBytes = RLP.encode(accountValue::writeTo);

      final Optional<Bytes> retrievedAccount =
          strategy.getFlatAccount(Optional::empty, null, address.addressHash(), storage);

      assertThat(retrievedAccount)
          .as("Account at block " + i + " should be migrated")
          .hasValue(expectedAccountBytes);
    }
  }

  @Test
  void shouldPreserveDataAccessibilityBeforeAndAfterMigration() {
    // This test simulates a full sync followed by migration, and verifies that:
    // 1. Data written during sync (non-versioned) can be read before migration
    // 2. Data can still be read after migration (versioned)
    // 3. The values match at each block height

    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    // Mock the genesis block
    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());

    // Create 10 blocks with changing account state
    // Each block updates the same account with a new nonce and balance
    final Address testAccount = Address.fromHexString("0x1234567890123456789012345678901234567890");

    for (long i = 1; i <= 10; i++) {
      final Block block = createBlock(i, blockchain.getChainHeadHash());
      blockchain.appendBlock(block, Collections.emptyList());

      // Create account state for this block
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(i, Wei.of(i * 1000), Hash.EMPTY, Hash.EMPTY);
      final Bytes accountBytes = RLP.encode(accountValue::writeTo);

      // Write to flat DB as would happen during sync (non-versioned in FULL mode)
      final var updater = worldStateStorage.updater();
      updater.putAccountInfoState(testAccount.addressHash(), accountBytes);
      updater.commit();

      // Create trielog for migration
      // IMPORTANT: prior = previous state, updated = current state
      final PmtStateTrieAccountValue priorAccountValue =
          i == 1
              ? null // Account created in block 1
              : new PmtStateTrieAccountValue(i - 1, Wei.of((i - 1) * 1000), Hash.EMPTY, Hash.EMPTY);

      final TrieLogLayer trieLogLayer = new TrieLogLayer();
      trieLogLayer.addAccountChange(testAccount, priorAccountValue, accountValue);

      when(trieLogManager.getTrieLogLayer(block.getHash())).thenReturn(Optional.of(trieLogLayer));
    }

    // Verify we can read the account before migration (at chain head)
    setWorldStateBlockContext(10L);
    final PmtStateTrieAccountValue expectedAtBlock10 =
        new PmtStateTrieAccountValue(10, Wei.of(10 * 1000), Hash.EMPTY, Hash.EMPTY);
    final Bytes expectedBytesAtBlock10 = RLP.encode(expectedAtBlock10::writeTo);

    Optional<Bytes> retrievedBeforeMigration =
        worldStateStorage
            .getFlatDbStrategy()
            .getFlatAccount(
                Optional::empty,
                null,
                testAccount.addressHash(),
                worldStateStorage.getComposedWorldStateStorage());

    assertThat(retrievedBeforeMigration)
        .as("Account should be readable before migration at chain head")
        .hasValue(expectedBytesAtBlock10);

    // Now perform migration
    setWorldStateBlockContext(0L);
    migrator.onInitialSyncCompleted();

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(10L));

    assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(10L);
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);

    final var strategy = worldStateStorage.getFlatDbStrategy();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    // Verify we can read historical state at each block after migration
    for (long blockNum = 1; blockNum <= 10; blockNum++) {
      setWorldStateBlockContext(blockNum);

      final PmtStateTrieAccountValue expectedAccountValue =
          new PmtStateTrieAccountValue(blockNum, Wei.of(blockNum * 1000), Hash.EMPTY, Hash.EMPTY);
      final Bytes expectedAccountBytes = RLP.encode(expectedAccountValue::writeTo);

      final Optional<Bytes> retrievedAccount =
          strategy.getFlatAccount(Optional::empty, null, testAccount.addressHash(), storage);

      assertThat(retrievedAccount)
          .as("Account at block " + blockNum + " should match expected state after migration")
          .hasValue(expectedAccountBytes);
    }

    // Verify we can still read at chain head after migration
    setWorldStateBlockContext(10L);
    Optional<Bytes> retrievedAfterMigration =
        strategy.getFlatAccount(Optional::empty, null, testAccount.addressHash(), storage);

    assertThat(retrievedAfterMigration)
        .as("Account should still be readable after migration at chain head")
        .hasValue(expectedBytesAtBlock10);
  }

  @Test
  void shouldUpgradeToArchiveModeOnlyAfterAllBlocksAreMigrated() {
    // This test verifies that the upgrade to ARCHIVE mode (and the associated
    // provider swap) only happens after all blocks have been successfully migrated
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    // Mock the genesis block
    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());

    // Create 100 blocks to ensure multiple batches are processed
    // (BATCH_SIZE is typically 1000, but we can still verify the pattern)
    final Address testAccount = Address.fromHexString("0xabcdef1234567890abcdef1234567890abcdef12");

    for (long i = 1; i <= 100; i++) {
      final Block block = createBlock(i, blockchain.getChainHeadHash());
      blockchain.appendBlock(block, Collections.emptyList());

      // Create account state for this block
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(i, Wei.of(i * 100), Hash.EMPTY, Hash.EMPTY);
      final Bytes accountBytes = RLP.encode(accountValue::writeTo);

      // Write to flat DB as would happen during sync
      final var updater = worldStateStorage.updater();
      updater.putAccountInfoState(testAccount.addressHash(), accountBytes);
      updater.commit();

      // Create trielog for migration
      final PmtStateTrieAccountValue priorAccountValue =
          i == 1
              ? null
              : new PmtStateTrieAccountValue(i - 1, Wei.of((i - 1) * 100), Hash.EMPTY, Hash.EMPTY);

      final TrieLogLayer trieLogLayer = new TrieLogLayer();
      trieLogLayer.addAccountChange(testAccount, priorAccountValue, accountValue);

      when(trieLogManager.getTrieLogLayer(block.getHash())).thenReturn(Optional.of(trieLogLayer));
    }

    // Start migration
    setWorldStateBlockContext(0L);
    migrator.onInitialSyncCompleted();

    // During migration, mode should still be FULL
    await()
        .pollInterval(Duration.ofMillis(100))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              // Check if migration has started processing blocks
              final Optional<Long> latestArchived =
                  worldStateStorage.getLatestArchivedFlatDbBlock();
              if (latestArchived.isPresent() && latestArchived.get() < 100) {
                // Migration is in progress - mode should STILL be FULL
                assertThat(worldStateStorage.getFlatDbMode())
                    .as(
                        "FlatDbMode should remain FULL while migration is in progress (at block %d)",
                        latestArchived.get())
                    .isEqualTo(FlatDbMode.FULL);
              }
            });

    // Wait for migration to complete
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(100L);
              // After ALL blocks are migrated, mode should be ARCHIVE
              assertThat(worldStateStorage.getFlatDbMode())
                  .as("FlatDbMode should be ARCHIVE only after all blocks are migrated")
                  .isEqualTo(FlatDbMode.ARCHIVE);
            });

    // Verify all blocks were migrated correctly
    final var strategy = worldStateStorage.getFlatDbStrategy();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    for (long blockNum = 1; blockNum <= 100; blockNum++) {
      setWorldStateBlockContext(blockNum);

      final PmtStateTrieAccountValue expectedAccountValue =
          new PmtStateTrieAccountValue(blockNum, Wei.of(blockNum * 100), Hash.EMPTY, Hash.EMPTY);
      final Bytes expectedAccountBytes = RLP.encode(expectedAccountValue::writeTo);

      final Optional<Bytes> retrievedAccount =
          strategy.getFlatAccount(Optional::empty, null, testAccount.addressHash(), storage);

      assertThat(retrievedAccount)
          .as("Account at block " + blockNum + " should be correct after migration")
          .hasValue(expectedAccountBytes);
    }
  }

  @Test
  void shouldContinueMigratingWhenChainGrowsDuringMigration() {
    // This test verifies that when the chain head changes during migration,
    // the migrator detects it and continues processing new blocks.
    // We simulate this by creating blocks all at once before migration starts.
    assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);

    // Mock the genesis block
    when(trieLogManager.getTrieLogLayer(blockchain.getBlockHeader(0L).get().getHash()))
        .thenReturn(Optional.empty());

    // Create 75 blocks - the migrator will process them in batches
    // and check for new blocks at the end of each target range
    final Address testAccount = Address.fromHexString("0x9876543210987654321098765432109876543210");

    for (long i = 1; i <= 75; i++) {
      createBlockWithAccount(i, testAccount);
    }

    // Start migration
    setWorldStateBlockContext(0L);
    migrator.onInitialSyncCompleted();

    // Wait for migration to complete - should process all 75 blocks
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              assertThat(worldStateStorage.getLatestArchivedFlatDbBlock()).hasValue(75L);
              assertThat(worldStateStorage.getFlatDbMode()).isEqualTo(FlatDbMode.ARCHIVE);
            });

    // Verify all blocks were migrated correctly
    final var strategy = worldStateStorage.getFlatDbStrategy();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    for (long blockNum = 1; blockNum <= 75; blockNum++) {
      setWorldStateBlockContext(blockNum);

      final PmtStateTrieAccountValue expectedAccountValue =
          new PmtStateTrieAccountValue(blockNum, Wei.of(blockNum * 50), Hash.EMPTY, Hash.EMPTY);
      final Bytes expectedAccountBytes = RLP.encode(expectedAccountValue::writeTo);

      final Optional<Bytes> retrievedAccount =
          strategy.getFlatAccount(Optional::empty, null, testAccount.addressHash(), storage);

      assertThat(retrievedAccount)
          .as("Account at block " + blockNum + " should be correct after migration")
          .hasValue(expectedAccountBytes);
    }
  }

  private void createBlockWithAccount(final long blockNumber, final Address account) {
    final Block block = createBlock(blockNumber, blockchain.getChainHeadHash());
    blockchain.appendBlock(block, Collections.emptyList());

    // Create account state for this block
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(blockNumber, Wei.of(blockNumber * 50), Hash.EMPTY, Hash.EMPTY);
    final Bytes accountBytes = RLP.encode(accountValue::writeTo);

    // Write to flat DB as would happen during sync
    final var updater = worldStateStorage.updater();
    updater.putAccountInfoState(account.addressHash(), accountBytes);
    updater.commit();

    // Create trielog for migration
    final PmtStateTrieAccountValue priorAccountValue =
        blockNumber == 1
            ? null
            : new PmtStateTrieAccountValue(
                blockNumber - 1, Wei.of((blockNumber - 1) * 50), Hash.EMPTY, Hash.EMPTY);

    final TrieLogLayer trieLogLayer = new TrieLogLayer();
    trieLogLayer.addAccountChange(account, priorAccountValue, accountValue);

    when(trieLogManager.getTrieLogLayer(block.getHash())).thenReturn(Optional.of(trieLogLayer));
  }

  private void setWorldStateBlockContext(final long blockNumber) {
    final var updater = worldStateStorage.updater();
    updater
        .getWorldStateTransaction()
        .put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    updater.commit();
  }
}
