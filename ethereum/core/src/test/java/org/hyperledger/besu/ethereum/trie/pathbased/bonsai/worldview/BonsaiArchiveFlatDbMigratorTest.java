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
            new NoOpMetricsSystem());
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
      trieLogLayer.addAccountChange(address, accountValue, accountValue);

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
