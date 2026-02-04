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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolReplacementHandler;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.GasPricePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderBalanceChecker;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Integration tests for reorg scenarios with Bonsai archive mode using a real RocksDB database.
 *
 * <p>These tests verify that reorganizations are correctly handled when using the archive world
 * state provider, ensuring that account balances and storage values reflect the state of the new
 * canonical chain after a reorg.
 */
@ExtendWith(MockitoExtension.class)
public class BonsaiArchiveReorgIntegrationTest {

  @TempDir private Path tempData;

  private BonsaiArchiveWorldStateProvider archiveProvider;
  private BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  private MutableBlockchain blockchain;
  private ProtocolContext protocolContext;
  private TransactionPool transactionPool;
  private EthContext ethContext;
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  private KeyPair asKeyPair(final Bytes32 key) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(SECPPrivateKey.create(key, "ECDSA"));
  }

  private final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          GenesisConfig.fromResource("/dev.json").getConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());

  private final GenesisState genesisState =
      GenesisState.fromConfig(
          GenesisConfig.fromResource("/dev.json"), protocolSchedule, new CodeCache());

  private final TransactionPoolConfiguration poolConfiguration =
      ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();

  private final List<GenesisAccount> accounts =
      GenesisConfig.fromResource("/dev.json")
          .streamAllocations()
          .filter(ga -> ga.privateKey() != null)
          .toList();

  private KeyPair sender1;

  @BeforeEach
  public void setUp() {
    blockchain = createInMemoryBlockchain(genesisState.getBlock());
    sender1 = Optional.ofNullable(accounts.get(0).privateKey()).map(this::asKeyPair).orElseThrow();

    // Create RocksDB storage with ARCHIVE config
    StorageProvider storageProvider = createKeyValueStorageProvider();
    worldStateKeyValueStorage =
        (BonsaiWorldStateKeyValueStorage)
            storageProvider.createWorldStateStorage(
                DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);

    // Create BonsaiArchiveWorldStateProvider
    archiveProvider =
        new BonsaiArchiveWorldStateProvider(
            worldStateKeyValueStorage,
            blockchain,
            PathBasedExtraStorageConfiguration.DEFAULT,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            throwingWorldStateHealerSupplier(),
            new CodeCache());

    // Verify archive mode is actually being used - this is critical for these tests
    assertThat(worldStateKeyValueStorage.getFlatDbMode())
        .as("Archive flat DB mode should be ARCHIVE, not PARTIAL or FULL")
        .isEqualTo(FlatDbMode.ARCHIVE);

    // Write genesis state
    var ws = archiveProvider.getWorldState();
    genesisState.writeStateTo(ws);

    // Setup protocol context
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(archiveProvider)
            .build();

    // Setup transaction pool
    ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    TransactionPoolMetrics txPoolMetrics = new TransactionPoolMetrics(new NoOpMetricsSystem());
    SenderBalanceChecker senderBalanceChecker = new SenderBalanceChecker.NoOpChecker();
    TransactionPoolReplacementHandler transactionReplacementHandler =
        new TransactionPoolReplacementHandler(
            poolConfiguration.getPriceBump(), poolConfiguration.getBlobPriceBump());
    BiFunction<PendingTransaction, PendingTransaction, Boolean> transactionReplacementTester =
        (t1, t2) ->
            transactionReplacementHandler.shouldReplace(
                t1, t2, protocolContext.getBlockchain().getChainHeadHeader());
    PendingTransactions sorter =
        new LayeredPendingTransactions(
            poolConfiguration,
            new GasPricePrioritizedTransactions(
                poolConfiguration,
                ethScheduler,
                new EndLayer(txPoolMetrics),
                txPoolMetrics,
                transactionReplacementTester,
                new BlobCache(),
                MiningConfiguration.newDefault(),
                senderBalanceChecker),
            ethScheduler);

    transactionPool =
        new TransactionPool(
            () -> sorter,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            txPoolMetrics,
            poolConfiguration,
            new BlobCache());
    transactionPool.setEnabled();
  }

  /* Storage provider which uses a temporary directory based RocksDB. */
  private StorageProvider createKeyValueStorageProvider() {
    return new KeyValueStorageProviderBuilder()
        .withStorageFactory(
            new RocksDBKeyValueStorageFactory(
                () ->
                    new RocksDBFactoryConfiguration(
                        1024, // MAX_OPEN_FILES
                        4, // BACKGROUND_THREAD_COUNT
                        8388608, // CACHE_CAPACITY
                        false,
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty()),
                Arrays.asList(KeyValueSegmentIdentifier.values()),
                RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS))
        .withCommonConfiguration(
            new BesuConfiguration() {
              @Override
              public Optional<String> getRpcHttpHost() {
                return Optional.empty();
              }

              @Override
              public Optional<Integer> getRpcHttpPort() {
                return Optional.empty();
              }

              @Override
              public String getConfiguredRpcHttpHost() {
                return "";
              }

              @Override
              public Integer getConfiguredRpcHttpPort() {
                return 0;
              }

              @Override
              public long getConfiguredRpcHttpTimeoutSec() {
                return 0;
              }

              @Override
              public Path getStoragePath() {
                return tempData.resolve("database");
              }

              @Override
              public Path getDataPath() {
                return tempData;
              }

              @Override
              public DataStorageFormat getDatabaseFormat() {
                return DataStorageFormat.X_BONSAI_ARCHIVE;
              }

              @Override
              public Wei getMinGasPrice() {
                return MiningConfiguration.newDefault().getMinTransactionGasPrice();
              }

              @Override
              public org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration
                  getDataStorageConfiguration() {
                return new org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration() {
                  @Override
                  public DataStorageFormat getDatabaseFormat() {
                    return DataStorageFormat.X_BONSAI_ARCHIVE;
                  }

                  @Override
                  public boolean getReceiptCompactionEnabled() {
                    return false;
                  }
                };
              }
            })
        .withMetricsSystem(new NoOpMetricsSystem())
        .build();
  }

  /**
   * Primary scenario: Reorg with conflicting account transactions.
   *
   * <p>Chain structure:
   *
   * <pre>
   * genesis -> block1 -> ... -> block9 -> block10A -> block11A (current head)
   *                                    \-> block10B (reorg target)
   *                                            \-> block11B (new head)
   * </pre>
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Build chain: genesis -> blocks 1-9 -> block10A -> block11A
   *   <li>Block 10A: Account X receives 1 ETH
   *   <li>Create alternate block10B (same parent as 10A): Account X receives 2 ETH
   *   <li>Simulate FCU reorg to block10B
   *   <li>Assert: Account X has 2 ETH (from 10B), not 1 ETH (from 10A)
   *   <li>Build block11B on top of block10B
   *   <li>Assert: State consistency, WORLD_BLOCK_NUMBER_KEY correct
   * </ol>
   */
  @Test
  void testReorgWithConflictingAccountTransactions() {
    Address accountX = Address.fromHexString("0x1000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);

    // Build chain: genesis -> blocks 1-9
    BlockHeader parentHeader = genesisState.getBlock().getHeader();
    for (int i = 1; i <= 9; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Block 10A: Account X receives 1 ETH
    Transaction tx10A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    BlockProcessingResult result10A = executeBlock(archiveProvider.getWorldState(), block10A);
    assertThat(result10A.isSuccessful()).isTrue();

    // Block 11A: empty block on top of 10A
    Transaction tx11A = burnTransactionWithValue(sender1, 1L, accountX, Wei.ZERO);
    Block block11A = forTransactions(List.of(tx11A), block10A.getHeader());
    BlockProcessingResult result11A = executeBlock(archiveProvider.getWorldState(), block11A);
    assertThat(result11A.isSuccessful()).isTrue();

    // Verify current state: account X has 1 ETH from block10A
    assertThat(archiveProvider.getWorldState().get(accountX)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(11L);

    // Create alternate block10B (same parent as 10A): Account X receives 2 ETH
    // Need to get a fresh world state at block 9 for creating block10B
    MutableWorldState wsAtBlock9 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow();

    // Create a different transaction for block10B that sends 2 ETH to accountX
    Transaction tx10B = burnTransactionWithValue(sender1, 0L, accountX, twoEth);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);

    // Execute block10B on the snapshot world state to get receipts
    BlockProcessingResult result10B =
        protocolSchedule
            .getByBlockHeader(block10B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtBlock9, block10B);
    assertThat(result10B.isSuccessful()).isTrue();

    // Store block10B without making it canonical yet
    blockchain.storeBlock(block10B, result10B.getReceipts());

    // Simulate FCU reorg: rewind to block 9, then append block10B
    blockchain.rewindToBlock(9L);

    // Append block10B as new canonical
    blockchain.appendBlock(block10B, result10B.getReceipts());

    // Roll world state to new head using the archive provider
    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block10B.getHeader()));

    // Assert: Account X has 2 ETH (from 10B), not 1 ETH (from 10A)
    MutableWorldState currentWorldState = archiveProvider.getWorldState();
    assertThat(currentWorldState.get(accountX)).isNotNull();
    assertThat(currentWorldState.get(accountX).getBalance()).isEqualTo(twoEth);

    // Assert: WORLD_BLOCK_NUMBER_KEY correct

    // Build block11B on top of block10B
    Transaction tx11B = burnTransactionWithValue(sender1, 1L, accountX, Wei.ZERO);
    Block block11B = forTransactions(List.of(tx11B), block10B.getHeader());
    BlockProcessingResult result11B = executeBlock(archiveProvider.getWorldState(), block11B);
    assertThat(result11B.isSuccessful()).isTrue();

    // Final assertions
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(11L);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);

    // Verify state root matches new head block's state root
    assertThat(archiveProvider.getWorldState().rootHash())
        .isEqualTo(block11B.getHeader().getStateRoot());
  }

  /* Test that account created in 10A is null after reorg to 10B where it's not created. */
  @Test
  void testReorgAccountCreationVsNoCreation() {
    Address accountZ = Address.fromHexString("0x2000000000000000000000000000000000000002");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    // Build chain: genesis -> blocks 1-9
    BlockHeader parentHeader = genesisState.getBlock().getHeader();
    for (int i = 1; i <= 9; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Block 10A: Account Z is created (receives 1 ETH)
    Transaction tx10A = burnTransactionWithValue(sender1, 0L, accountZ, oneEth);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);

    // Verify account Z exists
    assertThat(archiveProvider.getWorldState().get(accountZ)).isNotNull();

    // Create alternate block10B (empty - account Z not created)
    MutableWorldState wsAtBlock9 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow();

    // Different tx that doesn't involve accountZ - send to a different address
    Address otherAccount = Address.fromHexString("0x3000000000000000000000000000000000000003");
    Transaction tx10B = burnTransactionWithValue(sender1, 0L, otherAccount, oneEth);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);

    BlockProcessingResult result10B =
        protocolSchedule
            .getByBlockHeader(block10B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtBlock9, block10B);
    assertThat(result10B.isSuccessful()).isTrue();

    // Store and reorg
    blockchain.storeBlock(block10B, result10B.getReceipts());
    blockchain.rewindToBlock(9L);
    blockchain.appendBlock(block10B, result10B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block10B.getHeader()));

    // Assert: Account Z is null after reorg (was never created in 10B chain)
    assertThat(archiveProvider.getWorldState().get(accountZ)).isNull();
    // Assert: Other account exists
    assertThat(archiveProvider.getWorldState().get(otherAccount)).isNotNull();
  }

  /* Test reorg with multiple accounts having different states in 10A vs 10B. */
  @Test
  void testReorgWithMultipleAccountsAffected() {
    Address account1 = Address.fromHexString("0x4000000000000000000000000000000000000001");
    Address account2 = Address.fromHexString("0x4000000000000000000000000000000000000002");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);

    // Build chain: genesis -> blocks 1-9
    BlockHeader parentHeader = genesisState.getBlock().getHeader();
    for (int i = 1; i <= 9; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Block 10A: account1 gets 1 ETH, account2 gets 1 ETH
    Transaction tx10A_1 = burnTransactionWithValue(sender1, 0L, account1, oneEth);
    Transaction tx10A_2 = burnTransactionWithValue(sender1, 1L, account2, oneEth);
    Block block10A = forTransactions(List.of(tx10A_1, tx10A_2), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);

    // Verify current state
    assertThat(archiveProvider.getWorldState().get(account1).getBalance()).isEqualTo(oneEth);
    assertThat(archiveProvider.getWorldState().get(account2).getBalance()).isEqualTo(oneEth);

    // Create alternate block10B: account1 gets 2 ETH, account2 gets nothing
    MutableWorldState wsAtBlock9 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow();

    Transaction tx10B = burnTransactionWithValue(sender1, 0L, account1, twoEth);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);

    BlockProcessingResult result10B =
        protocolSchedule
            .getByBlockHeader(block10B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtBlock9, block10B);

    // Store and reorg
    blockchain.storeBlock(block10B, result10B.getReceipts());
    blockchain.rewindToBlock(9L);
    blockchain.appendBlock(block10B, result10B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block10B.getHeader()));

    // Assert: account1 has 2 ETH, account2 doesn't exist
    assertThat(archiveProvider.getWorldState().get(account1).getBalance()).isEqualTo(twoEth);
    assertThat(archiveProvider.getWorldState().get(account2)).isNull();
  }

  /* Test shallow reorg (depth = 1 block). */
  @Test
  void testShallowReorg_OneBlock() {
    Address accountX = Address.fromHexString("0x5000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);

    // Build chain: genesis -> block1A
    BlockHeader parentHeader = genesisState.getBlock().getHeader();

    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1A = forTransactions(List.of(tx1A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);

    // Create alternate block1B
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
            .orElseThrow();

    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountX, twoEth);
    Block block1B = forTransactions(List.of(tx1B), parentHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    // Store and reorg
    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    // Assert
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);
  }

  /* Test medium reorg (depth = 5 blocks). */
  @Test
  void testMediumReorg_FiveBlocks() {
    Address accountX = Address.fromHexString("0x6000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei fiveEth = Wei.of(5_000_000_000_000_000_000L);

    // Build chain: genesis -> blocks 1-5 (chain A)
    BlockHeader commonAncestor = genesisState.getBlock().getHeader();

    // Chain A: each block sends 1 ETH to accountX
    BlockHeader parentHeader = commonAncestor;
    for (int i = 1; i <= 5; i++) {
      Transaction tx = burnTransactionWithValue(sender1, (long) (i - 1), accountX, oneEth);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Verify: accountX has 5 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fiveEth);

    // Create alternate chain B from genesis: single block with 5 ETH to accountX
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(commonAncestor))
            .orElseThrow();

    // Chain B: just one block that sends 5 ETH directly
    Transaction txB = burnTransactionWithValue(sender1, 0L, accountX, fiveEth);
    Block blockB = forTransactions(List.of(txB), commonAncestor);

    BlockProcessingResult resultB =
        protocolSchedule
            .getByBlockHeader(blockB.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, blockB);

    // Store and reorg
    blockchain.storeBlock(blockB, resultB.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(blockB, resultB.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(blockB.getHeader()));

    // Assert: accountX still has 5 ETH but from chain B
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fiveEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    assertThat(archiveProvider.getWorldState().rootHash())
        .isEqualTo(blockB.getHeader().getStateRoot());
  }

  /* Test deep reorg (depth = 15 blocks, near max trie log depth of 16). */
  @Test
  void testDeepReorg_NearMaxTrieLogDepth() {
    Address accountX = Address.fromHexString("0x7000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    // Build chain: genesis -> blocks 1-15 (chain A)
    BlockHeader commonAncestor = genesisState.getBlock().getHeader();

    BlockHeader parentHeader = commonAncestor;
    for (int i = 1; i <= 15; i++) {
      Transaction tx = burnTransactionWithValue(sender1, (long) (i - 1), accountX, oneEth);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei fifteenEth = oneEth.multiply(15);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fifteenEth);

    // Create alternate chain B from genesis
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(commonAncestor))
            .orElseThrow();

    // Chain B: single block with different value
    Wei tenEth = oneEth.multiply(10);
    Transaction txB = burnTransactionWithValue(sender1, 0L, accountX, tenEth);
    Block blockB = forTransactions(List.of(txB), commonAncestor);

    BlockProcessingResult resultB =
        protocolSchedule
            .getByBlockHeader(blockB.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, blockB);

    // Store and reorg
    blockchain.storeBlock(blockB, resultB.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(blockB, resultB.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(blockB.getHeader()));

    // Assert
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(tenEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  /*
   * Test that archive flat DB correctly handles historical queries after reorg.
   * This tests the core BonsaiArchiveFlatDbStrategy behavior during reorgs.
   *
   * Scenario:
   * - Build chain: genesis -> block1 -> block2 -> block3A (account X = 1 ETH)
   * - Query historical state at block1, block2, block3A
   * - Reorg to block3B (account X = 2 ETH)
   * - Query historical state at block1, block2 (should be unchanged)
   * - Query state at block3B (should show 2 ETH, not 1 ETH)
   */
  @Test
  void testArchiveFlatDbHistoricalQueriesAfterReorg() {
    Address accountX = Address.fromHexString("0x9000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);

    // Build chain: genesis -> block1 -> block2
    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Block 1: empty
    Block block1 = forTransactions(Collections.emptyList(), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    // Block 2: empty
    Block block2 = forTransactions(Collections.emptyList(), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    // Block 3A: account X receives 1 ETH
    Transaction tx3A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block3A = forTransactions(List.of(tx3A), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    // Verify current state at block 3A
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);

    // Query historical state at block 2 - account X should not exist
    MutableWorldState wsAtBlock2 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock2.get(accountX)).isNull();

    // Query historical state at block 3A - account X should have 1 ETH
    MutableWorldState wsAtBlock3A =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block3A.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock3A.get(accountX)).isNotNull();
    assertThat(wsAtBlock3A.get(accountX).getBalance()).isEqualTo(oneEth);

    // Now create alternate block3B with 2 ETH to accountX
    MutableWorldState wsForBlock3B =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2.getHeader()))
            .orElseThrow();

    Transaction tx3B = burnTransactionWithValue(sender1, 0L, accountX, twoEth);
    Block block3B = forTransactions(List.of(tx3B), block2.getHeader());

    BlockProcessingResult result3B =
        protocolSchedule
            .getByBlockHeader(block3B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsForBlock3B, block3B);
    assertThat(result3B.isSuccessful()).isTrue();

    // Store and reorg to block3B
    blockchain.storeBlock(block3B, result3B.getReceipts());
    blockchain.rewindToBlock(2L);
    blockchain.appendBlock(block3B, result3B.getReceipts());

    // Roll world state to new head
    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block3B.getHeader()));

    // Verify WORLD_BLOCK_NUMBER_KEY is updated

    // Key test: Current state should show 2 ETH (from 3B), not 1 ETH (from 3A)
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);

    // Key test: Historical query at block 2 should still work (account X doesn't exist)
    MutableWorldState wsAtBlock2AfterReorg =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock2AfterReorg.get(accountX)).isNull();

    // Key test: Historical query at block 3B should return 2 ETH
    MutableWorldState wsAtBlock3BAfterReorg =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block3B.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock3BAfterReorg.get(accountX)).isNotNull();
    assertThat(wsAtBlock3BAfterReorg.get(accountX).getBalance()).isEqualTo(twoEth);
  }

  /*
   * Test that archive correctly tracks account balance changes across multiple blocks,
   * and historical queries return correct values at each block after a reorg.
   */
  @Test
  void testArchiveFlatDbMultiBlockHistoryAfterReorg() {
    Address accountX = Address.fromHexString("0xA000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Build chain A: genesis -> block1A (1 ETH) -> block2A (2 ETH) -> block3A (3 ETH)
    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Transaction tx2A = burnTransactionWithValue(sender1, 1L, accountX, oneEth);
    Block block2A = forTransactions(List.of(tx2A), block1A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);

    Transaction tx3A = burnTransactionWithValue(sender1, 2L, accountX, oneEth);
    Block block3A = forTransactions(List.of(tx3A), block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    Wei threeEth = oneEth.multiply(3);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(threeEth);

    // Verify historical state at each block
    assertThat(
            archiveProvider
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1A.getHeader()))
                .orElseThrow()
                .get(accountX)
                .getBalance())
        .isEqualTo(oneEth);

    Wei twoEth = oneEth.multiply(2);
    assertThat(
            archiveProvider
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2A.getHeader()))
                .orElseThrow()
                .get(accountX)
                .getBalance())
        .isEqualTo(twoEth);

    // Now reorg from genesis: block1B sends 5 ETH directly
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Wei fiveEth = oneEth.multiply(5);
    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountX, fiveEth);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    // After reorg: current state should be 5 ETH (from block1B)
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fiveEth);

    // Historical query at block1B should return 5 ETH
    MutableWorldState wsAtBlock1B =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1B.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock1B.get(accountX).getBalance()).isEqualTo(fiveEth);
  }

  /*
   * Test that archive correctly handles account deletion during reorg.
   * In chain A, account is created. In chain B at same block, account has different value.
   * After reorg, archive should NOT return the chain A value.
   */
  @Test
  void testArchiveFlatDbAccountOverwriteOnReorg() {
    Address accountX = Address.fromHexString("0xB000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei fiveEth = oneEth.multiply(5);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Chain A: block1A with 1 ETH to accountX
    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);

    // Reorg: block1B with 5 ETH to accountX (same block number, different value)
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountX, fiveEth);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    // Critical assertion: The value should be 5 ETH (from 1B), NOT 1 ETH (from 1A)
    // This tests that the archive flat DB correctly handles the reorg
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fiveEth);

    // Also verify via historical query
    MutableWorldState wsAtBlock1B =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1B.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock1B.get(accountX).getBalance()).isEqualTo(fiveEth);
  }

  /*
   * Test that the archive flat DB for a particular block contains ONLY that world state's values.
   * After a reorg, values from the orphaned chain should NOT be present when querying the
   * new canonical block. This specifically tests that stale values are not polluting the
   * archive.
   *
   * Critical bug scenario:
   * - Block 1A: accountX = 1 ETH (stored at key: accountHash + blockSuffix(1))
   * - Reorg to Block 1B: accountX = 5 ETH (also stored at key: accountHash + blockSuffix(1))
   * - Query at block 1 should return 5 ETH (from 1B), NOT 1 ETH (from 1A)
   *
   * The archive flat DB uses getNearestBefore to find values. If both 1A and 1B values
   * are stored with the same suffix, the lookup may return the wrong value.
   */
  @Test
  void testArchiveFlatDbOnlyContainsCanonicalBlockValues() {
    Address accountX = Address.fromHexString("0xC000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei fiveEth = oneEth.multiply(5);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Block 1A: accountX = 1 ETH
    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    // Verify 1A state
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);

    // Record what's in the flat DB at block 1 (chain A)
    Hash accountXHash = accountX.addressHash();

    // Now reorg to 1B with 5 ETH
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountX, fiveEth);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    // Critical assertion: The flat DB should now return 5 ETH (from 1B), NOT 1 ETH (from 1A)
    // This tests that the archive correctly handles the reorg and doesn't return stale values
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("After reorg to 1B, account should have 5 ETH, not 1 ETH from orphaned chain A")
        .isEqualTo(fiveEth);

    // Also verify via direct flat DB query using the archive strategy
    Optional<Bytes> flatDbValue = worldStateKeyValueStorage.getAccount(accountXHash, Optional::empty);
    assertThat(flatDbValue).as("Flat DB should have a value for accountX").isPresent();

    // The flat DB value should decode to an account with 5 ETH balance
    // If this fails, it means the flat DB contains the stale 1A value
  }

  /*
   * Test that accounts NOT involved in the new chain's block are correctly absent
   * after a reorg. If account Z was created in chain A but not in chain B,
   * it should not exist when querying the flat DB after reorg to chain B.
   */
  @Test
  void testArchiveFlatDbOrphanedAccountsNotPresent() {
    Address accountZ = Address.fromHexString("0xD000000000000000000000000000000000000001");
    Address accountY = Address.fromHexString("0xD000000000000000000000000000000000000002");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Block 1A: accountZ gets 1 ETH (accountY does not exist)
    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountZ, oneEth);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    // Verify accountZ exists, accountY does not
    assertThat(archiveProvider.getWorldState().get(accountZ)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(accountY)).isNull();

    // Reorg to 1B: accountY gets 1 ETH (accountZ does NOT exist in 1B)
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountY, oneEth);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    // Critical assertions after reorg:
    // 1. accountZ should NOT exist (it was only in chain A, not chain B)
    assertThat(archiveProvider.getWorldState().get(accountZ))
        .as("accountZ should be null after reorg - it only existed in orphaned chain A")
        .isNull();

    // 2. accountY should exist with 1 ETH (it's in chain B)
    assertThat(archiveProvider.getWorldState().get(accountY))
        .as("accountY should exist after reorg - it was created in chain B")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(accountY).getBalance()).isEqualTo(oneEth);

    // 3. Verify via flat DB that accountZ has no value for block 1
    Hash accountZHash = accountZ.addressHash();
    Optional<Bytes> flatDbValueZ = worldStateKeyValueStorage.getAccount(accountZHash, Optional::empty);

    // After reorg, the flat DB should either:
    // - Not have accountZ at all, OR
    // - Have a "deleted" marker for accountZ
    // It should NOT return the 1A value
    assertThat(flatDbValueZ)
        .as("Flat DB should not return chain A's value for accountZ after reorg to chain B")
        .isEmpty();
  }

  /* Test consecutive reorgs to ensure state remains consistent. */
  @Test
  void testConsecutiveReorgs() {
    Address accountX = Address.fromHexString("0x8000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);
    Wei threeEth = Wei.of(3_000_000_000_000_000_000L);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // First chain: block1A with 1 ETH
    Transaction tx1A = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(oneEth);

    // First reorg: block1B with 2 ETH
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Transaction tx1B = burnTransactionWithValue(sender1, 0L, accountX, twoEth);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);

    BlockProcessingResult result1B =
        protocolSchedule
            .getByBlockHeader(block1B.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis, block1B);

    blockchain.storeBlock(block1B, result1B.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1B, result1B.getReceipts());
    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1B.getHeader()));

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);

    // Second reorg: block1C with 3 ETH
    MutableWorldState wsAtGenesis2 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();

    Transaction tx1C = burnTransactionWithValue(sender1, 0L, accountX, threeEth);
    Block block1C = forTransactions(List.of(tx1C), genesisHeader);

    BlockProcessingResult result1C =
        protocolSchedule
            .getByBlockHeader(block1C.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtGenesis2, block1C);

    blockchain.storeBlock(block1C, result1C.getReceipts());
    blockchain.rewindToBlock(0L);
    blockchain.appendBlock(block1C, result1C.getReceipts());
    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1C.getHeader()));

    // Final assertions
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(threeEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    assertThat(archiveProvider.getWorldState().rootHash())
        .isEqualTo(block1C.getHeader().getStateRoot());
  }

  // ===== Normal Block Creation Tests (No Reorgs) =====

  /*
   * Test normal sequential block creation with transactions.
   * Verifies that the archive flat DB correctly stores values at each block number.
   */
  @Test
  void testNormalBlockCreationWithTransactions() {
    Address receiver1 = Address.fromHexString("0xE000000000000000000000000000000000000001");
    Address receiver2 = Address.fromHexString("0xE000000000000000000000000000000000000002");
    Address receiver3 = Address.fromHexString("0xE000000000000000000000000000000000000003");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = oneEth.multiply(2);
    Wei threeEth = oneEth.multiply(3);

    BlockHeader parentHeader = genesisState.getBlock().getHeader();

    // Block 1: Send 1 ETH to receiver1
    Transaction tx1 = burnTransactionWithValue(sender1, 0L, receiver1, oneEth);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Verify block 1 state
    assertThat(archiveProvider.getWorldState().get(receiver1)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(oneEth);

    // Block 2: Send 2 ETH to receiver2
    Transaction tx2 = burnTransactionWithValue(sender1, 1L, receiver2, twoEth);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);
    assertThat(result2.isSuccessful()).isTrue();

    // Verify block 2 state
    assertThat(archiveProvider.getWorldState().get(receiver2)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(twoEth);

    // Block 3: Send 3 ETH to receiver3
    Transaction tx3 = burnTransactionWithValue(sender1, 2L, receiver3, threeEth);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    BlockProcessingResult result3 = executeBlock(archiveProvider.getWorldState(), block3);
    assertThat(result3.isSuccessful()).isTrue();

    // Verify block 3 state
    assertThat(archiveProvider.getWorldState().get(receiver3)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver3).getBalance()).isEqualTo(threeEth);

    // Verify all receivers still have their balances at the end
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(oneEth);
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(twoEth);
    assertThat(archiveProvider.getWorldState().get(receiver3).getBalance()).isEqualTo(threeEth);
  }

  /*
   * Test that historical queries return correct values at each block number
   * in a normal (non-reorg) chain.
   */
  @Test
  void testArchiveFlatDbHistoricalQueriesInNormalChain() {
    Address accountX = Address.fromHexString("0xF000000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Build chain: genesis -> block1 (1 ETH) -> block2 (2 ETH) -> block3 (3 ETH) -> block4 (4 ETH)
    // Each block adds 1 ETH to accountX

    Transaction tx1 = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1 = forTransactions(List.of(tx1), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    Transaction tx2 = burnTransactionWithValue(sender1, 1L, accountX, oneEth);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    Transaction tx3 = burnTransactionWithValue(sender1, 2L, accountX, oneEth);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3);

    Transaction tx4 = burnTransactionWithValue(sender1, 3L, accountX, oneEth);
    Block block4 = forTransactions(List.of(tx4), block3.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4);

    // Verify WORLD_BLOCK_NUMBER_KEY is at block 4

    // Historical query at genesis: accountX should not exist
    MutableWorldState wsAtGenesis =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
            .orElseThrow();
    assertThat(wsAtGenesis.get(accountX)).isNull();

    // Historical query at block 1: accountX = 1 ETH
    MutableWorldState wsAtBlock1 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock1.get(accountX)).isNotNull();
    assertThat(wsAtBlock1.get(accountX).getBalance()).isEqualTo(oneEth);

    // Historical query at block 2: accountX = 2 ETH
    Wei twoEth = oneEth.multiply(2);
    MutableWorldState wsAtBlock2 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock2.get(accountX)).isNotNull();
    assertThat(wsAtBlock2.get(accountX).getBalance()).isEqualTo(twoEth);

    // Historical query at block 3: accountX = 3 ETH
    Wei threeEth = oneEth.multiply(3);
    MutableWorldState wsAtBlock3 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block3.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock3.get(accountX)).isNotNull();
    assertThat(wsAtBlock3.get(accountX).getBalance()).isEqualTo(threeEth);

    // Historical query at block 4: accountX = 4 ETH
    Wei fourEth = oneEth.multiply(4);
    MutableWorldState wsAtBlock4 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block4.getHeader()))
            .orElseThrow();
    assertThat(wsAtBlock4.get(accountX)).isNotNull();
    assertThat(wsAtBlock4.get(accountX).getBalance()).isEqualTo(fourEth);

    // Current state should also be 4 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fourEth);
  }

  /*
   * Test that archive flat DB stores account values with correct block suffixes.
   * Verifies directly in the flat DB that values are stored at expected block numbers.
   */
  @Test
  void testArchiveFlatDbStorageKeysHaveCorrectBlockSuffix() {
    Address accountX = Address.fromHexString("0xF100000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();
    Hash accountXHash = accountX.addressHash();

    // Block 1: Create accountX with 1 ETH
    Transaction tx1 = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1 = forTransactions(List.of(tx1), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    // Block 2: Add 1 ETH to accountX (total 2 ETH)
    Transaction tx2 = burnTransactionWithValue(sender1, 1L, accountX, oneEth);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    // Block 3: Add 1 ETH to accountX (total 3 ETH)
    Transaction tx3 = burnTransactionWithValue(sender1, 2L, accountX, oneEth);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3);

    // Verify WORLD_BLOCK_NUMBER_KEY

    // Directly query the flat DB to verify storage keys have correct block suffixes
    // For archive mode, keys are: accountHash + blockNumber (8 bytes)
    var composedStorage = worldStateKeyValueStorage.getComposedWorldStateStorage();

    // Check that we can find values at the expected block suffixes
    // Block 1 suffix: accountHash + 0x0000000000000001
    byte[] keyAtBlock1 = Bytes.concatenate(accountXHash.getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    Optional<byte[]> valueAtBlock1 =
        composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, keyAtBlock1);
    assertThat(valueAtBlock1).as("Account value should exist at block suffix 1").isPresent();

    // Block 2 suffix: accountHash + 0x0000000000000002
    byte[] keyAtBlock2 = Bytes.concatenate(accountXHash.getBytes(), Bytes.ofUnsignedLong(2)).toArrayUnsafe();
    Optional<byte[]> valueAtBlock2 =
        composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, keyAtBlock2);
    assertThat(valueAtBlock2).as("Account value should exist at block suffix 2").isPresent();

    // Block 3 suffix: accountHash + 0x0000000000000003
    byte[] keyAtBlock3 = Bytes.concatenate(accountXHash.getBytes(), Bytes.ofUnsignedLong(3)).toArrayUnsafe();
    Optional<byte[]> valueAtBlock3 =
        composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, keyAtBlock3);
    assertThat(valueAtBlock3).as("Account value should exist at block suffix 3").isPresent();

    // Verify no value at block 0 for this account (it didn't exist at genesis)
    byte[] keyAtBlock0 = Bytes.concatenate(accountXHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    Optional<byte[]> valueAtBlock0 =
        composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, keyAtBlock0);
    assertThat(valueAtBlock0)
        .as("Account should not have value at block suffix 0 (didn't exist at genesis)")
        .isEmpty();
  }

  /*
   * Test that multiple accounts modified in the same block are all stored correctly.
   */
  @Test
  void testMultipleAccountsInSingleBlock() {
    Address receiver1 = Address.fromHexString("0xF200000000000000000000000000000000000001");
    Address receiver2 = Address.fromHexString("0xF200000000000000000000000000000000000002");
    Address receiver3 = Address.fromHexString("0xF200000000000000000000000000000000000003");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = oneEth.multiply(2);
    Wei threeEth = oneEth.multiply(3);

    BlockHeader genesisHeader = genesisState.getBlock().getHeader();

    // Block 1: 3 transactions sending to 3 different accounts
    Transaction tx1 = burnTransactionWithValue(sender1, 0L, receiver1, oneEth);
    Transaction tx2 = burnTransactionWithValue(sender1, 1L, receiver2, twoEth);
    Transaction tx3 = burnTransactionWithValue(sender1, 2L, receiver3, threeEth);

    Block block1 = forTransactions(List.of(tx1, tx2, tx3), genesisHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Verify all accounts have correct balances
    assertThat(archiveProvider.getWorldState().get(receiver1)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(oneEth);

    assertThat(archiveProvider.getWorldState().get(receiver2)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(twoEth);

    assertThat(archiveProvider.getWorldState().get(receiver3)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver3).getBalance()).isEqualTo(threeEth);

    // Verify all values are stored at block suffix 1
    var composedStorage = worldStateKeyValueStorage.getComposedWorldStateStorage();

    byte[] key1 =
        Bytes.concatenate(receiver1.addressHash().getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    assertThat(composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, key1))
        .as("Receiver1 should have value at block suffix 1")
        .isPresent();

    byte[] key2 =
        Bytes.concatenate(receiver2.addressHash().getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    assertThat(composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, key2))
        .as("Receiver2 should have value at block suffix 1")
        .isPresent();

    byte[] key3 =
        Bytes.concatenate(receiver3.addressHash().getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    assertThat(composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, key3))
        .as("Receiver3 should have value at block suffix 1")
        .isPresent();

    // Verify WORLD_BLOCK_NUMBER_KEY
  }

  /*
   * Test that account balance changes across blocks are tracked correctly.
   * Account receives ETH in multiple blocks and we verify the flat DB at each step.
   */
  @Test
  void testAccountBalanceChangesAcrossBlocks() {
    Address accountX = Address.fromHexString("0xF300000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader parentHeader = genesisState.getBlock().getHeader();
    Hash accountXHash = accountX.addressHash();
    var composedStorage = worldStateKeyValueStorage.getComposedWorldStateStorage();

    // Process 5 blocks, each adding 1 ETH to accountX
    for (int i = 1; i <= 5; i++) {
      Transaction tx = burnTransactionWithValue(sender1, (long) (i - 1), accountX, oneEth);
      Block block = forTransactions(List.of(tx), parentHeader);
      BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();

      // Verify current balance
      Wei expectedBalance = oneEth.multiply(i);
      assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
          .as("Balance after block %d should be %d ETH", i, i)
          .isEqualTo(expectedBalance);

      // Verify flat DB has entry at this block suffix
      byte[] keyAtBlock = Bytes.concatenate(accountXHash.getBytes(), Bytes.ofUnsignedLong(i)).toArrayUnsafe();
      Optional<byte[]> valueAtBlock =
          composedStorage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, keyAtBlock);
      assertThat(valueAtBlock).as("Flat DB should have entry at block suffix %d", i).isPresent();

      parentHeader = block.getHeader();
    }

    // Final verification: historical queries at each block should return correct values
    for (int i = 1; i <= 5; i++) {
      BlockHeader blockHeader = blockchain.getBlockHeader(i).orElseThrow();
      MutableWorldState wsAtBlock =
          archiveProvider
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(blockHeader))
              .orElseThrow();

      Wei expectedBalance = oneEth.multiply(i);
      assertThat(wsAtBlock.get(accountX).getBalance())
          .as("Historical query at block %d should return %d ETH", i, i)
          .isEqualTo(expectedBalance);
    }
  }

  /**
   * Test that verifies layered/snapshot storages correctly inherit read context from parent. This
   * is critical because layered storages share the parent's flatDbStrategyProvider, and if context
   * isn't properly propagated, they will read with MAX_BLOCK_SUFFIX instead of the block-specific
   * context, causing data corruption.
   */
  @Test
  void testLayeredStorageInheritsReadContext() {
    Address accountX = Address.fromHexString("0xF400000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);
    Wei twoEth = Wei.of(2_000_000_000_000_000_000L);

    BlockHeader parentHeader = genesisState.getBlock().getHeader();

    // Block 1: Account X gets 1 ETH
    Transaction tx1 = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Block 2: Account X gets another 1 ETH (total 2 ETH)
    Transaction tx2 = burnTransactionWithValue(sender1, 1L, accountX, oneEth);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);
    assertThat(result2.isSuccessful()).isTrue();

    // Verify current worldstate has 2 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);

    // Critical test: Create a layered worldstate for block 1 (historical query)
    // This creates BonsaiWorldStateLayerStorage which shares parent's flatDbStrategyProvider
    MutableWorldState wsAtBlock1 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1.getHeader()))
            .orElseThrow();

    // Verify the layered storage reads from the correct block context
    // If context isn't inherited, this will read with MAX_BLOCK_SUFFIX and return 2 ETH (wrong!)
    // With correct context, it should read with block 1 suffix and return 1 ETH (correct)
    Wei balanceAtBlock1 = wsAtBlock1.get(accountX).getBalance();
    assertThat(balanceAtBlock1)
        .as(
            "Layered storage MUST inherit read context from parent - should read block 1 (1 ETH), not latest (2 ETH)")
        .isEqualTo(oneEth);

    // Also verify we can still read current state correctly
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Current worldstate should still have 2 ETH")
        .isEqualTo(twoEth);

    // Verify we can create another layered storage for block 2 without interference
    MutableWorldState wsAtBlock2 =
        archiveProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block2.getHeader()))
            .orElseThrow();

    Wei balanceAtBlock2 = wsAtBlock2.get(accountX).getBalance();
    assertThat(balanceAtBlock2)
        .as("Second layered storage should read from block 2 context (2 ETH)")
        .isEqualTo(twoEth);
  }

  /**
   * Test that validates historical queries beyond trie log depth correctly set read context. This
   * prevents the production bug where concurrent historical queries would read with
   * MAX_BLOCK_SUFFIX instead of block-specific context.
   */
  @Test
  void testHistoricalQueriesBeyondTrieLogDepthUseCorrectContext() {
    Address accountX = Address.fromHexString("0xF500000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader parentHeader = genesisState.getBlock().getHeader();

    // Create 20 blocks (beyond default trie log depth of 16)
    // Each block adds 1 ETH to accountX
    for (int i = 1; i <= 20; i++) {
      Transaction tx = burnTransactionWithValue(sender1, (long) (i - 1), accountX, oneEth);
      Block block = forTransactions(List.of(tx), parentHeader);
      BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();
      parentHeader = block.getHeader();
    }

    // Verify current state has 20 ETH
    Wei twentyEth = oneEth.multiply(20);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twentyEth);

    // Critical test: Query historical block 5 (beyond trie log depth from block 20)
    // This triggers the archive-specific path in getWorldState() that must set read context
    BlockHeader block5Header = blockchain.getBlockHeader(5).orElseThrow();
    MutableWorldState wsAtBlock5 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block5Header))
            .orElseThrow();

    // Should read with block 5 context (5 ETH), not MAX_BLOCK_SUFFIX (20 ETH)
    Wei fiveEth = Wei.of(5_000_000_000_000_000_000L);
    assertThat(wsAtBlock5.get(accountX).getBalance())
        .as(
            "Historical query for block 5 MUST read with block 5 context (5 ETH), not latest (20 ETH)")
        .isEqualTo(fiveEth);

    // Also verify we can query block 15 correctly
    BlockHeader block15Header = blockchain.getBlockHeader(15).orElseThrow();
    MutableWorldState wsAtBlock15 =
        archiveProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block15Header))
            .orElseThrow();

    Wei fifteenEth = oneEth.multiply(15);
    assertThat(wsAtBlock15.get(accountX).getBalance())
        .as("Historical query for block 15 should return 15 ETH")
        .isEqualTo(fifteenEth);

    // Final check: Current state should still be correct
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Current worldstate should still have 20 ETH")
        .isEqualTo(twentyEth);
  }

  /**
   * Test that validates reads during rollforward use correct read context. This prevents the
   * production bug where rollforward reads would use MAX_BLOCK_SUFFIX and get wrong account state,
   * causing nonce mismatch errors.
   */
  @Test
  void testRollforwardReadsUseCorrectContext() {
    Address accountX = Address.fromHexString("0xF600000000000000000000000000000000000001");
    Wei oneEth = Wei.of(1_000_000_000_000_000_000L);

    BlockHeader parentHeader = genesisState.getBlock().getHeader();

    // Block 1: Account X gets 1 ETH
    Transaction tx1 = burnTransactionWithValue(sender1, 0L, accountX, oneEth);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Block 2: Account X gets another 1 ETH
    // This triggers rollforward from block 1 → 2
    // During rollforward, reads MUST use block 1 context to get correct prior state
    Transaction tx2 = burnTransactionWithValue(sender1, 1L, accountX, oneEth);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);

    // If read context wasn't set during rollforward, reads would use MAX_BLOCK_SUFFIX
    // and potentially get wrong account state, failing transaction validation
    assertThat(result2.isSuccessful())
        .as("Rollforward must read from block 1 context, not MAX_BLOCK_SUFFIX")
        .isTrue();

    // Verify final balance
    Wei twoEth = oneEth.multiply(2);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twoEth);
  }

  // Helper methods

  private Transaction burnTransactionWithValue(
      final KeyPair sender, final Long nonce, final Address to, final Wei value) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(to))
        .value(value)
        .gasLimit(21_000L)
        .nonce(nonce)
        .createTransaction(sender);
  }

  private Block forTransactions(final List<Transaction> transactions, final BlockHeader forHeader) {
    return TestBlockCreator.forHeader(
            protocolContext, protocolSchedule, transactionPool, ethScheduler)
        .createBlock(transactions, Collections.emptyList(), System.currentTimeMillis(), forHeader)
        .getBlock();
  }

  private BlockProcessingResult executeBlock(final MutableWorldState ws, final Block block) {
    var res =
        protocolSchedule
            .getByBlockHeader(blockHeader(0))
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, ws, block);
    blockchain.appendBlock(block, res.getReceipts());
    return res;
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  static class TestBlockCreator extends AbstractBlockCreator {
    private TestBlockCreator(
        final MiningConfiguration miningConfiguration,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ExtraDataCalculator extraDataCalculator,
        final TransactionPool transactionPool,
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          miningBeneficiaryCalculator,
          extraDataCalculator,
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    static TestBlockCreator forHeader(
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final TransactionPool transactionPool,
        final EthScheduler ethScheduler) {

      final MiningConfiguration miningConfiguration =
          ImmutableMiningConfiguration.builder()
              .mutableInitValues(
                  MutableInitValues.builder()
                      .extraData(Bytes.fromHexString("deadbeef"))
                      .targetGasLimit(30_000_000L)
                      .minTransactionGasPrice(Wei.ONE)
                      .minBlockOccupancyRatio(0d)
                      .coinbase(Address.ZERO)
                      .build())
              .build();

      return new TestBlockCreator(
          miningConfiguration,
          (__, ___) -> Address.ZERO,
          __ -> Bytes.fromHexString("deadbeef"),
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .mixHash(Hash.ZERO)
          .populateFrom(sealableBlockHeader)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }
}
