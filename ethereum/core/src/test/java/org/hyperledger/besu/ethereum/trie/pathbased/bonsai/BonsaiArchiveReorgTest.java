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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
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
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Integration tests for reorg scenarios with Bonsai archive mode using in-memory storage.
 *
 * <p>These tests verify that reorganizations are correctly handled when using the archive world
 * state provider, ensuring that account balances reflect the state of the new canonical chain after
 * a reorg.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BonsaiArchiveReorgTest {

  private static final String GENESIS_CONFIG = "/dev.json";
  private static final Wei ONE_ETH = Wei.of(1_000_000_000_000_000_000L);
  private static final Wei TWO_ETH = ONE_ETH.multiply(2);
  private static final Wei THREE_ETH = ONE_ETH.multiply(3);
  private static final Wei FIVE_ETH = ONE_ETH.multiply(5);
  private static final Wei TEN_ETH = ONE_ETH.multiply(10);

  @Mock private EthContext ethContext;

  private ExecutionContextTestFixture fixture;
  private BonsaiArchiveWorldStateProvider archiveProvider;
  private BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  private MutableBlockchain blockchain;
  private ProtocolContext protocolContext;
  private ProtocolSchedule protocolSchedule;
  private TransactionPool transactionPool;
  private KeyPair sender;
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  private final TransactionPoolConfiguration poolConfiguration =
      ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();

  @BeforeEach
  public void setUp() {
    // Use ExecutionContextTestFixture for core setup
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .build();

    blockchain = fixture.getBlockchain();
    protocolContext = fixture.getProtocolContext();
    protocolSchedule = fixture.getProtocolSchedule();
    archiveProvider = (BonsaiArchiveWorldStateProvider) fixture.getStateArchive();
    worldStateKeyValueStorage =
        (BonsaiWorldStateKeyValueStorage) archiveProvider.getWorldStateKeyValueStorage();

    // Verify archive mode
    assertThat(worldStateKeyValueStorage.getFlatDbMode())
        .as("Should be in ARCHIVE mode")
        .isEqualTo(FlatDbMode.ARCHIVE);

    // Get sender key from genesis allocations
    sender =
        GenesisConfig.fromResource(GENESIS_CONFIG)
            .streamAllocations()
            .filter(ga -> ga.privateKey() != null)
            .findFirst()
            .map(ga -> asKeyPair(ga.privateKey()))
            .orElseThrow();

    // Setup transaction pool with mocks
    setupTransactionPool();
  }

  private void setupTransactionPool() {
    var mockEthPeers = mock(org.hyperledger.besu.ethereum.eth.manager.EthPeers.class);
    when(ethContext.getEthPeers()).thenReturn(mockEthPeers);
    when(mockEthPeers.subscribeConnect(any())).thenReturn(1L);
    when(ethContext.getScheduler()).thenReturn(ethScheduler);

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

  private KeyPair asKeyPair(final Bytes32 key) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(SECPPrivateKey.create(key, "ECDSA"));
  }

  @Test
  void testReorgWithConflictingAccountBalances() {
    Address accountX = Address.fromHexString("0x1000000000000000000000000000000000000001");

    // Build chain: genesis -> blocks 1-9 (empty)
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: Account X receives 1 ETH
    Transaction tx10A = createTransaction(accountX, ONE_ETH, 0L);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Create alternate block10B: Account X receives 2 ETH
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(accountX, TWO_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);

    executeReorg(block10B, wsAtBlock9, 9L);

    // Verify: Account X should have 2 ETH from block10B
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Account X balance should be 2 ETH from block10B after reorg")
        .isEqualTo(TWO_ETH);

    Hash headBlockHash =
        ((PathBasedWorldState) archiveProvider.getWorldState()).getWorldStateBlockHash();
    assertThat(headBlockHash).isEqualTo(block10B.getHash());
  }

  @Test
  void testReorgAccountCreationVsNoCreation() {
    Address accountZ = Address.fromHexString("0x2000000000000000000000000000000000000001");
    Address accountY = Address.fromHexString("0x2000000000000000000000000000000000000002");

    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: Account Z is created with 1 ETH
    Transaction tx10A = createTransaction(accountZ, ONE_ETH, 0L);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);
    assertThat(archiveProvider.getWorldState().get(accountZ)).isNotNull();

    // Reorg to block10B: Account Y gets 1 ETH
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(accountY, ONE_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);
    executeReorg(block10B, wsAtBlock9, 9L);

    // Account Z should not exist after reorg
    assertThat(archiveProvider.getWorldState().get(accountZ))
        .as("accountZ should be null after reorg")
        .isNull();
    assertThat(archiveProvider.getWorldState().get(accountY)).isNotNull();
  }

  @Test
  void testShallowReorg_OneBlock() {
    Address accountX = Address.fromHexString("0x3000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1A: Account X gets 1 ETH
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Reorg to block1B: Account X gets 2 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, TWO_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);
  }

  @Test
  void testMediumReorg_FiveBlocks() {
    Address accountX = Address.fromHexString("0x4000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: 5 blocks, each sending 1 ETH
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 5; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(FIVE_ETH);

    // Reorg from genesis: single block with 5 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction txB = createTransaction(accountX, FIVE_ETH, 0L);
    Block blockB = forTransactions(List.of(txB), genesisHeader);
    executeReorg(blockB, wsAtGenesis, 0L);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(FIVE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void testDeepReorg_FifteenBlocks() {
    Address accountX = Address.fromHexString("0x5000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: 15 blocks
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 15; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei fifteenEth = ONE_ETH.multiply(15);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fifteenEth);

    // Reorg from genesis: single block with 10 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction txB = createTransaction(accountX, TEN_ETH, 0L);
    Block blockB = forTransactions(List.of(txB), genesisHeader);
    executeReorg(blockB, wsAtGenesis, 0L);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void testHistoricalQueriesAfterReorg() {
    Address accountX = Address.fromHexString("0x6000000000000000000000000000000000000001");

    // Build chain: genesis -> block1 -> block2 (empty)
    BlockHeader block2Header = buildEmptyChainToBlock(2);

    // Block 3A: Account X gets 1 ETH
    Transaction tx3A = createTransaction(accountX, ONE_ETH, 0L);
    Block block3A = forTransactions(List.of(tx3A), block2Header);
    executeBlock(archiveProvider.getWorldState(), block3A);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Historical query at block 2 - account should not exist
    assertThat(getHistoricalWorldState(block2Header).get(accountX)).isNull();

    // Reorg to block3B: Account X gets 2 ETH
    MutableWorldState wsAtBlock2 = getHistoricalWorldState(block2Header);
    Transaction tx3B = createTransaction(accountX, TWO_ETH, 0L);
    Block block3B = forTransactions(List.of(tx3B), block2Header);
    executeReorg(block3B, wsAtBlock2, 2L);

    // Current state should be 2 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);

    // Historical query at block 2 should still work
    assertThat(getHistoricalWorldState(block2Header).get(accountX)).isNull();

    // Historical query at block3B should return 2 ETH
    assertThat(getHistoricalWorldState(block3B.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);
  }

  @Test
  void testConsecutiveReorgs() {
    Address accountX = Address.fromHexString("0x7000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: block1A with 1 ETH
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // First reorg: block1B with 2 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, TWO_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);

    // Second reorg: block1C with 3 ETH
    MutableWorldState wsAtGenesis2 = getHistoricalWorldState(genesisHeader);
    Transaction tx1C = createTransaction(accountX, THREE_ETH, 0L);
    Block block1C = forTransactions(List.of(tx1C), genesisHeader);
    executeReorg(block1C, wsAtGenesis2, 0L);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void testHistoricalQueriesInNormalChain() {
    Address accountX = Address.fromHexString("0x8000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Build chain: 4 blocks, each sending 1 ETH
    Block block1 =
        forTransactions(List.of(createTransaction(accountX, ONE_ETH, 0L)), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    Block block2 =
        forTransactions(List.of(createTransaction(accountX, ONE_ETH, 1L)), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    Block block3 =
        forTransactions(List.of(createTransaction(accountX, ONE_ETH, 2L)), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3);

    Block block4 =
        forTransactions(List.of(createTransaction(accountX, ONE_ETH, 3L)), block3.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4);

    Wei fourEth = ONE_ETH.multiply(4);

    // Verify historical queries
    assertThat(getHistoricalWorldState(genesisHeader).get(accountX)).isNull();
    assertThat(getHistoricalWorldState(block1.getHeader()).get(accountX).getBalance())
        .isEqualTo(ONE_ETH);
    assertThat(getHistoricalWorldState(block2.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);
    assertThat(getHistoricalWorldState(block3.getHeader()).get(accountX).getBalance())
        .isEqualTo(THREE_ETH);
    assertThat(getHistoricalWorldState(block4.getHeader()).get(accountX).getBalance())
        .isEqualTo(fourEth);

    // Current state
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fourEth);
  }

  @Test
  void testHistoricalQueriesBeyondTrieLogDepth() {
    Address accountX = Address.fromHexString("0x9000000000000000000000000000000000000001");
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Create 20 blocks (beyond default trie log depth of 16)
    for (int i = 0; i < 20; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei twentyEth = ONE_ETH.multiply(20);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twentyEth);

    // Query block 5 (beyond trie log depth from block 20)
    BlockHeader block5Header = blockchain.getBlockHeader(5).orElseThrow();
    assertThat(getHistoricalWorldState(block5Header).get(accountX).getBalance())
        .isEqualTo(FIVE_ETH);

    // Query block 15
    BlockHeader block15Header = blockchain.getBlockHeader(15).orElseThrow();
    Wei fifteenEth = ONE_ETH.multiply(15);
    assertThat(getHistoricalWorldState(block15Header).get(accountX).getBalance())
        .isEqualTo(fifteenEth);

    // Current state still correct
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twentyEth);
  }

  @Test
  void testReorgWithMultipleAccountsAffected() {
    Address account1 = Address.fromHexString("0x4000000000000000000000000000000000000001");
    Address account2 = Address.fromHexString("0x4000000000000000000000000000000000000002");

    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: account1 gets 1 ETH, account2 gets 1 ETH
    Transaction tx10A_1 = createTransaction(account1, ONE_ETH, 0L);
    Transaction tx10A_2 = createTransaction(account2, ONE_ETH, 1L);
    Block block10A = forTransactions(List.of(tx10A_1, tx10A_2), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);

    assertThat(archiveProvider.getWorldState().get(account1).getBalance()).isEqualTo(ONE_ETH);
    assertThat(archiveProvider.getWorldState().get(account2).getBalance()).isEqualTo(ONE_ETH);

    // Reorg: account1 gets 2 ETH, account2 gets nothing
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(account1, TWO_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);
    executeReorg(block10B, wsAtBlock9, 9L);

    assertThat(archiveProvider.getWorldState().get(account1).getBalance()).isEqualTo(TWO_ETH);
    assertThat(archiveProvider.getWorldState().get(account2)).isNull();
  }

  @Test
  void testDeepReorg_NearMaxTrieLogDepth() {
    Address accountX = Address.fromHexString("0x7000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: 15 blocks
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 15; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei fifteenEth = ONE_ETH.multiply(15);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fifteenEth);

    // Reorg from genesis: single block with 10 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction txB = createTransaction(accountX, TEN_ETH, 0L);
    Block blockB = forTransactions(List.of(txB), genesisHeader);
    executeReorg(blockB, wsAtGenesis, 0L);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void testArchiveFlatDbHistoricalQueriesAfterReorg() {
    Address accountX = Address.fromHexString("0x9000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1: empty
    Block block1 = forTransactions(Collections.emptyList(), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    // Block 2: empty
    Block block2 = forTransactions(Collections.emptyList(), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    // Block 3A: account X receives 1 ETH
    Transaction tx3A = createTransaction(accountX, ONE_ETH, 0L);
    Block block3A = forTransactions(List.of(tx3A), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Query historical state at block 2 - account X should not exist
    assertThat(getHistoricalWorldState(block2.getHeader()).get(accountX)).isNull();

    // Create block3B with 2 ETH to accountX
    MutableWorldState wsForBlock3B = getHistoricalWorldState(block2.getHeader());
    Transaction tx3B = createTransaction(accountX, TWO_ETH, 0L);
    Block block3B = forTransactions(List.of(tx3B), block2.getHeader());
    executeReorg(block3B, wsForBlock3B, 2L);

    // Current state should show 2 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);

    // Historical query at block 2 should still work
    assertThat(getHistoricalWorldState(block2.getHeader()).get(accountX)).isNull();

    // Historical query at block3B should return 2 ETH
    assertThat(getHistoricalWorldState(block3B.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);
  }

  @Test
  void testArchiveFlatDbMultiBlockHistoryAfterReorg() {
    Address accountX = Address.fromHexString("0xA000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Build chain A: 3 blocks each sending 1 ETH
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Transaction tx2A = createTransaction(accountX, ONE_ETH, 1L);
    Block block2A = forTransactions(List.of(tx2A), block1A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);

    Transaction tx3A = createTransaction(accountX, ONE_ETH, 2L);
    Block block3A = forTransactions(List.of(tx3A), block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(THREE_ETH);

    // Verify historical state at each block
    assertThat(getHistoricalWorldState(block1A.getHeader()).get(accountX).getBalance())
        .isEqualTo(ONE_ETH);
    assertThat(getHistoricalWorldState(block2A.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);

    // Reorg from genesis: block1B sends 5 ETH directly
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // After reorg: current state should be 5 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(FIVE_ETH);

    // Historical query at block1B should return 5 ETH
    assertThat(getHistoricalWorldState(block1B.getHeader()).get(accountX).getBalance())
        .isEqualTo(FIVE_ETH);
  }

  @Test
  void testArchiveFlatDbAccountOverwriteOnReorg() {
    Address accountX = Address.fromHexString("0xB000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: block1A with 1 ETH to accountX
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Reorg: block1B with 5 ETH to accountX (same block number, different value)
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // Critical assertion: The value should be 5 ETH (from 1B), NOT 1 ETH (from 1A)
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(FIVE_ETH);

    // Also verify via historical query
    assertThat(getHistoricalWorldState(block1B.getHeader()).get(accountX).getBalance())
        .isEqualTo(FIVE_ETH);
  }

  @Test
  void testArchiveFlatDbOnlyContainsCanonicalBlockValues() {
    Address accountX = Address.fromHexString("0xC000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1A: accountX = 1 ETH
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Reorg to 1B with 5 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // Critical assertion: The flat DB should return 5 ETH (from 1B), NOT 1 ETH (from 1A)
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("After reorg to 1B, account should have 5 ETH, not 1 ETH from orphaned chain A")
        .isEqualTo(FIVE_ETH);

    // Verify via direct flat DB query
    Hash accountXHash = accountX.addressHash();
    Optional<Bytes> flatDbValue = worldStateKeyValueStorage.getAccount(accountXHash);
    assertThat(flatDbValue).as("Flat DB should have a value for accountX").isPresent();
  }

  @Test
  void testArchiveFlatDbOrphanedAccountsNotPresent() {
    Address accountZ = Address.fromHexString("0xD000000000000000000000000000000000000001");
    Address accountY = Address.fromHexString("0xD000000000000000000000000000000000000002");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1A: accountZ gets 1 ETH (accountY does not exist)
    Transaction tx1A = createTransaction(accountZ, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountZ)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(accountY)).isNull();

    // Reorg to 1B: accountY gets 1 ETH (accountZ does NOT exist in 1B)
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountY, ONE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // accountZ should NOT exist after reorg
    assertThat(archiveProvider.getWorldState().get(accountZ))
        .as("accountZ should be null after reorg - it only existed in orphaned chain A")
        .isNull();

    // accountY should exist with 1 ETH
    assertThat(archiveProvider.getWorldState().get(accountY))
        .as("accountY should exist after reorg - it was created in chain B")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(accountY).getBalance()).isEqualTo(ONE_ETH);

    // Verify via flat DB that accountZ has no value
    Hash accountZHash = accountZ.addressHash();
    Optional<Bytes> flatDbValueZ = worldStateKeyValueStorage.getAccount(accountZHash);
    assertThat(flatDbValueZ)
        .as("Flat DB should not return chain A's value for accountZ after reorg to chain B")
        .isEmpty();
  }

  @Test
  void testNormalBlockCreationWithTransactions() {
    Address receiver1 = Address.fromHexString("0xE000000000000000000000000000000000000001");
    Address receiver2 = Address.fromHexString("0xE000000000000000000000000000000000000002");
    Address receiver3 = Address.fromHexString("0xE000000000000000000000000000000000000003");

    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Block 1: Send 1 ETH to receiver1
    Transaction tx1 = createTransaction(receiver1, ONE_ETH, 0L);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    assertThat(archiveProvider.getWorldState().get(receiver1)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(ONE_ETH);

    // Block 2: Send 2 ETH to receiver2
    Transaction tx2 = createTransaction(receiver2, TWO_ETH, 1L);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);
    assertThat(result2.isSuccessful()).isTrue();

    assertThat(archiveProvider.getWorldState().get(receiver2)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(TWO_ETH);

    // Block 3: Send 3 ETH to receiver3
    Transaction tx3 = createTransaction(receiver3, THREE_ETH, 2L);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    BlockProcessingResult result3 = executeBlock(archiveProvider.getWorldState(), block3);
    assertThat(result3.isSuccessful()).isTrue();

    // Verify all receivers still have their balances
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(ONE_ETH);
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(TWO_ETH);
    assertThat(archiveProvider.getWorldState().get(receiver3).getBalance()).isEqualTo(THREE_ETH);
  }

  @Test
  void testArchiveFlatDbHistoricalQueriesInNormalChain() {
    Address accountX = Address.fromHexString("0xF000000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Build chain: genesis -> block1 -> block2 -> block3 -> block4
    Transaction tx1 = createTransaction(accountX, ONE_ETH, 0L);
    Block block1 = forTransactions(List.of(tx1), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    Transaction tx2 = createTransaction(accountX, ONE_ETH, 1L);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    Transaction tx3 = createTransaction(accountX, ONE_ETH, 2L);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3);

    Transaction tx4 = createTransaction(accountX, ONE_ETH, 3L);
    Block block4 = forTransactions(List.of(tx4), block3.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4);

    Wei fourEth = ONE_ETH.multiply(4);

    // Historical queries
    assertThat(getHistoricalWorldState(genesisHeader).get(accountX)).isNull();
    assertThat(getHistoricalWorldState(block1.getHeader()).get(accountX).getBalance())
        .isEqualTo(ONE_ETH);
    assertThat(getHistoricalWorldState(block2.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);
    assertThat(getHistoricalWorldState(block3.getHeader()).get(accountX).getBalance())
        .isEqualTo(THREE_ETH);
    assertThat(getHistoricalWorldState(block4.getHeader()).get(accountX).getBalance())
        .isEqualTo(fourEth);

    // Current state
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(fourEth);
  }

  @Test
  void testArchiveFlatDbStorageKeysHaveCorrectBlockSuffix() {
    Address accountX = Address.fromHexString("0xF100000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1: Create accountX with 1 ETH
    Transaction tx1 = createTransaction(accountX, ONE_ETH, 0L);
    Block block1 = forTransactions(List.of(tx1), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1);

    // Block 2: Add 1 ETH to accountX (total 2 ETH)
    Transaction tx2 = createTransaction(accountX, ONE_ETH, 1L);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2);

    // Block 3: Add 1 ETH to accountX (total 3 ETH)
    Transaction tx3 = createTransaction(accountX, ONE_ETH, 2L);
    Block block3 = forTransactions(List.of(tx3), block2.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3);

    // Verify historical queries return correct values for each block
    assertThat(getHistoricalWorldState(block1.getHeader()).get(accountX).getBalance())
        .isEqualTo(ONE_ETH);
    assertThat(getHistoricalWorldState(block2.getHeader()).get(accountX).getBalance())
        .isEqualTo(TWO_ETH);
    assertThat(getHistoricalWorldState(block3.getHeader()).get(accountX).getBalance())
        .isEqualTo(THREE_ETH);
  }

  @Test
  void testMultipleAccountsInSingleBlock() {
    Address receiver1 = Address.fromHexString("0xF200000000000000000000000000000000000001");
    Address receiver2 = Address.fromHexString("0xF200000000000000000000000000000000000002");
    Address receiver3 = Address.fromHexString("0xF200000000000000000000000000000000000003");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1: 3 transactions sending to 3 different accounts
    Transaction tx1 = createTransaction(receiver1, ONE_ETH, 0L);
    Transaction tx2 = createTransaction(receiver2, TWO_ETH, 1L);
    Transaction tx3 = createTransaction(receiver3, THREE_ETH, 2L);

    Block block1 = forTransactions(List.of(tx1, tx2, tx3), genesisHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Verify all accounts have correct balances
    assertThat(archiveProvider.getWorldState().get(receiver1)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver1).getBalance()).isEqualTo(ONE_ETH);

    assertThat(archiveProvider.getWorldState().get(receiver2)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver2).getBalance()).isEqualTo(TWO_ETH);

    assertThat(archiveProvider.getWorldState().get(receiver3)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver3).getBalance()).isEqualTo(THREE_ETH);
  }

  @Test
  void testAccountBalanceChangesAcrossBlocks() {
    Address accountX = Address.fromHexString("0xF300000000000000000000000000000000000001");
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Process 5 blocks, each adding 1 ETH to accountX
    for (int i = 1; i <= 5; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) (i - 1));
      Block block = forTransactions(List.of(tx), parentHeader);
      BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();

      // Verify current balance
      Wei expectedBalance = ONE_ETH.multiply(i);
      assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
          .as("Balance after block %d should be %d ETH", i, i)
          .isEqualTo(expectedBalance);

      parentHeader = block.getHeader();
    }

    // Final verification: historical queries at each block should return correct values
    for (int i = 1; i <= 5; i++) {
      BlockHeader blockHeader = blockchain.getBlockHeader(i).orElseThrow();
      MutableWorldState wsAtBlock = getHistoricalWorldState(blockHeader);

      Wei expectedBalance = ONE_ETH.multiply(i);
      assertThat(wsAtBlock.get(accountX).getBalance())
          .as("Historical query at block %d should return %d ETH", i, i)
          .isEqualTo(expectedBalance);
    }
  }

  @Test
  void testLayeredStorageInheritsReadContext() {
    Address accountX = Address.fromHexString("0xF400000000000000000000000000000000000001");
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Block 1: Account X gets 1 ETH
    Transaction tx1 = createTransaction(accountX, ONE_ETH, 0L);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Block 2: Account X gets another 1 ETH (total 2 ETH)
    Transaction tx2 = createTransaction(accountX, ONE_ETH, 1L);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);
    assertThat(result2.isSuccessful()).isTrue();

    // Verify current worldstate has 2 ETH
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);

    // Critical test: Create a layered worldstate for block 1 (historical query)
    MutableWorldState wsAtBlock1 = getHistoricalWorldState(block1.getHeader());

    // Should read with block 1 context (1 ETH), not MAX_BLOCK_SUFFIX (2 ETH)
    Wei balanceAtBlock1 = wsAtBlock1.get(accountX).getBalance();
    assertThat(balanceAtBlock1)
        .as(
            "Layered storage MUST inherit read context from parent - should read block 1 (1 ETH), not latest (2 ETH)")
        .isEqualTo(ONE_ETH);

    // Verify we can still read current state correctly
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Current worldstate should still have 2 ETH")
        .isEqualTo(TWO_ETH);

    // Verify another layered storage for block 2 without interference
    MutableWorldState wsAtBlock2 = getHistoricalWorldState(block2.getHeader());
    Wei balanceAtBlock2 = wsAtBlock2.get(accountX).getBalance();
    assertThat(balanceAtBlock2)
        .as("Second layered storage should read from block 2 context (2 ETH)")
        .isEqualTo(TWO_ETH);
  }

  @Test
  void testHistoricalQueriesBeyondTrieLogDepthUseCorrectContext() {
    Address accountX = Address.fromHexString("0xF500000000000000000000000000000000000001");
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Create 20 blocks (beyond default trie log depth of 16)
    for (int i = 1; i <= 20; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) (i - 1));
      Block block = forTransactions(List.of(tx), parentHeader);
      BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();
      parentHeader = block.getHeader();
    }

    // Verify current state has 20 ETH
    Wei twentyEth = ONE_ETH.multiply(20);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(twentyEth);

    // Critical test: Query historical block 5 (beyond trie log depth from block 20)
    BlockHeader block5Header = blockchain.getBlockHeader(5).orElseThrow();
    MutableWorldState wsAtBlock5 = getHistoricalWorldState(block5Header);

    // Should read with block 5 context (5 ETH), not MAX_BLOCK_SUFFIX (20 ETH)
    assertThat(wsAtBlock5.get(accountX).getBalance())
        .as(
            "Historical query for block 5 MUST read with block 5 context (5 ETH), not latest (20 ETH)")
        .isEqualTo(FIVE_ETH);

    // Also verify we can query block 15 correctly
    BlockHeader block15Header = blockchain.getBlockHeader(15).orElseThrow();
    MutableWorldState wsAtBlock15 = getHistoricalWorldState(block15Header);

    Wei fifteenEth = ONE_ETH.multiply(15);
    assertThat(wsAtBlock15.get(accountX).getBalance())
        .as("Historical query for block 15 should return 15 ETH")
        .isEqualTo(fifteenEth);

    // Final check: Current state should still be correct
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Current worldstate should still have 20 ETH")
        .isEqualTo(twentyEth);
  }

  @Test
  void testRollforwardReadsUseCorrectContext() {
    Address accountX = Address.fromHexString("0xF600000000000000000000000000000000000001");
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Block 1: Account X gets 1 ETH
    Transaction tx1 = createTransaction(accountX, ONE_ETH, 0L);
    Block block1 = forTransactions(List.of(tx1), parentHeader);
    BlockProcessingResult result1 = executeBlock(archiveProvider.getWorldState(), block1);
    assertThat(result1.isSuccessful()).isTrue();

    // Block 2: Account X gets another 1 ETH
    Transaction tx2 = createTransaction(accountX, ONE_ETH, 1L);
    Block block2 = forTransactions(List.of(tx2), block1.getHeader());
    BlockProcessingResult result2 = executeBlock(archiveProvider.getWorldState(), block2);

    assertThat(result2.isSuccessful())
        .as("Rollforward must read from block 1 context, not MAX_BLOCK_SUFFIX")
        .isTrue();

    // Verify final balance
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(TWO_ETH);
  }

  // ===== Helper Methods =====

  private Transaction createTransaction(final Address to, final Wei value, final long nonce) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(to))
        .value(value)
        .gasLimit(21_000L)
        .nonce(nonce)
        .createTransaction(sender);
  }

  private Block forTransactions(final List<Transaction> transactions, final BlockHeader parent) {
    return TestBlockCreator.forHeader(
            protocolContext, protocolSchedule, transactionPool, ethScheduler)
        .createBlock(transactions, Collections.emptyList(), System.currentTimeMillis(), parent)
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

  private BlockHeader buildEmptyChainToBlock(final int blockCount) {
    BlockHeader parentHeader = fixture.getGenesis().getHeader();
    for (int i = 1; i <= blockCount; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }
    return parentHeader;
  }

  private MutableWorldState getHistoricalWorldState(final BlockHeader header) {
    return archiveProvider
        .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(header))
        .orElseThrow();
  }

  private void executeReorg(
      final Block alternateBlock,
      final MutableWorldState wsAtForkPoint,
      final long rewindToBlockNumber) {
    BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(alternateBlock.getHeader())
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, wsAtForkPoint, alternateBlock);
    assertThat(result.isSuccessful()).isTrue();

    // Persist to create trie log for reorg
    wsAtForkPoint.persist(alternateBlock.getHeader());

    blockchain.storeBlock(alternateBlock, result.getReceipts());
    blockchain.rewindToBlock(rewindToBlockNumber);
    blockchain.appendBlock(alternateBlock, result.getReceipts());

    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(alternateBlock.getHeader()));
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
