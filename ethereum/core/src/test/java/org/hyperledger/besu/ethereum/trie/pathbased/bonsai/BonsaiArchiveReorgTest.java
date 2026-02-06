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
  private static final long TRIE_LOG_DEPTH = 16L;
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
    // Use ExecutionContextTestFixture for core setup with configurable trie log depth
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .maxLayersToLoad(TRIE_LOG_DEPTH)
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
  void shouldHandleReorgWithConflictingAccountBalances() {
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
  void shouldHandleReorgAccountCreationVsNoCreation() {
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
  void shouldSupportHistoricalQueriesAfterReorg() {
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
  void shouldHandleConsecutiveReorgs() {
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
  void shouldHandleReorgWithMultipleAccountsAffected() {
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
  void shouldTrackAccountBalanceChangesAcrossBlocks() {
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
  void shouldHandleReorgToLongerAlternateChain() {
    Address accountX = Address.fromHexString("0xF700000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: 3 blocks, each sending 1 ETH (total 3 ETH)
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 3; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(3L);

    // Reorg to chain B: 5 blocks from genesis, each sending 2 ETH (total 10 ETH)
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    parentHeader = genesisHeader;

    // Build alternate chain of 5 blocks
    for (int i = 0; i < 5; i++) {
      Transaction tx = createTransaction(accountX, TWO_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);

      if (i == 0) {
        // First block triggers the reorg
        executeReorg(block, wsAtGenesis, 0L);
      } else {
        // Subsequent blocks extend the new chain
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    // Verify: Account X should have 10 ETH from 5 blocks of 2 ETH each
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Account X balance should be 10 ETH from longer chain B")
        .isEqualTo(TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber())
        .as("Chain head should be at block 5")
        .isEqualTo(5L);
  }

  @Test
  void shouldReturnOrphanedBlockStateForHistoricalQuery() {
    Address accountX = Address.fromHexString("0xF800000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1A: Account X gets 1 ETH
    Transaction tx1A = createTransaction(accountX, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(ONE_ETH);

    // Reorg to block1B: Account X gets 5 ETH
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(accountX, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // Current state should be 5 ETH from block1B
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Current balance should be 5 ETH from block1B")
        .isEqualTo(FIVE_ETH);

    // Query the orphaned block1A - archive mode preserves this data via trie logs
    // The trie log for block1A still exists and can be used to reconstruct the state
    Optional<MutableWorldState> orphanedWorldState =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1A.getHeader()));

    // The orphaned block data is still accessible in archive mode
    // This is useful for debugging and forensic analysis
    assertThat(orphanedWorldState)
        .as("Archive mode should return world state for orphaned block")
        .isPresent();

    // Archive mode preserves orphaned block state - returns the original 1 ETH
    // from block1A, not the canonical chain's 5 ETH from block1B
    assertThat(orphanedWorldState.get().get(accountX).getBalance())
        .as("Orphaned block query returns orphaned block's original state")
        .isEqualTo(ONE_ETH);
  }

  @Test
  void shouldHandleReorgAtTrieLogDepthBoundary() {
    // This test verifies behavior at the exact trie log depth boundary (16 blocks)
    Address accountX = Address.fromHexString("0xF900000000000000000000000000000000000001");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Build chain A: exactly TRIE_LOG_DEPTH (16) blocks
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < TRIE_LOG_DEPTH; i++) {
      Transaction tx = createTransaction(accountX, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei sixteenEth = ONE_ETH.multiply(TRIE_LOG_DEPTH);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance()).isEqualTo(sixteenEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(TRIE_LOG_DEPTH);

    // Get the fork point at exactly half the trie log depth
    long forkBlockNumber = TRIE_LOG_DEPTH / 2; // Block 8
    BlockHeader forkHeader = blockchain.getBlockHeader(forkBlockNumber).orElseThrow();

    // Reorg from block 8: create alternate chain with different values
    MutableWorldState wsAtFork = getHistoricalWorldState(forkHeader);
    Transaction txB = createTransaction(accountX, TEN_ETH, forkBlockNumber);
    Block blockB = forTransactions(List.of(txB), forkHeader);
    executeReorg(blockB, wsAtFork, forkBlockNumber);

    // Verify: Account X should have 8 ETH (from blocks 1-8) + 10 ETH (from block 9B) = 18 ETH
    Wei expectedBalance = ONE_ETH.multiply(forkBlockNumber).add(TEN_ETH);
    assertThat(archiveProvider.getWorldState().get(accountX).getBalance())
        .as("Account X balance should reflect the reorged chain")
        .isEqualTo(expectedBalance);

    // Historical query at fork point should still work
    assertThat(getHistoricalWorldState(forkHeader).get(accountX).getBalance())
        .as("Historical query at fork point should return balance at that block")
        .isEqualTo(ONE_ETH.multiply(forkBlockNumber));
  }

  @Test
  void shouldHandleStorageSlotChangesAcrossReorg() {
    // This test verifies that contract storage slots are correctly handled during reorg
    // We deploy a simple contract that stores a value in its constructor
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Contract bytecode: stores msg.value in storage slot 0
    // PUSH1 0x00 CALLVALUE SSTORE STOP = 60 00 34 55 00
    // With contract creation wrapper
    Bytes contractCode = Bytes.fromHexString("6000345500");
    Bytes initCode =
        Bytes.concatenate(
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(contractCode.size()), // code size
            Bytes.fromHexString("600b6000f3"), // DUP code, PUSH1 0, CODECOPY, RETURN
            contractCode);

    // Block 1A: Deploy contract with 1 ETH (stores 1 ETH in slot 0)
    Transaction deployTx1A = createContractDeployment(initCode, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist after deployment in chain A")
        .isNotNull();

    // Reorg to block1B: Deploy same contract with 5 ETH (stores 5 ETH in slot 0)
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction deployTx1B = createContractDeployment(initCode, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(deployTx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // Contract should still exist but with different storage
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist after reorg")
        .isNotNull();

    // Verify balance reflects the new deployment value
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .as("Contract balance should be 5 ETH from chain B deployment")
        .isEqualTo(FIVE_ETH);
  }

  @Test
  void shouldHandleContractDeploymentReorg() {
    // This test verifies that contract deployment is correctly handled when
    // a contract exists in one chain but not the other
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();
    Address receiver = Address.fromHexString("0xFA00000000000000000000000000000000000001");

    // Contract bytecode that just stores and stops
    Bytes contractCode = Bytes.fromHexString("6000345500"); // PUSH1 0, CALLVALUE, SSTORE, STOP
    Bytes initCode =
        Bytes.concatenate(
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(contractCode.size()), // code size
            Bytes.fromHexString("600b6000f3"), // code position, PUSH1 0, CODECOPY, RETURN
            contractCode);

    // Block 1A: Deploy a contract
    Transaction deployTx = createContractDeployment(initCode, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist in chain A")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).hasCode())
        .as("Contract should have code in chain A")
        .isTrue();

    // Reorg to block1B: Simple value transfer instead of contract deployment
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction valueTx = createTransaction(receiver, TWO_ETH, 0L);
    Block block1B = forTransactions(List.of(valueTx), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // After reorg: Contract should NOT exist (it was only deployed in chain A)
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should NOT exist after reorg to chain without deployment")
        .isNull();

    // Receiver should have the value from chain B
    assertThat(archiveProvider.getWorldState().get(receiver))
        .as("Receiver should exist in chain B")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(receiver).getBalance())
        .as("Receiver should have 2 ETH from chain B")
        .isEqualTo(TWO_ETH);
  }

  @Test
  void shouldHandleSelfDestructDuringReorg() {
    // Test: Contract exists and keeps funds in chain A, but self-destructs in chain B
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();
    Address beneficiary = Address.fromHexString("0xFB00000000000000000000000000000000000001");

    // Runtime code: PUSH20 beneficiary SELFDESTRUCT (22 bytes)
    // When called, sends all funds to beneficiary
    Bytes runtimeCode =
        Bytes.concatenate(
            Bytes.fromHexString("73"), // PUSH20
            beneficiary.getBytes(), // beneficiary address (20 bytes)
            Bytes.fromHexString("FF") // SELFDESTRUCT
            );

    // Init code pattern: copy runtime code to memory, then return it
    // PUSH1 <size> PUSH1 <offset> PUSH1 0 CODECOPY PUSH1 <size> PUSH1 0 RETURN
    // Total init prefix = 12 bytes, so runtime code starts at offset 12
    Bytes initCode =
        Bytes.concatenate(
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(runtimeCode.size()), // runtime code size (22)
            Bytes.fromHexString("600c"), // PUSH1 12 (code offset)
            Bytes.fromHexString("6000"), // PUSH1 0 (memory dest)
            Bytes.fromHexString("39"), // CODECOPY
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(runtimeCode.size()), // runtime code size (22)
            Bytes.fromHexString("6000"), // PUSH1 0 (return offset)
            Bytes.fromHexString("f3"), // RETURN
            runtimeCode);

    // Block 1A: Deploy the contract with 3 ETH, do NOT call it (no selfdestruct)
    Transaction deployTx = createContractDeployment(initCode, THREE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist in chain A")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .as("Contract should have 3 ETH in chain A")
        .isEqualTo(THREE_ETH);
    assertThat(archiveProvider.getWorldState().get(beneficiary))
        .as("Beneficiary should not exist in chain A (no selfdestruct called)")
        .isNull();

    // Reorg to chain B: Deploy contract AND call it to trigger selfdestruct
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);

    // Deploy the contract in chain B
    Transaction deployTxB = createContractDeployment(initCode, THREE_ETH, 0L);
    Block block1B = forTransactions(List.of(deployTxB), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // Now call the contract to trigger selfdestruct
    Transaction callTx = createContractCall(contractAddress, Bytes.EMPTY, Wei.ZERO, 1L);
    Block block2B = forTransactions(List.of(callTx), block1B.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2B);

    // After chain B: Beneficiary should have received the 3 ETH from selfdestruct
    assertThat(archiveProvider.getWorldState().get(beneficiary))
        .as("Beneficiary should exist after selfdestruct in chain B")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(beneficiary).getBalance())
        .as("Beneficiary should have received funds from selfdestruct")
        .isEqualTo(THREE_ETH);
  }

  @Test
  void shouldHandleCodeChangesViaCreate2DuringReorg() {
    // Test: Contract has different code at same address in different chains
    // Using same sender nonce means same contract address via CREATE
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Simple runtime codes with different stored values
    // Contract A: stores 0xAA in slot 0 then stops
    Bytes codeA = Bytes.fromHexString("60AA60005500"); // PUSH1 0xAA, PUSH1 0, SSTORE, STOP
    // Contract B: stores 0xBB in slot 0 then stops
    Bytes codeB = Bytes.fromHexString("60BB60005500"); // PUSH1 0xBB, PUSH1 0, SSTORE, STOP

    // Correct init code pattern:
    // PUSH1 <size> PUSH1 <offset> PUSH1 0 CODECOPY PUSH1 <size> PUSH1 0 RETURN
    // Total init prefix = 12 bytes, runtime code starts at offset 12
    Bytes initCodeA =
        Bytes.concatenate(
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(codeA.size()), // runtime code size (6)
            Bytes.fromHexString("600c"), // PUSH1 12 (code offset)
            Bytes.fromHexString("6000"), // PUSH1 0 (memory dest)
            Bytes.fromHexString("39"), // CODECOPY
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(codeA.size()), // runtime code size (6)
            Bytes.fromHexString("6000"), // PUSH1 0 (return offset)
            Bytes.fromHexString("f3"), // RETURN
            codeA);

    Bytes initCodeB =
        Bytes.concatenate(
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(codeB.size()), // runtime code size (6)
            Bytes.fromHexString("600c"), // PUSH1 12 (code offset)
            Bytes.fromHexString("6000"), // PUSH1 0 (memory dest)
            Bytes.fromHexString("39"), // CODECOPY
            Bytes.fromHexString("60"), // PUSH1
            Bytes.of(codeB.size()), // runtime code size (6)
            Bytes.fromHexString("6000"), // PUSH1 0 (return offset)
            Bytes.fromHexString("f3"), // RETURN
            codeB);

    // Block 1A: Deploy contract with code A
    Transaction deployTxA = createContractDeployment(initCodeA, Wei.ZERO, 0L);
    Block block1A = forTransactions(List.of(deployTxA), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist in chain A")
        .isNotNull();
    Bytes codeInChainA = archiveProvider.getWorldState().get(contractAddress).getCode();
    assertThat(codeInChainA).as("Contract should have code A").isEqualTo(codeA);

    // Reorg to chain B: Deploy contract with code B at same address
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction deployTxB = createContractDeployment(initCodeB, Wei.ZERO, 0L);
    Block block1B = forTransactions(List.of(deployTxB), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // After reorg: Contract should exist with code B
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist after reorg")
        .isNotNull();
    Bytes codeAfterReorg = archiveProvider.getWorldState().get(contractAddress).getCode();
    assertThat(codeAfterReorg).as("Contract should have code B after reorg").isEqualTo(codeB);
    assertThat(codeAfterReorg).as("Code should be different from chain A").isNotEqualTo(codeA);
  }

  @Test
  void shouldTrackAccountNonceAcrossReorg() {
    // Test: Account nonce differs between chain A and chain B
    Address recipient1 = Address.fromHexString("0xFC00000000000000000000000000000000000001");
    Address recipient2 = Address.fromHexString("0xFC00000000000000000000000000000000000002");
    Address recipient3 = Address.fromHexString("0xFC00000000000000000000000000000000000003");
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    Address senderAddress = Address.extract(sender.getPublicKey());

    // Get initial nonce
    long initialNonce = archiveProvider.getWorldState().get(senderAddress).getNonce();

    // Chain A: Sender makes 3 transactions (nonce increases by 3)
    Transaction tx1A = createTransaction(recipient1, ONE_ETH, initialNonce);
    Transaction tx2A = createTransaction(recipient2, ONE_ETH, initialNonce + 1);
    Transaction tx3A = createTransaction(recipient3, ONE_ETH, initialNonce + 2);
    Block block1A = forTransactions(List.of(tx1A, tx2A, tx3A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    long nonceAfterChainA = archiveProvider.getWorldState().get(senderAddress).getNonce();
    assertThat(nonceAfterChainA)
        .as("Nonce should increase by 3 after chain A")
        .isEqualTo(initialNonce + 3);

    // Reorg to chain B: Sender makes only 1 transaction (nonce increases by 1)
    MutableWorldState wsAtGenesis = getHistoricalWorldState(genesisHeader);
    Transaction tx1B = createTransaction(recipient1, TWO_ETH, initialNonce);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    executeReorg(block1B, wsAtGenesis, 0L);

    // After reorg: Nonce should reflect chain B (only 1 transaction)
    long nonceAfterReorg = archiveProvider.getWorldState().get(senderAddress).getNonce();
    assertThat(nonceAfterReorg)
        .as("Nonce should be initial + 1 after reorg to chain B")
        .isEqualTo(initialNonce + 1);

    // Verify only recipient1 exists with 2 ETH, others should not exist
    assertThat(archiveProvider.getWorldState().get(recipient1).getBalance())
        .as("Recipient1 should have 2 ETH from chain B")
        .isEqualTo(TWO_ETH);
    assertThat(archiveProvider.getWorldState().get(recipient2))
        .as("Recipient2 should not exist after reorg")
        .isNull();
    assertThat(archiveProvider.getWorldState().get(recipient3))
        .as("Recipient3 should not exist after reorg")
        .isNull();
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

  private Transaction createContractDeployment(
      final Bytes initCode, final Wei value, final long nonce) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.empty()) // Contract creation has no 'to' address
        .value(value)
        .payload(initCode)
        .gasLimit(100_000L) // Higher gas limit for contract creation
        .nonce(nonce)
        .createTransaction(sender);
  }

  private Transaction createContractCall(
      final Address contract, final Bytes data, final Wei value, final long nonce) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(contract))
        .value(value)
        .payload(data)
        .gasLimit(100_000L)
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
