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
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Integration tests for reorg scenarios with Bonsai archive mode using in-memory storage.
 *
 * <p>These tests verify that reorganizations are correctly handled when using the archive world
 * state provider, ensuring that account balances reflect the state of the new canonical chain after
 * a reorg.
 */
public class BonsaiArchiveReorgTest {

  private static final String GENESIS_CONFIG = "/dev.json";
  private static final long TRIE_LOG_DEPTH = 16L;
  private static final Wei ONE_ETH = Wei.of(1_000_000_000_000_000_000L);
  private static final Wei TWO_ETH = ONE_ETH.multiply(2);
  private static final Wei THREE_ETH = ONE_ETH.multiply(3);
  private static final Wei FIVE_ETH = ONE_ETH.multiply(5);
  private static final Wei TEN_ETH = ONE_ETH.multiply(10);

  private static final Address ACCOUNT_A =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ACCOUNT_B =
      Address.fromHexString("0x1000000000000000000000000000000000000002");
  private static final Address ACCOUNT_C =
      Address.fromHexString("0x1000000000000000000000000000000000000003");

  private ExecutionContextTestFixture fixture;
  private BonsaiArchiveWorldStateProvider archiveProvider;
  private MutableBlockchain blockchain;
  private ProtocolContext protocolContext;
  private ProtocolSchedule protocolSchedule;
  private TransactionPool transactionPool;
  private KeyPair sender;
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  @BeforeEach
  public void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .maxLayersToLoad(TRIE_LOG_DEPTH)
            .build();

    blockchain = fixture.getBlockchain();
    protocolContext = fixture.getProtocolContext();
    protocolSchedule = fixture.getProtocolSchedule();
    archiveProvider = (BonsaiArchiveWorldStateProvider) fixture.getStateArchive();
    assertThat(archiveProvider.getWorldStateKeyValueStorage().getFlatDbMode())
        .isEqualTo(FlatDbMode.ARCHIVE);

    sender =
        GenesisConfig.fromResource(GENESIS_CONFIG)
            .streamAllocations()
            .filter(ga -> ga.privateKey() != null)
            .findFirst()
            .map(ga -> asKeyPair(ga.privateKey()))
            .orElseThrow();

    transactionPool = Mockito.mock(TransactionPool.class);
  }

  private KeyPair asKeyPair(final Bytes32 key) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(SECPPrivateKey.create(key, "ECDSA"));
  }

  @Test
  void shouldHandleReorgWithConflictingAccountBalances() {
    // Build chain: genesis -> blocks 1-9 (empty)
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: Account receives 1 ETH
    Transaction tx10A = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Create alternate block10B: Account receives 2 ETH
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(ACCOUNT_A, TWO_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);

    executeReorg(block10B, wsAtBlock9, 9L);
    assertBalance(ACCOUNT_A, TWO_ETH);

    Hash headBlockHash =
        ((PathBasedWorldState) archiveProvider.getWorldState()).getWorldStateBlockHash();
    assertThat(headBlockHash).isEqualTo(block10B.getHash());
  }

  @Test
  void shouldHandleReorgAccountCreationVsNoCreation() {
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: Account A is created with 1 ETH
    Transaction tx10A = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Block block10A = forTransactions(List.of(tx10A), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);
    assertAccountExists(ACCOUNT_A);

    // Reorg to block10B: Account B gets 1 ETH instead
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(ACCOUNT_B, ONE_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);
    executeReorg(block10B, wsAtBlock9, 9L);

    // Account A should not exist after reorg
    assertAccountNull(ACCOUNT_A);
    assertAccountExists(ACCOUNT_B);
  }

  @Test
  void shouldSupportHistoricalQueriesAfterReorg() {
    // Build chain: genesis -> block1 -> block2 (empty)
    BlockHeader block2Header = buildEmptyChainToBlock(2);

    // Block 3A: Account gets 1 ETH
    Transaction tx3A = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Block block3A = forTransactions(List.of(tx3A), block2Header);
    executeBlock(archiveProvider.getWorldState(), block3A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Historical query at block 2 - account should not exist
    assertThat(getHistoricalWorldState(block2Header).get(ACCOUNT_A)).isNull();

    // Reorg to block3B: Account gets 2 ETH
    MutableWorldState wsAtBlock2 = getHistoricalWorldState(block2Header);
    Transaction tx3B = createTransaction(ACCOUNT_A, TWO_ETH, 0L);
    Block block3B = forTransactions(List.of(tx3B), block2Header);
    executeReorg(block3B, wsAtBlock2, 2L);

    // Current state should be 2 ETH
    assertBalance(ACCOUNT_A, TWO_ETH);

    // Historical query at block 2 should still work
    assertThat(getHistoricalWorldState(block2Header).get(ACCOUNT_A)).isNull();

    // Historical query at block3B should return 2 ETH
    assertThat(getHistoricalWorldState(block3B.getHeader()).get(ACCOUNT_A).getBalance())
        .isEqualTo(TWO_ETH);
  }

  @Test
  void shouldHandleConsecutiveReorgs() {
    // Chain A: block1A with 1 ETH
    Transaction tx1A = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // First reorg: block1B with 2 ETH
    Transaction tx1B = createTransaction(ACCOUNT_A, TWO_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);
    assertBalance(ACCOUNT_A, TWO_ETH);

    // Second reorg: block1C with 3 ETH
    Transaction tx1C = createTransaction(ACCOUNT_A, THREE_ETH, 0L);
    Block block1C = forTransactions(List.of(tx1C), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1C);
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void shouldHandleReorgWithMultipleAccountsAffected() {
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: ACCOUNT_A gets 1 ETH, ACCOUNT_B gets 1 ETH
    Transaction tx10A_1 = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Transaction tx10A_2 = createTransaction(ACCOUNT_B, ONE_ETH, 1L);
    Block block10A = forTransactions(List.of(tx10A_1, tx10A_2), parentHeader);
    executeBlock(archiveProvider.getWorldState(), block10A);
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, ONE_ETH);

    // Reorg: ACCOUNT_A gets 2 ETH, ACCOUNT_B gets nothing
    MutableWorldState wsAtBlock9 = getHistoricalWorldState(parentHeader);
    Transaction tx10B = createTransaction(ACCOUNT_A, TWO_ETH, 0L);
    Block block10B = forTransactions(List.of(tx10B), parentHeader);
    executeReorg(block10B, wsAtBlock9, 9L);

    assertBalance(ACCOUNT_A, TWO_ETH);
    assertAccountNull(ACCOUNT_B);
  }

  @Test
  void shouldTrackAccountBalanceChangesAcrossBlocks() {
    BlockHeader parentHeader = fixture.getGenesis().getHeader();

    // Process 5 blocks, each adding 1 ETH
    for (int i = 1; i <= 5; i++) {
      Transaction tx = createTransaction(ACCOUNT_A, ONE_ETH, (long) (i - 1));
      Block block = forTransactions(List.of(tx), parentHeader);
      BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();

      // Verify current balance
      Wei expectedBalance = ONE_ETH.multiply(i);
      assertThat(archiveProvider.getWorldState().get(ACCOUNT_A).getBalance())
          .as("Balance after block %d should be %d ETH", i, i)
          .isEqualTo(expectedBalance);

      parentHeader = block.getHeader();
    }

    // Final verification: historical queries at each block should return correct values
    for (int i = 1; i <= 5; i++) {
      BlockHeader blockHeader = blockchain.getBlockHeader(i).orElseThrow();
      MutableWorldState wsAtBlock = getHistoricalWorldState(blockHeader);

      Wei expectedBalance = ONE_ETH.multiply(i);
      assertThat(wsAtBlock.get(ACCOUNT_A).getBalance())
          .as("Historical query at block %d should return %d ETH", i, i)
          .isEqualTo(expectedBalance);
    }
  }

  @Test
  void shouldHandleReorgToLongerAlternateChain() {
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Chain A: 3 blocks, each sending 1 ETH (total 3 ETH)
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 3; i++) {
      Transaction tx = createTransaction(ACCOUNT_A, ONE_ETH, i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(3L);

    // Reorg to chain B: 5 blocks from genesis, each sending 2 ETH (total 10 ETH)
    parentHeader = genesisHeader;
    for (int i = 0; i < 5; i++) {
      Transaction tx = createTransaction(ACCOUNT_A, TWO_ETH, i);
      Block block = forTransactions(List.of(tx), parentHeader);

      if (i == 0) {
        reorgFromGenesis(block);
      } else {
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    // Verify: Account should have 10 ETH from 5 blocks of 2 ETH each
    assertBalance(ACCOUNT_A, TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(5L);
  }

  @Test
  void shouldReturnOrphanedBlockStateForHistoricalQuery() {
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Block 1A: Account gets 1 ETH
    Transaction tx1A = createTransaction(ACCOUNT_A, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(tx1A), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Reorg to block1B: Account gets 5 ETH
    Transaction tx1B = createTransaction(ACCOUNT_A, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(tx1B), genesisHeader);
    reorgFromGenesis(block1B);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Query the orphaned block1A - archive mode preserves this data via trie logs
    Optional<MutableWorldState> orphanedWorldState =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1A.getHeader()));

    // Archive mode preserves orphaned block state
    assertThat(orphanedWorldState).isPresent();
    assertThat(orphanedWorldState.get().get(ACCOUNT_A).getBalance()).isEqualTo(ONE_ETH);
  }

  @Test
  void shouldHandleReorgAtTrieLogDepthBoundary() {
    // This test verifies behavior at the exact trie log depth boundary (16 blocks)
    BlockHeader genesisHeader = fixture.getGenesis().getHeader();

    // Build chain A: exactly TRIE_LOG_DEPTH (16) blocks
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < TRIE_LOG_DEPTH; i++) {
      Transaction tx = createTransaction(ACCOUNT_A, ONE_ETH, (long) i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    Wei sixteenEth = ONE_ETH.multiply(TRIE_LOG_DEPTH);
    assertBalance(ACCOUNT_A, sixteenEth);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(TRIE_LOG_DEPTH);

    // Get the fork point at exactly half the trie log depth
    long forkBlockNumber = TRIE_LOG_DEPTH / 2; // Block 8
    BlockHeader forkHeader = blockchain.getBlockHeader(forkBlockNumber).orElseThrow();

    // Reorg from block 8: create alternate chain with different values
    MutableWorldState wsAtFork = getHistoricalWorldState(forkHeader);
    Transaction txB = createTransaction(ACCOUNT_A, TEN_ETH, forkBlockNumber);
    Block blockB = forTransactions(List.of(txB), forkHeader);
    executeReorg(blockB, wsAtFork, forkBlockNumber);

    // Verify: Account should have 8 ETH (from blocks 1-8) + 10 ETH (from block 9B) = 18 ETH
    Wei expectedBalance = ONE_ETH.multiply(forkBlockNumber).add(TEN_ETH);
    assertBalance(ACCOUNT_A, expectedBalance);

    // Historical query at fork point should still work
    assertThat(getHistoricalWorldState(forkHeader).get(ACCOUNT_A).getBalance())
        .isEqualTo(ONE_ETH.multiply(forkBlockNumber));
  }

  @Test
  void shouldHandleStorageSlotChangesAcrossReorg() {
    // Contract that stores msg.value in storage slot 0
    Bytes runtimeCode = Bytes.fromHexString("6000345500"); // PUSH1 0, CALLVALUE, SSTORE, STOP
    Bytes initCode = createInitCode(runtimeCode);

    // Block 1A: Deploy contract with 1 ETH
    Transaction deployTx1A = createContractDeployment(initCode, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx1A), fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();

    // Reorg to block1B: Deploy same contract with 5 ETH
    Transaction deployTx1B = createContractDeployment(initCode, FIVE_ETH, 0L);
    Block block1B = forTransactions(List.of(deployTx1B), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);

    // Contract should exist with new balance
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);
  }

  @Test
  void shouldHandleContractDeploymentReorg() {
    Bytes runtimeCode = Bytes.fromHexString("6000345500"); // PUSH1 0, CALLVALUE, SSTORE, STOP
    Bytes initCode = createInitCode(runtimeCode);

    // Block 1A: Deploy a contract
    Transaction deployTx = createContractDeployment(initCode, ONE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx), fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).hasCode()).isTrue();

    // Reorg to block1B: Simple value transfer instead of contract deployment
    Transaction valueTx = createTransaction(ACCOUNT_A, TWO_ETH, 0L);
    Block block1B = forTransactions(List.of(valueTx), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);

    // After reorg: Contract should NOT exist, recipient should have 2 ETH
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNull();
    assertAccountExists(ACCOUNT_A);
    assertBalance(ACCOUNT_A, TWO_ETH);
  }

  @Test
  void shouldHandleSelfDestructDuringReorg() {
    // Runtime code: PUSH20 beneficiary SELFDESTRUCT
    Bytes runtimeCode =
        Bytes.concatenate(
            Bytes.fromHexString("73"), ACCOUNT_B.getBytes(), Bytes.fromHexString("FF"));
    Bytes initCode = createInitCode(runtimeCode);

    // Block 1A: Deploy contract with 3 ETH, do NOT call it
    Transaction deployTx = createContractDeployment(initCode, THREE_ETH, 0L);
    Block block1A = forTransactions(List.of(deployTx), fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(THREE_ETH);
    assertAccountNull(ACCOUNT_B);

    // Reorg to chain B: Deploy contract AND call it to trigger selfdestruct
    Transaction deployTxB = createContractDeployment(initCode, THREE_ETH, 0L);
    Block block1B = forTransactions(List.of(deployTxB), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);

    // Call the contract to trigger selfdestruct
    Transaction callTx = createContractCall(contractAddress, Bytes.EMPTY, Wei.ZERO, 1L);
    Block block2B = forTransactions(List.of(callTx), block1B.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2B);

    // Beneficiary should have received the 3 ETH from selfdestruct
    assertAccountExists(ACCOUNT_B);
    assertBalance(ACCOUNT_B, THREE_ETH);
  }

  @Test
  void shouldHandleCodeChangesViaCreate2DuringReorg() {
    // Contract A: stores 0xAA in slot 0; Contract B: stores 0xBB in slot 0
    Bytes codeA = Bytes.fromHexString("60AA60005500");
    Bytes codeB = Bytes.fromHexString("60BB60005500");
    Bytes initCodeA = createInitCode(codeA);
    Bytes initCodeB = createInitCode(codeB);

    // Block 1A: Deploy contract with code A
    Transaction deployTxA = createContractDeployment(initCodeA, Wei.ZERO, 0L);
    Block block1A = forTransactions(List.of(deployTxA), fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getCode()).isEqualTo(codeA);

    // Reorg to chain B: Deploy contract with code B at same address
    Transaction deployTxB = createContractDeployment(initCodeB, Wei.ZERO, 0L);
    Block block1B = forTransactions(List.of(deployTxB), fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);

    // After reorg: Contract should exist with code B
    assertThat(archiveProvider.getWorldState().get(contractAddress)).isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getCode()).isEqualTo(codeB);
  }

  @Test
  void shouldTrackAccountNonceAcrossReorg() {
    Address senderAddress = Address.extract(sender.getPublicKey());
    long initialNonce = archiveProvider.getWorldState().get(senderAddress).getNonce();

    // Chain A: Sender makes 3 transactions (nonce increases by 3)
    Block block1A =
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_A, ONE_ETH, initialNonce),
                createTransaction(ACCOUNT_B, ONE_ETH, initialNonce + 1),
                createTransaction(ACCOUNT_C, ONE_ETH, initialNonce + 2)),
            fixture.getGenesis().getHeader());
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertThat(archiveProvider.getWorldState().get(senderAddress).getNonce())
        .isEqualTo(initialNonce + 3);

    // Reorg to chain B: Sender makes only 1 transaction
    Block block1B =
        forTransactions(
            List.of(createTransaction(ACCOUNT_A, TWO_ETH, initialNonce)),
            fixture.getGenesis().getHeader());
    reorgFromGenesis(block1B);

    // After reorg: Nonce should reflect chain B (only 1 transaction)
    assertThat(archiveProvider.getWorldState().get(senderAddress).getNonce())
        .isEqualTo(initialNonce + 1);
    assertBalance(ACCOUNT_A, TWO_ETH);
    assertAccountNull(ACCOUNT_B);
    assertAccountNull(ACCOUNT_C);
  }

  private void assertBalance(final Address address, final Wei expectedBalance) {
    assertThat(archiveProvider.getWorldState().get(address).getBalance()).isEqualTo(expectedBalance);
  }

  private void assertAccountExists(final Address address) {
    assertThat(archiveProvider.getWorldState().get(address)).isNotNull();
  }

  private void assertAccountNull(final Address address) {
    assertThat(archiveProvider.getWorldState().get(address)).isNull();
  }

  private Bytes createInitCode(final Bytes runtimeCode) {
    return Bytes.concatenate(
        Bytes.fromHexString("60"),
        Bytes.of(runtimeCode.size()),
        Bytes.fromHexString("600c60003960"),
        Bytes.of(runtimeCode.size()),
        Bytes.fromHexString("6000f3"),
        runtimeCode);
  }

  private void reorgFromGenesis(final Block alternateBlock) {
    executeReorg(alternateBlock, getHistoricalWorldState(fixture.getGenesis().getHeader()), 0L);
  }

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
        .to(Optional.empty())
        .value(value)
        .payload(initCode)
        .gasLimit(100_000L)
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
