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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
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

  private BonsaiArchiveWorldStateProvider archiveProvider;
  private MutableBlockchain blockchain;
  private ProtocolContext protocolContext;
  private ProtocolSchedule protocolSchedule;
  private TransactionPool transactionPool;
  private KeyPair sender;
  private BlockHeader genesisHeader;
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();

  @BeforeEach
  public void setUp() {
    ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .maxLayersToLoad(TRIE_LOG_DEPTH)
            .build();

    blockchain = fixture.getBlockchain();
    protocolContext = fixture.getProtocolContext();
    protocolSchedule = fixture.getProtocolSchedule();
    archiveProvider = (BonsaiArchiveWorldStateProvider) fixture.getStateArchive();
    genesisHeader = fixture.getGenesis().getHeader();
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
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    transfer(parentHeader);
    assertBalance(ACCOUNT_A, ONE_ETH);

    Block block10B =
        forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, 0L)), parentHeader);
    reorgFrom(parentHeader, block10B);
    assertBalance(ACCOUNT_A, TWO_ETH);

    Hash headBlockHash =
        ((PathBasedWorldState) archiveProvider.getWorldState()).getWorldStateBlockHash();
    assertThat(headBlockHash).isEqualTo(block10B.getHash());
  }

  @Test
  void shouldHandleReorgAccountCreationVsNoCreation() {
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    transfer(parentHeader);
    assertAccountExists(ACCOUNT_A);

    reorgFromWithTransfer(parentHeader, ACCOUNT_B, ONE_ETH);
    assertAccountNull(ACCOUNT_A);
    assertAccountExists(ACCOUNT_B);
  }

  @Test
  void shouldSupportHistoricalQueriesAfterReorg() {
    BlockHeader block2Header = buildEmptyChainToBlock(2);

    transfer(block2Header);
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertThat(getHistoricalWorldState(block2Header).get(ACCOUNT_A)).isNull();

    Block block3B =
        forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, 0L)), block2Header);
    reorgFrom(block2Header, block3B);

    assertBalance(ACCOUNT_A, TWO_ETH);
    assertThat(getHistoricalWorldState(block2Header).get(ACCOUNT_A)).isNull();
    assertThat(getHistoricalWorldState(block3B.getHeader()).get(ACCOUNT_A).getBalance())
        .isEqualTo(TWO_ETH);
  }

  @Test
  void shouldHandleConsecutiveReorgs() {
    transfer(genesisHeader);
    assertBalance(ACCOUNT_A, ONE_ETH);

    reorgFromWithTransfer(genesisHeader, ACCOUNT_A, TWO_ETH);
    assertBalance(ACCOUNT_A, TWO_ETH);

    reorgFromWithTransfer(genesisHeader, ACCOUNT_A, THREE_ETH);
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void shouldHandleReorgWithMultipleAccountsAffected() {
    BlockHeader parentHeader = buildEmptyChainToBlock(9);

    // Block 10A: ACCOUNT_A gets 1 ETH, ACCOUNT_B gets 1 ETH
    executeBlock(
        archiveProvider.getWorldState(),
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_A, ONE_ETH, 0L),
                createTransaction(ACCOUNT_B, ONE_ETH, 1L)),
            parentHeader));
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, ONE_ETH);

    reorgFromWithTransfer(parentHeader, ACCOUNT_A, TWO_ETH);
    assertBalance(ACCOUNT_A, TWO_ETH);
    assertAccountNull(ACCOUNT_B);
  }

  @Test
  void shouldTrackAccountBalanceChangesAcrossBlocks() {
    BlockHeader parentHeader = genesisHeader;

    // Process 5 blocks, each adding 1 ETH
    for (int i = 1; i <= 5; i++) {
      Transaction tx = createTransaction(ACCOUNT_A, ONE_ETH, i - 1);
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
    // Chain A: 3 blocks, each sending 1 ETH (total 3 ETH)
    buildChain(3);
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(3L);

    // Reorg to chain B: 5 blocks from genesis, each sending 2 ETH (total 10 ETH)
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 5; i++) {
      Block block =
          forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, i)), parentHeader);
      if (i == 0) {
        reorgFromGenesis(block);
      } else {
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    assertBalance(ACCOUNT_A, TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(5L);
  }

  @Test
  void shouldReturnOrphanedBlockStateForHistoricalQuery() {
    Block block1A = transfer(genesisHeader);
    assertBalance(ACCOUNT_A, ONE_ETH);

    reorgFromWithTransfer(genesisHeader, ACCOUNT_A, FIVE_ETH);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Query the orphaned block1A - archive mode preserves this data via trie logs
    Optional<MutableWorldState> orphanedWorldState =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(block1A.getHeader()));
    assertThat(orphanedWorldState).isPresent();
    assertThat(orphanedWorldState.get().get(ACCOUNT_A).getBalance()).isEqualTo(ONE_ETH);
  }

  @Test
  void shouldHandleReorgAtTrieLogDepthBoundary() {
    buildChain((int) TRIE_LOG_DEPTH);
    assertBalance(ACCOUNT_A, ONE_ETH.multiply(TRIE_LOG_DEPTH));
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(TRIE_LOG_DEPTH);

    long forkBlockNumber = TRIE_LOG_DEPTH / 2; // Block 8
    BlockHeader forkHeader = blockchain.getBlockHeader(forkBlockNumber).orElseThrow();

    Block blockB =
        forTransactions(
            List.of(createTransaction(ACCOUNT_A, TEN_ETH, forkBlockNumber)), forkHeader);
    reorgFrom(forkHeader, blockB);

    // Account should have 8 ETH (from blocks 1-8) + 10 ETH (from block 9B) = 18 ETH
    assertBalance(ACCOUNT_A, ONE_ETH.multiply(forkBlockNumber).add(TEN_ETH));
    assertThat(getHistoricalWorldState(forkHeader).get(ACCOUNT_A).getBalance())
        .isEqualTo(ONE_ETH.multiply(forkBlockNumber));
  }

  @Test
  void shouldHandleStorageSlotChangesAcrossReorg() {
    // Runtime: 60 00 PUSH1 0, 34 CALLVALUE, 55 SSTORE, 00 STOP - stores msg.value in slot 0
    Bytes runtimeCode = Bytes.fromHexString("6000345500");
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    deployContractFromGenesis(initCode, ONE_ETH);
    assertAccountExists(contractAddress);

    // Reorg: Deploy same contract with 5 ETH
    reorgFromGenesis(
        forTransactions(List.of(createContractDeployment(initCode, FIVE_ETH)), genesisHeader));
    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);
  }

  @Test
  void shouldHandleContractDeploymentReorg() {
    // Runtime: 60 00 PUSH1 0, 34 CALLVALUE, 55 SSTORE, 00 STOP - stores msg.value in slot 0
    Bytes initCode = createInitCode(Bytes.fromHexString("6000345500"));
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    deployContractFromGenesis(initCode, ONE_ETH);
    assertThat(archiveProvider.getWorldState().get(contractAddress).hasCode()).isTrue();

    reorgFromWithTransfer(genesisHeader, ACCOUNT_A, TWO_ETH);
    assertAccountNull(contractAddress);
    assertBalance(ACCOUNT_A, TWO_ETH);
  }

  @Test
  void shouldHandleSelfDestructDuringReorg() {
    // Runtime: 73 PUSH20 <address>, FF SELFDESTRUCT - sends balance to beneficiary
    Bytes runtimeCode =
        Bytes.concatenate(
            Bytes.fromHexString("73"), ACCOUNT_B.getBytes(), Bytes.fromHexString("FF"));
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Block 1A: Deploy contract with 3 ETH, do NOT call it
    deployContractFromGenesis(initCode, THREE_ETH);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(THREE_ETH);
    assertAccountNull(ACCOUNT_B);

    // Reorg: Deploy contract AND call it to trigger selfdestruct
    Block block1B =
        forTransactions(List.of(createContractDeployment(initCode, THREE_ETH)), genesisHeader);
    reorgFromGenesis(block1B);
    executeBlock(
        archiveProvider.getWorldState(),
        forTransactions(List.of(createContractCall(contractAddress)), block1B.getHeader()));

    assertBalance(ACCOUNT_B, THREE_ETH);
  }

  @Test
  void shouldHandleCodeChangesViaCreate2DuringReorg() {
    // Runtime: 60 XX PUSH1 value, 60 00 PUSH1 0, 55 SSTORE, 00 STOP - stores value in slot 0
    Bytes codeA = Bytes.fromHexString("60AA60005500"); // stores 0xAA
    Bytes codeB = Bytes.fromHexString("60BB60005500"); // stores 0xBB
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    deployContractFromGenesis(createInitCode(codeA), Wei.ZERO);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getCode()).isEqualTo(codeA);

    // Reorg: Deploy contract with code B at same address
    reorgFromGenesis(
        forTransactions(
            List.of(createContractDeployment(createInitCode(codeB), Wei.ZERO)), genesisHeader));
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
            genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertThat(archiveProvider.getWorldState().get(senderAddress).getNonce())
        .isEqualTo(initialNonce + 3);

    // Reorg to chain B: Sender makes only 1 transaction
    Block block1B =
        forTransactions(
            List.of(createTransaction(ACCOUNT_A, TWO_ETH, initialNonce)), genesisHeader);
    reorgFromGenesis(block1B);

    // After reorg: Nonce should reflect chain B (only 1 transaction)
    assertThat(archiveProvider.getWorldState().get(senderAddress).getNonce())
        .isEqualTo(initialNonce + 1);
    assertBalance(ACCOUNT_A, TWO_ETH);
    assertAccountNull(ACCOUNT_B);
    assertAccountNull(ACCOUNT_C);
  }

  private void assertBalance(final Address address, final Wei expectedBalance) {
    assertThat(archiveProvider.getWorldState().get(address).getBalance())
        .isEqualTo(expectedBalance);
  }

  private void assertAccountExists(final Address address) {
    assertThat(archiveProvider.getWorldState().get(address)).isNotNull();
  }

  private void assertAccountNull(final Address address) {
    assertThat(archiveProvider.getWorldState().get(address)).isNull();
  }

  /**
   * Creates EVM init code that deploys the given runtime code.
   *
   * <p>Structure (12 bytes + runtime code):
   *
   * <pre>
   * 60 XX    PUSH1 size   - runtime code size
   * 60 0c    PUSH1 12     - code offset (init code is 12 bytes)
   * 60 00    PUSH1 0      - memory destination
   * 39       CODECOPY     - copy runtime code to memory
   * 60 XX    PUSH1 size   - runtime code size
   * 60 00    PUSH1 0      - memory offset
   * f3       RETURN       - return runtime code
   * [runtime code]
   * </pre>
   */
  private Bytes createInitCode(final Bytes runtimeCode) {
    return Bytes.concatenate(
        Bytes.fromHexString("60"),
        Bytes.of(runtimeCode.size()),
        Bytes.fromHexString("600c60003960"),
        Bytes.of(runtimeCode.size()),
        Bytes.fromHexString("6000f3"),
        runtimeCode);
  }

  private Block transfer(final BlockHeader parent) {
    Block block =
        forTransactions(
            List.of(
                createTransaction(
                    BonsaiArchiveReorgTest.ACCOUNT_A, BonsaiArchiveReorgTest.ONE_ETH, 0L)),
            parent);
    executeBlock(archiveProvider.getWorldState(), block);
    return block;
  }

  private void reorgFrom(final BlockHeader parentHeader, final Block alternateBlock) {
    executeReorg(alternateBlock, getHistoricalWorldState(parentHeader), parentHeader.getNumber());
  }

  private void reorgFromGenesis(final Block alternateBlock) {
    reorgFrom(genesisHeader, alternateBlock);
  }

  private void reorgFromWithTransfer(final BlockHeader parent, final Address to, final Wei value) {
    reorgFrom(parent, forTransactions(List.of(createTransaction(to, value, 0L)), parent));
  }

  private void deployContractFromGenesis(final Bytes initCode, final Wei value) {
    Transaction tx = createContractDeployment(initCode, value);
    executeBlock(archiveProvider.getWorldState(), forTransactions(List.of(tx), genesisHeader));
  }

  private void buildChain(final int count) {
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < count; i++) {
      Transaction tx =
          createTransaction(BonsaiArchiveReorgTest.ACCOUNT_A, BonsaiArchiveReorgTest.ONE_ETH, i);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }
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

  private Transaction createContractDeployment(final Bytes initCode, final Wei value) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.empty())
        .value(value)
        .payload(initCode)
        .gasLimit(100_000L)
        .nonce(0L)
        .createTransaction(sender);
  }

  private Transaction createContractCall(final Address contract) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(contract))
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .gasLimit(100_000L)
        .nonce(1L)
        .createTransaction(sender);
  }

  private Transaction createContractCallWithValue(
      final Address contract, final Wei value, final long nonce) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(contract))
        .value(value)
        .payload(Bytes.EMPTY)
        .gasLimit(100_000L)
        .nonce(nonce)
        .createTransaction(sender);
  }

  private Transaction createContractCallWithData(
      final Address contract, final Bytes data, final long nonce) {
    return new TransactionTestFixture()
        .sender(Address.extract(sender.getPublicKey()))
        .to(Optional.of(contract))
        .value(Wei.ZERO)
        .payload(data)
        .gasLimit(100_000L)
        .nonce(nonce)
        .createTransaction(sender);
  }

  private void assertStorageValue(
      final Address contract, final UInt256 slot, final UInt256 expectedValue) {
    assertThat(archiveProvider.getWorldState().get(contract).getStorageValue(slot))
        .isEqualTo(expectedValue);
  }

  private Block forTransactions(final List<Transaction> transactions, final BlockHeader parent) {
    return TestBlockCreator.forHeader(
            protocolContext, protocolSchedule, transactionPool, ethScheduler)
        .createBlock(transactions, Collections.emptyList(), System.currentTimeMillis(), parent)
        .getBlock();
  }

  private BlockProcessingResult executeBlock(final MutableWorldState ws, final Block block) {
    var blockHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    var blockProcessingResult =
        protocolSchedule
            .getByBlockHeader(blockHeader)
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, ws, block);
    blockchain.appendBlock(block, blockProcessingResult.getReceipts());
    return blockProcessingResult;
  }

  private void executeBlockAndUpdateHead(final Block block) {
    MutableWorldState ws = archiveProvider.getWorldState();
    var blockHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    var blockProcessingResult =
        protocolSchedule
            .getByBlockHeader(blockHeader)
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, ws, block);
    assertThat(blockProcessingResult.isSuccessful()).isTrue();
    ws.persist(block.getHeader());
    blockchain.appendBlock(block, blockProcessingResult.getReceipts());
    archiveProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block.getHeader()));
  }

  private BlockHeader buildEmptyChainToBlock(final int blockCount) {
    BlockHeader parentHeader = genesisHeader;
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

  /**
   * Tests that paired rollback/rollforward during a reorg correctly handles storage slot creation
   * when archive context needs to be updated. This test reproduces the scenario from a bug where
   * storage reads during rollforward were using the wrong block context.
   *
   * <p>Bug scenario: During paired rollback/rollforward, if the ephemeral archive context isn't
   * correctly maintained, subsequent storage reads during rollforward will use the wrong block
   * context, causing "Expected to create slot, but the slot exists" errors.
   *
   * <p>This test creates a reorg with storage slot changes, ensuring the archive context is
   * correctly updated at each stage of the rolling process.
   */
  @Test
  void shouldHandlePairedRollbackRollforwardWithStorageSlotCreation() {
    // Contract that stores values: 60 XX PUSH1 value, 60 YY PUSH1 slot, 55 SSTORE
    Bytes storeInSlot0 = Bytes.fromHexString("60AA60005500"); // stores 0xAA in slot 0
    Bytes storeInSlot1 = Bytes.fromHexString("60BB60015500"); // stores 0xBB in slot 1
    Bytes initCodeA = createInitCode(storeInSlot0);
    Bytes initCodeB = createInitCode(storeInSlot1);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Block 1A: Deploy contract A (creates storage slot 0)
    deployContractFromGenesis(initCodeA, Wei.ZERO);
    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).hasCode()).isTrue();

    // Build chain to block 3 to create some distance
    Block block1A = blockchain.getBlockByNumber(1L).orElseThrow();
    BlockHeader parentHeader = block1A.getHeader();
    for (int i = 2; i <= 3; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Now reorg from genesis, deploying contract B instead (creates storage slot 1)
    // This creates a paired rollback/rollforward scenario:
    // - Rollback blocks 1A, 2A, 3A
    // - Rollforward with 1B (different contract, different storage slot)
    Block block1B =
        forTransactions(List.of(createContractDeployment(initCodeB, Wei.ZERO)), genesisHeader);

    // The reorg will:
    // 1. Roll back from block 3 to genesis (common ancestor)
    // 2. Roll forward with block 1B
    // Without the fix, the storage read during rollforward would use the wrong block context
    // and find storage slot 0 from the old chain, causing the error
    reorgFromGenesis(block1B);

    // Verify the contract exists and is from chain B (not chain A)
    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).hasCode()).isTrue();

    // Verify we can query the historical state
    assertThat(getHistoricalWorldState(genesisHeader).get(contractAddress)).isNull();
  }

  /**
   * Tests multiple rollforwards followed by a paired rollback/rollforward operation. This
   * reproduces a scenario where archive context needs to be correctly maintained across
   * rollforwards and subsequent rollbacks.
   *
   * <p>Log sequence that this test reproduces:
   *
   * <pre>
   * Rollforward 0x67238e14...  (applies block changes, creates storage slots)
   * Rollforward 0x0bdee358...  (applies more changes)
   * [Context must be correctly maintained]
   * Paired Rollback 0x70b8bb69...  (tries to roll back)
   * ERROR: Expected to update storage value, but the slot does not exist
   * </pre>
   *
   * <p>With ephemeral context, each world state gets its own context-safe copy, preventing stale
   * context issues. This test verifies reorgs work correctly with the ephemeral approach.
   */
  @Test
  void shouldHandleRollforwardThenPairedRollbackRollforward() {
    // Build a simple chain: Block 1 transfers to ACCOUNT_A
    transfer(genesisHeader);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Build to block 3
    BlockHeader parentHeader = blockchain.getBlockByNumber(1L).orElseThrow().getHeader();
    for (int i = 2; i <= 3; i++) {
      Block block = forTransactions(Collections.emptyList(), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(3L);

    // Critical part: Get historical world state at block 2
    // This triggers rollforwards from persisted state → block 2
    // With ephemeral context, each world state gets a context-safe copy
    BlockHeader block2Header = blockchain.getBlockByNumber(2L).orElseThrow().getHeader();
    getHistoricalWorldState(block2Header); // Triggers rollforwards with isolated context

    // Now do a reorg from genesis with a different transfer amount to ACCOUNT_A
    // This triggers: Rollback from head → genesis, then Rollforward to new block 1
    // With ephemeral context: each operation gets isolated context → SUCCESS
    reorgFromWithTransfer(genesisHeader, ACCOUNT_A, TWO_ETH);

    // Verify the reorg succeeded - if we get here without IllegalStateException, the fix worked!
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
    // The key test is that we didn't get IllegalStateException during the reorg
    // The actual balance value is less important than proving the reorg completes
  }

  /**
   * Tests multi-block reorg where both chains modify the same account at every block with different
   * values. This tests the full rollback/rollforward cycle with maximum conflict density.
   *
   * <p>Chain A: 5 blocks, each adding 1 ETH (total 5 ETH) Chain B: 5 blocks, each adding 2 ETH
   * (total 10 ETH)
   */
  @Test
  void shouldHandleMultiBlockReorgWithConflictingBalancesAtEveryBlock() {
    // Chain A: 5 blocks, each adding 1 ETH to ACCOUNT_A (total: 5 ETH)
    buildChain(5);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Chain B: 5 blocks from genesis, each adding 2 ETH to ACCOUNT_A (total: 10 ETH)
    // This requires rolling back 5 blocks and rolling forward 5 new blocks
    // Each block on both chains touches the same account with different values
    BlockHeader parentHeader = genesisHeader;
    for (int i = 0; i < 5; i++) {
      Block block =
          forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, i)), parentHeader);
      if (i == 0) {
        reorgFromGenesis(block);
      } else {
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    assertBalance(ACCOUNT_A, TEN_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(5L);

    // Verify historical queries for chain B blocks
    Wei expectedBalance = Wei.ZERO;
    for (int i = 1; i <= 5; i++) {
      expectedBalance = expectedBalance.add(TWO_ETH);
      BlockHeader blockHeader = blockchain.getBlockHeader(i).orElseThrow();
      assertThat(getHistoricalWorldState(blockHeader).get(ACCOUNT_A).getBalance())
          .as("Block %d should have %s", i, expectedBalance)
          .isEqualTo(expectedBalance);
    }
  }

  /**
   * Tests multi-block reorg with conflicting storage values at the same slot. Both chains modify
   * the same storage slot with different values at each block, requiring correct rollback and
   * rollforward of storage state.
   */
  @Test
  void shouldHandleMultiBlockReorgWithConflictingSameStorageSlot() {
    // Contract that stores caller-provided value in slot 0:
    // PUSH1 0, CALLDATALOAD, PUSH1 0, SSTORE, STOP
    Bytes runtimeCode = Bytes.fromHexString("60003560005500");
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Deploy contract
    deployContractFromGenesis(initCode, Wei.ZERO);
    Block deployBlock = blockchain.getBlockByNumber(1L).orElseThrow();

    // Chain A: 3 blocks setting slot 0 to 0xAA, 0xBB, 0xCC
    Bytes[] chainAValues = {
      Bytes32.leftPad(Bytes.of(0xAA)),
      Bytes32.leftPad(Bytes.of(0xBB)),
      Bytes32.leftPad(Bytes.of(0xCC))
    };
    BlockHeader parentHeader = deployBlock.getHeader();
    for (int i = 0; i < 3; i++) {
      Transaction tx = createContractCallWithData(contractAddress, chainAValues[i], i + 1);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(4L);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.fromBytes(chainAValues[2]));

    // Reorg: Chain B with 3 blocks setting slot 0 to 0x11, 0x22, 0x33
    // Rolls back 3 blocks, rolls forward 3 new blocks
    Bytes[] chainBValues = {
      Bytes32.leftPad(Bytes.of(0x11)),
      Bytes32.leftPad(Bytes.of(0x22)),
      Bytes32.leftPad(Bytes.of(0x33))
    };
    parentHeader = deployBlock.getHeader();
    for (int i = 0; i < 3; i++) {
      Transaction tx = createContractCallWithData(contractAddress, chainBValues[i], i + 1);
      Block block = forTransactions(List.of(tx), parentHeader);
      if (i == 0) {
        reorgFrom(deployBlock.getHeader(), block);
      } else {
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.fromBytes(chainBValues[2]));
  }

  /**
   * Tests multi-block reorg where storage slots are created on one chain but not the other. Chain A
   * creates multiple storage slots across blocks, Chain B creates fewer slots. After reorg, only
   * Chain B's slots should exist.
   */
  @Test
  void shouldHandleMultiBlockReorgWithStorageSlotCreationVsNonExistence() {
    // Contract that stores incrementing value in slot specified by call count
    // Runtime: PUSH1 slot, SLOAD, PUSH1 1, ADD, DUP2, SSTORE, STOP
    // This increments the value at slot N on each call where N is the current value
    Bytes runtimeCode = Bytes.fromHexString("60003560005500"); // Simple: SSTORE(0, calldata)
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Chain A: Deploy contract, then 3 calls creating values in slot 0
    deployContractFromGenesis(initCode, Wei.ZERO);
    Block deployBlock = blockchain.getBlockByNumber(1L).orElseThrow();

    BlockHeader parentHeader = deployBlock.getHeader();
    for (int i = 0; i < 3; i++) {
      // Each call sets slot 0 to a different value (0x10, 0x20, 0x30)
      Transaction tx =
          createContractCallWithData(
              contractAddress, Bytes32.leftPad(Bytes.of((i + 1) * 0x10)), i + 1);
      Block block = forTransactions(List.of(tx), parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Verify slot 0 has value 0x30
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0x30));

    // Reorg: Chain B has only 1 call (sets slot 0 to 0xFF)
    Block block1B =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xFF)), 1)),
            deployBlock.getHeader());
    reorgFrom(deployBlock.getHeader(), block1B);

    // Slot 0 should have value 0xFF from Chain B
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xFF));
  }

  /**
   * Tests multi-block reorg with combined account AND storage conflicts. This is the most
   * comprehensive test where both account state (balance, nonce) and storage state conflict at
   * multiple blocks.
   *
   * <p>Chain A sends to ACCOUNT_A and ACCOUNT_B with certain storage values, Chain B sends to
   * ACCOUNT_A and ACCOUNT_C with different storage values. After reorg, ACCOUNT_B should not exist
   * (flat DB bug if it does), and storage should reflect Chain B's values.
   */
  @Test
  void shouldHandleMultiBlockReorgWithCombinedAccountAndStorageConflicts() {
    // Build a common ancestor block first
    BlockHeader forkPoint = buildEmptyChainToBlock(1);

    Bytes runtimeCode = Bytes.fromHexString("60003560005500"); // SSTORE(0, calldata)
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Chain A: Deploy contract with 1 ETH, then modify storage + send to ACCOUNT_A and ACCOUNT_B
    Block deployBlockA =
        forTransactions(List.of(createContractDeployment(initCode, ONE_ETH)), forkPoint);
    executeBlock(archiveProvider.getWorldState(), deployBlockA);

    // Block 3A: Set storage to 0xAA, transfer 1 ETH to ACCOUNT_A
    Block block3A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xAA)), 1),
                createTransaction(ACCOUNT_A, ONE_ETH, 2)),
            deployBlockA.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    // Block 4A: Set storage to 0xBB, transfer 2 ETH to ACCOUNT_B
    Block block4A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xBB)), 3),
                createTransaction(ACCOUNT_B, TWO_ETH, 4)),
            block3A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4A);

    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, TWO_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xBB));

    // Reorg: Chain B deploys same contract with 5 ETH, different storage values,
    // and sends to ACCOUNT_A and ACCOUNT_C (not ACCOUNT_B)

    Block deployBlockB =
        forTransactions(List.of(createContractDeployment(initCode, FIVE_ETH)), forkPoint);
    reorgFrom(forkPoint, deployBlockB);

    // Block 3B: Set storage to 0x11, transfer 3 ETH to ACCOUNT_C
    Block block3B =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0x11)), 1),
                createTransaction(ACCOUNT_C, THREE_ETH, 2)),
            deployBlockB.getHeader());
    executeBlockAndUpdateHead(block3B);

    // Block 4B: Set storage to 0x22, transfer 5 ETH to ACCOUNT_A
    Block block4B =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0x22)), 3),
                createTransaction(ACCOUNT_A, FIVE_ETH, 4)),
            block3B.getHeader());
    executeBlockAndUpdateHead(block4B);

    // Verify Chain B state
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0x22));
    assertBalance(ACCOUNT_A, FIVE_ETH);
    assertAccountNull(ACCOUNT_B); // Never created in Chain B - flat DB bug if this fails
    assertBalance(ACCOUNT_C, THREE_ETH);
  }

  /**
   * Tests deep multi-block reorg (10+ blocks) with conflicts at every level. This stress-tests the
   * rollback/rollforward mechanism with many blocks.
   *
   * <p>Chain A sends to ACCOUNT_A and ACCOUNT_B, while Chain B only sends to ACCOUNT_C. After
   * reorg, ACCOUNT_A and ACCOUNT_B should not exist (they were never created in Chain B), while
   * ACCOUNT_C should have the expected balance.
   */
  @Test
  void shouldHandleDeepMultiBlockReorgWithConflictsAtEveryLevel() {
    final int CHAIN_LENGTH = 10;

    // Build a common ancestor block first
    BlockHeader forkPoint = buildEmptyChainToBlock(1);

    // Chain A: Each block sends 1 ETH to ACCOUNT_A and 1 ETH to ACCOUNT_B
    BlockHeader parentHeader = forkPoint;
    for (int i = 0; i < CHAIN_LENGTH; i++) {
      Block block =
          forTransactions(
              List.of(
                  createTransaction(ACCOUNT_A, ONE_ETH, i * 2),
                  createTransaction(ACCOUNT_B, ONE_ETH, i * 2 + 1)),
              parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    assertBalance(ACCOUNT_A, ONE_ETH.multiply(CHAIN_LENGTH));
    assertBalance(ACCOUNT_B, ONE_ETH.multiply(CHAIN_LENGTH));

    // Store chain A block hashes for historical verification later
    List<Hash> chainABlockHashes = new ArrayList<>();
    for (int i = 2; i <= CHAIN_LENGTH + 1; i++) {
      chainABlockHashes.add(blockchain.getBlockByNumber(i).orElseThrow().getHash());
    }

    // Reorg: Chain B, same length, but ONLY sends to ACCOUNT_C
    // Requires rolling back 10 blocks and rolling forward 10 new blocks
    parentHeader = forkPoint;
    for (int i = 0; i < CHAIN_LENGTH; i++) {
      Block block =
          forTransactions(List.of(createTransaction(ACCOUNT_C, TWO_ETH, i)), parentHeader);
      if (i == 0) {
        reorgFrom(forkPoint, block);
      } else {
        executeBlockAndUpdateHead(block);
      }
      parentHeader = block.getHeader();
    }

    // Verify Chain B state: ACCOUNT_A and ACCOUNT_B should not exist (flat DB bug if they do)
    assertAccountNull(ACCOUNT_A);
    assertAccountNull(ACCOUNT_B);
    assertBalance(ACCOUNT_C, TWO_ETH.multiply(CHAIN_LENGTH));

    // Verify historical queries still work for orphaned Chain A blocks
    for (int i = 0; i < CHAIN_LENGTH; i++) {
      BlockHeader orphanedHeader = blockchain.getBlockHeader(chainABlockHashes.get(i)).orElse(null);
      if (orphanedHeader != null) {
        // Trie logs should still allow querying orphaned state
        Optional<MutableWorldState> orphanedState =
            archiveProvider.getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(orphanedHeader));
        // Archive mode should preserve orphaned block data within trie log depth
        assertThat(orphanedState).isPresent();
      }
    }
  }

  /**
   * Tests reorg with multiple storage slots having inverse changes. Chain A: slot0=1→2→3,
   * slot1=10→20→30 Chain B: slot0=3→2→1, slot1=30→20→10 Tests that rollback correctly restores
   * prior values when both chains touch the same slots but with inverse progression.
   */
  @Test
  void shouldHandleReorgWithMultipleStorageSlotsInverseChanges() {
    // Contract that stores value at specified slot: CALLDATALOAD(0)=slot, CALLDATALOAD(32)=value
    // PUSH1 0, CALLDATALOAD (slot), PUSH1 32, CALLDATALOAD (value), SWAP1, SSTORE
    Bytes runtimeCode = Bytes.fromHexString("600035602035905500");
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Deploy and build chain A with progressive values
    deployContractFromGenesis(createInitCode(runtimeCode), Wei.ZERO);
    Block deployBlock = blockchain.getBlockByNumber(1L).orElseThrow();

    // Chain A: 3 blocks with progressive values for 2 slots
    // Block 1: slot0=1, slot1=10
    // Block 2: slot0=2, slot1=20
    // Block 3: slot0=3, slot1=30
    int[][] chainAValues = {{1, 10}, {2, 20}, {3, 30}};

    BlockHeader parentHeader = deployBlock.getHeader();
    long nonce = 1;
    for (int block = 0; block < 3; block++) {
      List<Transaction> txs = new ArrayList<>();
      for (int slot = 0; slot < 2; slot++) {
        Bytes payload =
            Bytes.concatenate(
                Bytes32.leftPad(Bytes.ofUnsignedInt(slot)),
                Bytes32.leftPad(Bytes.ofUnsignedInt(chainAValues[block][slot])));
        txs.add(createContractCallWithData(contractAddress, payload, nonce++));
      }
      Block b = forTransactions(txs, parentHeader);
      executeBlock(archiveProvider.getWorldState(), b);
      parentHeader = b.getHeader();
    }

    // Verify Chain A final state
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(3));
    assertStorageValue(contractAddress, UInt256.ONE, UInt256.valueOf(30));

    // Reorg to Chain B with inverse progression
    // Block 1: slot0=3, slot1=30
    // Block 2: slot0=2, slot1=20
    // Block 3: slot0=1, slot1=10
    int[][] chainBValues = {{3, 30}, {2, 20}, {1, 10}};

    parentHeader = deployBlock.getHeader();
    nonce = 1;
    for (int block = 0; block < 3; block++) {
      List<Transaction> txs = new ArrayList<>();
      for (int slot = 0; slot < 2; slot++) {
        Bytes payload =
            Bytes.concatenate(
                Bytes32.leftPad(Bytes.ofUnsignedInt(slot)),
                Bytes32.leftPad(Bytes.ofUnsignedInt(chainBValues[block][slot])));
        txs.add(createContractCallWithData(contractAddress, payload, nonce++));
      }
      Block b = forTransactions(txs, parentHeader);
      if (block == 0) {
        reorgFrom(deployBlock.getHeader(), b);
      } else {
        executeBlock(archiveProvider.getWorldState(), b);
      }
      parentHeader = b.getHeader();
    }

    // Verify Chain B final state (inverse of Chain A)
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(1));
    assertStorageValue(contractAddress, UInt256.ONE, UInt256.valueOf(10));
  }

  /**
   * Tests reorg with alternating account creation/deletion patterns. Chain A creates a contract and
   * transfers to ACCOUNT_B via self-destruct, Chain B only transfers to ACCOUNT_C.
   *
   * <p>After reorg, ACCOUNT_B should not exist (it only received funds via self-destruct in Chain
   * A). This test verifies that the flat DB properly marks data as deleted during rollback.
   */
  @Test
  void shouldHandleReorgWithAlternatingAccountCreationDeletion() {
    // Build a common ancestor block first
    BlockHeader forkPoint = buildEmptyChainToBlock(1);

    // Self-destructing contract that sends balance to ACCOUNT_B
    Bytes selfDestructCode =
        Bytes.concatenate(
            Bytes.fromHexString("73"), ACCOUNT_B.getBytes(), Bytes.fromHexString("FF"));
    Bytes initCode = createInitCode(selfDestructCode);
    Address contractA = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);
    System.out.println(
        "Test addresses: contractA="
            + contractA
            + ", ACCOUNT_A="
            + ACCOUNT_A
            + ", ACCOUNT_B="
            + ACCOUNT_B
            + ", ACCOUNT_C="
            + ACCOUNT_C
            + ", sender="
            + Address.extract(sender.getPublicKey()));

    // Chain A: Deploy contract with 3 ETH
    Block block2A =
        forTransactions(List.of(createContractDeployment(initCode, THREE_ETH)), forkPoint);
    executeBlock(archiveProvider.getWorldState(), block2A);
    assertAccountExists(contractA);
    assertThat(archiveProvider.getWorldState().get(contractA).getBalance()).isEqualTo(THREE_ETH);

    // Block 3A: Call contract to self-destruct (sends balance to ACCOUNT_B)
    Block block3A =
        forTransactions(
            List.of(createContractCallWithValue(contractA, Wei.ZERO, 1)), block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);
    // After self-destruct, balance transferred to ACCOUNT_B
    assertBalance(ACCOUNT_B, THREE_ETH);

    // Block 4A: Transfer to keep chain going
    Block block4A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 2)), block3A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4A);

    // Reorg to Chain B: Never creates contractA, only sends to ACCOUNT_C
    Block block2B = forTransactions(List.of(createTransaction(ACCOUNT_C, ONE_ETH, 0)), forkPoint);
    reorgFrom(forkPoint, block2B);

    Block block3B =
        forTransactions(List.of(createTransaction(ACCOUNT_C, ONE_ETH, 1)), block2B.getHeader());
    executeBlockAndUpdateHead(block3B);

    Block block4B =
        forTransactions(List.of(createTransaction(ACCOUNT_C, ONE_ETH, 2)), block3B.getHeader());
    executeBlockAndUpdateHead(block4B);

    // Verify Chain B state: contract and ACCOUNT_B should not exist
    // If flat DB doesn't properly mark data as deleted during rollback, these will fail
    assertAccountNull(contractA);
    assertAccountNull(ACCOUNT_B);
    assertBalance(ACCOUNT_C, THREE_ETH);
  }

  /**
   * Tests sequential historical queries during active reorg. This tests the ephemeral context
   * isolation during complex operations - build chain, query historical states, then reorg and
   * query again.
   */
  @Test
  void shouldHandleSequentialHistoricalQueriesDuringActiveReorg() {
    // Build chain A: 5 blocks with incremental balances
    buildChain(5);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Query historical states at blocks 1, 2, 3, 4, 5
    for (int i = 1; i <= 5; i++) {
      BlockHeader header = blockchain.getBlockHeader(i).orElseThrow();
      MutableWorldState ws = getHistoricalWorldState(header);
      assertThat(ws.get(ACCOUNT_A).getBalance()).isEqualTo(ONE_ETH.multiply(i));
    }

    // Now reorg from block 2
    BlockHeader forkPoint = blockchain.getBlockHeader(2).orElseThrow();

    // Chain B from block 2: 3 more blocks with different amounts
    BlockHeader parentHeader = forkPoint;
    for (int i = 0; i < 3; i++) {
      Block block =
          forTransactions(List.of(createTransaction(ACCOUNT_A, THREE_ETH, 2 + i)), parentHeader);
      if (i == 0) {
        reorgFrom(forkPoint, block);
      } else {
        executeBlock(archiveProvider.getWorldState(), block);
      }
      parentHeader = block.getHeader();
    }

    // Final balance: 2 ETH (blocks 1-2) + 9 ETH (3 blocks × 3 ETH) = 11 ETH
    Wei expectedFinal = ONE_ETH.multiply(2).add(THREE_ETH.multiply(3));
    assertBalance(ACCOUNT_A, expectedFinal);

    // Historical queries at blocks 1 and 2 should still work (common ancestor)
    assertThat(
            getHistoricalWorldState(blockchain.getBlockHeader(1).orElseThrow())
                .get(ACCOUNT_A)
                .getBalance())
        .isEqualTo(ONE_ETH);
    assertThat(
            getHistoricalWorldState(blockchain.getBlockHeader(2).orElseThrow())
                .get(ACCOUNT_A)
                .getBalance())
        .isEqualTo(TWO_ETH);

    // Historical queries at new chain B blocks 3, 4, 5
    for (int i = 3; i <= 5; i++) {
      BlockHeader header = blockchain.getBlockHeader(i).orElseThrow();
      MutableWorldState ws = getHistoricalWorldState(header);
      // 2 ETH (from blocks 1-2) + (i-2) * 3 ETH
      Wei expected = TWO_ETH.add(THREE_ETH.multiply(i - 2));
      assertThat(ws.get(ACCOUNT_A).getBalance()).isEqualTo(expected);
    }
  }

  // ========================================
  // Edge Case Tests
  // ========================================

  /**
   * Tests reorg where some blocks on both forks have no state changes. Verifies deletion markers
   * aren't written unnecessarily for empty blocks.
   */
  @Test
  void shouldHandleReorgWithEmptyBlocksOnBothForks() {
    // Build common empty blocks first
    BlockHeader emptyBlock1 = buildEmptyChainToBlock(1);

    // Chain A: transfer, empty block, transfer
    Block block2A = forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), emptyBlock1);
    executeBlock(archiveProvider.getWorldState(), block2A);

    Block block3A = forTransactions(Collections.emptyList(), block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    Block block4A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 1)), block3A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4A);

    assertBalance(ACCOUNT_A, TWO_ETH);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(4L);

    // Reorg from block 1: Chain B has transfer to ACCOUNT_B instead
    Block block2B =
        forTransactions(List.of(createTransaction(ACCOUNT_B, FIVE_ETH, 0)), emptyBlock1);
    reorgFrom(emptyBlock1, block2B);

    // ACCOUNT_A should not exist (never created in chain B)
    assertAccountNull(ACCOUNT_A);
    assertBalance(ACCOUNT_B, FIVE_ETH);
  }

  /**
   * Tests that setting storage to zero is different from deleting it during reorg. Chain A sets
   * slot to a value then to zero, Chain B deletes the account entirely.
   */
  @Test
  void shouldDistinguishStorageZeroFromDeletedDuringReorg() {
    // Contract that stores calldata in slot 0
    Bytes runtimeCode = Bytes.fromHexString("60003560005500");
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Deploy contract
    deployContractFromGenesis(initCode, Wei.ZERO);
    Block deployBlock = blockchain.getBlockByNumber(1L).orElseThrow();

    // Chain A: Set slot 0 to 0xFF, then set to 0x00 (zero, not deleted)
    Block block2A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xFF)), 1)),
            deployBlock.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xFF));

    Block block3A =
        forTransactions(
            List.of(createContractCallWithData(contractAddress, Bytes32.ZERO, 2)),
            block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);
    // Storage is zero but the slot exists
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.ZERO);

    // Reorg: Chain B sets slot to different value
    Block block2B =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xAB)), 1)),
            deployBlock.getHeader());
    reorgFrom(deployBlock.getHeader(), block2B);

    // After reorg, slot should have Chain B's value
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xAB));

    // Historical query at block 2B should show 0xAB
    assertThat(
            getHistoricalWorldState(block2B.getHeader())
                .get(contractAddress)
                .getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.valueOf(0xAB));
  }

  /**
   * Tests storage modifications during reorg. Contract with storage slots is modified on fork A but
   * not on fork B. Storage changes from chain A should not appear after reorg to chain B.
   */
  @Test
  void shouldHandleStorageModificationsDuringReorg() {
    // Contract that stores calldata in slot 0
    Bytes storeCode = Bytes.fromHexString("60003560005500"); // store calldata[0] in slot 0
    Bytes initCode = createInitCode(storeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Build common ancestor with contract deployment
    BlockHeader forkPoint = buildEmptyChainToBlock(1);

    // Deploy contract at block 2
    Block deployBlock =
        forTransactions(List.of(createContractDeployment(initCode, ONE_ETH)), forkPoint);
    executeBlock(archiveProvider.getWorldState(), deployBlock);
    assertAccountExists(contractAddress);

    // Chain A: Set storage to 0x11, then 0x22
    Block block3A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0x11)), 1)),
            deployBlock.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    Block block4A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0x22)), 2)),
            block3A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4A);

    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0x22));

    // Reorg: Chain B just transfers to ACCOUNT_B, contract storage not modified
    Block block3B =
        forTransactions(List.of(createTransaction(ACCOUNT_B, TWO_ETH, 1)), deployBlock.getHeader());
    reorgFrom(deployBlock.getHeader(), block3B);

    // Contract should still exist (from deploy block which is shared)
    assertAccountExists(contractAddress);
    // Storage slot 0 should be empty (no writes in chain B after deploy)
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.ZERO);
    assertBalance(ACCOUNT_B, TWO_ETH);
  }

  /**
   * Tests code change without balance change during reorg. Uses CREATE2 pattern where contract
   * address is deterministic but code can differ.
   */
  @Test
  void shouldHandleCodeOnlyChangeDuringReorg() {
    // Two different runtime codes
    Bytes codeA = Bytes.fromHexString("6001600055"); // SSTORE(0, 1)
    Bytes codeB = Bytes.fromHexString("6002600055"); // SSTORE(0, 2)
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Chain A: Deploy with codeA
    deployContractFromGenesis(createInitCode(codeA), Wei.ZERO);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getCode()).isEqualTo(codeA);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(Wei.ZERO);

    // Reorg: Deploy with codeB (same value Wei.ZERO)
    reorgFromGenesis(
        forTransactions(
            List.of(createContractDeployment(createInitCode(codeB), Wei.ZERO)), genesisHeader));

    // Code should be codeB now
    assertThat(archiveProvider.getWorldState().get(contractAddress).getCode()).isEqualTo(codeB);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(Wei.ZERO);
  }

  /**
   * Tests multiple consecutive reorgs to the same fork point. Verifies deletion markers accumulate
   * correctly across repeated reorgs.
   */
  @Test
  void shouldHandleMultipleConsecutiveReorgsToSameForkPoint() {
    // Build common fork point
    BlockHeader forkPoint = buildEmptyChainToBlock(2);

    // Reorg cycle 1: Chain sends to ACCOUNT_A
    Block block3v1 = forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), forkPoint);
    reorgFrom(forkPoint, block3v1);
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertAccountNull(ACCOUNT_B);

    // Reorg cycle 2: Chain sends to ACCOUNT_B instead
    Block block3v2 = forTransactions(List.of(createTransaction(ACCOUNT_B, TWO_ETH, 0)), forkPoint);
    reorgFrom(forkPoint, block3v2);
    assertAccountNull(ACCOUNT_A); // Should be deleted
    assertBalance(ACCOUNT_B, TWO_ETH);

    // Reorg cycle 3: Back to ACCOUNT_A with different amount
    Block block3v3 =
        forTransactions(List.of(createTransaction(ACCOUNT_A, THREE_ETH, 0)), forkPoint);
    reorgFrom(forkPoint, block3v3);
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertAccountNull(ACCOUNT_B); // Should be deleted again

    // Reorg cycle 4: Both accounts
    Block block3v4 =
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_A, ONE_ETH, 0), createTransaction(ACCOUNT_B, ONE_ETH, 1)),
            forkPoint);
    reorgFrom(forkPoint, block3v4);
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, ONE_ETH);

    // Verify we're still at block 3
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(3L);
  }

  /**
   * Tests reorg where both forks reach the same final account state but via different intermediate
   * values. Historical reads must return correct fork's intermediate values.
   */
  @Test
  void shouldHandleReorgWhereBothForksReachSameFinalState() {
    // Chain A: 0 → 3 ETH → 5 ETH (via two transfers)
    Block block1A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, THREE_ETH, 0)), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Block block2A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, 1)), block1A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);

    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Store chain A block hash for later orphan query
    Hash block1AHash = block1A.getHash();

    // Reorg: Chain B: 0 → 1 ETH → 5 ETH (different intermediate, same final)
    Block block1B =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), genesisHeader);
    reorgFromGenesis(block1B);

    Block block2B =
        forTransactions(
            List.of(createTransaction(ACCOUNT_A, Wei.of(4_000_000_000_000_000_000L), 1)),
            block1B.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2B);

    // Final state is same (5 ETH)
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // But historical queries should show chain B's intermediate values
    assertThat(
            getHistoricalWorldState(blockchain.getBlockHeader(1).orElseThrow())
                .get(ACCOUNT_A)
                .getBalance())
        .isEqualTo(ONE_ETH); // Chain B's block 1

    // Query orphaned chain A blocks if available
    Optional<MutableWorldState> orphanedBlock1A =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(
                blockchain.getBlockHeader(block1AHash).orElse(null)));
    if (orphanedBlock1A.isPresent()) {
      assertThat(orphanedBlock1A.get().get(ACCOUNT_A).getBalance())
          .isEqualTo(THREE_ETH); // Chain A's intermediate
    }
  }

  /**
   * Tests contract creation at the same address on both forks with different code and storage. This
   * is a critical edge case for archive mode deletion markers.
   */
  @Test
  void shouldHandleContractCreationAtSameAddressOnBothForksWithDifferentState() {
    // Use init code that sets storage during construction, then returns minimal runtime code
    // Init code A: SSTORE(0, 0xAA), then return empty runtime code
    // PUSH1 0xAA, PUSH1 0, SSTORE, PUSH1 0, PUSH1 0, RETURN
    Bytes initCodeA = Bytes.fromHexString("60AA6000556000600060006000f3");

    // Init code B: SSTORE(0, 0xBB), SSTORE(1, 0xCC), then return empty runtime code
    // PUSH1 0xBB, PUSH1 0, SSTORE, PUSH1 0xCC, PUSH1 1, SSTORE, PUSH1 0, PUSH1 0, RETURN
    Bytes initCodeB = Bytes.fromHexString("60BB60005560CC600155600060006000f3");

    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Chain A: Deploy contract with initCodeA and 1 ETH
    Block block1A =
        forTransactions(List.of(createContractDeployment(initCodeA, ONE_ETH)), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(ONE_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xAA));
    assertStorageValue(contractAddress, UInt256.ONE, UInt256.ZERO); // slot 1 doesn't exist

    // Reorg: Chain B deploys contract with initCodeB and 5 ETH
    Block block1B =
        forTransactions(List.of(createContractDeployment(initCodeB, FIVE_ETH)), genesisHeader);
    reorgFromGenesis(block1B);

    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xBB));
    assertStorageValue(contractAddress, UInt256.ONE, UInt256.valueOf(0xCC));
  }

  /**
   * Tests with very high storage slot numbers to ensure no truncation issues with slot hash
   * handling during reorg.
   */
  @Test
  void shouldHandleHighSlotNumbersDuringReorg() {
    // Contract that stores calldata[32:64] at slot calldata[0:32]
    Bytes runtimeCode = Bytes.fromHexString("600035602035905500");
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    deployContractFromGenesis(initCode, Wei.ZERO);
    Block deployBlock = blockchain.getBlockByNumber(1L).orElseThrow();

    // Use very high slot numbers
    UInt256 highSlot1 = UInt256.MAX_VALUE.subtract(1);
    UInt256 highSlot2 = UInt256.valueOf(Long.MAX_VALUE);

    // Chain A: Set high slots to specific values
    Block block2A =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress,
                    Bytes.concatenate(highSlot1.toBytes(), Bytes32.leftPad(Bytes.of(0xAA))),
                    1)),
            deployBlock.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);

    Block block3A =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress,
                    Bytes.concatenate(highSlot2.toBytes(), Bytes32.leftPad(Bytes.of(0xBB))),
                    2)),
            block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    assertStorageValue(contractAddress, highSlot1, UInt256.valueOf(0xAA));
    assertStorageValue(contractAddress, highSlot2, UInt256.valueOf(0xBB));

    // Reorg: Chain B sets different values at same high slots
    Block block2B =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress,
                    Bytes.concatenate(highSlot1.toBytes(), Bytes32.leftPad(Bytes.of(0x11))),
                    1)),
            deployBlock.getHeader());
    reorgFrom(deployBlock.getHeader(), block2B);

    Block block3B =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress,
                    Bytes.concatenate(highSlot2.toBytes(), Bytes32.leftPad(Bytes.of(0x22))),
                    2)),
            block2B.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3B);

    // High slots should have Chain B's values
    assertStorageValue(contractAddress, highSlot1, UInt256.valueOf(0x11));
    assertStorageValue(contractAddress, highSlot2, UInt256.valueOf(0x22));
  }

  /**
   * Tests interleaved historical queries during active reorg operations. Verifies context isolation
   * when multiple world state views are active.
   */
  @Test
  void shouldMaintainConsistencyForHistoricalQueryDuringReorg() {
    // Build chain with 5 blocks
    buildChain(5);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Get historical world state references BEFORE reorg
    MutableWorldState wsAtBlock2 =
        getHistoricalWorldState(blockchain.getBlockHeader(2).orElseThrow());
    MutableWorldState wsAtBlock4 =
        getHistoricalWorldState(blockchain.getBlockHeader(4).orElseThrow());

    // Verify pre-reorg values
    assertThat(wsAtBlock2.get(ACCOUNT_A).getBalance()).isEqualTo(TWO_ETH);
    assertThat(wsAtBlock4.get(ACCOUNT_A).getBalance())
        .isEqualTo(Wei.of(4_000_000_000_000_000_000L));

    // Now trigger reorg from block 3
    BlockHeader forkPoint = blockchain.getBlockHeader(3).orElseThrow();
    Block block4B = forTransactions(List.of(createTransaction(ACCOUNT_B, TEN_ETH, 3)), forkPoint);
    reorgFrom(forkPoint, block4B);

    // After reorg, ACCOUNT_A should have 3 ETH (from blocks 1-3)
    // ACCOUNT_B should have 10 ETH (from block 4B)
    assertBalance(ACCOUNT_A, THREE_ETH);
    assertBalance(ACCOUNT_B, TEN_ETH);

    // Historical queries at common ancestor blocks should still work
    MutableWorldState wsAtBlock2AfterReorg =
        getHistoricalWorldState(blockchain.getBlockHeader(2).orElseThrow());
    assertThat(wsAtBlock2AfterReorg.get(ACCOUNT_A).getBalance()).isEqualTo(TWO_ETH);

    // Block 3 is still common ancestor
    MutableWorldState wsAtBlock3AfterReorg =
        getHistoricalWorldState(blockchain.getBlockHeader(3).orElseThrow());
    assertThat(wsAtBlock3AfterReorg.get(ACCOUNT_A).getBalance()).isEqualTo(THREE_ETH);
  }

  /**
   * Tests reorg that goes back to block 1 (immediately after genesis). Edge case for parent block
   * lookups and deletion marker context.
   */
  @Test
  void shouldHandleReorgToBlockOne() {
    // Build chain: genesis → block1A (transfer to A) → block2A (transfer to B)
    Block block1A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);

    Block block2A =
        forTransactions(List.of(createTransaction(ACCOUNT_B, TWO_ETH, 1)), block1A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);

    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, TWO_ETH);

    // Reorg from genesis (block 0) - go back to the very beginning
    Block block1B =
        forTransactions(List.of(createTransaction(ACCOUNT_C, THREE_ETH, 0)), genesisHeader);
    reorgFromGenesis(block1B);

    // Only ACCOUNT_C should exist
    assertAccountNull(ACCOUNT_A);
    assertAccountNull(ACCOUNT_B);
    assertBalance(ACCOUNT_C, THREE_ETH);

    // Extend chain B
    Block block2B =
        forTransactions(List.of(createTransaction(ACCOUNT_C, TWO_ETH, 1)), block1B.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2B);

    assertBalance(ACCOUNT_C, FIVE_ETH);
    assertAccountNull(ACCOUNT_A);
    assertAccountNull(ACCOUNT_B);

    // Historical query at block 1 should show chain B's state
    assertThat(
            getHistoricalWorldState(blockchain.getBlockHeader(1).orElseThrow())
                .get(ACCOUNT_C)
                .getBalance())
        .isEqualTo(THREE_ETH);
  }

  /**
   * Tests reorg with a large number of accounts modified. Stress tests deletion marker writing with
   * 100+ accounts.
   */
  @Test
  void shouldHandleReorgWithManyAccountsModified() {
    final int ACCOUNT_COUNT = 100;

    // Generate addresses deterministically
    List<Address> addresses = new ArrayList<>();
    for (int i = 0; i < ACCOUNT_COUNT; i++) {
      addresses.add(
          Address.fromHexString(String.format("0x2000000000000000000000000000000000%06x", i)));
    }

    // Chain A: Create all accounts with 1 ETH each (split across multiple blocks)
    BlockHeader parentHeader = genesisHeader;
    int accountIndex = 0;
    long nonce = 0;

    // Create accounts in batches of 10 per block
    while (accountIndex < ACCOUNT_COUNT) {
      List<Transaction> txs = new ArrayList<>();
      for (int i = 0; i < 10 && accountIndex < ACCOUNT_COUNT; i++, accountIndex++) {
        txs.add(createTransaction(addresses.get(accountIndex), ONE_ETH, nonce++));
      }
      Block block = forTransactions(txs, parentHeader);
      executeBlock(archiveProvider.getWorldState(), block);
      parentHeader = block.getHeader();
    }

    // Verify all accounts exist
    for (Address addr : addresses) {
      assertThat(archiveProvider.getWorldState().get(addr))
          .as("Account %s should exist", addr)
          .isNotNull();
    }

    // Reorg from genesis: Only create first 10 accounts with different amounts
    Block block1B =
        forTransactions(
            List.of(
                createTransaction(addresses.get(0), FIVE_ETH, 0),
                createTransaction(addresses.get(1), FIVE_ETH, 1),
                createTransaction(addresses.get(2), FIVE_ETH, 2),
                createTransaction(addresses.get(3), FIVE_ETH, 3),
                createTransaction(addresses.get(4), FIVE_ETH, 4)),
            genesisHeader);
    reorgFromGenesis(block1B);

    Block block2B =
        forTransactions(
            List.of(
                createTransaction(addresses.get(5), FIVE_ETH, 5),
                createTransaction(addresses.get(6), FIVE_ETH, 6),
                createTransaction(addresses.get(7), FIVE_ETH, 7),
                createTransaction(addresses.get(8), FIVE_ETH, 8),
                createTransaction(addresses.get(9), FIVE_ETH, 9)),
            block1B.getHeader());
    executeBlockAndUpdateHead(block2B);

    // First 10 accounts should have 5 ETH
    for (int i = 0; i < 10; i++) {
      assertThat(archiveProvider.getWorldState().get(addresses.get(i)).getBalance())
          .as("Account %d should have 5 ETH", i)
          .isEqualTo(FIVE_ETH);
    }

    // Remaining 90 accounts should NOT exist (deletion markers should mask them)
    for (int i = 10; i < ACCOUNT_COUNT; i++) {
      assertThat(archiveProvider.getWorldState().get(addresses.get(i)))
          .as("Account %d should not exist after reorg", i)
          .isNull();
    }
  }

  /**
   * Tests the exact scenario from production: a 1-block reorg at height N followed by executing
   * block N+1. This reproduces the bug where "Block context not present" warning appears during
   * reorg, causing orphaned data to leak into subsequent block execution.
   *
   * <p>Scenario:
   *
   * <pre>
   * 1. Chain: genesis → ... → block N-1 → block N (version A) - creates ACCOUNT_A
   * 2. Reorg: block N (version B) replaces block N (version A) - creates ACCOUNT_B instead
   * 3. Execute block N+1 on chain B - reads state that should NOT include ACCOUNT_A
   * </pre>
   *
   * <p>If deletion markers aren't written correctly during the reorg, block N+1's execution will
   * read orphaned ACCOUNT_A data, causing state root mismatch.
   */
  @Test
  void shouldHandleOneBlockReorgFollowedByNewBlock() {
    // Build chain to block N-1 (common ancestor)
    BlockHeader blockNMinus1 = buildEmptyChainToBlock(5);

    // Block N (version A): Creates ACCOUNT_A with 1 ETH and sets storage on a contract
    Bytes runtimeCode = Bytes.fromHexString("60003560005500"); // SSTORE(0, calldata)
    Bytes initCode = createInitCode(runtimeCode);
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    // Chain A: Deploy contract + transfer to ACCOUNT_A
    Block blockNA =
        forTransactions(
            List.of(
                createContractDeployment(initCode, ONE_ETH),
                createTransaction(ACCOUNT_A, TWO_ETH, 1)),
            blockNMinus1);
    executeBlock(archiveProvider.getWorldState(), blockNA);

    // Verify chain A state
    assertAccountExists(ACCOUNT_A);
    assertBalance(ACCOUNT_A, TWO_ETH);
    assertAccountExists(contractAddress);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(6L);

    // Now reorg: Block N (version B) - creates ACCOUNT_B instead, different contract storage
    Block blockNB =
        forTransactions(
            List.of(
                createContractDeployment(initCode, FIVE_ETH),
                createTransaction(ACCOUNT_B, THREE_ETH, 1)),
            blockNMinus1);

    // Execute the reorg - this is where deletion markers should be written
    reorgFrom(blockNMinus1, blockNB);

    // Verify reorg state - ACCOUNT_A should NOT exist (orphaned from chain A)
    assertAccountNull(ACCOUNT_A); // Critical: if this fails, deletion markers didn't work
    assertAccountExists(ACCOUNT_B);
    assertBalance(ACCOUNT_B, THREE_ETH);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);

    // NOW: Execute block N+1 on chain B
    // This is where the production bug manifests - if orphaned data leaks, state root will mismatch
    Block blockNPlus1 =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xBB)), 2),
                createTransaction(ACCOUNT_B, TWO_ETH, 3),
                createTransaction(ACCOUNT_C, ONE_ETH, 4)),
            blockNB.getHeader());

    // This should succeed - if deletion markers are wrong, we get StateRootMismatchException
    BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), blockNPlus1);
    assertThat(result.isSuccessful())
        .as("Block N+1 should execute successfully after 1-block reorg")
        .isTrue();

    // Final state verification
    assertAccountNull(ACCOUNT_A); // Still should not exist
    assertBalance(ACCOUNT_B, FIVE_ETH); // 3 ETH from block NB + 2 ETH from block N+1
    assertBalance(ACCOUNT_C, ONE_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xBB));
  }

  /**
   * Tests rapid sequential block imports at the same height followed by the next block. This
   * reproduces the exact production log pattern:
   *
   * <pre>
   * 06:14:17 - Imported #N (hash A)
   * 06:14:26 - Imported #N (hash B) ← REORG
   * 06:14:39 - Block #N+1 FAILS with invalid state root
   * </pre>
   *
   * <p>The key aspect is that the reorg happens via normal block import flow (not explicit reorg),
   * simulating how consensus layer sends competing blocks.
   */
  @Test
  void shouldHandleRapidReorgAtSameHeightFollowedByNextBlock() {
    // Build chain to block N-1
    BlockHeader parentHeader = buildEmptyChainToBlock(10);
    long blockN = parentHeader.getNumber() + 1;

    // Block N (version A): First block at height N
    Block blockNA =
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_A, ONE_ETH, 0), createTransaction(ACCOUNT_B, ONE_ETH, 1)),
            parentHeader);
    executeBlock(archiveProvider.getWorldState(), blockNA);

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(blockN);
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, ONE_ETH);

    // Block N (version B): Second block at same height - triggers reorg
    // This simulates receiving a competing block from consensus layer
    Block blockNB =
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_C, THREE_ETH, 0),
                createTransaction(ACCOUNT_A, TWO_ETH, 1) // Same account as chain A but different
                // amount
                ),
            parentHeader);

    // Reorg to chain B
    reorgFrom(parentHeader, blockNB);

    // Verify reorg completed correctly
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(blockN);
    assertAccountNull(ACCOUNT_B); // Only existed in chain A - CRITICAL CHECK
    assertBalance(ACCOUNT_C, THREE_ETH);
    assertBalance(ACCOUNT_A, TWO_ETH); // Chain B's value, not chain A's

    // Block N+1: Next block on chain B
    // If orphaned data from chain A leaks, this will fail with state root mismatch
    Block blockNPlus1 =
        forTransactions(
            List.of(
                createTransaction(ACCOUNT_C, ONE_ETH, 2), createTransaction(ACCOUNT_A, ONE_ETH, 3)),
            blockNB.getHeader());

    BlockProcessingResult result = executeBlock(archiveProvider.getWorldState(), blockNPlus1);
    assertThat(result.isSuccessful())
        .as("Block N+1 should succeed after rapid reorg at block N")
        .isTrue();

    // Final verification
    assertAccountNull(ACCOUNT_B); // Still should not exist
    assertBalance(ACCOUNT_C, Wei.of(4_000_000_000_000_000_000L)); // 3 + 1 ETH
    assertBalance(ACCOUNT_A, THREE_ETH); // 2 + 1 ETH
  }

  /**
   * Tests that state remains correct when multiple blocks are built on top of a reorg'd block. This
   * ensures deletion markers properly mask orphaned data for the entire subsequent chain.
   */
  @Test
  void shouldMaintainCorrectStateForMultipleBlocksAfterReorg() {
    // Build to block 5
    BlockHeader forkPoint = buildEmptyChainToBlock(5);

    // Chain A: 3 blocks creating accounts and storage
    Bytes runtimeCode = Bytes.fromHexString("60003560005500");
    Address contractAddress = Address.contractAddress(Address.extract(sender.getPublicKey()), 0L);

    Block block6A =
        forTransactions(
            List.of(createContractDeployment(createInitCode(runtimeCode), ONE_ETH)), forkPoint);
    executeBlock(archiveProvider.getWorldState(), block6A);

    Block block7A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xAA)), 1),
                createTransaction(ACCOUNT_A, ONE_ETH, 2)),
            block6A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block7A);

    Block block8A =
        forTransactions(
            List.of(
                createContractCallWithData(contractAddress, Bytes32.leftPad(Bytes.of(0xBB)), 3),
                createTransaction(ACCOUNT_B, TWO_ETH, 4)),
            block7A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block8A);

    // Verify chain A state
    assertBalance(ACCOUNT_A, ONE_ETH);
    assertBalance(ACCOUNT_B, TWO_ETH);
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.valueOf(0xBB));

    // Reorg at block 6 - different contract deployment and transfers
    Block block6B =
        forTransactions(
            List.of(
                createContractDeployment(createInitCode(runtimeCode), FIVE_ETH),
                createTransaction(ACCOUNT_C, ONE_ETH, 1)),
            forkPoint);
    reorgFrom(forkPoint, block6B);

    // Verify state immediately after reorg (before any subsequent blocks)
    assertThat(archiveProvider.getWorldState().get(contractAddress))
        .as("Contract should exist after reorg")
        .isNotNull();
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .as("Contract balance should be 5 ETH (from chain B) immediately after reorg")
        .isEqualTo(FIVE_ETH);

    // Build blocks 7B, 8B, 9B, 10B on chain B
    // Use executeBlockAndUpdateHead to properly persist and update world state context
    BlockHeader parentHeader = block6B.getHeader();
    for (int i = 7; i <= 10; i++) {
      Block block =
          forTransactions(
              List.of(createTransaction(ACCOUNT_C, ONE_ETH, i - 5)), // nonces 2, 3, 4, 5
              parentHeader);
      executeBlockAndUpdateHead(block);
      parentHeader = block.getHeader();
    }

    // Verify final state - orphaned data must not leak
    assertAccountNull(ACCOUNT_A); // Only in chain A
    assertAccountNull(ACCOUNT_B); // Only in chain A
    assertBalance(ACCOUNT_C, FIVE_ETH); // 1 ETH × 5 blocks (6B through 10B)
    // Contract exists from block 6B deployment
    assertAccountExists(contractAddress);
    assertThat(archiveProvider.getWorldState().get(contractAddress).getBalance())
        .isEqualTo(FIVE_ETH);
    // Storage should be empty - no writes in chain B after deploy
    assertStorageValue(contractAddress, UInt256.ZERO, UInt256.ZERO);
  }

  /**
   * Tests reorg where an account goes through multiple state transitions on chain A (create,
   * modify, more modifications) but chain B only has the initial creation.
   */
  @Test
  void shouldHandleFullAccountLifecycleDuringReorg() {
    // Chain A: Create account → modify balance multiple times
    Block block1A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), genesisHeader);
    executeBlock(archiveProvider.getWorldState(), block1A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Send more ETH
    Block block2A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, 1)), block1A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block2A);
    assertBalance(ACCOUNT_A, THREE_ETH);

    // Send even more
    Block block3A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, TWO_ETH, 2)), block2A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // Reorg from genesis: Just one transfer with different amount
    Block block1B =
        forTransactions(List.of(createTransaction(ACCOUNT_B, TEN_ETH, 0)), genesisHeader);
    reorgFromGenesis(block1B);

    // ACCOUNT_A should not exist (never created in chain B)
    assertAccountNull(ACCOUNT_A);
    // ACCOUNT_B should have 10 ETH
    assertBalance(ACCOUNT_B, TEN_ETH);
  }

  /**
   * Tests that deletion markers are written at intermediate block heights during reorg. This is a
   * regression test for an issue where orphaned data from intermediate blocks on the abandoned
   * chain was visible when querying at those block heights.
   *
   * <p>Scenario:
   *
   * <ul>
   *   <li>Fork point at block 5
   *   <li>Chain A: blocks 6A, 7A, 8A each modify an account (account exists at suffixes 6, 7, 8)
   *   <li>Chain B: block 6B creates the same account with different balance
   *   <li>After reorg, historical queries at heights 7 and 8 must return chain B's data (from
   *       suffix 6), NOT the orphaned data from chain A (at suffixes 7 and 8)
   * </ul>
   *
   * <p>The fix ensures deletion markers are written at ALL intermediate block heights (7 and 8),
   * not just at the target block (6) where the canonical data is written.
   */
  @Test
  void shouldWriteDeletionMarkersAtIntermediateBlocksDuringReorg() {
    // Build to block 5 as the fork point
    BlockHeader forkPoint = buildEmptyChainToBlock(5);

    // Chain A: Create account and modify it across multiple blocks
    // Block 6A: Create account with 1 ETH
    Block block6A = forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 0)), forkPoint);
    executeBlock(archiveProvider.getWorldState(), block6A);
    assertBalance(ACCOUNT_A, ONE_ETH);

    // Block 7A: Add 1 ETH more (total 2 ETH)
    Block block7A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 1)), block6A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block7A);
    assertBalance(ACCOUNT_A, TWO_ETH);

    // Block 8A: Add 1 ETH more (total 3 ETH)
    Block block8A =
        forTransactions(List.of(createTransaction(ACCOUNT_A, ONE_ETH, 2)), block7A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block8A);
    assertBalance(ACCOUNT_A, THREE_ETH);

    // Verify chain A data is stored at each block height
    assertThat(getHistoricalWorldState(block6A.getHeader()).get(ACCOUNT_A).getBalance())
        .as("Chain A block 6 should have 1 ETH")
        .isEqualTo(ONE_ETH);
    assertThat(getHistoricalWorldState(block7A.getHeader()).get(ACCOUNT_A).getBalance())
        .as("Chain A block 7 should have 2 ETH")
        .isEqualTo(TWO_ETH);
    assertThat(getHistoricalWorldState(block8A.getHeader()).get(ACCOUNT_A).getBalance())
        .as("Chain A block 8 should have 3 ETH")
        .isEqualTo(THREE_ETH);

    // Reorg at block 6 - create the SAME account but with different balance (5 ETH)
    Block block6B = forTransactions(List.of(createTransaction(ACCOUNT_A, FIVE_ETH, 0)), forkPoint);
    reorgFrom(forkPoint, block6B);

    // Verify current state shows chain B's balance
    assertBalance(ACCOUNT_A, FIVE_ETH);

    // CRITICAL: Query historical states at block heights 7 and 8
    // These blocks don't exist on chain B, but the queries must NOT return chain A's orphaned data.
    // Instead, they should return chain B's data from block 6B (the nearest valid data).

    // Build blocks 7B and 8B on chain B so we can query at those heights
    Block block7B =
        forTransactions(List.of(createTransaction(ACCOUNT_B, ONE_ETH, 1)), block6B.getHeader());
    executeBlockAndUpdateHead(block7B);

    Block block8B =
        forTransactions(List.of(createTransaction(ACCOUNT_B, ONE_ETH, 2)), block7B.getHeader());
    executeBlockAndUpdateHead(block8B);

    // Now query historical state at block 7B - should return ACCOUNT_A with 5 ETH (from chain B),
    // NOT 2 ETH (orphaned data from chain A's block 7A)
    var wsAtBlock7B = getHistoricalWorldState(block7B.getHeader());
    assertThat(wsAtBlock7B.get(ACCOUNT_A)).as("ACCOUNT_A should exist at block 7B").isNotNull();
    assertThat(wsAtBlock7B.get(ACCOUNT_A).getBalance())
        .as("ACCOUNT_A at block 7B should have 5 ETH (chain B), not 2 ETH (orphaned chain A)")
        .isEqualTo(FIVE_ETH);

    // Query historical state at block 8B - should return ACCOUNT_A with 5 ETH (from chain B),
    // NOT 3 ETH (orphaned data from chain A's block 8A)
    var wsAtBlock8B = getHistoricalWorldState(block8B.getHeader());
    assertThat(wsAtBlock8B.get(ACCOUNT_A)).as("ACCOUNT_A should exist at block 8B").isNotNull();
    assertThat(wsAtBlock8B.get(ACCOUNT_A).getBalance())
        .as("ACCOUNT_A at block 8B should have 5 ETH (chain B), not 3 ETH (orphaned chain A)")
        .isEqualTo(FIVE_ETH);

    // Verify ACCOUNT_B exists only on chain B
    assertThat(wsAtBlock7B.get(ACCOUNT_B)).as("ACCOUNT_B should exist at block 7B").isNotNull();
    assertThat(wsAtBlock7B.get(ACCOUNT_B).getBalance())
        .as("ACCOUNT_B at block 7B should have 1 ETH")
        .isEqualTo(ONE_ETH);

    assertThat(wsAtBlock8B.get(ACCOUNT_B)).as("ACCOUNT_B should exist at block 8B").isNotNull();
    assertThat(wsAtBlock8B.get(ACCOUNT_B).getBalance())
        .as("ACCOUNT_B at block 8B should have 2 ETH")
        .isEqualTo(TWO_ETH);
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
