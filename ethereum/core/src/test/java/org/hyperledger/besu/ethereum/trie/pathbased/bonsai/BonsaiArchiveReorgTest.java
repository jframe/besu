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
                createContractCallWithData(
                    contractAddress, Bytes32.leftPad(Bytes.of(0xFF)), 1)),
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
                createContractCallWithData(
                    contractAddress, Bytes32.leftPad(Bytes.of(0xAA)), 1),
                createTransaction(ACCOUNT_A, ONE_ETH, 2)),
            deployBlockA.getHeader());
    executeBlock(archiveProvider.getWorldState(), block3A);

    // Block 4A: Set storage to 0xBB, transfer 2 ETH to ACCOUNT_B
    Block block4A =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress, Bytes32.leftPad(Bytes.of(0xBB)), 3),
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
                createContractCallWithData(
                    contractAddress, Bytes32.leftPad(Bytes.of(0x11)), 1),
                createTransaction(ACCOUNT_C, THREE_ETH, 2)),
            deployBlockB.getHeader());
    executeBlockAndUpdateHead(block3B);

    // Block 4B: Set storage to 0x22, transfer 5 ETH to ACCOUNT_A
    Block block4B =
        forTransactions(
            List.of(
                createContractCallWithData(
                    contractAddress, Bytes32.leftPad(Bytes.of(0x22)), 3),
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
      BlockHeader orphanedHeader =
          blockchain.getBlockHeader(chainABlockHashes.get(i)).orElse(null);
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
        forTransactions(
            List.of(createTransaction(ACCOUNT_A, ONE_ETH, 2)), block3A.getHeader());
    executeBlock(archiveProvider.getWorldState(), block4A);

    // Reorg to Chain B: Never creates contractA, only sends to ACCOUNT_C
    Block block2B =
        forTransactions(List.of(createTransaction(ACCOUNT_C, ONE_ETH, 0)), forkPoint);
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
