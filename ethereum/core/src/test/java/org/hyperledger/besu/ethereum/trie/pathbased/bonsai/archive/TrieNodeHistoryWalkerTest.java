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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link TrieNodeHistoryWalker}.
 *
 * <p>Each test builds a real chain using the {@link BonsaiWorldState} fixture pattern: changes are
 * applied to a pre-compute world state to derive the correct state root, then the same changes are
 * applied to a source world state that also writes genuine trie logs. The walker is then run
 * against that source chain to populate {@code TRIE_NODE_HISTORY_ARCHIVE}.
 */
class TrieNodeHistoryWalkerTest {

  private static final Address ALT_ADDRESS =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final DataStorageConfiguration STORAGE_CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
  private static final long AWAIT_TIMEOUT_SECONDS = 10L;

  private final List<TrieNodeHistoryWalker> walkers = new ArrayList<>();
  private final List<ExecutorService> executors = new ArrayList<>();

  @BeforeEach
  void setUp() {}

  @AfterEach
  void tearDown() {
    walkers.forEach(
        w -> {
          try {
            w.close();
          } catch (final Exception ignored) {
            // ignore on teardown
          }
        });
    executors.forEach(ExecutorService::shutdownNow);
  }

  // -------------------------------------------------------------------------------------
  // Test 1: walker reconstructs history for every processed block
  // -------------------------------------------------------------------------------------

  @Test
  void walkerReconstructsEveryBlockItProcesses() {
    // 3 blocks, maxLayers=1 → target = 2; blocks 1 and 2 are processed
    final ChainFixture fixture = buildChain(3, 1L);
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, 1L);
    walker.start();

    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker.getWalkedBlockNumber()).isEqualTo(2L));

    // Root trie-node history entries must be present for blocks 1 and 2.
    final Bytes rootKey = ArchiveNodeKey.account(Bytes.EMPTY);
    assertThat(fixture.historyReader.nodeAt(rootKey, 1L)).isPresent();
    assertThat(fixture.historyReader.nodeAt(rootKey, 2L)).isPresent();

    // The root node bytes must hash to the correct state root at each block.
    final Hash stateRoot1 = fixture.headers[0].getStateRoot();
    final Hash stateRoot2 = fixture.headers[1].getStateRoot();
    assertThat(fixture.historyReader.nodeAt(rootKey, 1L).map(Hash::hash)).hasValue(stateRoot1);
    assertThat(fixture.historyReader.nodeAt(rootKey, 2L).map(Hash::hash)).hasValue(stateRoot2);

    // Block 3 is inside the reorg window and must not have been processed.
    assertThat(walker.getWalkedBlockNumber()).isLessThan(3L);
  }

  // -------------------------------------------------------------------------------------
  // Test 2: walker never touches blocks inside the reorg window
  // -------------------------------------------------------------------------------------

  @Test
  void walkerNeverProcessesBlocksInsideTheReorgWindow() {
    // 5 blocks, maxLayers=2 → target = 3; blocks 1-3 are processed, 4-5 are not
    final ChainFixture fixture = buildChain(5, 2L);
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, 2L);
    walker.start();

    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker.getWalkedBlockNumber()).isEqualTo(3L));

    // Boundary: block 3 is processed; block 4 (inside reorg window) is not.
    assertThat(walker.getWalkedBlockNumber())
        .isLessThanOrEqualTo(
            fixture.blockchain.getChainHeadBlockNumber()
                - fixture.trieLogManager.getMaxLayersToLoad());

    final Bytes rootKey = ArchiveNodeKey.account(Bytes.EMPTY);
    assertThat(fixture.historyReader.nodeAt(rootKey, 3L)).isPresent();
    // nodeAt uses getLatestBefore (seek-for-prev) semantics: nodeAt(key, 4) would return the
    // block-3 entry rather than empty.  Use exact point-lookups to verify that no entry was
    // *written at* blocks 4 or 5.
    assertThat(fixture.historyStore.get(rootKey, 4L)).isEmpty();
    assertThat(fixture.historyStore.get(rootKey, 5L)).isEmpty();
  }

  // -------------------------------------------------------------------------------------
  // Test 3: walker resumes from persisted progress after a simulated restart
  // -------------------------------------------------------------------------------------

  @Test
  void walkerResumesFromPersistedProgressAfterRestart() {
    // Build 3 blocks, maxLayers=1 → target=2; first walker processes blocks 1-2.
    final ChainFixture fixture = buildChain(3, 1L);
    final TrieNodeHistoryWalker walker1 = createAndRegisterWalker(fixture, 1L);
    walker1.start();

    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker1.getWalkedBlockNumber()).isEqualTo(2L));
    // Simulate node restart: close the walker (deregisters blockchain observer).
    closeQuietly(walker1);

    // Extend the chain: append blocks 4 and 5 so target becomes 4.
    fixture.appendBlock(); // block 4
    fixture.appendBlock(); // block 5, head=5, target=4

    // Second walker loads persisted progress (lastIndexedBlock=2) and resumes from block 3.
    final TrieNodeHistoryProgress resumedProgress =
        TrieNodeHistoryProgress.load(fixture.composedStorage);
    assertThat(resumedProgress.lastIndexedBlock()).isEqualTo(2L);

    final TrieNodeHistoryWalker walker2 = createAndRegisterWalker(fixture, 1L);
    walker2.start();

    // Walker2 must reach block 4 (the new target after head=5, maxLayers=1).
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker2.getWalkedBlockNumber()).isEqualTo(4L));

    // Verify history is complete for all processed blocks 1-4.
    final Bytes rootKey = ArchiveNodeKey.account(Bytes.EMPTY);
    for (int i = 0; i < 4; i++) {
      assertThat(fixture.historyReader.nodeAt(rootKey, i + 1L)).isPresent();
    }
  }

  // -------------------------------------------------------------------------------------
  // Test 4: walker halts on a state-root mismatch and does not continue
  // -------------------------------------------------------------------------------------

  @Test
  void walkerHaltsOnStateRootMismatch() {
    // Build 2 normal blocks with maxLayers=0 so that ALL blocks are within the target.
    final ChainFixture fixture = buildChain(2, 0L);

    // Build block 3 with a deliberately wrong state root (Hash.ZERO).
    final BlockHeader badHeader3 =
        new BlockHeaderTestFixture()
            .number(3L)
            .parentHash(fixture.headers[1].getHash())
            .stateRoot(Hash.ZERO) // intentionally wrong
            .buildHeader();

    // Construct a trie log for block 3 that creates a new account.
    // When the walker replays this via rollForward + persist(badHeader3), the computed
    // state root will be non-zero → StateRootMismatchException.
    final PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(0L, Wei.of(42L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final TrieLogLayer badTrieLog = new TrieLogLayer();
    badTrieLog.addAccountChange(ALT_ADDRESS, null, newAccountValue);
    // BonsaiTrieLogFactory.writeTo reads getBlockHash(); set it before serializing.
    badTrieLog.setBlockHash(badHeader3.getHash());

    // Serialize and inject the trie log into the source storage.
    final byte[] serialized = new BonsaiTrieLogFactory().serialize(badTrieLog);
    final KeyValueStorageTransaction logTx =
        fixture.sourceStorage.getTrieLogStorage().startTransaction();
    logTx.put(badHeader3.getHash().getBytes().toArrayUnsafe(), serialized);
    logTx.commit();

    // Append the bad block to the blockchain.
    fixture.blockchain.appendBlock(new Block(badHeader3, BlockBody.empty()), List.of());

    // Walker processes blocks 1-2 successfully, then encounters block 3 and halts.
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, 0L);
    walker.start();

    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker.halted.get()).isTrue());

    // Walker must not have advanced past block 2.
    assertThat(walker.getWalkedBlockNumber()).isLessThan(3L);

    // No history entry for block 3 must have been written.
    // Use an exact point-lookup: nodeAt(key, 3) uses getLatestBefore semantics and would return
    // the block-2 entry rather than empty, giving a false failure.
    final Bytes rootKey = ArchiveNodeKey.account(Bytes.EMPTY);
    assertThat(fixture.historyStore.get(rootKey, 3L)).isEmpty();
  }

  // =====================================================================================
  // Helpers
  // =====================================================================================

  private TrieNodeHistoryWalker createAndRegisterWalker(
      final ChainFixture fixture, final long maxLayersForWalker) {
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), STORAGE_CONFIG);
    flatDbStrategyProvider.loadFlatDbStrategy(fixture.composedStorage);

    final TrieNodeHistoryWalkerWorldState walkerWorldState =
        new TrieNodeHistoryWalkerWorldState(
            flatDbStrategyProvider,
            fixture.composedStorage,
            fixture.historyReader,
            fixture.historyStore);

    final TrieNodeHistoryProgress progress = TrieNodeHistoryProgress.load(fixture.composedStorage);

    // Walker's realTrieLogManager reads trie logs from sourceStorage with the test maxLayers.
    final TrieLogManager walkerTrieLogManager =
        new TrieLogManager(fixture.blockchain, fixture.sourceStorage, maxLayersForWalker, null);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executors.add(executor);

    // null GenesisState is safe: all test fixtures use Hash.EMPTY_TRIE_HASH genesis.
    final TrieNodeHistoryWalker walker =
        new TrieNodeHistoryWalker(
            walkerWorldState,
            walkerTrieLogManager,
            fixture.blockchain,
            progress,
            fixture.composedStorage,
            executor,
            /* genesisState= */ null);
    walkers.add(walker);
    return walker;
  }

  private static void closeQuietly(final TrieNodeHistoryWalker walker) {
    try {
      walker.close();
    } catch (final Exception ignored) {
      // ignore — already tracked for teardown
    }
  }

  // =====================================================================================
  // Chain fixture
  // =====================================================================================

  /**
   * Builds a chain of {@code numBlocks} blocks backed by real in-memory bonsai world state storage,
   * producing genuine trie logs that the walker can replay.
   *
   * <p>Two world states share the same change sequence:
   *
   * <ul>
   *   <li>A pre-compute world state (throwaway storage, {@link NoOpTrieLogManager}) used only to
   *       derive the correct state root via {@code persist(null)}.
   *   <li>A source world state (fixture storage, real {@link TrieLogManager}) that writes the
   *       canonical trie logs via {@code persist(header)}.
   * </ul>
   *
   * The {@code maxLayersForWalker} parameter is set on the fixture's TrieLogManager so tests can
   * share the same source TrieLogManager for walker creation.
   */
  private ChainFixture buildChain(final int numBlocks, final long maxLayersForSource) {
    // -- pre-compute world state (throwaway) ------------------------------------------
    final BonsaiWorldStateKeyValueStorage preComputeStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), STORAGE_CONFIG);
    final PathBasedCodeCache preComputeCodeCache = new PathBasedCodeCache();
    final WorldStateConfig wsConfig =
        WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
            .parallelStateRootComputationEnabled(false)
            .build();
    final BonsaiWorldState preComputeWorldState =
        new BonsaiWorldState(
            preComputeStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                preComputeStorage, EvmConfiguration.DEFAULT, preComputeCodeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            wsConfig,
            preComputeCodeCache);

    // -- source world state (real trie logs) ------------------------------------------
    final ChainFixture fixture = new ChainFixture();
    fixture.sourceStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), STORAGE_CONFIG);
    fixture.composedStorage = fixture.sourceStorage.getComposedWorldStateStorage();
    fixture.historyStore = new TrieNodeHistoryStore(fixture.composedStorage);
    fixture.historyReader = new TrieNodeHistoryReader(fixture.historyStore);

    // Genesis block (empty account trie → Hash.EMPTY_TRIE_HASH as state root).
    final BlockHeader genesisHeader =
        new BlockHeaderTestFixture().number(0L).stateRoot(Hash.EMPTY_TRIE_HASH).buildHeader();
    final Block genesisBlock = new Block(genesisHeader, BlockBody.empty());
    fixture.blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisBlock);

    fixture.trieLogManager =
        new TrieLogManager(fixture.blockchain, fixture.sourceStorage, maxLayersForSource, null);

    final PathBasedCodeCache srcCodeCache = new PathBasedCodeCache();
    fixture.sourceWorldState =
        new BonsaiWorldState(
            fixture.sourceStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                fixture.sourceStorage, EvmConfiguration.DEFAULT, srcCodeCache),
            fixture.trieLogManager,
            EvmConfiguration.DEFAULT,
            wsConfig,
            srcCodeCache);
    fixture.preComputeWorldState = preComputeWorldState;
    fixture.headers = new BlockHeader[numBlocks];
    fixture.lastParentHash = genesisHeader.getHash();

    for (int i = 0; i < numBlocks; i++) {
      fixture.appendBlockInternal(i + 1);
    }
    return fixture;
  }

  /** Mutable fixture that owns the source and pre-compute world states and the blockchain. */
  private static class ChainFixture {
    MutableBlockchain blockchain;
    BonsaiWorldStateKeyValueStorage sourceStorage;
    TrieLogManager trieLogManager;
    SegmentedKeyValueStorage composedStorage;
    TrieNodeHistoryStore historyStore;
    TrieNodeHistoryReader historyReader;
    BlockHeader[] headers; // 0-indexed: headers[i] = header for block i+1
    BonsaiWorldState sourceWorldState;
    BonsaiWorldState preComputeWorldState;
    Hash lastParentHash;
    int blockCount = 0;

    /**
     * Appends the next block (blockCount+1) to the chain, writes a real trie log to source storage,
     * and records the header.
     */
    void appendBlock() {
      // Expand headers array if needed.
      if (blockCount >= headers.length) {
        final BlockHeader[] grown = new BlockHeader[blockCount + 4];
        System.arraycopy(headers, 0, grown, 0, headers.length);
        headers = grown;
      }
      appendBlockInternal(blockCount + 1);
    }

    private void appendBlockInternal(final int blockNum) {
      // 1. Pre-compute: apply changes, derive state root via persist(null).
      applyBlockChanges(preComputeWorldState.updater(), blockNum);
      preComputeWorldState.persist(null);
      final Hash stateRoot = preComputeWorldState.rootHash();

      // 2. Build canonical header using the derived state root.
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(blockNum)
              .parentHash(lastParentHash)
              .stateRoot(stateRoot)
              .buildHeader();

      // 3. Source world state: apply same changes, persist with the real header (writes trie log).
      applyBlockChanges(sourceWorldState.updater(), blockNum);
      sourceWorldState.persist(header);

      // 4. Append to blockchain (does NOT validate the state root).
      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());

      // 5. Track state.
      if (blockNum - 1 < headers.length) {
        headers[blockNum - 1] = header;
      }
      lastParentHash = header.getHash();
      blockCount = blockNum;
    }

    /**
     * Applies deterministic account changes for the given block number to {@code updater}. Commits
     * the updater before returning.
     */
    private static void applyBlockChanges(final WorldUpdater updater, final int blockNum) {
      final MutableAccount account = updater.getOrCreate(TEST_ADDRESS);
      account.setBalance(Wei.of((long) blockNum * 100));
      updater.commit();
    }

    private static final Address TEST_ADDRESS =
        Address.fromHexString("0x1111111111111111111111111111111111111111");
  }
}
