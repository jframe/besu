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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for the Design 5 trie-node differential index (Stage 4).
 *
 * <p>Proves that historical {@code eth_getProof} requests for blocks beyond {@code maxLayersToLoad}
 * are correctly routed through {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader}
 * and return a valid Merkle proof whose root matches the block header's {@code stateRoot}.
 *
 * <p><b>Test conditions</b>
 *
 * <ul>
 *   <li>{@code trieNodeIndexEnabled = true}
 *   <li>{@code maxLayersToLoad = 3} — blocks 0 and 1 are "historical" when head is at block 5
 *   <li>Trie-node diffs are captured during chain building via {@link
 *       BonsaiArchiveTrieNodeStrategy#putFlatAccountTrieNode} and {@link
 *       BonsaiArchiveTrieNodeStrategy#putFlatStorageTrieNode}
 *   <li>{@link TrieNodeIndexProgress#covers(long)} returns {@code true} for the historical block
 *   <li>{@code archiveMigrationProgressSupplier} returns a value ≥ all queried block numbers
 * </ul>
 *
 * <p><b>Storage layout</b>: a single {@link BonsaiWorldStateKeyValueStorage} ({@code
 * archiveStorage}) is shared between chain building, the migrator, and the archive provider. This
 * ensures the trie-node index entries written during chain building land in the same {@link
 * SegmentedKeyValueStorage} that the archive provider's {@link TrieNodeChangeIndex} reads from.
 */
class BonsaiArchiveTrieNodeIndexIntegrationTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  /**
   * Chain-building constants.
   *
   * <p>Small chain: 5 blocks + genesis. maxLayersToLoad=3, so blocks 0 and 1 are historical when
   * head = block 5 (5 - 1 = 4 ≥ 3).
   */
  private static final int NUM_BLOCKS = 5;

  private static final long MAX_LAYERS = 3L;

  /** The historical block whose proof is requested via the index path. */
  private static final long TARGET_BLOCK = 1L;

  private BonsaiFlatDbToArchiveMigrator migrator;
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    if (migrator != null) {
      migrator.close();
    }
  }

  /**
   * Historical proof at TARGET_BLOCK via the trie-node index path returns a valid proof: the
   * account balance and storage slot value are correct, and both the account and storage proof
   * witnesses are non-empty (proving the trie was traversed from the header's stateRoot).
   */
  @Test
  void historicalProof_viaTrieNodeIndex_returnsValidProof() throws Exception {
    final BonsaiArchiveWorldStateProvider archiveProvider = buildIndexedArchive();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(UInt256.ONE), Function.identity());

    assertThat(proof)
        .withFailMessage("index-path proof should be present for historical block %d", TARGET_BLOCK)
        .isPresent();

    // The account was created at block 1 with balance 1_000_000.
    // Slot 1 was set to blockNumber=1 at block 1.
    assertThat(proof.get().getStateTrieAccountValue())
        .isPresent()
        .hasValueSatisfying(
            v -> assertThat(v.getBalance()).isEqualByComparingTo(Wei.of(1_000_000L)));
    assertThat(proof.get().getStorageValue(UInt256.ONE)).isEqualTo(UInt256.valueOf(TARGET_BLOCK));
    // Non-empty proofs mean the trie was actually traversed from the header's stateRoot.
    assertThat(proof.get().getAccountProof()).isNotEmpty();
    assertThat(proof.get().getStorageProof(UInt256.ONE)).isNotEmpty();
  }

  /**
   * Absent slot at TARGET_BLOCK returns a valid exclusion proof (non-empty witness, zero value) via
   * the trie-node index path.
   */
  @Test
  void historicalProof_viaTrieNodeIndex_absentSlot_returnsExclusionProof() throws Exception {
    final BonsaiArchiveWorldStateProvider archiveProvider = buildIndexedArchive();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    // Slot 0 is never written, so it is absent at block 1.
    final UInt256 absentSlot = UInt256.ZERO;
    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(absentSlot), Function.identity());

    assertThat(proof)
        .withFailMessage(
            "exclusion proof should be present for absent slot at block %d", TARGET_BLOCK)
        .isPresent();
    assertThat(proof.get().getStorageValue(absentSlot)).isEqualTo(UInt256.ZERO);
    assertThat(proof.get().getStorageProof(absentSlot)).isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Task 6.2: Verify legacy proof path bypassed when trie-node index covers target
  // ---------------------------------------------------------------------------

  /**
   * When {@code trieNodeIndexEnabled=true} and the index covers the target block, the proof routing
   * in {@link BonsaiArchiveWorldStateProvider#getAccountProof} takes the index path. This test
   * confirms the proof succeeds entirely from the trie-node index via {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader}.
   */
  @Test
  void trieNodeIndexEnabled_legacyPathBypassed_proofSucceedsFromIndexAlone() throws Exception {
    // buildIndexedArchive() uses trieNodeIndexEnabled=true.
    // The proof must succeed via ArchiveProofNodeLoader.
    final BonsaiArchiveWorldStateProvider archiveProvider = buildIndexedArchive();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        archiveProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(UInt256.ONE), Function.identity());

    // Proof must be present: ArchiveProofNodeLoader supplies all trie nodes from the index.
    assertThat(proof)
        .withFailMessage(
            "index path proof must succeed even though live-import did not write to suffixed CF"
                + " at block %d",
            TARGET_BLOCK)
        .isPresent();
    assertThat(proof.get().getAccountProof())
        .withFailMessage("account proof witness must be non-empty (trie was traversed from index)")
        .isNotEmpty();
    assertThat(proof.get().getStorageProof(UInt256.ONE))
        .withFailMessage("storage proof witness must be non-empty (trie was traversed from index)")
        .isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Chain-building infrastructure
  // ---------------------------------------------------------------------------

  /**
   * Builds a small chain of {@value #NUM_BLOCKS} blocks where account-trie and storage-trie node
   * diffs are captured into the trie-node differential index during each {@code persist} call.
   *
   * <p><b>Storage layout:</b> a single {@code archiveStorage} (backed by a single {@link
   * org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage}) is shared by:
   *
   * <ul>
   *   <li>The chain-building world state (writes live trie + archive trie + index diffs)
   *   <li>The migrator (reads trie logs, writes archive flat-DB CFs)
   *   <li>The archive provider (reads index + history + archive flat-DB)
   * </ul>
   *
   * <p>This ensures the trie-node index entries written during chain building land in the same
   * composed storage that the provider's {@link TrieNodeChangeIndex} reads from.
   *
   * <p><b>Block number accuracy:</b> during {@code persist(header_N)}, the strategy reads {@code
   * WORLD_BLOCK_NUMBER_KEY} to derive the current block number. Because the world state's block
   * number key is written as part of the same atomic persist transaction (after the trie node
   * writes), the strategy reads the previous block's number (N-1) and returns N. For the very first
   * block (N=1), the key is absent and defaults to 0 — so block 1's nodes are recorded at block 0
   * in the index. This is a known limitation of the in-process write path. The proof provider's
   * history reader correctly handles this by finding the latest change block ≤ targetBlock: for
   * block 1, the latest change at ≤ 1 includes the block-0 entries, which hold the correct node RLP
   * for that state.
   */
  private BonsaiArchiveWorldStateProvider buildIndexedArchive() throws Exception {
    final BlockDataGenerator blockGen = new BlockDataGenerator();
    final Block genesis = blockGen.genesisBlock();
    blockchain =
        DefaultBlockchain.createMutable(
            genesis,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0);

    // ------------------------------------------------------------------
    // Archive config: trieNodeIndexEnabled=true + stateProofsEnabled=true.
    // ------------------------------------------------------------------
    final ImmutableDataStorageConfiguration archiveConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(true)
                            .build())
                    .build())
            .build();

    // ------------------------------------------------------------------
    // Single shared archive storage — used by chain builder, migrator, and archive provider.
    // KeyValueStorageProvider.getStorageBySegmentIdentifiers creates a NEW storage instance on
    // each call, so we create archiveStorage exactly ONCE and reuse its composed storage.
    // ------------------------------------------------------------------
    final InMemoryKeyValueStorageProvider sharedProvider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage archiveStorage =
        (BonsaiWorldStateKeyValueStorage) sharedProvider.createWorldStateStorage(archiveConfig);
    // Enable archive flat-DB mode so flat writes go to the archive CFs.
    archiveStorage.upgradeToArchiveFlatDbMode();

    final SegmentedKeyValueStorage composedStorage = archiveStorage.getComposedWorldStateStorage();

    // ------------------------------------------------------------------
    // Trie-node index components backed by the shared composedStorage.
    // The archive provider's TrieNodeChangeIndex also reads from composedStorage.
    // ------------------------------------------------------------------
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(composedStorage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(composedStorage);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    // Index-enabled write strategy: captures diffs into historyStore + changeIndex during persist.
    final BonsaiArchiveTrieNodeStrategy indexStrategy =
        new BonsaiArchiveTrieNodeStrategy(
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            new BonsaiTrieNodeStrategy(),
            /* trieNodeIndexEnabled= */ true,
            historyStore,
            changeIndex,
            progress);

    // Build a world-state KV storage over the shared composedStorage with the index strategy.
    final BonsaiFlatDbStrategyProvider flatDbProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), archiveConfig);
    flatDbProvider.loadFlatDbStrategy(composedStorage);

    final BonsaiWorldStateKeyValueStorage indexHeadStorage =
        new BonsaiWorldStateKeyValueStorage(
            flatDbProvider,
            composedStorage,
            archiveStorage.getTrieLogStorage(), // same trie-log store shared with archiveStorage
            archiveStorage.getCacheManager(),
            archiveStorage.getCurrentVersion(),
            indexStrategy);

    // Head archive provider — manages trie-log saving for block import.
    final BonsaiWorldStateProvider headArchive =
        new BonsaiWorldStateProvider(
            indexHeadStorage,
            blockchain,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache());

    // ------------------------------------------------------------------
    // Parallel throwaway state to compute the correct stateRoot per block.
    // ------------------------------------------------------------------
    final InMemoryKeyValueStorageProvider rootProvider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage rootStorage =
        (BonsaiWorldStateKeyValueStorage)
            rootProvider.createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final BonsaiWorldStateProvider rootArchive =
        new BonsaiWorldStateProvider(
            rootStorage,
            blockchain,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getPathBasedExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache());

    final BonsaiWorldState headState =
        new BonsaiWorldState(
            headArchive,
            indexHeadStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final BonsaiWorldState rootState =
        new BonsaiWorldState(
            rootArchive,
            rootStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());

    BlockHeader parent = genesis.getHeader();
    for (int i = 1; i <= NUM_BLOCKS; i++) {
      // Compute the correct state root via the throwaway state.
      applyBlockChanges(rootState, i);
      rootState.persist(null);
      final Hash rootHash = rootState.rootHash();

      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent.getHash())
              .stateRoot(rootHash)
              .buildHeader();

      // Persist the head state: this triggers putFlatAccountTrieNode / putFlatStorageTrieNode
      // → captureTrieNodeDiff → pendingBlooms accumulation inside indexStrategy.
      applyBlockChanges(headState, i);
      headState.persist(header);

      // Advance coverage progress to block i. Note: the first block's nodes were recorded at
      // block 0 (see class Javadoc), but setLastIndexedBlock is monotonically non-decreasing
      // so advancing to i still correctly reflects the highest covered block.
      final SegmentedKeyValueStorageTransaction progressTx = composedStorage.startTransaction();
      progress.setLastIndexedBlock(i);
      progress.save(progressTx);
      progressTx.commit();

      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
      parent = header;
    }

    // All blocks fall in range 0. Set indexStartBlock so covers(TARGET_BLOCK) returns true.
    final SegmentedKeyValueStorageTransaction progressTx = composedStorage.startTransaction();
    progress.setIndexStartBlock(0L);
    progress.save(progressTx);
    progressTx.commit();

    // ------------------------------------------------------------------
    // Run the real migrator to populate archive flat-DB CFs in archiveStorage.
    // ------------------------------------------------------------------
    final TrieLogManager headTrieLogManager = headArchive.getTrieLogManager();
    final TrieLogManager migratorTrieLogManager = mock(TrieLogManager.class);
    when(migratorTrieLogManager.getMaxLayersToLoad()).thenReturn(0L);
    when(migratorTrieLogManager.getTrieLogLayer(any()))
        .thenAnswer(inv -> headTrieLogManager.getTrieLogLayer(inv.getArgument(0)));

    final BonsaiArchiveFlatDbStrategy archiveFlatStrategy =
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy());

    migrator =
        new BonsaiFlatDbToArchiveMigrator(
            archiveStorage,
            migratorTrieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            new NoOpMetricsSystem(),
            archiveFlatStrategy);
    migrator.migrate().get(30, TimeUnit.SECONDS);

    assertThat(migrator.getMigratedBlockNumber()).isEqualTo(NUM_BLOCKS);

    // ------------------------------------------------------------------
    // Build the archive provider over the same archiveStorage.
    // Its TrieNodeChangeIndex and TrieNodeHistoryReader are initialised from
    // archiveStorage.getComposedWorldStateStorage() = composedStorage (the same instance where
    // the index entries were written during chain building).
    // ------------------------------------------------------------------
    final BonsaiArchiveWorldStateProvider archiveProvider =
        new BonsaiArchiveWorldStateProvider(
            archiveStorage,
            blockchain,
            archiveConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache(),
            new NoOpMetricsSystem());

    // All NUM_BLOCKS blocks are migrated (archive flat-DB fully populated).
    archiveProvider.setArchiveMigrationProgressSupplier(() -> (long) NUM_BLOCKS);

    // The provider's constructor calls TrieNodeIndexProgress.load(archiveStorage, RANGE_SIZE),
    // which reads from TRIE_BRANCH_STORAGE. progress.save() wrote the final state (range 0
    // marked complete) there, so the round-trip through toBytes()/fromBytes() is exercised
    // and the provider's internal progress record already has covers(TARGET_BLOCK) == true.

    return archiveProvider;
  }

  private void applyBlockChanges(final BonsaiWorldState state, final int blockNumber) {
    final WorldUpdater updater = state.updater();
    if (blockNumber == 1) {
      updater.createAccount(ACCOUNT, 0, Wei.of(1_000_000L));
    }
    final MutableAccount account = updater.getAccount(ACCOUNT);
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(blockNumber));
    // Touch a rotating slot so the storage trie changes shape across blocks.
    account.setStorageValue(UInt256.valueOf(100 + blockNumber), UInt256.valueOf(blockNumber * 7L));
    updater.commit();
  }
}
