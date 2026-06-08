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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Reference proof vector test — Task 6.1 of Design 5.
 *
 * <p>Builds a single chain that populates <em>both</em> the legacy archive path (rollback +
 * reconstruct) and the Design-5 trie-node differential index, then asserts that the two paths
 * produce byte-for-byte identical {@link WorldStateProof} outputs.
 *
 * <h2>Test conditions</h2>
 *
 * <ul>
 *   <li>{@code NUM_BLOCKS = 10}, {@code TARGET_BLOCK = 2} — target is old enough that {@code
 *       headBlock - targetBlock >= MAX_LAYERS} is satisfied (10 - 2 = 8 ≥ 3).
 *   <li>{@code INTERVAL = 4} — trie checkpoints at blocks 3, 7, 11, … The legacy path must roll
 *       back from checkpoint block 3 to block 2.
 *   <li>{@code MAX_LAYERS = 3} — both historical gates ({@code isHistoricalQuery} and the index
 *       routing condition in {@code getAccountProof}) trigger for block 2.
 *   <li>Two {@link BonsaiArchiveWorldStateProvider} instances share the same underlying storage:
 *       <ol>
 *         <li><b>legacyProvider</b> — {@code trieNodeIndexEnabled=false}, {@code
 *             stateProofsEnabled=true}; uses the archive-rollback path.
 *         <li><b>indexProvider</b> — {@code trieNodeIndexEnabled=true}, {@code
 *             stateProofsEnabled=true}; uses {@link
 *             org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader}.
 *       </ol>
 * </ul>
 *
 * <h2>Byte-identical comparison rationale</h2>
 *
 * Both paths ultimately call {@code WorldStateProofProvider.getAccountProof(stateRoot, address,
 * slots)} with the <em>same</em> stateRoot (from the block header). The trie traversal is
 * deterministic: same root → same path → same sequence of node bytes. The node bytes returned by
 * both loaders are the RLP of the same MPT nodes, so the proof witness lists must be
 * byte-identical.
 *
 * <h2>Exclusion proofs</h2>
 *
 * Two additional test cases assert correctness of non-membership witnesses:
 *
 * <ol>
 *   <li><b>Absent account</b> — an address that was never written; account proof is non-empty (trie
 *       traversal followed the branch down to the point of divergence) and balance/nonce are zero.
 *   <li><b>Absent storage slot</b> — a slot key that was never written; slot value is zero and the
 *       storage witness is non-empty.
 * </ol>
 */
class BonsaiArchiveTrieNodeIndexVectorTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  /** An address that is never created in any block — used for exclusion-proof assertions. */
  private static final Address ABSENT_ACCOUNT =
      Address.fromHexString("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead");

  private static final int NUM_BLOCKS = 10;
  private static final long MAX_LAYERS = 3L;
  private static final long INTERVAL = 4L; // trie checkpoints at blocks 3, 7, …
  private static final long TARGET_BLOCK = 2L; // historical: 10 - 2 = 8 ≥ MAX_LAYERS

  /** A storage slot that is never written — used for storage exclusion proofs. */
  private static final UInt256 ABSENT_SLOT = UInt256.ZERO;

  /** A storage slot that IS written at TARGET_BLOCK. */
  private static final UInt256 PRESENT_SLOT = UInt256.ONE;

  private BonsaiFlatDbToArchiveMigrator migrator;
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    if (migrator != null) {
      migrator.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Vector test 1: present account + present slot — byte-identical comparison
  // ---------------------------------------------------------------------------

  /**
   * The index path and the legacy path must produce byte-identical account proof witness lists for
   * a present account at TARGET_BLOCK.
   */
  @Test
  void accountProof_indexPath_byteIdenticalToLegacyPath_presentAccount() throws Exception {
    final Providers providers = buildProviders();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> legacyProof =
        providers.legacyProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(PRESENT_SLOT), Function.identity());
    final Optional<WorldStateProof> indexProof =
        providers.indexProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(PRESENT_SLOT), Function.identity());

    assertThat(legacyProof)
        .withFailMessage("legacy proof absent for block %d", TARGET_BLOCK)
        .isPresent();
    assertThat(indexProof)
        .withFailMessage("index proof absent for block %d", TARGET_BLOCK)
        .isPresent();

    // Slot value correctness (slot 1 = blockNumber at that block).
    assertThat(legacyProof.get().getStorageValue(PRESENT_SLOT))
        .isEqualTo(UInt256.valueOf(TARGET_BLOCK));
    assertThat(indexProof.get().getStorageValue(PRESENT_SLOT))
        .isEqualTo(UInt256.valueOf(TARGET_BLOCK));

    // Byte-identical account proof witnesses.
    final List<Bytes> legacyAccountNodes = legacyProof.get().getAccountProof();
    final List<Bytes> indexAccountNodes = indexProof.get().getAccountProof();
    assertThat(indexAccountNodes)
        .withFailMessage("account proof witness differs between index and legacy paths")
        .isEqualTo(legacyAccountNodes);

    // Byte-identical storage proof witnesses.
    final List<Bytes> legacyStorageNodes = legacyProof.get().getStorageProof(PRESENT_SLOT);
    final List<Bytes> indexStorageNodes = indexProof.get().getStorageProof(PRESENT_SLOT);
    assertThat(indexStorageNodes)
        .withFailMessage("storage proof witness differs between index and legacy paths")
        .isEqualTo(legacyStorageNodes);
  }

  // ---------------------------------------------------------------------------
  // Vector test 2: absent storage slot — exclusion proof, byte-identical
  // ---------------------------------------------------------------------------

  /**
   * Both paths must return a non-empty witness (non-membership proof) and zero value for a slot
   * that was never written, and the witness lists must be byte-identical.
   */
  @Test
  void storageExclusionProof_indexPath_byteIdenticalToLegacyPath_absentSlot() throws Exception {
    final Providers providers = buildProviders();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> legacyProof =
        providers.legacyProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(ABSENT_SLOT), Function.identity());
    final Optional<WorldStateProof> indexProof =
        providers.indexProvider.getAccountProof(
            targetHeader, ACCOUNT, List.of(ABSENT_SLOT), Function.identity());

    assertThat(legacyProof)
        .withFailMessage("legacy exclusion proof absent for absent slot")
        .isPresent();
    assertThat(indexProof)
        .withFailMessage("index exclusion proof absent for absent slot")
        .isPresent();

    // Both must return zero for the absent slot.
    assertThat(legacyProof.get().getStorageValue(ABSENT_SLOT)).isEqualTo(UInt256.ZERO);
    assertThat(indexProof.get().getStorageValue(ABSENT_SLOT)).isEqualTo(UInt256.ZERO);

    // A valid non-membership proof has a non-empty witness (the path to the divergence point).
    assertThat(legacyProof.get().getStorageProof(ABSENT_SLOT))
        .withFailMessage("legacy exclusion proof witness must be non-empty")
        .isNotEmpty();
    assertThat(indexProof.get().getStorageProof(ABSENT_SLOT))
        .withFailMessage("index exclusion proof witness must be non-empty")
        .isNotEmpty();

    // Byte-identical account witnesses.
    assertThat(indexProof.get().getAccountProof())
        .withFailMessage(
            "account witness differs between index and legacy paths (absent-slot test)")
        .isEqualTo(legacyProof.get().getAccountProof());

    // Byte-identical storage exclusion witnesses.
    assertThat(indexProof.get().getStorageProof(ABSENT_SLOT))
        .withFailMessage(
            "storage exclusion witness differs between index and legacy paths (absent-slot test)")
        .isEqualTo(legacyProof.get().getStorageProof(ABSENT_SLOT));
  }

  // ---------------------------------------------------------------------------
  // Vector test 3: absent account — exclusion proof, byte-identical
  // ---------------------------------------------------------------------------

  /**
   * Both paths must return a non-empty account witness (non-membership proof) for an address that
   * was never created, with zero balance/nonce and no state-trie account value.
   */
  @Test
  void accountExclusionProof_indexPath_byteIdenticalToLegacyPath_absentAccount() throws Exception {
    final Providers providers = buildProviders();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> legacyProof =
        providers.legacyProvider.getAccountProof(
            targetHeader, ABSENT_ACCOUNT, List.of(), Function.identity());
    final Optional<WorldStateProof> indexProof =
        providers.indexProvider.getAccountProof(
            targetHeader, ABSENT_ACCOUNT, List.of(), Function.identity());

    assertThat(legacyProof)
        .withFailMessage("legacy exclusion proof absent for absent account")
        .isPresent();
    assertThat(indexProof)
        .withFailMessage("index exclusion proof absent for absent account")
        .isPresent();

    // Absent account has no state-trie value.
    assertThat(legacyProof.get().getStateTrieAccountValue()).isEmpty();
    assertThat(indexProof.get().getStateTrieAccountValue()).isEmpty();

    // A valid non-membership account proof has a non-empty witness.
    assertThat(legacyProof.get().getAccountProof())
        .withFailMessage("legacy absent-account witness must be non-empty")
        .isNotEmpty();
    assertThat(indexProof.get().getAccountProof())
        .withFailMessage("index absent-account witness must be non-empty")
        .isNotEmpty();

    // Byte-identical account exclusion witnesses.
    assertThat(indexProof.get().getAccountProof())
        .withFailMessage(
            "account exclusion witness differs between index and legacy paths (absent-account test)")
        .isEqualTo(legacyProof.get().getAccountProof());
  }

  // ---------------------------------------------------------------------------
  // Chain-building infrastructure
  // ---------------------------------------------------------------------------

  /**
   * Holds both providers sharing the same underlying archive storage.
   *
   * @param legacyProvider archive provider with {@code trieNodeIndexEnabled=false}
   * @param indexProvider archive provider with {@code trieNodeIndexEnabled=true}
   */
  private record Providers(
      BonsaiArchiveWorldStateProvider legacyProvider,
      BonsaiArchiveWorldStateProvider indexProvider) {}

  /**
   * Builds a single chain of {@value #NUM_BLOCKS} blocks that populates:
   *
   * <ol>
   *   <li>The legacy archive column families (via migrator).
   *   <li>The trie-node differential index (via {@link BonsaiArchiveTrieNodeStrategy} with {@code
   *       trieNodeIndexEnabled=true}).
   * </ol>
   *
   * Returns two providers over the same underlying storage, one for each path. Called per-test for
   * full isolation; chain build is fast enough that {@code @BeforeEach} sharing is unnecessary.
   */
  private Providers buildProviders() throws Exception {
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
    // Shared archive storage — both paths read from here.
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
                            .archiveTrieNodeCheckpointInterval(INTERVAL)
                            .trieNodeIndexEnabled(true)
                            .build())
                    .build())
            .build();

    final InMemoryKeyValueStorageProvider sharedProvider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage archiveStorage =
        (BonsaiWorldStateKeyValueStorage) sharedProvider.createWorldStateStorage(archiveConfig);
    archiveStorage.upgradeToArchiveFlatDbMode();

    final SegmentedKeyValueStorage composedStorage = archiveStorage.getComposedWorldStateStorage();

    // ------------------------------------------------------------------
    // Trie-node index components.
    // ------------------------------------------------------------------
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(composedStorage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(composedStorage);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    final BonsaiArchiveTrieNodeStrategy indexStrategy =
        new BonsaiArchiveTrieNodeStrategy(
            INTERVAL,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            new BonsaiTrieNodeStrategy(),
            /* trieNodeIndexEnabled= */ true,
            historyStore,
            changeIndex,
            progress);

    // Build a head storage over the shared composedStorage using the index strategy.
    final BonsaiFlatDbStrategyProvider flatDbProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), archiveConfig);
    flatDbProvider.loadFlatDbStrategy(composedStorage);

    final BonsaiWorldStateKeyValueStorage indexHeadStorage =
        new BonsaiWorldStateKeyValueStorage(
            flatDbProvider,
            composedStorage,
            archiveStorage.getTrieLogStorage(),
            archiveStorage.getCacheManager(),
            archiveStorage.getCurrentVersion(),
            indexStrategy);

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

    // Throwaway state to compute stateRoot per block.
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
      applyBlockChanges(rootState, i);
      rootState.persist(null);
      final Hash rootHash = rootState.rootHash();

      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent.getHash())
              .stateRoot(rootHash)
              .buildHeader();

      applyBlockChanges(headState, i);
      headState.persist(header);

      // Advance coverage progress for block i.
      final SegmentedKeyValueStorageTransaction progressTx = composedStorage.startTransaction();
      progress.setLastIndexedBlock(i);
      progress.save(progressTx);
      progressTx.commit();

      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
      parent = header;
    }

    // Set indexStartBlock so covers(TARGET_BLOCK) == true for the index provider.
    final SegmentedKeyValueStorageTransaction progressTx = composedStorage.startTransaction();
    progress.setIndexStartBlock(0L);
    progress.save(progressTx);
    progressTx.commit();

    // ------------------------------------------------------------------
    // Run the migrator to populate the legacy archive CFs.
    // ------------------------------------------------------------------
    final TrieLogManager headTrieLogManager = headArchive.getTrieLogManager();
    final TrieLogManager migratorTrieLogManager = mock(TrieLogManager.class);
    when(migratorTrieLogManager.getMaxLayersToLoad()).thenReturn(0L);
    when(migratorTrieLogManager.getTrieLogLayer(any()))
        .thenAnswer(inv -> headTrieLogManager.getTrieLogLayer(inv.getArgument(0)));

    final BonsaiArchiveFlatDbStrategy archiveFlatStrategy =
        new BonsaiArchiveFlatDbStrategy(
            new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy(), INTERVAL);

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
    // Legacy provider: stateProofsEnabled=true, trieNodeIndexEnabled=false.
    // It will use the archive rollback+reconstruct path.
    // ------------------------------------------------------------------
    final ImmutableDataStorageConfiguration legacyConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(true)
                            .archiveTrieNodeCheckpointInterval(INTERVAL)
                            .trieNodeIndexEnabled(false)
                            .build())
                    .build())
            .build();

    final BonsaiArchiveWorldStateProvider legacyProvider =
        new BonsaiArchiveWorldStateProvider(
            archiveStorage,
            blockchain,
            legacyConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache(),
            new NoOpMetricsSystem());
    legacyProvider.setArchiveMigrationProgressSupplier(() -> (long) NUM_BLOCKS);

    // ------------------------------------------------------------------
    // Index provider: trieNodeIndexEnabled=true, stateProofsEnabled=true.
    // It will use the ArchiveProofNodeLoader path for TARGET_BLOCK.
    // ------------------------------------------------------------------
    final BonsaiArchiveWorldStateProvider indexProvider =
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
    indexProvider.setArchiveMigrationProgressSupplier(() -> (long) NUM_BLOCKS);

    // ------------------------------------------------------------------
    // Defensive: confirm ABSENT_SLOT (slot 0) was never written during chain building.
    // If applyBlockChanges is ever changed to write slot 0, the storageExclusionProof tests
    // would silently exercise a present-slot proof instead of a non-membership proof.
    // Fail loudly here so a future contributor sees a clear guard failure, not a confusing
    // byte-mismatch downstream.
    // ------------------------------------------------------------------
    final Optional<WorldStateProof> zeroSlotCheck =
        legacyProvider.getAccountProof(
            blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow(),
            ACCOUNT,
            List.of(ABSENT_SLOT),
            Function.identity());
    assertThat(zeroSlotCheck)
        .withFailMessage("guard: absent-slot check proof must be present")
        .hasValueSatisfying(
            proof ->
                assertThat(proof.getStorageValue(ABSENT_SLOT))
                    .withFailMessage(
                        "ABSENT_SLOT (slot 0) must be zero at TARGET_BLOCK — "
                            + "applyBlockChanges must not write slot 0")
                    .isEqualTo(UInt256.ZERO));

    return new Providers(legacyProvider, indexProvider);
  }

  /**
   * Applies deterministic state changes for block {@code blockNumber}.
   *
   * <ul>
   *   <li>Block 1: creates ACCOUNT with balance 1_000_000 Wei.
   *   <li>Every block: sets slot 1 to {@code blockNumber} and slot {@code (100 + blockNumber)} to
   *       {@code blockNumber * 7} (forces the storage trie to change shape across blocks).
   * </ul>
   */
  private void applyBlockChanges(final BonsaiWorldState state, final int blockNumber) {
    final WorldUpdater updater = state.updater();
    if (blockNumber == 1) {
      updater.createAccount(ACCOUNT, 0, Wei.of(1_000_000L));
    }
    final MutableAccount account = updater.getAccount(ACCOUNT);
    account.setStorageValue(UInt256.ONE, UInt256.valueOf(blockNumber));
    account.setStorageValue(UInt256.valueOf(100 + blockNumber), UInt256.valueOf(blockNumber * 7L));
    updater.commit();
  }
}
