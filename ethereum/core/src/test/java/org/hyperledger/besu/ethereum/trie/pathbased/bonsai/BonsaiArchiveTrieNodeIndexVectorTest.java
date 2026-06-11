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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
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
import java.util.function.Function;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Reference proof vector test — Design-5 trie-node differential index.
 *
 * <p>Builds a chain of {@value #NUM_BLOCKS} blocks and asserts that the Design-5 trie-node index
 * path ({@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader})
 * returns correct {@link WorldStateProof} results for a historical block.
 *
 * <h2>Test conditions</h2>
 *
 * <ul>
 *   <li>{@code NUM_BLOCKS = 10}, {@code TARGET_BLOCK = 2} — target is old enough that {@code
 *       headBlock - targetBlock >= MAX_LAYERS} is satisfied (10 - 2 = 8 ≥ 3).
 *   <li>{@code MAX_LAYERS = 3} — both historical gates ({@code isHistoricalQuery} and the index
 *       routing condition in {@code getAccountProof}) trigger for block 2.
 * </ul>
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
  private static final long TARGET_BLOCK = 2L; // historical: 10 - 2 = 8 ≥ MAX_LAYERS

  /** A storage slot that is never written — used for storage exclusion proofs. */
  private static final UInt256 ABSENT_SLOT = UInt256.ZERO;

  /** A storage slot that IS written at TARGET_BLOCK. */
  private static final UInt256 PRESENT_SLOT = UInt256.ONE;

  private MutableBlockchain blockchain;

  // ---------------------------------------------------------------------------
  // Vector test 1: present account + present slot
  // ---------------------------------------------------------------------------

  /** The index path must return the correct slot value and non-empty witnesses for TARGET_BLOCK. */
  @Test
  void accountProof_indexPath_presentAccount() throws Exception {
    final BonsaiArchiveWorldStateProvider provider = buildProvider();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        provider.getAccountProof(targetHeader, ACCOUNT, List.of(PRESENT_SLOT), Function.identity());

    assertThat(proof).withFailMessage("index proof absent for block %d", TARGET_BLOCK).isPresent();

    // Slot 1 is set to blockNumber at each block.
    assertThat(proof.get().getStorageValue(PRESENT_SLOT)).isEqualTo(UInt256.valueOf(TARGET_BLOCK));

    assertThat(proof.get().getAccountProof()).isNotEmpty();
    assertThat(proof.get().getStorageProof(PRESENT_SLOT)).isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Vector test 2: absent storage slot — exclusion proof
  // ---------------------------------------------------------------------------

  /**
   * The index path must return a non-empty witness (non-membership proof) and zero value for a slot
   * that was never written.
   */
  @Test
  void storageExclusionProof_indexPath_absentSlot() throws Exception {
    final BonsaiArchiveWorldStateProvider provider = buildProvider();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        provider.getAccountProof(targetHeader, ACCOUNT, List.of(ABSENT_SLOT), Function.identity());

    assertThat(proof).withFailMessage("index exclusion proof absent for absent slot").isPresent();

    assertThat(proof.get().getStorageValue(ABSENT_SLOT)).isEqualTo(UInt256.ZERO);

    assertThat(proof.get().getStorageProof(ABSENT_SLOT))
        .withFailMessage("exclusion proof witness must be non-empty")
        .isNotEmpty();

    assertThat(proof.get().getAccountProof()).isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Vector test 3: absent account — exclusion proof
  // ---------------------------------------------------------------------------

  /**
   * The index path must return a non-empty account witness (non-membership proof) for an address
   * that was never created, with no state-trie account value.
   */
  @Test
  void accountExclusionProof_indexPath_absentAccount() throws Exception {
    final BonsaiArchiveWorldStateProvider provider = buildProvider();
    final BlockHeader targetHeader = blockchain.getBlockHeader(TARGET_BLOCK).orElseThrow();

    final Optional<WorldStateProof> proof =
        provider.getAccountProof(targetHeader, ABSENT_ACCOUNT, List.of(), Function.identity());

    assertThat(proof)
        .withFailMessage("index exclusion proof absent for absent account")
        .isPresent();

    assertThat(proof.get().getStateTrieAccountValue()).isEmpty();

    assertThat(proof.get().getAccountProof())
        .withFailMessage("absent-account witness must be non-empty")
        .isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Chain-building infrastructure
  // ---------------------------------------------------------------------------

  /**
   * Builds a single chain of {@value #NUM_BLOCKS} blocks, populating the trie-node differential
   * index via {@link BonsaiArchiveTrieNodeStrategy} with {@code trieNodeIndexEnabled=true}. Called
   * per-test for full isolation.
   */
  private BonsaiArchiveWorldStateProvider buildProvider() throws Exception {
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
    // Archive storage with proofs enabled.
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
    // Provider over the indexed archive storage.
    // ------------------------------------------------------------------
    final BonsaiArchiveWorldStateProvider provider =
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
    provider.setArchiveMigrationProgressSupplier(() -> (long) NUM_BLOCKS);

    // ------------------------------------------------------------------
    // Defensive: confirm ABSENT_SLOT (slot 0) was never written during chain building.
    // If applyBlockChanges is ever changed to write slot 0, the storageExclusionProof tests
    // would silently exercise a present-slot proof instead of a non-membership proof.
    // Fail loudly here so a future contributor sees a clear guard failure, not a confusing
    // assertion failure downstream.
    // ------------------------------------------------------------------
    final Optional<WorldStateProof> zeroSlotCheck =
        provider.getAccountProof(
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

    return provider;
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
