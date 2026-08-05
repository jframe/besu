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
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration tests for the trie-node history walker and archive proof system.
 *
 * <p>Each test drives the full pipeline: a real chain is built using the {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.LogRollingTests} fixture pattern (real trie
 * logs, real {@link BonsaiWorldState}, real {@link org.hyperledger.besu.ethereum.core.BlockHeader}s
 * with correct state roots), the walker is run to completion, and proofs are then served through
 * {@link BonsaiArchiveWorldStateProvider} and verified against the corresponding header's state
 * root.
 *
 * <p>Scenarios covered:
 *
 * <ol>
 *   <li><b>Full lifecycle</b> — account created, mutated across a {@link
 *       TrieNodeHistoryReader#CHECKPOINT_INTERVAL} boundary, then deleted; correct proof at every
 *       intermediate block (membership for mutation blocks, non-membership for the deletion block).
 *   <li><b>Storage slots</b> — an account with storage mutated over several blocks; correct storage
 *       proof at each.
 *   <li><b>Reorg immunity</b> — a real fork that reorgs the chain <em>within</em> the reorg window;
 *       after the walker advances past the fork point, every served proof matches the canonical
 *       chain.
 *   <li><b>Coverage boundary</b> — proofs inside the reorg window route to the trie-log path and
 *       succeed; proofs outside it route through history and succeed. No block returns empty.
 * </ol>
 */
class TrieNodeHistoryWalkerIntegrationTest {

  private static final long MAX_LAYERS = 2L;
  private static final long MAX_LAYERS_REORG = 3L;
  private static final long AWAIT_TIMEOUT_SECONDS = 30L;
  private static final DataStorageConfiguration STORAGE_CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;

  private static final Address ADDRESS_A =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address ADDRESS_B =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final UInt256 SLOT_1 = UInt256.ONE;
  private static final UInt256 SLOT_2 = UInt256.valueOf(2);

  private final List<TrieNodeHistoryWalker> walkers = new ArrayList<>();
  private final List<ExecutorService> executors = new ArrayList<>();

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
  // Scenario 1: Full lifecycle — account created and mutated across CHECKPOINT_INTERVAL
  // -------------------------------------------------------------------------------------

  /**
   * Verifies that the walker correctly reconstructs proofs for every block it processes, including
   * those that require a backward walk through DIFF entries to a FULL checkpoint, and that the
   * proof for a deletion block shows the account as absent.
   *
   * <p>Block structure (23 blocks, maxLayersToLoad=2):
   *
   * <ul>
   *   <li>Blocks 1–18: ADDRESS_A balance set to {@code blockNum * 100} (18 mutations). Block 1
   *       writes FULL|CREATION; blocks 2–16 write DIFFs (counter 1–15); block 17 hits the
   *       CHECKPOINT_INTERVAL threshold and writes a new FULL; block 18 writes a DIFF.
   *   <li>Blocks 19–20: ADDRESS_B mutated to advance the head (keeps the reorg window above 18).
   *   <li>Block 21: ADDRESS_A deleted.
   *   <li>Blocks 22–23: ADDRESS_B mutated to keep the reorg window above block 21.
   * </ul>
   *
   * The walker processes blocks 1–21. Proofs for ADDRESS_A at blocks 1–18 must show membership; the
   * proof at block 21 must show non-membership ({@code stateTrieAccountValue().isEmpty()}).
   */
  @Test
  void fullLifecycle_proofCorrectAcrossCheckpointIntervalBoundary() {
    // Blocks 1..18 mutate ADDRESS_A; blocks 19..20 are head pads; block 21 deletes ADDRESS_A;
    // blocks 22..23 are more pads to keep the reorg window above block 21.
    final ChainFixture fixture = buildChain(20, MAX_LAYERS);
    fixture.appendDeleteBlock(ADDRESS_A); // block 21
    fixture.appendPadBlock(); // block 22
    fixture.appendPadBlock(); // block 23

    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, MAX_LAYERS);
    walker.start();

    final long expectedTarget = 23L - MAX_LAYERS; // = 21
    awaitWalkerBlock(walker, expectedTarget);

    // Verify proofs at blocks 1..18 (ADDRESS_A mutation blocks).
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(fixture.composedStorage);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);
    final TrieNodeHistoryProgress progress = TrieNodeHistoryProgress.load(fixture.composedStorage);
    final BonsaiArchiveWorldStateProvider provider =
        newProvider(fixture.sourceStorage, fixture.blockchain, historyReader, progress, MAX_LAYERS);

    for (int i = 1; i <= 18; i++) {
      final BlockHeader header = fixture.headers[i - 1];
      final Optional<WorldStateProof> result =
          provider.getAccountProof(header, ADDRESS_A, List.of(), Function.identity());
      assertThat(result).as("proof at block %d must be present", i).isPresent();
      assertThat(result.get().getStateTrieAccountValue())
          .as("ADDRESS_A must exist at block %d", i)
          .isPresent();
      assertProofRootMatchesStateRoot(result.get(), header, i);
    }

    // Block 21: ADDRESS_A deleted — proof must show non-membership.
    final BlockHeader deleteHeader = fixture.headers[20]; // 0-indexed: block 21 → index 20
    final Optional<WorldStateProof> deleteProof =
        provider.getAccountProof(deleteHeader, ADDRESS_A, List.of(), Function.identity());
    assertThat(deleteProof).as("deletion proof at block 21 must be present").isPresent();
    assertThat(deleteProof.get().getStateTrieAccountValue())
        .as("ADDRESS_A must be absent at block 21 (deleted)")
        .isEmpty();
    assertProofRootMatchesStateRoot(deleteProof.get(), deleteHeader, 21);
  }

  // -------------------------------------------------------------------------------------
  // Scenario 2: Storage slots — correct storage proof at each block
  // -------------------------------------------------------------------------------------

  /**
   * Verifies that storage proofs are correctly reconstructed from the trie-node history.
   *
   * <p>Block structure (6 blocks, maxLayersToLoad=2):
   *
   * <ul>
   *   <li>Block 1: create ADDRESS_A, SLOT_1=100, SLOT_2=200.
   *   <li>Block 2: SLOT_1=200.
   *   <li>Block 3: SLOT_1=300.
   *   <li>Blocks 4–6: ADDRESS_B mutated to advance the head.
   * </ul>
   *
   * Walker processes blocks 1–4. Storage proofs for SLOT_1 and SLOT_2 at blocks 1–3 must be present
   * with the correct values and account-proof first nodes hashing to the state root.
   */
  @Test
  void storageSlots_proofCorrectAtEachBlock() {
    final ChainFixture fixture = buildChain(6, MAX_LAYERS);
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, MAX_LAYERS);
    walker.start();

    final long expectedTarget = 6L - MAX_LAYERS; // = 4
    awaitWalkerBlock(walker, expectedTarget);

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(fixture.composedStorage);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);
    final TrieNodeHistoryProgress progress = TrieNodeHistoryProgress.load(fixture.composedStorage);
    final BonsaiArchiveWorldStateProvider provider =
        newProvider(fixture.sourceStorage, fixture.blockchain, historyReader, progress, MAX_LAYERS);

    // Block 1: SLOT_1=100, SLOT_2=200
    verifyStorageProof(provider, fixture.headers[0], 100L, 200L);
    // Block 2: SLOT_1=200, SLOT_2=200
    verifyStorageProof(provider, fixture.headers[1], 200L, 200L);
    // Block 3: SLOT_1=300, SLOT_2=200
    verifyStorageProof(provider, fixture.headers[2], 300L, 200L);
  }

  // -------------------------------------------------------------------------------------
  // Scenario 3: Reorg immunity — walker tracks canonical chain after reorg
  // -------------------------------------------------------------------------------------

  /**
   * Verifies that the walker produces proofs consistent with the canonical chain after a
   * within-window reorg. The reorg happens entirely inside the reorg window so the walker never
   * sees the orphaned chain.
   *
   * <p>Timeline (maxLayersToLoad=3):
   *
   * <ol>
   *   <li>Canonical blocks 1–5 are added (ADDRESS_A mutations); walker target = 5−3 = 2.
   *   <li>Walker processes canonical blocks 1 and 2.
   *   <li>Fork blocks 3', 4', 5', 6' are constructed from block 2's state (ADDRESS_B mutations).
   *       Block 3' carries {@link Difficulty#ONE} so it outweighs the current canonical chain and
   *       triggers a reorg when appended to the blockchain.
   *   <li>After block 6' is added (head=6, target=3), the walker processes block 3, which is now
   *       the canonical fork block 3' (ADDRESS_B created, ADDRESS_A unchanged).
   *   <li>Proofs for ADDRESS_A at canonical blocks 1, 2, and 3 (= fork block 3') must be present
   *       and hash-correct.
   * </ol>
   */
  @Test
  void reorgImmunity_walkerTracksCanonicalChainAfterReorg() throws Exception {
    // Step 1: build canonical chain with blocks 1..5 (ADDRESS_A mutations).
    final ChainFixture canonical = buildChain(5, MAX_LAYERS_REORG);
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(canonical, MAX_LAYERS_REORG);
    walker.start();

    // Wait for walker to process the initial target: 5−3 = 2.
    awaitWalkerBlock(walker, 2L);

    // Step 2: Build fork blocks 3', 4', 5', 6' starting from block 2's state.
    //   - fork pre-compute derives correct state roots
    //   - fork source writes trie logs; we then copy them into the canonical trie-log storage
    final BonsaiWorldState forkPreCompute = buildForkPreCompute(canonical.headers);
    final BonsaiWorldStateKeyValueStorage forkSourceStorage =
        newForkSourceStorage(canonical.sourceStorage.getTrieLogStorage());
    final BonsaiWorldState forkSourceWorld =
        buildForkSourceWorldState(forkSourceStorage, canonical);

    // Fork blocks start at number 3 (parent = canonical block 2) and run to 6.
    final BlockHeader[] forkHeaders = new BlockHeader[4]; // indices 0-3 = blocks 3',4',5',6'
    Hash forkParentHash = canonical.headers[1].getHash(); // block 2 hash
    for (int i = 0; i < 4; i++) {
      final int blockNum = 3 + i; // 3, 4, 5, 6
      // Apply ADDRESS_B change on both pre-compute and fork source.
      applyForkChange(forkPreCompute.updater(), blockNum);
      forkPreCompute.persist(null);
      final Hash stateRoot = forkPreCompute.rootHash();

      applyForkChange(forkSourceWorld.updater(), blockNum);

      final Difficulty diff = (i == 0) ? Difficulty.ONE : Difficulty.ZERO;
      final BlockHeader forkHeader =
          new BlockHeaderTestFixture()
              .number(blockNum)
              .parentHash(forkParentHash)
              .stateRoot(stateRoot)
              .difficulty(diff)
              .buildHeader();

      forkSourceWorld.persist(forkHeader);
      forkHeaders[i] = forkHeader;
      forkParentHash = forkHeader.getHash();

      // Append to the canonical blockchain.
      // Adding block 3' (difficulty=ONE) triggers the reorg; 4', 5', 6' extend the new canonical.
      canonical.blockchain.appendBlock(new Block(forkHeader, BlockBody.empty()), List.of());
    }

    // After block 6' (number=6) is appended, head=6, walker target = 6−3 = 3.
    awaitWalkerBlock(walker, 3L);

    // Step 3: Verify proofs. After the reorg canonical block 3 is forkHeaders[0] (block 3').
    //   ADDRESS_A is unchanged from block 2 at block 3', so a proof for ADDRESS_A is valid.
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(canonical.composedStorage);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);
    final TrieNodeHistoryProgress progress =
        TrieNodeHistoryProgress.load(canonical.composedStorage);
    final BonsaiArchiveWorldStateProvider provider =
        newProvider(
            canonical.sourceStorage,
            canonical.blockchain,
            historyReader,
            progress,
            MAX_LAYERS_REORG);

    // Canonical blocks 1 and 2 (pre-fork) — ADDRESS_A exists.
    for (int i = 1; i <= 2; i++) {
      final BlockHeader header = canonical.headers[i - 1];
      final Optional<WorldStateProof> result =
          provider.getAccountProof(header, ADDRESS_A, List.of(), Function.identity());
      assertThat(result).as("canonical block %d proof must be present", i).isPresent();
      assertThat(result.get().getStateTrieAccountValue())
          .as("ADDRESS_A must exist at canonical block %d", i)
          .isPresent();
      assertProofRootMatchesStateRoot(result.get(), header, i);
    }

    // Canonical block 3 is now fork block 3' — ADDRESS_A is still present (unchanged at fork).
    final BlockHeader canonicalBlock3 = forkHeaders[0];
    final Optional<WorldStateProof> proofAt3 =
        provider.getAccountProof(canonicalBlock3, ADDRESS_A, List.of(), Function.identity());
    assertThat(proofAt3).as("canonical block 3 (fork 3') proof must be present").isPresent();
    assertThat(proofAt3.get().getStateTrieAccountValue())
        .as("ADDRESS_A must still exist at fork block 3'")
        .isPresent();
    assertProofRootMatchesStateRoot(proofAt3.get(), canonicalBlock3, 3);
  }

  // -------------------------------------------------------------------------------------
  // Scenario 4: Coverage boundary — both trie-log and history paths serve valid proofs
  // -------------------------------------------------------------------------------------

  /**
   * Verifies that no block in the queried range returns an unavailable proof.
   *
   * <p>Block structure (5 blocks, maxLayersToLoad=2):
   *
   * <ul>
   *   <li>Walker processes blocks 1–3 (= 5−2); {@link TrieNodeHistoryProgress} covers 1–3.
   *   <li>Block 3 is outside the reorg window (5−3=2 ≥ 2) → history path.
   *   <li>Block 4 is inside the reorg window (5−4=1 < 2) → trie-log path via super.
   * </ul>
   *
   * Both must return non-empty proofs with the account present and the first proof node hashing to
   * the correct state root.
   */
  @Test
  void coverageBoundary_bothPathsServingValidProofs() {
    final ChainFixture fixture = buildChain(5, MAX_LAYERS);
    final TrieNodeHistoryWalker walker = createAndRegisterWalker(fixture, MAX_LAYERS);
    walker.start();

    final long expectedTarget = 5L - MAX_LAYERS; // = 3
    awaitWalkerBlock(walker, expectedTarget);

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(fixture.composedStorage);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);
    final TrieNodeHistoryProgress progress = TrieNodeHistoryProgress.load(fixture.composedStorage);
    final BonsaiArchiveWorldStateProvider provider =
        newProvider(fixture.sourceStorage, fixture.blockchain, historyReader, progress, MAX_LAYERS);

    // Block 3: history path (5−3 = 2 ≥ maxLayersToLoad=2).
    final BlockHeader header3 = fixture.headers[2];
    final Optional<WorldStateProof> historyProof =
        provider.getAccountProof(header3, ADDRESS_A, List.of(), Function.identity());
    assertThat(historyProof).as("history-path proof at block 3 must be present").isPresent();
    assertThat(historyProof.get().getStateTrieAccountValue())
        .as("ADDRESS_A must exist at block 3 (history path)")
        .isPresent();
    assertProofRootMatchesStateRoot(historyProof.get(), header3, 3);

    // Block 4: trie-log path (5−4 = 1 < maxLayersToLoad=2).
    final BlockHeader header4 = fixture.headers[3];
    final Optional<WorldStateProof> trieLogProof =
        provider.getAccountProof(header4, ADDRESS_A, List.of(), Function.identity());
    assertThat(trieLogProof).as("trie-log-path proof at block 4 must be present").isPresent();
    assertThat(trieLogProof.get().getStateTrieAccountValue())
        .as("ADDRESS_A must exist at block 4 (trie-log path)")
        .isPresent();
    assertProofRootMatchesStateRoot(trieLogProof.get(), header4, 4);
  }

  // =====================================================================================
  // Assertion helpers
  // =====================================================================================

  /** Asserts that the first node in the account-proof path hashes to the header's state root. */
  private static void assertProofRootMatchesStateRoot(
      final WorldStateProof proof, final BlockHeader header, final int blockNum) {
    assertThat(proof.getAccountProof())
        .as("account proof at block %d must be non-empty", blockNum)
        .isNotEmpty();
    assertThat(Hash.hash(proof.getAccountProof().getFirst()))
        .as("proof root at block %d must hash to state root", blockNum)
        .isEqualTo(header.getStateRoot());
  }

  /** Verifies SLOT_1 and SLOT_2 values against expected storage values at the given header. */
  private void verifyStorageProof(
      final BonsaiArchiveWorldStateProvider provider,
      final BlockHeader header,
      final long expectedSlot1Value,
      final long expectedSlot2Value) {
    final Optional<WorldStateProof> result =
        provider.getAccountProof(header, ADDRESS_A, List.of(SLOT_1, SLOT_2), Function.identity());
    assertThat(result)
        .as("storage proof at block %d must be present", header.getNumber())
        .isPresent();
    assertThat(result.get().getStateTrieAccountValue())
        .as("ADDRESS_A must exist at block %d", header.getNumber())
        .isPresent();
    assertProofRootMatchesStateRoot(result.get(), header, (int) header.getNumber());
    assertThat(result.get().getStorageValue(SLOT_1))
        .as("SLOT_1 value at block %d", header.getNumber())
        .isEqualTo(UInt256.valueOf(expectedSlot1Value));
    assertThat(result.get().getStorageValue(SLOT_2))
        .as("SLOT_2 value at block %d", header.getNumber())
        .isEqualTo(UInt256.valueOf(expectedSlot2Value));
  }

  // =====================================================================================
  // Walker helpers
  // =====================================================================================

  private TrieNodeHistoryWalker createAndRegisterWalker(
      final ChainFixture fixture, final long maxLayersForWalker) {
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), STORAGE_CONFIG);
    flatDbStrategyProvider.loadFlatDbStrategy(fixture.composedStorage);

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(fixture.composedStorage);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);

    final TrieNodeHistoryWalkerWorldState walkerWorldState =
        new TrieNodeHistoryWalkerWorldState(
            flatDbStrategyProvider, fixture.composedStorage, historyReader, historyStore);

    final TrieNodeHistoryProgress progress = TrieNodeHistoryProgress.load(fixture.composedStorage);
    final TrieLogManager walkerTrieLogManager =
        new TrieLogManager(fixture.blockchain, fixture.sourceStorage, maxLayersForWalker, null);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executors.add(executor);

    // null GenesisState is safe here: all test fixtures use Hash.EMPTY_TRIE_HASH genesis so
    // bootstrapGenesis() is never invoked.
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

  private static void awaitWalkerBlock(final TrieNodeHistoryWalker walker, final long target) {
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(walker.getWalkedBlockNumber()).isEqualTo(target));
  }

  // =====================================================================================
  // Provider helper
  // =====================================================================================

  private BonsaiArchiveWorldStateProvider newProvider(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final MutableBlockchain blockchain,
      final TrieNodeHistoryReader trieNodeHistoryReader,
      final TrieNodeHistoryProgress trieNodeHistoryProgress,
      final long maxLayersToLoad) {
    final ImmutableDataStorageConfiguration config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(maxLayersToLoad)
                    .build())
            .build();

    return new BonsaiArchiveWorldStateProvider(
        worldStateStorage,
        blockchain,
        config,
        new NoOpBonsaiCachedMerkleTrieLoader(), // required; null causes NPE in rollback preloader
        null,
        EvmConfiguration.DEFAULT,
        throwingWorldStateHealerSupplier(),
        new PathBasedCodeCache(),
        new NoOpMetricsSystem(),
        trieNodeHistoryReader,
        trieNodeHistoryProgress);
  }

  // =====================================================================================
  // Chain fixture
  // =====================================================================================

  /**
   * Builds a chain of {@code numBlocks} blocks, deriving correct state roots via a throwaway
   * pre-compute world state and writing genuine trie logs via a source world state.
   *
   * <p>Change pattern per block:
   *
   * <ul>
   *   <li>Blocks 1–(numBlocks−2): ADDRESS_A balance set to {@code blockNum × 100}. Storage changes
   *       are applied for blocks 1–4 (SLOT_1/SLOT_2 values vary by block).
   *   <li>Last two blocks: ADDRESS_B balance set (keeps the head advancing without perturbing
   *       ADDRESS_A near the top of the chain, so the walker can process ADDRESS_A changes safely).
   * </ul>
   */
  private ChainFixture buildChain(final int numBlocks, final long maxLayersForSource) {
    // -- Pre-compute world state (throwaway) ------------------------------------------
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

    // -- Source world state (real trie logs) ------------------------------------------
    final ChainFixture fixture = new ChainFixture();
    fixture.sourceStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), STORAGE_CONFIG);
    fixture.composedStorage = fixture.sourceStorage.getComposedWorldStateStorage();

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

    for (int i = 1; i <= numBlocks; i++) {
      fixture.appendBlockInternal(i, numBlocks);
    }
    return fixture;
  }

  /** Applies per-block account changes on the given updater and commits it. */
  private static void applyBlockChanges(
      final WorldUpdater updater, final int blockNum, final int totalBlocks) {
    // The last two blocks mutate ADDRESS_B to advance the head without touching ADDRESS_A.
    final boolean isHeadPad = blockNum > totalBlocks - 2;
    if (isHeadPad) {
      final MutableAccount b = updater.getOrCreate(ADDRESS_B);
      b.setBalance(Wei.of((long) blockNum * 10));
    } else {
      final MutableAccount a = updater.getOrCreate(ADDRESS_A);
      a.setBalance(Wei.of((long) blockNum * 100));
      // Storage: blocks 1..4 set slot values to exercise the storage-trie proof path.
      if (blockNum <= 4) {
        a.setStorageValue(SLOT_1, UInt256.valueOf((long) blockNum * 100));
        // SLOT_2 only updated at block 1 (stays at 200 thereafter).
        if (blockNum == 1) {
          a.setStorageValue(SLOT_2, UInt256.valueOf(200L));
        }
      }
    }
    updater.commit();
  }

  // =====================================================================================
  // Reorg-scenario helpers
  // =====================================================================================

  /**
   * Creates a fork pre-compute world state starting from scratch. It applies blocks 1 and 2 of the
   * canonical chain (ADDRESS_A) to reach block 2's state root, then the caller can apply ADDRESS_B
   * changes for fork blocks 3', 4', 5', 6'.
   */
  private BonsaiWorldState buildForkPreCompute(final BlockHeader[] canonicalHeaders) {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), STORAGE_CONFIG);
    final PathBasedCodeCache cc = new PathBasedCodeCache();
    final WorldStateConfig wsConfig =
        WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
            .parallelStateRootComputationEnabled(false)
            .build();
    final BonsaiWorldState ws =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(storage, EvmConfiguration.DEFAULT, cc),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            wsConfig,
            cc);
    // Replay blocks 1 and 2 to reach the fork point's state.
    for (int i = 1; i <= 2; i++) {
      applyBlockChanges(ws.updater(), i, /* totalBlocks (not a pad block) */ 100);
      ws.persist(canonicalHeaders[i - 1]);
    }
    return ws;
  }

  /**
   * Creates a fork source storage sharing the canonical's trie-log storage. Trie logs written by
   * the fork world state go directly to the canonical storage, making them visible to the walker's
   * {@link TrieLogManager}.
   */
  private BonsaiWorldStateKeyValueStorage newForkSourceStorage(
      final org.hyperledger.besu.plugin.services.storage.KeyValueStorage canonicalTrieLogStorage) {
    final BonsaiFlatDbStrategyProvider forkFlatDbProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), STORAGE_CONFIG);
    final SegmentedKeyValueStorage forkComposed =
        new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    forkFlatDbProvider.loadFlatDbStrategy(forkComposed);
    return new BonsaiWorldStateKeyValueStorage(
        forkFlatDbProvider,
        forkComposed,
        canonicalTrieLogStorage, // trie logs shared with canonical
        FlatDbCacheManager.NO_OP_CACHE,
        0L);
  }

  /**
   * Creates a fork source world state backed by the given fork storage. Applies blocks 1 and 2 of
   * the canonical chain so the fork world state starts from block 2's state, matching the pre-
   * compute at that point. The trie logs for blocks 1 and 2 are no-ops (already in canonical
   * storage).
   */
  private BonsaiWorldState buildForkSourceWorldState(
      final BonsaiWorldStateKeyValueStorage forkStorage, final ChainFixture canonical) {
    final PathBasedCodeCache cc = new PathBasedCodeCache();
    final WorldStateConfig wsConfig =
        WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
            .parallelStateRootComputationEnabled(false)
            .build();
    final TrieLogManager forkTrieLogManager =
        new TrieLogManager(canonical.blockchain, forkStorage, MAX_LAYERS_REORG, null);
    final BonsaiWorldState ws =
        new BonsaiWorldState(
            forkStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(forkStorage, EvmConfiguration.DEFAULT, cc),
            forkTrieLogManager,
            EvmConfiguration.DEFAULT,
            wsConfig,
            cc);
    // Replay canonical blocks 1 and 2 so the fork world starts from block 2's state.
    for (int i = 1; i <= 2; i++) {
      applyBlockChanges(ws.updater(), i, /* not a pad block */ 100);
      ws.persist(canonical.headers[i - 1]);
    }
    return ws;
  }

  /** Applies ADDRESS_B balance change for the given fork block number, then commits. */
  private static void applyForkChange(final WorldUpdater updater, final int blockNum) {
    final MutableAccount b = updater.getOrCreate(ADDRESS_B);
    b.setBalance(Wei.of((long) blockNum * 77));
    updater.commit();
  }

  // =====================================================================================
  // ChainFixture
  // =====================================================================================

  /** Mutable fixture that owns the source and pre-compute world states and the blockchain. */
  private static class ChainFixture {
    MutableBlockchain blockchain;
    BonsaiWorldStateKeyValueStorage sourceStorage;
    TrieLogManager trieLogManager;
    SegmentedKeyValueStorage composedStorage;
    BlockHeader[] headers; // 0-indexed: headers[i] = header for block i+1
    BonsaiWorldState sourceWorldState;
    BonsaiWorldState preComputeWorldState;
    Hash lastParentHash;

    private void appendBlockInternal(final int blockNum, final int totalBlocks) {
      // 1. Pre-compute: derive the correct state root.
      applyBlockChanges(preComputeWorldState.updater(), blockNum, totalBlocks);
      preComputeWorldState.persist(null);
      final Hash stateRoot = preComputeWorldState.rootHash();

      // 2. Build the canonical header.
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(blockNum)
              .parentHash(lastParentHash)
              .stateRoot(stateRoot)
              .buildHeader();

      // 3. Source world state: apply same changes and persist (writes the trie log).
      applyBlockChanges(sourceWorldState.updater(), blockNum, totalBlocks);
      sourceWorldState.persist(header);

      // 4. Register in the blockchain.
      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());

      headers[blockNum - 1] = header;
      lastParentHash = header.getHash();
    }

    /**
     * Appends one block that deletes {@code address} from the state. Grows the {@code headers}
     * array by one slot so the new header is accessible at index {@code headers.length - 1}.
     */
    void appendDeleteBlock(final Address address) {
      final int blockNum = headers.length + 1;
      headers = Arrays.copyOf(headers, headers.length + 1);

      // 1. Pre-compute: delete the account and derive the new state root.
      final WorldUpdater preUp = preComputeWorldState.updater();
      preUp.deleteAccount(address);
      preUp.commit();
      preComputeWorldState.persist(null);
      final Hash stateRoot = preComputeWorldState.rootHash();

      // 2. Build the canonical header.
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(blockNum)
              .parentHash(lastParentHash)
              .stateRoot(stateRoot)
              .buildHeader();

      // 3. Source world state: same deletion + trie log.
      final WorldUpdater srcUp = sourceWorldState.updater();
      srcUp.deleteAccount(address);
      srcUp.commit();
      sourceWorldState.persist(header);

      // 4. Register in the blockchain.
      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
      headers[blockNum - 1] = header;
      lastParentHash = header.getHash();
    }

    /**
     * Appends one pad block that mutates ADDRESS_B without changing ADDRESS_A. Grows the {@code
     * headers} array by one slot.
     */
    void appendPadBlock() {
      final int blockNum = headers.length + 1;
      headers = Arrays.copyOf(headers, headers.length + 1);

      // 1. Pre-compute.
      final WorldUpdater preUp = preComputeWorldState.updater();
      preUp.getOrCreate(ADDRESS_B).setBalance(Wei.of((long) blockNum * 10));
      preUp.commit();
      preComputeWorldState.persist(null);
      final Hash stateRoot = preComputeWorldState.rootHash();

      // 2. Build the canonical header.
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(blockNum)
              .parentHash(lastParentHash)
              .stateRoot(stateRoot)
              .buildHeader();

      // 3. Source world state.
      final WorldUpdater srcUp = sourceWorldState.updater();
      srcUp.getOrCreate(ADDRESS_B).setBalance(Wei.of((long) blockNum * 10));
      srcUp.commit();
      sourceWorldState.persist(header);

      // 4. Register in the blockchain.
      blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of());
      headers[blockNum - 1] = header;
      lastParentHash = header.getHash();
    }
  }
}
