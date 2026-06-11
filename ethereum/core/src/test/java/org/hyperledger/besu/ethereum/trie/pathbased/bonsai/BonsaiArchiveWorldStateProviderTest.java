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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveWorldStateProviderTest {

  private static final long MAX_LAYERS = 512L;
  private static final long CHAIN_HEAD = 10000L;

  private Blockchain blockchain;
  private BlockHeader chainHeadHeader;
  private BonsaiArchiveWorldStateProvider provider;

  @BeforeEach
  void setup() {
    blockchain = mock(Blockchain.class);
    chainHeadHeader = new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader();
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadHeader);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
    when(blockchain.getBlockHeader(chainHeadHeader.getHash()))
        .thenReturn(Optional.of(chainHeadHeader));

    provider = createProvider(true);
  }

  @Test
  void historicalQuery_returnsBonsaiWorldStateBackedByArchiveReadStorage() {
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    provider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void historicalQuery_migratorBehindQueryBlock_fallsThroughToSuper() {
    // Migrator has not yet processed the requested block.
    // Provider should fall through to super.getWorldState(), which cannot serve blocks
    // beyond maxLayersToLoad via trie-log rollback → empty result.
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber - 1);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void historicalQuery_atExactBoundary_migratorBehind_fallsThroughToSuper() {
    // Models the "new block just arrived, migrator hasn't caught up yet" race:
    // queryBlock sits exactly at head - maxLayersToLoad (the historical routing threshold),
    // migrator progress is one block short. Gate must refuse the archive route.
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS;
    final BlockHeader boundaryHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(boundaryHeader.getHash()))
        .thenReturn(Optional.of(boundaryHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber - 1);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(boundaryHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void historicalQuery_migratorAtQueryBlock_usesArchive() {
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    provider.setArchiveMigrationProgressSupplier(() -> queryBlockNumber);

    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void recentQuery_returnsBonsaiWorldStateBackedByMainStorage() {
    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void headUpdateQuery_returnsBonsaiWorldStateBackedByMainStorage() {
    final var result =
        provider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void historicalQuery_beforeMigrationComplete_returnsEmpty() {
    // Migration not yet complete: flatDbMode is FULL, not ARCHIVE.
    // The archive path is skipped; super.getWorldState() is called instead,
    // which cannot serve blocks beyond maxLayersToLoad via trie-log rollback.
    final BonsaiArchiveWorldStateProvider providerNotYetMigrated = createProvider(false);

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();

    final var result =
        providerNotYetMigrated.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void recentQuery_beforeMigrationComplete_returnsBonsaiWorldStateBackedByMainStorage() {
    // Migration not yet complete: falls through to super.getWorldState() which serves
    // recent blocks normally via trie-log rollback from the cached head state.
    final BonsaiArchiveWorldStateProvider providerNotYetMigrated = createProvider(false);

    final var result =
        providerNotYetMigrated.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  // --- Proofs path tests ---

  @Test
  void proofsEnabled_targetIsHistoricalBlock_returnsProofWorldState() {
    final BonsaiArchiveWorldStateProvider proofsProvider = createProviderWithProofs(true, true);
    proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final long targetNumber = 99L;
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture().number(targetNumber).buildHeader();
    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(targetNumber)).thenReturn(Optional.of(targetHeader));

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void proofsEnabled_headUpdateQuery_bypassesProofPath() {
    // shouldWorldStateUpdateHead=true means isHistoricalQuery returns false; falls through to
    // super.getWorldState() which returns the cached head state normally.
    final BonsaiArchiveWorldStateProvider proofsProvider = createProviderWithProofs(true, true);

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHeadHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
  }

  @Test
  void proofsEnabled_blockHashNotInChain_fallsThroughToSuper() {
    // Block 50 is far behind the head but isHistoricalQuery requires archiveMigrationProgress
    // to cover the target block. With default progress=-1 the gate fails and super is called,
    // which cannot serve this block → empty result.
    final BonsaiArchiveWorldStateProvider proofsProvider = createProviderWithProofs(true, true);

    final BlockHeader unknownHeader = new BlockHeaderTestFixture().number(50L).buildHeader();

    final var result =
        proofsProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(unknownHeader));

    assertThat(result).isEmpty();
  }

  @Test
  void proofsDisabled_historicalBlock_usesArchiveFlatDbPath() {
    // When stateProofsEnabled=false the proofs branch is skipped; historical queries
    // use the archive flat-db path as before.
    final BonsaiArchiveWorldStateProvider archiveProvider = createProviderWithProofs(true, false);

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD - MAX_LAYERS - 1).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    archiveProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final var result =
        archiveProvider.getWorldState(
            WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(historicalHeader));

    assertThat(result).isPresent();
    final BonsaiWorldState worldState = (BonsaiWorldState) result.get();
    assertThat(worldState.getWorldStateStorage().getFlatDbStrategy())
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  // --- Trie-node index proof routing tests ---

  /**
   * Flag off → falls through to parent. Parent cannot serve blocks beyond maxLayersToLoad via
   * trie-log rollback so the result is empty.
   */
  @Test
  void trieNodeIndex_flagOff_fallsThroughToParent() {
    final BonsaiArchiveWorldStateProvider indexProvider =
        createProviderWithTrieNodeIndex(true, false);
    // mark range 0 as complete so only the flag matters
    markRange0Complete(indexProvider);

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - MAX_LAYERS - 1)
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    indexProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final Optional<Optional<WorldStateProof>> result =
        indexProvider.getAccountProof(
            historicalHeader, Address.ZERO, Collections.emptyList(), Optional::of);

    // Parent path is taken; blocks beyond maxLayersToLoad can't be served → empty
    assertThat(result).isEmpty();
  }

  /**
   * Flag on but the range is not yet covered (progress bitmap not set) → falls through to parent.
   */
  @Test
  void trieNodeIndex_flagOn_notCovered_fallsThroughToParent() {
    final BonsaiArchiveWorldStateProvider indexProvider =
        createProviderWithTrieNodeIndex(true, true);
    // do NOT mark range 0 complete

    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - MAX_LAYERS - 1)
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    indexProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final Optional<Optional<WorldStateProof>> result =
        indexProvider.getAccountProof(
            historicalHeader, Address.ZERO, Collections.emptyList(), Optional::of);

    // Coverage gate fails; falls through to parent → empty
    assertThat(result).isEmpty();
  }

  /**
   * Flag on, range covered, but the block is near-head (within maxLayersToLoad) → falls through to
   * the parent path. The test does not populate the near-head trie-log cache, so the parent cannot
   * serve this block and returns empty — confirming the age gate, not just the coverage gate,
   * controls routing.
   */
  @Test
  void trieNodeIndex_flagOn_covered_nearHead_fallsThroughToParent() {
    final BonsaiArchiveWorldStateProvider indexProvider =
        createProviderWithTrieNodeIndex(true, true);
    markRange0Complete(indexProvider);

    // Block is within maxLayersToLoad of the chain head → NOT historical
    final BlockHeader nearHeadHeader =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - 1)
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .buildHeader();
    when(blockchain.getBlockHeader(nearHeadHeader.getHash()))
        .thenReturn(Optional.of(nearHeadHeader));

    final Optional<Optional<WorldStateProof>> result =
        indexProvider.getAccountProof(
            nearHeadHeader, Address.ZERO, Collections.emptyList(), Optional::of);

    // Parent path is taken; trie-log cache is not populated for this block in the test → empty.
    assertThat(result).isEmpty();
  }

  /**
   * Flag on, range covered, historical block → trie-node index path is taken. The stateRoot is set
   * to the empty-trie root hash so the proof provider can generate a valid (empty-account) proof
   * without any real trie data in storage.
   */
  @Test
  void trieNodeIndex_flagOn_covered_historical_usesIndexPath() {
    final BonsaiArchiveWorldStateProvider indexProvider =
        createProviderWithTrieNodeIndex(true, true);
    markRange0Complete(indexProvider);

    final Hash emptyTrieRoot = Hash.EMPTY_TRIE_HASH;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - MAX_LAYERS - 1)
            .stateRoot(emptyTrieRoot)
            .buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    indexProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    final Optional<Optional<WorldStateProof>> result =
        indexProvider.getAccountProof(
            historicalHeader, Address.ZERO, Collections.emptyList(), Optional::of);

    // The trie-node index path was taken: isWorldStateAvailable returns true, the empty-trie root
    // requires no node lookups, so the proof provider returns a non-empty WorldStateProof
    // (account absent in the empty trie, but a valid proof is still produced).
    assertThat(result).isPresent();
    assertThat(result.get()).isPresent();
  }

  /**
   * The mapper function is applied to the proof result. Verify that a mapper returning empty
   * propagates correctly (empty outer Optional).
   */
  @Test
  void trieNodeIndex_flagOn_covered_mapperReturnsEmpty_propagatesEmpty() {
    final BonsaiArchiveWorldStateProvider indexProvider =
        createProviderWithTrieNodeIndex(true, true);
    markRange0Complete(indexProvider);

    final Hash emptyTrieRoot = Hash.EMPTY_TRIE_HASH;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture()
            .number(CHAIN_HEAD - MAX_LAYERS - 1)
            .stateRoot(emptyTrieRoot)
            .buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));
    indexProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

    // Mapper always returns empty regardless of proof content.
    final Function<Optional<WorldStateProof>, Optional<String>> mapper = proof -> Optional.empty();
    final Optional<String> result =
        indexProvider.getAccountProof(
            historicalHeader, Address.ZERO, Collections.emptyList(), mapper);

    assertThat(result).isEmpty();
  }

  // ---- Helpers ----

  /**
   * Injects a {@link TrieNodeIndexProgress} covering [0, {@link ArchiveNodeKey#RANGE_SIZE}) into
   * the provider, making {@link TrieNodeIndexProgress#covers(long)} return {@code true} for any
   * block in that window.
   */
  private static void markRange0Complete(final BonsaiArchiveWorldStateProvider provider) {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    progress.setIndexStartBlock(0L);
    progress.setLastIndexedBlock(ArchiveNodeKey.RANGE_SIZE - 1L);
    provider.setTrieNodeIndexProgress(progress);
  }

  private BonsaiArchiveWorldStateProvider createProvider(final boolean archiveModeReady) {
    return createProviderInternal(archiveModeReady, false);
  }

  private BonsaiArchiveWorldStateProvider createProviderWithProofs(
      final boolean archiveModeReady, final boolean stateProofsEnabled) {
    return createProviderInternal(archiveModeReady, stateProofsEnabled);
  }

  private BonsaiArchiveWorldStateProvider createProviderWithTrieNodeIndex(
      final boolean archiveModeReady, final boolean trieNodeIndexEnabled) {
    return createProviderInternal(archiveModeReady, trieNodeIndexEnabled);
  }

  private BonsaiArchiveWorldStateProvider createProviderInternal(
      final boolean archiveModeReady, final boolean stateProofsEnabled) {
    final var config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(MAX_LAYERS)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(stateProofsEnabled)
                            .build())
                    .build())
            .build();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), config);
    if (archiveModeReady) {
      worldStateStorage.upgradeToArchiveFlatDbMode();
    }

    // Seed the chain head block hash so the head world state is cached under it,
    // allowing non-historical queries to find it via cachedWorldStorageManager.
    final var tx = worldStateStorage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        chainHeadHeader.getHash().getBytes().toArrayUnsafe());
    tx.commit();

    return new BonsaiArchiveWorldStateProvider(
        worldStateStorage,
        blockchain,
        config,
        null,
        null,
        EvmConfiguration.DEFAULT,
        () -> null,
        new CodeCache(),
        new NoOpMetricsSystem());
  }
}
