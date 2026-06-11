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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveReadFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiveWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  private final BonsaiWorldStateKeyValueStorage archiveReadStorage;
  private final CodeCache codeCache;
  private final WorldStateConfig archiveWorldStateConfig;
  private final boolean stateProofsEnabled;
  private volatile LongSupplier archiveMigrationProgressSupplier = () -> -1L;

  // Design 5 trie-node differential index — proof routing
  private final TrieNodeChangeIndex trieNodeChangeIndex;
  private final TrieNodeHistoryStore trieNodeHistoryStore;
  private final TrieNodeHistoryReader trieNodeHistoryReader;
  // volatile so that the migration thread's in-place mutations to lastIndexedBlock /
  // indexStartBlock (which are themselves volatile) are published consistently. The reference
  // is shared with the migrator; both sides mutate the same instance in-place.
  private volatile TrieNodeIndexProgress trieNodeIndexProgress;

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final DataStorageConfiguration dataStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache,
      final MetricsSystem metricsSystem) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
    this.codeCache = codeCache;
    this.archiveWorldStateConfig =
        WorldStateConfig.newBuilder(worldStateConfig).trieDisabled(true).build();
    this.stateProofsEnabled =
        dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getStateProofsEnabled();
    final BonsaiArchiveReadFlatDbStrategyProvider archiveProvider =
        new BonsaiArchiveReadFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    archiveProvider.loadFlatDbStrategy(worldStateKeyValueStorage.getComposedWorldStateStorage());
    this.archiveReadStorage =
        new BonsaiWorldStateKeyValueStorage(
            archiveProvider,
            worldStateKeyValueStorage.getComposedWorldStateStorage(),
            worldStateKeyValueStorage.getTrieLogStorage(),
            worldStateKeyValueStorage.getCacheManager(),
            worldStateKeyValueStorage.getCurrentVersion(),
            stateProofsEnabled
                ? new BonsaiArchiveTrieNodeStrategy(null) // reads archive; migrator owns all writes
                : new BonsaiTrieNodeStrategy());

    // Initialise the Design-5 trie-node index components from the shared archive storage.
    // All five ARCHIVE column families live in the same composedWorldStateStorage, so we reuse it.
    final SegmentedKeyValueStorage archiveStorage =
        worldStateKeyValueStorage.getComposedWorldStateStorage();
    this.trieNodeChangeIndex = new TrieNodeChangeIndex(archiveStorage, ArchiveNodeKey.RANGE_SIZE);
    this.trieNodeHistoryStore = new TrieNodeHistoryStore(archiveStorage);
    this.trieNodeHistoryReader =
        new TrieNodeHistoryReader(trieNodeHistoryStore, trieNodeChangeIndex);
    this.trieNodeIndexProgress =
        TrieNodeIndexProgress.load(archiveStorage, ArchiveNodeKey.RANGE_SIZE);

    // The trie-node index is written exclusively by BonsaiFlatDbToArchiveMigrator (bulk migration
    // and ongoing catch-up via migrateTrieBlock). The live block-import path must NOT write index
    // entries: during bulk migration it would append high offsets (e.g. block 6760) before the
    // migrator reaches the corresponding low offsets (e.g. block 0 for the same naturalKey),
    // violating RangeRelativeOffsetList's non-decreasing monotonicity invariant.
    //
    // The live worldStateKeyValueStorage therefore retains its default BonsaiTrieNodeStrategy (a
    // plain pass-through to TRIE_BRANCH_STORAGE that does not touch the index CFs).
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (isHistoricalQuery(queryParams)) {
      LOG.debug(
          "Returning archive state without verifying state root for block {}",
          queryParams.getBlockHeader().getNumber());
      return rollMutableArchiveStateToBlockHash(
          newFrozenArchiveWorldState(archiveWorldStateConfig),
          queryParams.getBlockHeader().getBlockHash());
    }
    return super.getWorldState(queryParams);
  }

  private BonsaiWorldState newFrozenArchiveWorldState(final WorldStateConfig config) {
    final BonsaiWorldState worldState =
        new BonsaiArchiveWorldState(this, archiveReadStorage, evmConfiguration, config, codeCache);
    // Freeze before persisting to ensure the historical block number does not affect the database
    worldState.freezeStorage();
    return worldState;
  }

  /**
   * Sets the supplier used by {@code isHistoricalQuery} to check the highest block number that has
   * been migrated to Bonsai archive storage.
   *
   * <p>Until this is called, the default supplier returns {@code -1}, which denies all
   * archive-backed historical queries and falls back to trie-log rollback via {@code super}.
   *
   * @param supplier returns the highest block number available in Bonsai archive storage
   */
  public void setArchiveMigrationProgressSupplier(final LongSupplier supplier) {
    this.archiveMigrationProgressSupplier = supplier;
  }

  /**
   * Replaces the in-memory {@link TrieNodeIndexProgress} record used by the coverage gate in {@link
   * #getAccountProof}.
   *
   * <p>This method exists primarily for testing (to inject a pre-populated progress record without
   * going through storage serialisation) and for live updates when ranges are marked complete by
   * the background indexer.
   *
   * @param progress the new progress record; must not be {@code null}
   */
  public void setTrieNodeIndexProgress(final TrieNodeIndexProgress progress) {
    this.trieNodeIndexProgress = java.util.Objects.requireNonNull(progress, "progress");
  }

  public TrieNodeHistoryStore getTrieNodeHistoryStore() {
    return trieNodeHistoryStore;
  }

  public TrieNodeChangeIndex getTrieNodeChangeIndex() {
    return trieNodeChangeIndex;
  }

  public TrieNodeIndexProgress getTrieNodeIndexProgress() {
    return trieNodeIndexProgress;
  }

  private boolean isHistoricalQuery(final WorldStateQueryParams queryParams) {
    final long queryBlock = queryParams.getBlockHeader().getNumber();
    return worldStateKeyValueStorage.getFlatDbMode().equals(FlatDbMode.ARCHIVE)
        && !queryParams.shouldWorldStateUpdateHead()
        && blockchain.getChainHeadHeader().getNumber() - queryBlock
            >= trieLogManager.getMaxLayersToLoad()
        && archiveMigrationProgressSupplier.getAsLong() >= queryBlock;
  }

  /**
   * Routes {@code eth_getProof} to the appropriate path depending on the block age and whether the
   * trie-node differential index (Design 5) covers the requested block.
   *
   * <h3>Routing logic</h3>
   *
   * <ol>
   *   <li><strong>Historical + index enabled + covered</strong> — the block is too old for trie-log
   *       rollback, the flag {@code stateProofsEnabled} is on, and {@link
   *       TrieNodeIndexProgress#covers(long)} confirms that the range containing the target block
   *       has been fully indexed. In this case an {@link ArchiveProofNodeLoader} is built and a
   *       {@link WorldStateProofProvider} is driven directly from the stateRoot stored in the block
   *       header, without any trie-log replay or archive-world-state rolling.
   *   <li><strong>All other cases</strong> — delegate to the parent implementation which either
   *       performs trie-log rollback (near-head) or the existing archive-proof rolling path.
   * </ol>
   *
   * @param blockHeader the block whose state should be proved
   * @param accountAddress the account to prove
   * @param accountStorageKeys the storage slots to include
   * @param mapper transforms the raw {@link WorldStateProof} (or empty) into the caller's return
   *     type
   * @return the mapped result, or empty if the proof could not be generated
   */
  @Override
  public <U> Optional<U> getAccountProof(
      final org.hyperledger.besu.plugin.data.BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {

    final long targetBlock = blockHeader.getNumber();
    final long headBlock = blockchain.getChainHeadHeader().getNumber();
    final long maxLayers = trieLogManager.getMaxLayersToLoad();

    // Historical + index enabled + block indexed → use Design-5 index path.
    // The archiveMigrationProgressSupplier check is intentionally OMITTED here: the Design-5
    // ArchiveProofNodeLoader reads only from TRIE_NODE_HISTORY_ARCHIVE and the live trie — it does
    // not need the flat-DB archive CFs (ACCOUNT_INFO_STATE_ARCHIVE, ACCOUNT_STORAGE_ARCHIVE) that
    // the migration populates.  Including the supplier check caused the index path to be blocked
    // whenever the migrator was behind the chain head, forcing all proofs through the legacy
    // seekForPrev rollback path even though the trie-node index was fully populated.
    //
    // covers() checks block in [indexStartBlock, lastIndexedBlock] — handles both live-import and
    // migrator paths.
    final boolean blockIsIndexed = trieNodeIndexProgress.covers(targetBlock);
    LOG.debug(
        "getAccountProof routing: target={} head={} maxLayers={} gap={} indexEnabled={}"
            + " blockIsIndexed={} lastIndexed={} indexStart={}",
        targetBlock,
        headBlock,
        maxLayers,
        headBlock - targetBlock,
        stateProofsEnabled,
        blockIsIndexed,
        trieNodeIndexProgress.lastIndexedBlock(),
        trieNodeIndexProgress.indexStartBlock());
    if (stateProofsEnabled && headBlock - targetBlock >= maxLayers && blockIsIndexed) {

      final Hash stateRoot = blockHeader.getStateRoot();
      final SegmentedKeyValueStorage liveStorage =
          worldStateKeyValueStorage.getComposedWorldStateStorage();
      final ArchiveProofNodeLoader archiveLoader =
          new ArchiveProofNodeLoader(
              trieNodeChangeIndex, trieNodeHistoryReader, liveStorage, targetBlock);

      // Build a WorldStateStorageCoordinator whose trie-node accessors delegate to the
      // ArchiveProofNodeLoader. isWorldStateAvailable always returns true: the stateRoot comes from
      // a trusted block header and the gates above confirm the archive index covers targetBlock.
      // If a node is nevertheless absent (e.g. pruned from the live trie and not yet in history),
      // the trie traversal throws MerkleTrieException, which the catch block below converts to
      // Optional.empty() — so returning true here is safe.
      final WorldStateStorageCoordinator archiveCoordinator =
          new WorldStateStorageCoordinator(worldStateKeyValueStorage) {
            @Override
            public boolean isWorldStateAvailable(final Bytes32 nodeHash, final Hash blockHash) {
              return true;
            }

            @Override
            public Optional<Bytes> getAccountStateTrieNode(
                final Bytes location, final Bytes32 nodeHash) {
              return archiveLoader.accountNodeLoader().getNode(location, nodeHash);
            }

            @Override
            public Optional<Bytes> getAccountStorageTrieNode(
                final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
              return archiveLoader
                  .storageNodeLoader(Bytes32.wrap(accountHash.getBytes()))
                  .getNode(location, nodeHash);
            }
          };

      try {
        final WorldStateProofProvider proofProvider =
            new WorldStateProofProvider(archiveCoordinator);
        return mapper.apply(
            proofProvider.getAccountProof(stateRoot, accountAddress, accountStorageKeys));
      } catch (final Exception ex) {
        LOG.error(
            "failed trie-node-index proof query for block {} ({}): {}",
            targetBlock,
            blockHeader.getBlockHash().getBytes().toShortHexString(),
            ex.getMessage(),
            ex);
        return Optional.empty();
      }
    }

    // Fall back to the parent implementation (trie-log rollback or archive-proof rolling).
    return super.getAccountProof(blockHeader, accountAddress, accountStorageKeys, mapper);
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    LOG.trace(
        "Rolling mutable archive world state to block hash {}", blockHash.getBytes().toHexString());
    try {
      // Simply persist the block hash/number and state root for this archive state
      mutableState.persist(blockchain.getBlockHeader(blockHash).get());
      LOG.trace(
          "Archive rolling finished, {} now at {}",
          mutableState.getWorldStateStorage().getClass().getSimpleName(),
          blockHash);
      return Optional.of(mutableState);
    } catch (final MerkleTrieException re) {
      // need to throw to trigger the heal
      throw re;
    } catch (final Exception e) {
      LOG.atInfo()
          .setMessage("State rolling failed on {} for block hash {}: {}")
          .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
          .addArgument(blockHash)
          .addArgument(e)
          .log();
      return Optional.empty();
    }
  }
}
