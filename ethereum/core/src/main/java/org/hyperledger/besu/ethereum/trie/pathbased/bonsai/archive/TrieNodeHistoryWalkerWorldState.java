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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

/**
 * Assembles and owns an isolated {@link BonsaiWorldState} used by the trie-node history walker.
 *
 * <p>The world state is wired with:
 *
 * <ul>
 *   <li>A {@link HistoryOnlyWriteStorage} filter so that {@code persist()} never overwrites live
 *       flat DB state — only writes to {@code TRIE_NODE_HISTORY_ARCHIVE} reach real storage.
 *   <li>A per-block {@link BonsaiArchiveTrieNodeStrategy} installed via {@link
 *       #setStrategyForBlock(long)}.
 *   <li>{@link PathBasedWorldStateUpdateAccumulator} flags {@code skipCodeRoll} and {@code
 *       trustTrieLogPriorValue} set once at construction.
 * </ul>
 *
 * <p>The walker is single-threaded; storage is built once and the strategy is swapped per block via
 * {@link BonsaiWorldStateKeyValueStorage#setTrieNodeStrategy}, which is read fresh at every {@code
 * updater()} call.
 */
public class TrieNodeHistoryWalkerWorldState {

  private final BonsaiWorldStateKeyValueStorage walkerStorage;
  private final BonsaiWorldState worldState;
  private final TrieNodeHistoryReader historyReader;
  private final TrieNodeHistoryStore historyStore;

  /**
   * Constructs the walker world state.
   *
   * @param flatDbStrategyProvider the node's existing flat-DB strategy provider (reused, not owned)
   * @param composedWorldStateStorage the node's live composed storage (reads forwarded, writes
   *     filtered)
   * @param historyReader reads historical trie-node versions from the archive column family
   * @param historyStore writes trie-node history entries to the archive column family
   */
  public TrieNodeHistoryWalkerWorldState(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final TrieNodeHistoryReader historyReader,
      final TrieNodeHistoryStore historyStore) {
    this.historyReader = historyReader;
    this.historyStore = historyStore;

    final SegmentedKeyValueStorage filtered =
        new HistoryOnlyWriteStorage(composedWorldStateStorage);
    final PathBasedCodeCache codeCache = new PathBasedCodeCache();
    this.walkerStorage =
        new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider,
            filtered,
            new InMemoryKeyValueStorage(), // trie logs: never written, never read
            FlatDbCacheManager.NO_OP_CACHE,
            0L);
    this.worldState =
        new BonsaiWorldState(
            walkerStorage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                walkerStorage, EvmConfiguration.DEFAULT, codeCache),
            new WalkerTrieLogManager(),
            EvmConfiguration.DEFAULT,
            WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
                .parallelStateRootComputationEnabled(false)
                .build(),
            codeCache);
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>) worldState.updater();
    accumulator.setSkipCodeRoll(true);
    accumulator.setTrustTrieLogPriorValue(true);
  }

  /**
   * Returns the assembled world state. The caller (Task 5 walker) uses this to replay trie logs and
   * call {@code persist()}.
   */
  public BonsaiWorldState getWorldState() {
    return worldState;
  }

  /**
   * Installs a fresh {@link BonsaiArchiveTrieNodeStrategy} for the given block number. Must be
   * called before replaying the trie log for that block.
   *
   * <p>Safe because {@link BonsaiWorldStateKeyValueStorage#updater()} reads the strategy field at
   * call time — no {@code Updater} instance caches it across blocks.
   *
   * @param blockNumber the block whose history is about to be captured
   */
  public void setStrategyForBlock(final long blockNumber) {
    walkerStorage.setTrieNodeStrategy(
        new BonsaiArchiveTrieNodeStrategy(historyReader, historyStore, blockNumber));
  }

  /**
   * Local no-op subclass of {@link NoOpTrieLogManager}.
   *
   * <p>Although {@link NoOpTrieLogManager#saveTrieLog} does not NPE (its constructor calls {@code
   * super(null, null, 0, null)}, and {@code setupTrieLogFactory(null)} falls back to {@code new
   * BonsaiTrieLogFactory()} rather than returning null), it still unnecessarily serializes the full
   * accumulator state into a {@link org.hyperledger.besu.plugin.services.trielogs.TrieLog} object.
   * The walker replays blocks solely to capture trie-node history; producing trie logs is wrong and
   * wasteful, so this subclass overrides {@code saveTrieLog} to truly do nothing.
   */
  private static final class WalkerTrieLogManager extends NoOpTrieLogManager {
    @Override
    public synchronized void saveTrieLog(
        final PathBasedWorldStateUpdateAccumulator<?> localUpdater,
        final Hash forWorldStateRootHash,
        final BlockHeader forBlockHeader,
        final PathBasedWorldState forWorldState) {
      // intentionally empty: the walker replays blocks to capture trie-node history only;
      // it must never produce or persist trie logs
    }
  }
}
