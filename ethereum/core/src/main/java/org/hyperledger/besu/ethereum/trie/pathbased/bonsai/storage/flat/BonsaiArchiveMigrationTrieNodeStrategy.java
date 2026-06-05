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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;

/**
 * Trie node strategy used by the Bonsai archive flat-DB migrator while it recomputes the historical
 * trie at each checkpoint.
 *
 * <p>Reads first check {@code TRIE_BRANCH_STORAGE_ARCHIVE} (via the parent's nearest-before lookup
 * for previously persisted checkpoint nodes) then fall back to the standard {@code
 * TRIE_BRANCH_STORAGE} for genesis/unchanged nodes that have not yet been written to the archive
 * CF. Writes go only to {@code TRIE_BRANCH_STORAGE_ARCHIVE}.
 *
 * <p>When the trie-node differential index is enabled (non-null {@code historyStore} / {@code
 * changeIndex}), each {@code put} also captures a diff-codec entry and change-block index entry so
 * that migrated blocks contribute to the historical proof index.
 */
public class BonsaiArchiveMigrationTrieNodeStrategy extends BonsaiArchiveTrieNodeStrategy {

  /**
   * Creates a migration strategy without trie-node differential index support.
   *
   * @param trieNodeCheckpointInterval the archive checkpoint interval
   * @param trieLoader optional trie loader for cache warming
   */
  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    // Base strategy falls back to TRIE_BRANCH_STORAGE so genesis/unchanged nodes are accessible
    // before the first checkpoint persist writes them to the archive CF.
    super(trieNodeCheckpointInterval, trieLoader, new BonsaiTrieNodeStrategy());
  }

  /**
   * Creates a migration strategy with trie-node differential index support enabled.
   *
   * <p>Each node write during trie-replay checkpoints will also emit a diff-codec entry into {@code
   * historyStore} and a change-block record into {@code changeIndex}, exactly as the live block
   * import path does via {@link BonsaiArchiveTrieNodeStrategy}.
   *
   * @param trieNodeCheckpointInterval the archive checkpoint interval (must not be null)
   * @param trieLoader optional trie loader for cache warming
   * @param historyStore the diff-entry store to write history entries to
   * @param changeIndex the change-block index to record mutations in
   * @param progress the coverage-progress tracker to advance after each block; may be null
   */
  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval,
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex,
      final TrieNodeIndexProgress progress) {
    super(
        trieNodeCheckpointInterval,
        trieLoader,
        new BonsaiTrieNodeStrategy(),
        true,
        historyStore,
        changeIndex,
        progress);
  }
}
