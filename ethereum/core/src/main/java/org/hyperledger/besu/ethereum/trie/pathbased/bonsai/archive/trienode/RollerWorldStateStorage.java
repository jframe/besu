/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import org.apache.tuweni.bytes.Bytes;

/**
 * The roller's scratch world-state storage, seeded at a block from the trie-node archive instead of
 * by rolling back a snapshot of head.
 *
 * <p>The base reads nothing from head except contract code, so head clears and flat-DB upgrades
 * cannot invalidate it. Trie nodes come from the archive at the state's current block; flat values
 * from the batch's own writes, else a trie walk. Each {@link #seed} returns a fresh layer, which
 * bounds the scratch writes held in memory to one batch.
 */
public final class RollerWorldStateStorage {

  private final BonsaiWorldStateKeyValueStorage base;

  public RollerWorldStateStorage(
      final BonsaiWorldStateKeyValueStorage headStorage, final ArchiveTrieNodeStrategy capture) {
    this.base =
        new BonsaiWorldStateKeyValueStorage(
            new RollerFlatDbStrategyProvider(
                headStorage.getFlatDbStrategy().getCodeStorageStrategy()),
            new CodeOnlyKeyValueStorage(headStorage.getComposedWorldStateStorage()),
            headStorage.getTrieLogStorage(),
            FlatDbCacheManager.NO_OP_CACHE,
            0L,
            new RollerTrieNodeStrategy(capture));
  }

  /**
   * Returns a fresh scratch layer holding the world state at {@code header}.
   *
   * @param header the block to seed at; its trie must be in the archive
   * @return the seeded layer
   */
  public BonsaiWorldStateLayerStorage seed(final BlockHeader header) {
    final BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(base);
    final SegmentedKeyValueStorageTransaction tx =
        layer.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, header.getStateRoot().getBytes().toArrayUnsafe());
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        header.getBlockHash().getBytes().toArrayUnsafe());
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
    tx.commit();
    return layer;
  }
}
