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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.NavigableMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Stitches downloaded account ranges into the Forest tracked account trie. */
final class ForestTrieStitcher {

  private final WorldStateStorageCoordinator coordinator;

  ForestTrieStitcher(final WorldStateStorageCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * Stitches the downloaded account range into the Forest tracked trie.
   *
   * <p>Opens the trie at {@code currentTrackedRoot}, puts every account KV pair, commits (writing
   * only the dirty spine), and returns the new root. The caller must write the returned root to the
   * Forest pointer in the same updater batch.
   */
  Bytes32 stitchAccounts(
      final Bytes32 currentTrackedRoot,
      final NavigableMap<Bytes32, Bytes> downloadedAccounts,
      final WorldStateKeyValueStorage.Updater updater) {

    if (downloadedAccounts.isEmpty()) {
      return currentTrackedRoot;
    }

    final Function<Bytes, Bytes> identity = Function.identity();
    final NodeLoader loader =
        (location, hash) -> coordinator.getAccountStateTrieNode(location, hash);

    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(loader, currentTrackedRoot, identity, identity);

    for (final var entry : downloadedAccounts.entrySet()) {
      trie.put(entry.getKey(), entry.getValue());
    }

    trie.commit(
        (location, hash, value) ->
            WorldStateStorageCoordinator.applyForStrategy(
                updater,
                onBonsai -> {},
                onForest -> onForest.putAccountStateTrieNode(hash, value)));

    return Bytes32.wrap(trie.getRootHash());
  }
}
