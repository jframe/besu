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
package org.hyperledger.besu.ethereum.trie.forest.migration;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Rebuilds a Forest world-state node set from Bonsai {@link
 * org.hyperledger.besu.plugin.services.trielogs.TrieLog}s by replaying each block's state diff
 * directly into Merkle-Patricia Tries at the hash level, without re-executing any EVM transactions.
 *
 * <p>Each applied trie log mutates the account state trie (and, where required, per-account storage
 * tries) and writes the resulting nodes into the supplied {@link ForestWorldStateKeyValueStorage}.
 * After applying a layer the reconstructed state root is verified against the expected state root
 * carried by the block; a mismatch indicates the replay diverged from the canonical chain and the
 * changes for that layer are rolled back.
 */
public class BonsaiTrieLogToForestConverter {
  private final ForestWorldStateKeyValueStorage forestStorage;
  private Bytes32 currentRootHash;

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage.
   *
   * @param forestStorage the Forest world-state storage to populate
   */
  public BonsaiTrieLogToForestConverter(final ForestWorldStateKeyValueStorage forestStorage) {
    this.forestStorage = forestStorage;
    this.currentRootHash = Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());
  }

  /**
   * Returns the current account state trie root hash reconstructed so far.
   *
   * @return the current state root hash
   */
  public Hash currentRootHash() {
    return Hash.wrap(currentRootHash);
  }

  /**
   * Replays a single Bonsai trie log into the Forest account state trie, persists the resulting
   * trie nodes, and verifies that the reconstructed state root matches the block's expected state
   * root.
   *
   * @param layer the Bonsai trie log describing the block's state diff
   * @param expectedStateRoot the canonical post-block state root to verify against
   * @return the reconstructed (and verified) state root
   * @throws IllegalStateException if the reconstructed state root does not match the expected root
   */
  public Hash applyTrieLog(final TrieLog layer, final Hash expectedStateRoot) {
    final ForestWorldStateKeyValueStorage.Updater updater = forestStorage.updater();
    final NodeLoader accountLoader =
        (location, hash) -> forestStorage.getAccountStateTrieNode(hash);
    final StoredMerklePatriciaTrie<Bytes32, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<>(accountLoader, currentRootHash, b -> b, b -> b);

    final Map<Address, ? extends TrieLog.LogTuple<AccountValue>> accountChanges =
        layer.getAccountChanges();
    for (final var entry : accountChanges.entrySet()) {
      final Address address = entry.getKey();
      final AccountValue updated = entry.getValue().getUpdated();
      final Bytes32 addressHash = Bytes32.wrap(address.addressHash().getBytes());
      if (updated == null) {
        accountTrie.remove(addressHash);
      } else {
        accountTrie.put(addressHash, RLP.encode(updated::writeTo));
      }
    }

    accountTrie.commit((location, hash, value) -> updater.putAccountStateTrieNode(hash, value));
    final Bytes32 newRoot = accountTrie.getRootHash();
    if (!newRoot.equals(Bytes32.wrap(expectedStateRoot.getBytes()))) {
      updater.rollback();
      throw new IllegalStateException(
          "Reconstructed state root "
              + Hash.wrap(newRoot)
              + " does not match expected "
              + expectedStateRoot);
    }
    updater.commit();
    currentRootHash = newRoot;
    return Hash.wrap(newRoot);
  }
}
