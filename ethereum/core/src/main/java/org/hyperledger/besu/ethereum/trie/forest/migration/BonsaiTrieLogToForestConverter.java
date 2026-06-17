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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;

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
  // Retained for use by applyTrieLog (added in subsequent step); not yet read by the skeleton.
  @SuppressWarnings("UnusedVariable")
  private final ForestWorldStateKeyValueStorage forestStorage;

  // Reassigned by applyTrieLog (added in a subsequent step), so cannot be final.
  @SuppressWarnings("FieldCanBeFinal")
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
}
