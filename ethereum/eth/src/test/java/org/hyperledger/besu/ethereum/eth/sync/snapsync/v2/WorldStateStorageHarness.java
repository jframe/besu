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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Seeds and reads world state through either Bonsai (flat DB) or Forest (MPT) for applier tests.
 */
interface WorldStateStorageHarness {
  WorldStateStorageCoordinator coordinator();

  /** Returns {@code true} for Forest harnesses; {@code false} for Bonsai harnesses. */
  default boolean isForest() {
    return false;
  }

  /**
   * Returns the underlying {@link ForestWorldStateKeyValueStorage}. Throws for non-Forest
   * harnesses.
   */
  default ForestWorldStateKeyValueStorage forestStorage() {
    throw new UnsupportedOperationException(
        "forestStorage() is only available for Forest harnesses");
  }

  /**
   * Persists the given root as the Forest account-trie root pointer. Throws for non-Forest
   * harnesses.
   */
  default void seedAccountTrieRoot(final Bytes32 root) {
    throw new UnsupportedOperationException(
        "seedAccountTrieRoot() is only available for Forest harnesses");
  }

  /**
   * Returns the current committed account-trie root. Forest harnesses return the root tracked
   * internally; calling this on a Bonsai harness throws.
   */
  default Bytes32 commitAndGetAccountRoot() {
    throw new UnsupportedOperationException(
        "commitAndGetAccountRoot() is only available for Forest harnesses");
  }

  /**
   * Called after an applier batch commit to hand back the new account-trie root. No-op: Forest
   * harnesses read the root from storage, so this is no longer needed.
   */
  default void updateAccountRoot(final Bytes32 newRoot) {}

  void seedAccount(Address address, long nonce, Wei balance, Hash storageRoot, Hash codeHash);

  void seedStorageSlot(Address address, UInt256 slotKey, UInt256 value);

  void seedCode(Address address, Bytes code);

  Optional<PmtStateTrieAccountValue> readAccount(Address address);

  Optional<UInt256> readStorageSlot(Address address, UInt256 slotKey);

  Optional<Bytes> readCode(Address address);

  String label();

  @Override
  String toString();
}
