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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/**
 * The roller's flat reads: a value written earlier in the batch, else a walk of the trie at the
 * state's current block (which the roller's trie-node strategy reads from the archive). Owned by
 * the roller rather than reusing the deprecated partial flat-DB strategy.
 */
final class RollerFlatDbStrategy extends BonsaiFlatDbStrategy {

  RollerFlatDbStrategy(final CodeStorageStrategy codeStorageStrategy) {
    super(new NoOpMetricsSystem(), codeStorageStrategy);
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage) {
    final Optional<Bytes> flat =
        storage.get(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
    if (flat.isPresent()) {
      return flat;
    }
    return worldStateRootHashSupplier
        .get()
        .flatMap(
            root ->
                new StoredMerklePatriciaTrie<>(
                        new StoredNodeFactory<>(
                            nodeLoader, Function.identity(), Function.identity()),
                        Bytes32.wrap(root))
                    .get(accountHash.getBytes()));
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {
    final Optional<Bytes> flat =
        storage
            .get(
                ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes())
                    .toArrayUnsafe())
            .map(Bytes::wrap);
    if (flat.isPresent()) {
      return flat;
    }
    final Optional<Hash> storageRoot = storageRootSupplier.get();
    if (storageRoot.isEmpty() || worldStateRootHashSupplier.get().isEmpty()) {
      return Optional.empty();
    }
    return new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity()),
            Bytes32.wrap(storageRoot.get().getBytes()))
        .get(storageSlotKey.getSlotHash().getBytes())
        .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));
  }
}
