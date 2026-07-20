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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * {@link TrieNodeStrategy} decorator that measures the real FULL/DIFF encoded size of every
 * trie-node write during a calibration replay, without changing the write path's behavior.
 *
 * <p>For each write, both the {@link TrieNodeDiffCodec#encodeFull} and {@link
 * TrieNodeDiffCodec#encodeDiff} sizes are computed (regardless of which one the real write path
 * would choose) and accumulated into a {@link CalibrationResult}, keyed by nibble-path depth and
 * node shape (branch vs short). The prior node is read from {@code TRIE_BRANCH_STORAGE} before
 * delegating, mirroring the read-before-write pattern in {@code
 * BonsaiArchiveTrieNodeStrategy#putFlatAccountTrieNode}. Reads and removals are pure delegation —
 * they are not part of what calibration measures.
 */
public final class RecordingTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy delegate;
  private final CalibrationResult result = new CalibrationResult();

  public RecordingTrieNodeStrategy(final TrieNodeStrategy delegate) {
    this.delegate = delegate;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return delegate.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return delegate.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Optional<Bytes> prior =
        storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe()).map(Bytes::wrap);
    record(prior.orElse(null), node, location.size(), location.size() + 8, true);

    delegate.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
    final Optional<Bytes> prior =
        storage.get(TRIE_BRANCH_STORAGE, accountHashLocation.toArrayUnsafe()).map(Bytes::wrap);
    record(prior.orElse(null), node, location.size(), accountHashLocation.size() + 8, false);

    delegate.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    delegate.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  /** Returns the {@link CalibrationResult} accumulated from writes observed so far. */
  public CalibrationResult result() {
    return result;
  }

  private void record(
      final Bytes prior,
      final Bytes newNode,
      final int depth,
      final int keySize,
      final boolean isAccountPath) {
    final int fullSize = TrieNodeDiffCodec.encodeFull(newNode).size();
    final int diffSize =
        prior == null ? fullSize : TrieNodeDiffCodec.encodeDiff(prior, newNode).size();
    final boolean isBranch = TrieNodeDiffCodec.nodeArity(newNode) == 17;
    result.record(depth, isBranch, fullSize, diffSize, keySize, isAccountPath);
  }
}
