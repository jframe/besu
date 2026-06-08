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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Default trie node strategy. Reads and writes go to a single plain-key trie segment
 * (TRIE_BRANCH_STORAGE by default). The target segment is configurable so the archive migrator can
 * reuse the same point-lookup behaviour against its dedicated migration column family.
 *
 * <p><strong>Storage format:</strong> values are stored as {@code nodeHash[32] ‖ nodeBytes}. The
 * 32-byte keccak hash prefix enables a hash-first fast path in {@code ArchiveProofNodeLoader}: if
 * the stored hash matches the expected hash the live node is at the target block and no history
 * lookup is needed (1 read total for cold nodes).
 */
public class BonsaiTrieNodeStrategy implements TrieNodeStrategy {

  /** Number of bytes occupied by the hash prefix stored before each node value. */
  public static final int HASH_PREFIX_BYTES = 32;

  private final SegmentIdentifier trieSegment;

  public BonsaiTrieNodeStrategy() {
    this(TRIE_BRANCH_STORAGE);
  }

  public BonsaiTrieNodeStrategy(final SegmentIdentifier trieSegment) {
    this.trieSegment = trieSegment;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return storage
        .get(trieSegment, location.toArrayUnsafe())
        .filter(raw -> raw.length >= HASH_PREFIX_BYTES)
        .map(raw -> stripHashPrefix(raw));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return storage
        .get(trieSegment, Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe())
        .filter(raw -> raw.length >= HASH_PREFIX_BYTES)
        .map(raw -> stripHashPrefix(raw));
  }

  static Bytes stripHashPrefix(final byte[] raw) {
    final int nodeLen = raw.length - HASH_PREFIX_BYTES;
    return nodeLen == 0 ? Bytes.EMPTY : Bytes.wrap(raw, HASH_PREFIX_BYTES, nodeLen);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    transaction.put(
        trieSegment,
        location.toArrayUnsafe(),
        Bytes.concatenate(nodeHash, node).toArrayUnsafe());
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    transaction.put(
        trieSegment,
        Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe(),
        Bytes.concatenate(nodeHash, node).toArrayUnsafe());
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    transaction.remove(trieSegment, location.toArrayUnsafe());
  }
}
