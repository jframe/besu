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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Archive trie node strategy for read-only archive proof serving. Reads and writes delegate to the
 * base strategy (live trie reads/writes). The append-only history capture is handled exclusively by
 * {@link org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.ArchiveTrieBuilder} during
 * migration; the live block-import path no longer captures diffs (Design-5 Part 5 redesign).
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  /**
   * Plain point-lookup strategy used for the "current trie" reads/writes. Defaults to {@link
   * BonsaiTrieNodeStrategy} over TRIE_BRANCH_STORAGE.
   */
  protected final TrieNodeStrategy baseStrategy;

  private final BonsaiCachedMerkleTrieLoader trieLoader;

  public BonsaiArchiveTrieNodeStrategy() {
    this(null, new BonsaiTrieNodeStrategy());
  }

  public BonsaiArchiveTrieNodeStrategy(final BonsaiCachedMerkleTrieLoader trieLoader) {
    this(trieLoader, new BonsaiTrieNodeStrategy());
  }

  protected BonsaiArchiveTrieNodeStrategy(
      final BonsaiCachedMerkleTrieLoader trieLoader, final TrieNodeStrategy baseStrategy) {
    this.trieLoader = trieLoader;
    this.baseStrategy = baseStrategy;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (trieLoader != null) {
      trieLoader.putAccountNode(nodeHash, node);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    if (trieLoader != null) {
      trieLoader.putStorageNode(nodeHash, node);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }
}
