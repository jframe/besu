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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The roller's {@link TrieNodeStrategy}: reads the trie at the roller world state's current block
 * from the trie-node archive, and hands every write and commit hook to the capturing {@link
 * ArchiveTrieNodeStrategy}.
 *
 * <p>The current block is the one recorded in the storage a read is made against. The roller
 * commits block b's history before it rolls block b + 1, so that trie is always in the archive. The
 * caller ({@code BonsaiWorldStateKeyValueStorage.getTrieNode}) verifies the node hash.
 */
public final class RollerTrieNodeStrategy implements TrieNodeStrategy {

  private final ArchiveTrieNodeStrategy capture;
  private final ArchiveHistoryReader historyReader;

  public RollerTrieNodeStrategy(final ArchiveTrieNodeStrategy capture) {
    this.capture = Objects.requireNonNull(capture);
    this.historyReader = Objects.requireNonNull(capture.historyReader());
  }

  @Override
  public Optional<Bytes> getTrieNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final Bytes32 nodeHash) {
    final long block =
        storage
            .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
            .map(b -> Bytes.wrap(b).toLong())
            .orElseThrow(
                () -> new IllegalStateException("roller storage has no world-state block number"));
    return historyReader.nodeAt(ArchiveNodeKey.of(key), block);
  }

  @Override
  public void putTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key,
      final Bytes32 nodeHash,
      final Bytes node) {
    capture.putTrieNode(storage, transaction, key, nodeHash, node);
  }

  @Override
  public void removeTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key) {
    capture.removeTrieNode(storage, transaction, key);
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    capture.onBeforeCommit(storage, transaction);
  }

  @Override
  public void onBeforeBlockCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final long blockNumber) {
    capture.onBeforeBlockCommit(storage, transaction, blockNumber);
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    capture.onRollback(transaction);
  }

  @Override
  public boolean requiresPerBlockPersist() {
    return capture.requiresPerBlockPersist();
  }
}
