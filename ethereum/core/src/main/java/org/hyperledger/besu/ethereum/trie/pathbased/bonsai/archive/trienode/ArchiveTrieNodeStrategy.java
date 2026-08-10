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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that captures every trie-node write into the bonsai archive column
 * family ({@code TRIE_BRANCH_STORAGE_ARCHIVE}) so that historical {@code eth_getProof} requests can
 * be served without replaying trie-log diffs.
 *
 * <p>Each put is delegated to the wrapped {@code base} strategy first (live flat DB), then, if the
 * capture gate is open, the full bare-RLP node is written into the archive in the same transaction
 * under an {@link ArchiveNodeKey} that encodes the block number.
 *
 * <p>The gate is a {@code LongSupplier} returning the highest block safe to archive. In practice,
 * it returns {@code Long.MAX_VALUE} while the node is behind the network head ({@code
 * !syncState.isInSync()}) and {@code Long.MIN_VALUE} once at the head, preventing live blocks
 * within the reorg window from entering the archive. Block 0 (genesis) is always captured.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy base;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveNodeHistoryProgress historyProgress;
  private final LongSupplier highestSafeBlockSupplier;

  private long cachedBlockNumber = Long.MIN_VALUE;
  private boolean blockNumberCached = false;
  private long capturedBlock = Long.MIN_VALUE;
  private SegmentedKeyValueStorageTransaction owningTransaction;

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveNodeHistoryProgress historyProgress,
      final LongSupplier highestSafeBlockSupplier) {
    this.base = Objects.requireNonNull(base);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.highestSafeBlockSupplier = Objects.requireNonNull(highestSafeBlockSupplier);
  }

  private boolean shouldCapture(final long block) {
    return block == 0L || block <= highestSafeBlockSupplier.getAsLong();
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    if (!blockNumberCached) {
      cachedBlockNumber =
          storage
              .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
              .map(b -> Bytes.wrap(b).toLong() + 1L)
              .orElse(0L);
      blockNumberCached = true;
    }
    return cachedBlockNumber;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return base.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    if (shouldCapture(block)) {
      historyStore.put(transaction, ArchiveNodeKey.account(location), block, node);
      capturedBlock = block;
      owningTransaction = transaction;
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
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    if (shouldCapture(block)) {
      historyStore.put(
          transaction, ArchiveNodeKey.storage(accountHash.getBytes(), location), block, node);
      capturedBlock = block;
      owningTransaction = transaction;
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return;
    }
    blockNumberCached = false;
    if (capturedBlock == Long.MIN_VALUE) {
      return;
    }
    historyProgress.setIndexStartBlock(capturedBlock);
    historyProgress.setLastIndexedBlock(capturedBlock);
    historyProgress.save(transaction);
    capturedBlock = Long.MIN_VALUE;
    owningTransaction = null;
  }

  @Override
  public void onDiscard(final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return;
    }
    blockNumberCached = false;
    capturedBlock = Long.MIN_VALUE;
    owningTransaction = null;
  }
}
