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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Archive-aware trie node strategy: reads resolve the node as of the previous block via {@link
 * TrieNodeHistoryReader}; writes capture a FULL/DIFF history entry via {@link
 * TrieNodeHistoryStore}. One immutable instance per block — the block number is explicit in the
 * constructor rather than inferred from live storage.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeHistoryReader historyReader;
  private final TrieNodeHistoryStore historyStore;
  private final long blockNumber;

  public BonsaiArchiveTrieNodeStrategy(
      final TrieNodeHistoryReader historyReader,
      final TrieNodeHistoryStore historyStore,
      final long blockNumber) {
    this.historyReader = Objects.requireNonNull(historyReader);
    this.historyStore = Objects.requireNonNull(historyStore);
    if (blockNumber < 0) {
      throw new IllegalArgumentException("blockNumber must be >= 0, got " + blockNumber);
    }
    this.blockNumber = blockNumber;
  }

  /**
   * Reads resolve the node as of the PREVIOUS block, which is the base this block diffs against.
   */
  private Optional<Bytes> priorVersionOf(final Bytes naturalKey) {
    if (blockNumber == 0) {
      // Genesis has no prior version, and nodeAt rejects negative blocks.
      return Optional.empty();
    }
    return historyReader.nodeAt(naturalKey, blockNumber - 1);
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return priorVersionOf(ArchiveNodeKey.account(location));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return priorVersionOf(ArchiveNodeKey.storage(accountHash.getBytes(), location));
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes naturalKey = ArchiveNodeKey.account(location);
    captureTrieNodeDiff(
        transaction,
        naturalKey,
        location,
        blockNumber,
        priorVersionOf(naturalKey).orElse(null),
        node);
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);
    captureTrieNodeDiff(
        transaction,
        naturalKey,
        location,
        blockNumber,
        priorVersionOf(naturalKey).orElse(null),
        node);
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final Bytes naturalKey = ArchiveNodeKey.account(location);
    priorVersionOf(naturalKey)
        .ifPresent(
            priorNode ->
                historyStore.put(
                    transaction,
                    naturalKey,
                    blockNumber,
                    0,
                    ArchiveTrieNodeCodec.encodeDiff(priorNode, null)));
  }

  private void captureTrieNodeDiff(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Bytes priorNode,
      final Bytes newNode) {
    if (priorNode == null) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeDiff(null, newNode));
      return;
    }
    final Optional<TrieNodeHistoryStore.HistoryEntry> priorEntryOpt =
        historyStore.getLatestBefore(naturalKey, block);
    if (priorEntryOpt.isEmpty() || priorEntryOpt.get().codecEntry().isDeletion()) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
      return;
    }
    if (location.isEmpty()) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
      return;
    }
    final int priorCounter = priorEntryOpt.get().counter();
    if (priorCounter + 1 >= TrieNodeHistoryReader.CHECKPOINT_INTERVAL) {
      historyStore.put(tx, naturalKey, block, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
    } else {
      historyStore.put(
          tx,
          naturalKey,
          block,
          priorCounter + 1,
          ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode));
    }
  }
}
