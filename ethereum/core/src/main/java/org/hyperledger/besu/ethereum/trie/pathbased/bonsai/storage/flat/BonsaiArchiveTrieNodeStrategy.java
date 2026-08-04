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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Archive-aware trie node strategy: reads delegate to {@code baseStrategy} unchanged; writes
 * additionally capture a FULL/DIFF history entry and advance {@link TrieNodeHistoryProgress}.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy baseStrategy;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeHistoryProgress historyProgress;

  public BonsaiArchiveTrieNodeStrategy(
      final TrieNodeStrategy baseStrategy,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeHistoryProgress historyProgress) {
    this.baseStrategy = Objects.requireNonNull(baseStrategy);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
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
    // Read the prior node through baseStrategy, NOT via a hardcoded TRIE_BRANCH_STORAGE get:
    // baseStrategy's target segment is configurable (Task 7), and the diff base must come from
    // wherever baseStrategy actually writes. BonsaiTrieNodeStrategy's getter does no hash
    // filtering, which is exactly what's wanted here (we want the stored bytes, whatever they are).
    final Bytes priorNode =
        baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null);
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    captureTrieNodeDiff(
        transaction, ArchiveNodeKey.account(location), location, block, priorNode, node);
    advanceHistoryProgress(transaction, block);
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes priorNode =
        baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage).orElse(null);
    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    captureTrieNodeDiff(
        transaction,
        ArchiveNodeKey.storage(accountHash.getBytes(), location),
        location,
        block,
        priorNode,
        node);
    advanceHistoryProgress(transaction, block);
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    // nodeHash is genuinely unknown at removal time. BonsaiTrieNodeStrategy ignores the parameter
    // (it does a plain point lookup), so null is safe here — see the javadoc added to
    // TrieNodeStrategy's two read methods (Task 7) stating that implementations must tolerate a
    // null nodeHash, so this contract is explicit rather than incidental.
    final Bytes priorNode =
        baseStrategy.getFlatAccountTrieNode(location, null, storage).orElse(null);
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (priorNode != null) {
      final long block = currentBlockNumber(storage);
      historyStore.put(
          transaction,
          ArchiveNodeKey.account(location),
          block,
          0,
          ArchiveTrieNodeCodec.encodeDiff(priorNode, null));
      advanceHistoryProgress(transaction, block);
    }
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    // Established pattern, reused verbatim from
    // BonsaiArchiveFlatDbStrategy.getStateArchiveContextForWrite
    // (ethereum/core/.../bonsai/archive/BonsaiArchiveFlatDbStrategy.java:57-74): current committed
    // WORLD_BLOCK_NUMBER_KEY + 1, or 0 if absent (genesis). See Step 6 for the block-1 regression
    // test.
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
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

  /**
   * Last block for which progress was already persisted, to avoid re-saving on every node write.
   */
  private volatile long lastSavedProgressBlock = Long.MIN_VALUE;

  private void advanceHistoryProgress(
      final SegmentedKeyValueStorageTransaction tx, final long block) {
    historyProgress.setLastIndexedBlock(block);
    historyProgress.setIndexStartBlock(block);
    // A block writes thousands of trie nodes; persist the (16-byte, idempotent) progress record
    // once per block rather than once per node.
    if (block != lastSavedProgressBlock) {
      historyProgress.save(tx);
      lastSavedProgressBlock = block;
    }
  }

  /** Shared with the read path so writer and reader observe the same coverage window (Task 13). */
  public TrieNodeHistoryStore getHistoryStore() {
    return historyStore;
  }

  /** Shared with the read path — see {@link #getHistoryStore()}. */
  public TrieNodeHistoryProgress getHistoryProgress() {
    return historyProgress;
  }
}
