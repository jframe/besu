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
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Archive-aware trie node strategy for the live block-import path. Reads and writes delegate to a
 * base {@link TrieNodeStrategy} (the live flat DB); writes additionally capture a FULL/DIFF history
 * entry and advance {@link TrieNodeHistoryProgress}.
 *
 * <p>Capture is gated so a block {@code N} is only recorded when {@code N == 0} (genesis, always
 * final) or {@code N <= highestSafeBlock}, where {@code highestSafeBlock = bestChainHeight -
 * maxLayersToLoad}. This trails the head by {@code maxLayersToLoad}, matching {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiFlatDbToArchiveMigrator}, and
 * never records a reorg-window block. The gate never suppresses the delegated live write — block
 * import must always proceed.
 *
 * <p>The diff base is the value read from the base strategy <em>before</em> the put. During
 * sequential import the live flat DB still holds block {@code N-1}'s value at that moment, so the
 * live read is the correct previous-block diff base.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy baseStrategy;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeHistoryProgress historyProgress;
  private volatile LongSupplier highestSafeBlockSupplier;
  private volatile long lastSavedProgressBlock = Long.MIN_VALUE;
  // Gate decision cached per block: block-import is single-threaded, so no synchronisation needed.
  // Invalidated when highestSafeBlockSupplier changes and on each block transition.
  private long gatedBlockNumber = Long.MIN_VALUE;
  private boolean gatedCapture = false;

  public BonsaiArchiveTrieNodeStrategy(
      final TrieNodeStrategy baseStrategy,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeHistoryProgress historyProgress,
      final LongSupplier highestSafeBlockSupplier) {
    this.baseStrategy = Objects.requireNonNull(baseStrategy);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.highestSafeBlockSupplier = Objects.requireNonNull(highestSafeBlockSupplier);
  }

  /**
   * Replaces the "highest safe block to capture" supplier. Used during startup wiring once {@code
   * syncState} exists; before that a placeholder keeps the gate closed for all blocks except
   * genesis.
   */
  public void setHighestSafeBlockSupplier(final LongSupplier supplier) {
    this.highestSafeBlockSupplier = Objects.requireNonNull(supplier);
    this.gatedBlockNumber = Long.MIN_VALUE; // Invalidate gate cache on supplier change
  }

  private boolean shouldCapture(final long block) {
    return block == 0L || block <= highestSafeBlockSupplier.getAsLong();
  }

  private boolean shouldCaptureBlock(final long block) {
    if (block != gatedBlockNumber) {
      gatedCapture = shouldCapture(block);
      gatedBlockNumber = block;
    }
    return gatedCapture;
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
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCaptureBlock(block);
    final Bytes priorNode =
        capture
            ? baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null)
            : null;
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (capture) {
      captureTrieNodeDiff(
          transaction, ArchiveNodeKey.account(location), location, block, priorNode, node);
      advanceHistoryProgress(transaction, block);
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
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCaptureBlock(block);
    final Bytes priorNode =
        capture
            ? baseStrategy
                .getFlatStorageTrieNode(accountHash, location, nodeHash, storage)
                .orElse(null)
            : null;
    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    if (capture) {
      captureTrieNodeDiff(
          transaction,
          ArchiveNodeKey.storage(accountHash.getBytes(), location),
          location,
          block,
          priorNode,
          node);
      advanceHistoryProgress(transaction, block);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    final boolean capture = shouldCaptureBlock(block);
    // nodeHash is unknown at removal time; BonsaiTrieNodeStrategy ignores it (plain point lookup).
    final Bytes priorNode =
        capture ? baseStrategy.getFlatAccountTrieNode(location, null, storage).orElse(null) : null;
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (capture && priorNode != null) {
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
    // Established pattern, mirrored from
    // BonsaiArchiveFlatDbStrategy.getStateArchiveContextForWrite:
    // current committed WORLD_BLOCK_NUMBER_KEY + 1, or 0 if absent (genesis).
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
}
