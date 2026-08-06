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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * <p>Capture is buffered per block and applied at {@link #flushCaptures}, which the Updater invokes
 * before every composed-tx commit. The diff base is read from committed storage at flush time;
 * during sequential import the live flat DB still holds block {@code N-1}'s value at that moment
 * (the block's own writes sit in the uncommitted transaction), so the read is the correct
 * previous-block diff base — same value the prior inline implementation read.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  /**
   * One buffered write awaiting capture computation. accountHash null => account trie; newNode null
   * => removal.
   */
  record CaptureRequest(
      Bytes naturalKey,
      Bytes location,
      long block,
      Hash accountHash,
      Bytes32 nodeHash,
      Bytes newNode) {}

  /** A computed history entry ready to apply to the transaction. */
  record CaptureResult(Bytes historyKey, Bytes storedValue) {}

  private final TrieNodeStrategy baseStrategy;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeHistoryProgress historyProgress;
  private volatile LongSupplier highestSafeBlockSupplier;
  // Gate decision cached per block: block-import is single-threaded, so no synchronisation needed.
  // Invalidated when highestSafeBlockSupplier changes and on each block transition.
  private long gatedBlockNumber = Long.MIN_VALUE;
  private boolean gatedCapture = false;

  private final List<CaptureRequest> pendingRequests = new ArrayList<>();
  private long bufferedBlock = Long.MIN_VALUE;

  // WORLD_BLOCK_NUMBER_KEY is constant within a block (only this import thread's uncommitted tx
  // changes it); cache it between flush/discard boundaries instead of reading it on every put.
  private long cachedBlockNumber = Long.MIN_VALUE;
  private boolean blockNumberCached = false;

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
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.account(location), location, block, null, nodeHash, node));
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
    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.storage(accountHash.getBytes(), location),
              location,
              block,
              accountHash,
              nodeHash,
              node));
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (shouldCaptureBlock(block)) {
      enqueue(
          new CaptureRequest(ArchiveNodeKey.account(location), location, block, null, null, null));
    }
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

  private void enqueue(final CaptureRequest request) {
    if (!pendingRequests.isEmpty() && bufferedBlock != request.block()) {
      throw new IllegalStateException(
          "trie-node capture buffer holds block "
              + bufferedBlock
              + " but received a write for block "
              + request.block()
              + " — previous block was neither flushed nor discarded");
    }
    bufferedBlock = request.block();
    pendingRequests.add(request);
  }

  /**
   * Computes the history entry for one buffered write. Reads only committed storage (the block's
   * own writes sit in the uncommitted transaction), so during sequential import the flat DB still
   * holds block N-1's value — the correct diff base. Safe to call from any thread; never touches
   * the transaction. Returns empty for a removal of a node with no live prior (nothing to record).
   */
  private Optional<CaptureResult> computeCapture(
      final CaptureRequest request, final SegmentedKeyValueStorage storage) {
    final Bytes priorNode =
        request.accountHash() == null
            ? baseStrategy
                .getFlatAccountTrieNode(request.location(), request.nodeHash(), storage)
                .orElse(null)
            : baseStrategy
                .getFlatStorageTrieNode(
                    request.accountHash(), request.location(), request.nodeHash(), storage)
                .orElse(null);

    if (request.newNode() == null) { // removal
      if (priorNode == null) {
        return Optional.empty();
      }
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeDiff(priorNode, null)));
    }
    if (priorNode == null) {
      return Optional.of(
          result(request, 0, ArchiveTrieNodeCodec.encodeDiff(null, request.newNode())));
    }
    if (request.location().isEmpty()) { // roots are always FULL — no seek needed
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final Optional<TrieNodeHistoryStore.HistoryEntry> priorEntryOpt =
        historyStore.getLatestBefore(request.naturalKey(), request.block());
    if (priorEntryOpt.isEmpty() || priorEntryOpt.get().codecEntry().isDeletion()) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final int priorCounter = priorEntryOpt.get().counter();
    if (priorCounter + 1 >= TrieNodeHistoryReader.CHECKPOINT_INTERVAL) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    return Optional.of(
        result(
            request,
            priorCounter + 1,
            ArchiveTrieNodeCodec.encodeDiff(priorNode, request.newNode())));
  }

  private static CaptureResult result(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new CaptureResult(
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        TrieNodeHistoryStore.encodeStoredValue(counter, codecEntry));
  }

  @Override
  public void flushCaptures(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    blockNumberCached = false; // the commit this precedes will change WORLD_BLOCK_NUMBER_KEY
    if (pendingRequests.isEmpty()) {
      return;
    }
    final long block = bufferedBlock;
    try {
      // Belt-and-braces: keyed by historyKey, last write wins — matches sequential tx.put order.
      final Map<Bytes, Bytes> results = new LinkedHashMap<>();
      for (final CaptureRequest request : pendingRequests) {
        computeCapture(request, storage)
            .ifPresent(r -> results.put(r.historyKey(), r.storedValue()));
      }
      results.forEach((key, value) -> historyStore.putEncoded(transaction, key, value));
    } finally {
      pendingRequests.clear();
      bufferedBlock = Long.MIN_VALUE;
    }
    historyProgress.setLastIndexedBlock(block);
    historyProgress.setIndexStartBlock(block);
    historyProgress.save(transaction);
  }

  @Override
  public void discardCaptures() {
    pendingRequests.clear();
    bufferedBlock = Long.MIN_VALUE;
    blockNumberCached = false;
  }
}
