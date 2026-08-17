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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so historical {@code eth_getProof} requests don't need trie-log
 * replay.
 *
 * <p>Each put delegates to {@code base} (live flat DB) first, then — if the archive gate is open —
 * reads the prior node value from the layered storage chain on the import thread (where it is
 * typically in the top few in-memory layers) and enqueues a {@link CaptureRequest} for async
 * processing. Workers compute the history entry ({@link ArchiveNodeHistoryStore#getLatestBefore} +
 * encode FULL/DIFF) off the import thread. Results are joined and applied to the transaction in
 * {@link #onBeforeCommit}, which runs immediately before commit.
 *
 * <p>Reading the prior node on the import thread — rather than inside a worker — avoids disk I/O in
 * the common case (the prior value is hot in the in-memory layered chain) and eliminates any need
 * for worker threads to access the layered storage. An archiving gap — gate closed then reopened,
 * or a restart — forces the next block to write FULL, since the newest archive entry no longer
 * matches the flat DB.
 *
 * <p>The buffer is owned by the transaction whose puts filled it. This strategy instance is shared
 * by every Updater created from the same storage. {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator}
 * opens its own updater mid-block and calls {@code commitTrieLogOnly()} (→ {@code onRollback}).
 * {@link #onBeforeCommit} and {@link #onRollback} therefore ignore calls carrying a transaction
 * other than the owning one.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy {

  /**
   * One buffered write awaiting capture computation. {@code newNode == null} signals removal.
   * {@code priorNode} is the committed value at this location before the current block's write,
   * resolved on the import thread from the layered storage chain.
   */
  record CaptureRequest(
      Bytes naturalKey,
      Bytes location,
      long block,
      Hash accountHash,
      Bytes32 nodeHash,
      Bytes newNode,
      Bytes priorNode) {}

  /** A computed history entry ready to apply to the transaction. */
  record CaptureResult(Bytes historyKey, Bytes storedValue) {}

  private static final int CAPTURE_CHUNK_SIZE = 64;
  private static final AtomicInteger CAPTURE_THREAD_COUNTER = new AtomicInteger();

  /**
   * Shared capture pool. Deliberately NOT the trie ForkJoinPool: that pool is saturated with
   * hashing exactly while captures run, and capture tasks are read-latency-bound, not CPU-bound.
   * Daemon threads — process-lifetime, no shutdown needed.
   */
  private static final ExecutorService CAPTURE_POOL =
      Executors.newFixedThreadPool(
          Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() / 2)),
          runnable -> {
            final Thread thread =
                new Thread(runnable, "trie-capture-" + CAPTURE_THREAD_COUNTER.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          });

  private final TrieNodeStrategy base;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveNodeHistoryProgress historyProgress;
  private final BooleanSupplier archiveGate;

  // Block-number cache — cleared in onBeforeCommit/onRollback so the next block re-reads it.
  private long cachedBlockNumber = Long.MIN_VALUE;
  private boolean blockNumberCached = false;

  // Gate decision cached per block: block-import is single-threaded, so no synchronisation needed.
  private long gatedBlockNumber = Long.MIN_VALUE;
  private boolean gatedCapture = false;

  // Per-block capture buffer.
  private final List<CaptureRequest> pendingRequests = new ArrayList<>();
  private long bufferedBlock = Long.MIN_VALUE;
  private boolean bufferedChainContiguous = false;
  private final List<Future<List<CaptureResult>>> inFlight = new ArrayList<>();
  // The transaction whose puts filled the buffer; flush/discard from any other transaction is
  // ignored (see class javadoc). Identity comparison: each Updater holds one transaction object.
  private SegmentedKeyValueStorageTransaction owningTransaction;

  private long lastArchivedBlock = -1L;

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveNodeHistoryProgress historyProgress,
      final BooleanSupplier archiveGate) {
    this.base = Objects.requireNonNull(base);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.archiveGate = Objects.requireNonNull(archiveGate);
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

  private boolean shouldCaptureBlock(final long block) {
    if (block != gatedBlockNumber) {
      gatedCapture = block == 0L || archiveGate.getAsBoolean();
      gatedBlockNumber = block;
    }
    return gatedCapture;
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
    final long block = currentBlockNumber(storage);
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null);
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.account(location), location, block, null, nodeHash, node, prior),
          transaction);
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
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage).orElse(null);
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.storage(accountHash.getBytes(), location),
              location,
              block,
              accountHash,
              nodeHash,
              node,
              prior),
          transaction);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (shouldCaptureBlock(block)) {
      final Bytes prior =
          block == 0L
              ? null
              : base.getFlatAccountTrieNode(location, Bytes32.ZERO, storage).orElse(null);
      enqueue(
          new CaptureRequest(
              ArchiveNodeKey.account(location), location, block, null, Bytes32.ZERO, null, prior),
          transaction);
    }
  }

  private void enqueue(
      final CaptureRequest request, final SegmentedKeyValueStorageTransaction transaction) {
    if (pendingRequests.isEmpty() && inFlight.isEmpty()) {
      bufferedBlock = request.block();
      bufferedChainContiguous = request.block() == lastArchivedBlock + 1L;
      owningTransaction = transaction;
    } else if (bufferedBlock != request.block()) {
      throw new IllegalStateException(
          "trie-node capture buffer holds block "
              + bufferedBlock
              + " but received a write for block "
              + request.block()
              + " — previous block was neither flushed nor discarded");
    }
    pendingRequests.add(request);
    if (pendingRequests.size() >= CAPTURE_CHUNK_SIZE) {
      submitChunk();
    }
  }

  private void submitChunk() {
    final List<CaptureRequest> chunk = List.copyOf(pendingRequests);
    final boolean chainContiguous = bufferedChainContiguous;
    pendingRequests.clear();
    inFlight.add(
        CAPTURE_POOL.submit(
            () -> {
              final List<CaptureResult> results = new ArrayList<>(chunk.size());
              for (final CaptureRequest request : chunk) {
                computeCapture(request, chainContiguous).ifPresent(results::add);
              }
              return results;
            }));
  }

  // Safe to call from any thread; never touches storage or the transaction.
  // Returns empty for a removal of a node with no live prior (nothing to record).
  private Optional<CaptureResult> computeCapture(
      final CaptureRequest request, final boolean chainContiguous) {
    final Bytes priorNode = request.priorNode();

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
    if (request.location().isEmpty() || request.block() == 0L || !chainContiguous) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final Optional<ArchiveNodeHistoryStore.HistoryEntry> priorEntryOpt =
        historyStore.getLatestBefore(request.naturalKey(), request.block() - 1L);
    if (priorEntryOpt.isEmpty() || priorEntryOpt.get().codecEntry().isDeletion()) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    final int counter = priorEntryOpt.get().counter() + 1;
    if (counter >= ArchiveHistoryReader.CHECKPOINT_INTERVAL) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(request.newNode())));
    }
    return Optional.of(
        result(request, counter, ArchiveTrieNodeCodec.encodeDiff(priorNode, request.newNode())));
  }

  private static CaptureResult result(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new CaptureResult(
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        ArchiveNodeHistoryStore.encodeStoredValue(counter, codecEntry));
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return;
    }
    blockNumberCached = false;
    if (pendingRequests.isEmpty() && inFlight.isEmpty()) {
      return;
    }
    final long block = bufferedBlock;
    if (!pendingRequests.isEmpty()) {
      submitChunk();
    }
    try {
      // Belt-and-braces: keyed by historyKey, last write wins — matches sequential tx.put order
      // (chunks are joined in submission order, which is put order).
      final Map<Bytes, Bytes> results = new LinkedHashMap<>();
      for (final Future<List<CaptureResult>> future : inFlight) {
        for (final CaptureResult r : future.get()) {
          results.put(r.historyKey(), r.storedValue());
        }
      }
      results.forEach((key, value) -> historyStore.putEncoded(transaction, key, value));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted while flushing trie-node captures", e);
    } catch (final ExecutionException e) {
      throw new RuntimeException("trie-node capture failed", e.getCause());
    } finally {
      inFlight.clear();
      pendingRequests.clear();
      bufferedBlock = Long.MIN_VALUE;
      owningTransaction = null;
    }
    historyProgress.record(transaction, block);
    lastArchivedBlock = block;
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return;
    }
    // Join rather than cancel: cancel(true) only sets the interrupt flag and does not prevent a
    // worker mid-read from accessing storage that gets closed during a pipeline-abort rollback.
    // Joining ensures every worker has finished reading committed storage before we return.
    boolean interrupted = false;
    for (final Future<List<CaptureResult>> future : inFlight) {
      try {
        future.get();
      } catch (final InterruptedException e) {
        interrupted = true;
      } catch (final CancellationException | ExecutionException ignored) {
        // already cancelled or failed — results are discarded either way
      }
    }
    inFlight.clear();
    pendingRequests.clear();
    bufferedBlock = Long.MIN_VALUE;
    blockNumberCached = false;
    owningTransaction = null;
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
