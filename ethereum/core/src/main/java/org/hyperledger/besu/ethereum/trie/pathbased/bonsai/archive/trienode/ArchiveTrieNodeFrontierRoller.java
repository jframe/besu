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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background component that keeps the trie-node archive advancing after initial sync.
 *
 * <p>Trails a finalized frontier ({@code finalized} block, or {@code head − maxLayersToLoad} for
 * chains without finality). Per catch-up: takes a mutable layer over canonical head, rolls it back
 * to the cursor, then rolls forward one block at a time. Each block's {@code persist()} recomputes
 * the trie and emits {@code putFlat*TrieNode} writes that the archive capture strategy records via
 * a canonical archive transaction. Base (layer) writes stay in the discarded layer, never reaching
 * canonical. Because the frontier is always ≤ the finality/retention horizon, the capture path
 * never observes a reorg.
 */
public class ArchiveTrieNodeFrontierRoller implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeFrontierRoller.class);

  static final byte[] ARCHIVE_TRIE_NODE_SYNC_PROGRESS =
      "ARCHIVE_TRIE_NODE_SYNC_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final BonsaiWorldStateProvider worldStateProvider;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final ExecutorService trieCapturePool;
  private final int shallowCheckpointInterval;
  private final int deepCheckpointInterval;

  final AtomicLong cursor = new AtomicLong(0L);
  final AtomicLong ongoingTarget = new AtomicLong(0L);
  final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  volatile OptionalLong blockObserverId = OptionalLong.empty();
  private volatile boolean closed = false;

  public ArchiveTrieNodeFrontierRoller(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final BonsaiWorldStateProvider worldStateProvider,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final ExecutorService trieCapturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval) {
    this.worldStateStorage = worldStateStorage;
    this.worldStateProvider = worldStateProvider;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.trieCapturePool = trieCapturePool;
    this.shallowCheckpointInterval = shallowCheckpointInterval;
    this.deepCheckpointInterval = deepCheckpointInterval;
  }

  /**
   * Seeds the cursor from the Phase-A handoff, registers a block observer, and schedules an initial
   * catch-up.
   */
  public void startOngoing(final long initialCursor) {
    if (closed) {
      return;
    }
    final long persisted = loadPersistedCursor().orElse(0L);
    cursor.set(Math.max(initialCursor, persisted));
    ongoingTarget.set(
        FrontierTargetCalculator.computeFrontierTarget(
            blockchain, trieLogManager.getMaxLayersToLoad()));

    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    ongoingTarget.accumulateAndGet(
                        FrontierTargetCalculator.computeFrontierTarget(
                            blockchain, trieLogManager.getMaxLayersToLoad()),
                        Math::max);
                    scheduleCatchUpIfNeeded();
                  }
                }));
    scheduleCatchUpIfNeeded();
  }

  private void scheduleCatchUpIfNeeded() {
    if (closed || !catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug("Frontier roller executor shut down; skipping catch-up to {}", ongoingTarget.get());
    }
  }

  private void catchUp() {
    try {
      captureRange(cursor.get(), ongoingTarget.get());
    } finally {
      catchUpRunning.set(false);
      if (!closed && cursor.get() < ongoingTarget.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  /**
   * Captures archive-history entries for all blocks in {@code (fromExclusive, toInclusive]}. Uses a
   * mutable layer over canonical head: rolls it back to {@code fromExclusive}, then rolls forward
   * block-by-block, persisting each block so trie-node writes are captured into a canonical archive
   * transaction. The layer itself is discarded after this call (never committed to canonical).
   */
  @VisibleForTesting
  void captureRange(final long fromExclusive, final long toInclusive) {
    if (toInclusive <= fromExclusive) {
      return;
    }
    final ArchiveTrieNodeStrategy rollerStrategy =
        ArchiveTrieNodeStrategy.createRollerStrategy(
            worldStateStorage.getComposedWorldStateStorage(),
            trieCapturePool,
            shallowCheckpointInterval,
            deepCheckpointInterval);
    final BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(worldStateStorage);
    try {
      layer.setTrieNodeStrategy(rollerStrategy);
      final BonsaiWorldState state = worldStateProvider.newTrieEnabledWorldState(layer);
      positionAtCursor(state, fromExclusive);

      final ArchiveTrieNodeWriter writer = rollerStrategy.getTrieNodeWriter();

      for (long b = fromExclusive + 1; b <= toInclusive; b++) {
        final BlockHeader header = blockchain.getBlockHeader(b).orElseThrow();
        final TrieLog log = trieLogManager.getTrieLogLayer(header.getHash()).orElseThrow();

        final SegmentedKeyValueStorageTransaction canonicalTx =
            worldStateStorage.getComposedWorldStateStorage().startLowPriorityTransaction();
        writer.setArchiveWriteTransaction(canonicalTx);

        final PathBasedWorldStateUpdateAccumulator<?> acc =
            (PathBasedWorldStateUpdateAccumulator<?>) state.getAccumulator();
        acc.rollForward(log);
        acc.commit();
        state.persist(header); // recompute → emits node writes → onBeforeCommit → canonicalTx

        canonicalTx.put(
            TRIE_BRANCH_STORAGE_ARCHIVE,
            ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
            Bytes.ofUnsignedLong(b).toArrayUnsafe());
        canonicalTx.commit(); // archive history + coverage + cursor, atomically
        writer.setArchiveWriteTransaction(null);
        cursor.set(b);
      }
    } finally {
      try {
        layer.close(); // discard layer writes; never committed to canonical
      } catch (final Exception e) {
        LOG.warn("Failed to close layer storage in frontier roller", e);
      }
    }
  }

  private void positionAtCursor(final BonsaiWorldState state, final long targetBlock) {
    final long head = blockchain.getChainHeadBlockNumber();
    final PathBasedWorldStateUpdateAccumulator<?> acc =
        (PathBasedWorldStateUpdateAccumulator<?>) state.getAccumulator();
    for (long b = head; b > targetBlock; b--) {
      final BlockHeader header = blockchain.getBlockHeader(b).orElseThrow();
      final TrieLog log = trieLogManager.getTrieLogLayer(header.getHash()).orElseThrow();
      acc.rollBack(log);
    }
    acc.commit();
    // No archive redirect during positioning (override is null): any node writes go to the
    // discarded layer transaction, not to canonical.
    state.persist(blockchain.getBlockHeader(targetBlock).orElseThrow());
  }

  @VisibleForTesting
  OptionalLong loadPersistedCursor() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_TRIE_NODE_SYNC_PROGRESS)
        .map(b -> OptionalLong.of(Bytes.wrap(b).toLong()))
        .orElse(OptionalLong.empty());
  }

  public long getCursor() {
    return cursor.get();
  }

  @Override
  public void close() {
    closed = true;
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    executorService.shutdownNow();
  }
}
