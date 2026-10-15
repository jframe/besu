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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the trie-node archive advancing after initial sync. Once a block is at or below the {@link
 * ArchiveFrontier}, the roller recomputes its trie on a scratch world state and lets {@link
 * ArchiveTrieNodeStrategy} capture it, committing the history to canonical storage while the
 * scratch state's own writes are discarded.
 *
 * <p>Each catch-up seeds an uncached world state over a layer on a snapshot of head, rolls it back
 * to the last indexed block, then rolls forward one block per persist so every block's trie-node
 * changes are captured individually. Runs on its own single thread, single-flight.
 */
public class ArchiveTrieNodeRoller implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeRoller.class);

  static final int MIN_BATCH = 16;
  static final int MAX_BATCH = 256;

  private final BonsaiWorldStateProvider provider;
  private final Blockchain blockchain;
  private final ArchiveTrieNodeStrategy strategy;
  private final ArchiveCoverageTracker coverage;
  private final LongSupplier frontier;
  private final ScheduledExecutorService executor;
  private final int minBatch;
  private final int maxBatch;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile boolean closed;
  private OptionalLong observerId = OptionalLong.empty();

  public ArchiveTrieNodeRoller(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final ArchiveTrieNodeStrategy strategy,
      final LongSupplier frontier,
      final ScheduledExecutorService executor) {
    this(provider, blockchain, strategy, frontier, executor, MIN_BATCH, MAX_BATCH);
  }

  @VisibleForTesting
  ArchiveTrieNodeRoller(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final ArchiveTrieNodeStrategy strategy,
      final LongSupplier frontier,
      final ScheduledExecutorService executor,
      final int minBatch,
      final int maxBatch) {
    this.provider = Objects.requireNonNull(provider);
    this.blockchain = Objects.requireNonNull(blockchain);
    this.strategy = Objects.requireNonNull(strategy);
    this.coverage = strategy.getCoverageTracker();
    this.frontier = Objects.requireNonNull(frontier);
    this.executor = Objects.requireNonNull(executor);
    this.minBatch = minBatch;
    this.maxBatch = maxBatch;
  }

  /** Observes new canonical heads and schedules a catch-up when enough blocks are final. */
  public synchronized void start() {
    observerId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    scheduleCatchUpIfNeeded();
                  }
                }));
    scheduleCatchUpIfNeeded();
  }

  private long target() {
    return Math.min(frontier.getAsLong(), blockchain.getChainHeadBlockNumber());
  }

  void scheduleCatchUpIfNeeded() {
    if (closed) {
      return;
    }
    final OptionalLong last = coverage.lastIndexedBlock();
    if (last.isEmpty() || target() - last.getAsLong() < minBatch) {
      return;
    }
    if (running.compareAndSet(false, true)) {
      try {
        executor.execute(this::catchUp);
      } catch (final RejectedExecutionException e) {
        running.set(false);
      }
    }
  }

  @VisibleForTesting
  void catchUp() {
    boolean failed = false;
    try {
      while (!closed) {
        final OptionalLong last = coverage.lastIndexedBlock();
        if (last.isEmpty()) {
          return;
        }
        final long target = target();
        if (target <= last.getAsLong()) {
          return;
        }
        rollBatch(last.getAsLong(), Math.min(target, last.getAsLong() + maxBatch));
      }
    } catch (final RuntimeException e) {
      failed = true;
      LOG.error(
          "Archive trie-node roller failed after block {}; retrying on the next canonical head",
          coverage.lastIndexedBlock().orElse(-1L),
          e);
    } finally {
      running.set(false);
      if (!failed) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  private void rollBatch(final long cursor, final long end) {
    final BlockHeader cursorHeader = header(cursor);
    final BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(provider.getWorldStateKeyValueStorage());
    try {
      // a bare snapshot's transaction is a shared no-op, so a mutable layer holds the scratch
      // writes
      final BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(snapshot);
      final BonsaiWorldState worldState = provider.createUncachedWorldState(layer);
      try {
        if (!provider.rollWorldStateToBlockHash(worldState, cursorHeader.getHash())) {
          throw new IllegalStateException("could not seed roller world state at block " + cursor);
        }
        final SegmentedKeyValueStorage scratch = layer.getComposedWorldStateStorage();
        strategy.registerRollerStorage(scratch);
        try {
          for (long block = cursor + 1; block <= end && !closed; block++) {
            rollForward(worldState, header(block));
          }
        } finally {
          strategy.unregisterRollerStorage(scratch);
        }
      } finally {
        worldState.close();
      }
    } finally {
      try {
        snapshot.close();
      } catch (final Exception e) {
        LOG.debug("Failed to close roller snapshot", e);
      }
    }
    LOG.debug("Archive trie-node roller indexed blocks {}..{}", cursor + 1, end);
  }

  private void rollForward(final BonsaiWorldState worldState, final BlockHeader header) {
    final TrieLog trieLog =
        provider
            .getTrieLogManager()
            .getTrieLogLayer(header.getHash())
            .orElseThrow(
                () ->
                    new IllegalStateException("missing trie log for block " + header.getNumber()));
    final PathBasedWorldStateUpdateAccumulator<?> updater = worldState.updater();
    updater.rollForward(trieLog);
    updater.commit();
    // persist verifies the computed root against the header before the archive commit
    worldState.persist(header);
    if (coverage.lastIndexedBlock().orElse(-1L) != header.getNumber()) {
      throw new IllegalStateException(
          "roller persisted block " + header.getNumber() + " but archive coverage did not advance");
    }
  }

  private BlockHeader header(final long number) {
    return blockchain
        .getBlockHeader(number)
        .orElseThrow(() -> new IllegalStateException("missing block header " + number));
  }

  @Override
  public synchronized void close() {
    closed = true;
    observerId.ifPresent(blockchain::removeObserver);
    observerId = OptionalLong.empty();
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Archive trie-node roller did not stop within 10s");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
