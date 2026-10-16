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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
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
 *
 * <p>The roller stops for good (until restart) rather than retrying when it can never succeed: the
 * trie log for the next block is missing (the node was not fully synced from genesis), or the seed
 * would roll back more than {@code maxLayersToLoad} blocks, whose cost scales with the gap.
 */
public class ArchiveTrieNodeRoller implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeRoller.class);

  static final int MIN_BATCH = 16;
  static final int MAX_BATCH = 256;
  static final long MAX_BACKOFF_BLOCKS = 1024L;
  static final int MAX_RESEEDS_PER_CATCH_UP = 3;
  private static final long PROGRESS_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final BonsaiWorldStateProvider provider;
  private final Blockchain blockchain;
  private final ArchiveTrieNodeStrategy strategy;
  private final ArchiveCoverageTracker coverage;
  private final LongSupplier frontier;
  private final ScheduledExecutorService executor;
  private final long maxLayersToLoad;
  private final int minBatch;
  private final int maxBatch;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile boolean closed;
  private volatile boolean stopped;
  private OptionalLong observerId = OptionalLong.empty();

  // Failure backoff: after a failed catch-up, no retry is scheduled until the chain head reaches
  // retryAtHead. Written only on the roller thread.
  private volatile long retryAtHead = Long.MIN_VALUE;
  private int consecutiveFailures;

  // Progress logging, roller thread only: INFO at most every 30 s; ranges suppressed in between
  // are folded into the next INFO line.
  private long lastProgressLogNanos;
  private boolean progressLogged;
  private long unloggedFrom = -1L;

  public ArchiveTrieNodeRoller(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final ArchiveTrieNodeStrategy strategy,
      final LongSupplier frontier,
      final ScheduledExecutorService executor,
      final MetricsSystem metricsSystem,
      final long maxLayersToLoad) {
    this(
        provider,
        blockchain,
        strategy,
        frontier,
        executor,
        metricsSystem,
        maxLayersToLoad,
        MIN_BATCH,
        MAX_BATCH);
  }

  @VisibleForTesting
  ArchiveTrieNodeRoller(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final ArchiveTrieNodeStrategy strategy,
      final LongSupplier frontier,
      final ScheduledExecutorService executor,
      final MetricsSystem metricsSystem,
      final long maxLayersToLoad,
      final int minBatch,
      final int maxBatch) {
    this.provider = Objects.requireNonNull(provider);
    this.blockchain = Objects.requireNonNull(blockchain);
    this.strategy = Objects.requireNonNull(strategy);
    this.coverage = strategy.getCoverageTracker();
    this.frontier = Objects.requireNonNull(frontier);
    this.executor = Objects.requireNonNull(executor);
    this.maxLayersToLoad = maxLayersToLoad;
    this.minBatch = minBatch;
    this.maxBatch = maxBatch;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_proofs_last_indexed_block",
        "The last block whose trie-node history is indexed for Bonsai archive proofs",
        () -> coverage.lastIndexedBlock().orElse(-1L));
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
    if (closed || stopped || blockchain.getChainHeadBlockNumber() < retryAtHead) {
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
    final long startedAfter = coverage.lastIndexedBlock().orElse(-1L);
    int reseeds = 0;
    try {
      while (!closed && !stopped) {
        final OptionalLong last = coverage.lastIndexedBlock();
        if (last.isEmpty()) {
          return;
        }
        final long target = target();
        // a residual gap below minBatch waits for more blocks rather than paying for a reseed
        if (target - last.getAsLong() < minBatch) {
          return;
        }
        if (!canSeed(last.getAsLong())) {
          stopped = true;
          return;
        }
        try {
          rollBatch(last.getAsLong(), Math.min(target, last.getAsLong() + maxBatch));
        } catch (final SnapshotInvalidatedException e) {
          if (++reseeds > MAX_RESEEDS_PER_CATCH_UP) {
            throw e;
          }
          LOG.info(
              "Bonsai archive proofs: world state storage changed under the roller (e.g. the "
                  + "flat-archive upgrade); reseeding after block {}",
              coverage.lastIndexedBlock().orElse(-1L));
        }
      }
    } catch (final RuntimeException e) {
      failed = true;
      onFailure(e);
    } finally {
      if (!failed) {
        consecutiveFailures = 0;
        retryAtHead = Long.MIN_VALUE;
      }
      logProgress(startedAfter);
      running.set(false);
      if (!failed) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  // Deterministic failures (e.g. a missing trie log deep in the seed roll) would otherwise repeat
  // on every new head: wait 1, 2, 4, ... up to MAX_BACKOFF_BLOCKS head blocks between attempts, and
  // log the stack trace only once per streak.
  private void onFailure(final RuntimeException e) {
    consecutiveFailures++;
    final long backoff = Math.min(MAX_BACKOFF_BLOCKS, 1L << Math.min(consecutiveFailures - 1, 30));
    final long head = blockchain.getChainHeadBlockNumber();
    retryAtHead = head + backoff;
    final long last = coverage.lastIndexedBlock().orElse(-1L);
    if (consecutiveFailures == 1) {
      LOG.error(
          "Archive trie-node roller failed after block {}; retrying once the chain head reaches {}",
          last,
          retryAtHead,
          e);
    } else {
      LOG.warn(
          "Archive trie-node roller failed again after block {} ({} consecutive failures): {}; "
              + "retrying once the chain head reaches {}",
          last,
          consecutiveFailures,
          e.toString(),
          retryAtHead);
    }
  }

  private void logProgress(final long startedAfter) {
    final long last = coverage.lastIndexedBlock().orElse(-1L);
    if (last <= startedAfter) {
      return;
    }
    if (unloggedFrom < 0) {
      unloggedFrom = startedAfter + 1L;
    }
    final long now = System.nanoTime();
    if (progressLogged && now - lastProgressLogNanos < PROGRESS_LOG_INTERVAL_NANOS) {
      LOG.debug("Archive trie-node roller indexed blocks {}..{}", startedAfter + 1L, last);
      return;
    }
    LOG.info(
        "Bonsai archive proofs indexed blocks {}..{} (frontier {}, head {})",
        unloggedFrom,
        last,
        frontier.getAsLong(),
        blockchain.getChainHeadBlockNumber());
    progressLogged = true;
    lastProgressLogNanos = now;
    unloggedFrom = -1L;
  }

  /**
   * Whether a batch can be seeded at {@code cursor}. False means it never will be on this run, so
   * the caller stops the roller; the reason is logged once at WARN.
   */
  private boolean canSeed(final long cursor) {
    final long next = cursor + 1L;
    final boolean hasNextTrieLog =
        blockchain
            .getBlockHeader(next)
            .flatMap(header -> provider.getTrieLogManager().getTrieLogLayer(header.getHash()))
            .isPresent();
    if (!hasNextTrieLog) {
      LOG.warn(
          "Bonsai archive proofs cannot advance past block {}: trie log for block {} is missing "
              + "(archive proofs require a full sync from genesis); proof capture stopped until "
              + "restart",
          cursor,
          next);
      return false;
    }
    final long gap = blockchain.getChainHeadBlockNumber() - cursor;
    if (gap > maxLayersToLoad) {
      LOG.warn(
          "Bonsai archive proofs stopped at block {}: {} blocks behind head exceeds "
              + "--bonsai-historical-block-limit={}; raise the limit and restart to catch up",
          cursor,
          gap,
          maxLayersToLoad);
      return false;
    }
    return true;
  }

  private void rollBatch(final long cursor, final long end) {
    final BlockHeader cursorHeader = header(cursor);
    final BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(provider.getWorldStateKeyValueStorage());
    // The head storage closes its snapshots when it is cleared or its flat DB is upgraded to
    // ARCHIVE; reads and writes on a closed snapshot fail, and nothing failing that way was
    // archived (persist verifies the root first), so the batch reseeds on fresh storage.
    final AtomicBoolean invalidated = new AtomicBoolean(false);
    final long subscriberId =
        snapshot.subscribe(
            new StorageSubscriber() {
              @Override
              public void onCloseStorage() {
                invalidated.set(true);
              }
            });
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
    } catch (final RuntimeException e) {
      if (invalidated.get()) {
        throw new SnapshotInvalidatedException(e);
      }
      throw e;
    } finally {
      snapshot.unSubscribe(subscriberId);
      // an invalidated snapshot is already closed; closing it again only logs a warning
      if (!invalidated.get()) {
        try {
          snapshot.close();
        } catch (final Exception e) {
          LOG.debug("Failed to close roller snapshot", e);
        }
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

  /** The roller's snapshot was closed under it mid-batch by a change to the head storage. */
  private static final class SnapshotInvalidatedException extends RuntimeException {
    SnapshotInvalidatedException(final Throwable cause) {
      super("roller snapshot was closed by a change to the head storage", cause);
    }
  }

  @VisibleForTesting
  boolean isStopped() {
    return stopped;
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
