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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves the snap/2 world state download to a recent pivot while it runs (cf. snap-new2
 * SyncAttempt.maybeMove / moveTowards / applyStep). Every move fetches its headers and BALs first
 * and only then asks the world state to apply them; a failed move leaves the world state untouched
 * and is retried on the next tick.
 */
public final class SnapV2PivotMover {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2PivotMover.class);

  static final int PIVOT_DISTANCE = 64;
  static final int MOVE_THRESHOLD = 32;
  static final Duration TICK = Duration.ofSeconds(2);
  // deepest reorg below the old pivot that can be recovered: the healer reads the orphaned BALs
  static final int MAX_REORG_DEPTH = 95;
  // headers of recent moves kept for reorg recovery
  private static final int REMEMBERED_HEADERS = 128;

  @FunctionalInterface
  public interface BalSource {
    CompletableFuture<Void> fetch(List<BlockHeader> headers, int rounds);
  }

  private final SnapV2HeadSource heads;
  private final SnapV2HeaderFetcher headers;
  private final BalSource bals;
  private final SnapV2MoveTarget target;
  private final PivotUpdateListener listener;
  private final Executor applyExecutor;
  private final AtomicBoolean moving = new AtomicBoolean();
  private final Map<Hash, BlockHeader> remembered = new HashMap<>();
  private volatile boolean stopped;
  private volatile ScheduledFuture<?> timer;

  public SnapV2PivotMover(
      final SnapV2HeadSource heads,
      final SnapV2HeaderFetcher headers,
      final BalSource bals,
      final SnapV2MoveTarget target,
      final PivotUpdateListener listener,
      final Executor applyExecutor) {
    this.heads = heads;
    this.headers = headers;
    this.bals = bals;
    this.target = target;
    this.listener = listener;
    this.applyExecutor = applyExecutor;
    remember(target.currentPivot());
  }

  public void start(final EthScheduler scheduler) {
    timer = scheduler.scheduleFutureTaskWithFixedDelay(this::tick, TICK, TICK);
  }

  public void stop() {
    stopped = true;
    final ScheduledFuture<?> scheduled = timer;
    if (scheduled != null) {
      scheduled.cancel(false);
    }
  }

  /** One check; at most one move runs at a time. */
  CompletableFuture<Void> tick() {
    if (inactive() || !moving.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    long from = -1;
    CompletableFuture<Void> move;
    try {
      final BlockHeader pivot = target.currentPivot();
      from = pivot.getNumber();
      move = heads.head().thenCompose(head -> head.map(h -> moveTowards(pivot, h)).orElse(done()));
    } catch (final RuntimeException e) {
      // a synchronous failure must not leave the mover stuck in "moving" or kill the timer
      move = CompletableFuture.failedFuture(e);
    }
    final long movingFrom = from;
    return move.handle(
        (ignored, error) -> {
          try {
            if (error != null && !inactive()) {
              onFailedMove(movingFrom, error);
            }
          } finally {
            moving.set(false);
          }
          return null;
        });
  }

  private void onFailedMove(final long from, final Throwable error) {
    final Throwable cause = unwrap(error);
    if (cause instanceof ReorgUnrecoverableException) {
      // the state cannot be brought to the new pivot: end the download (cf. snap-new2
      // RestartException)
      target.abort(cause);
      return;
    }
    LOG.warn("snap/2 pivot move from {} failed, retrying: {}", from, rootMessage(error));
    // a task that completed while the failed move drained skipped its completion check
    target.checkCompletion(target.currentPivot());
  }

  private CompletableFuture<Void> moveTowards(final BlockHeader pivot, final BlockHeader head) {
    if (head.getNumber() - pivot.getNumber() < PIVOT_DISTANCE + MOVE_THRESHOLD) {
      return done();
    }
    final long targetNumber = head.getNumber() - PIVOT_DISTANCE;
    return headers
        .chainDown(head, pivot.getNumber() + 1)
        .thenCompose(
            gap -> {
              final List<BlockHeader> upToTarget =
                  gap.subList(0, (int) (targetNumber - pivot.getNumber()));
              if (gap.getFirst().getParentHash().equals(pivot.getHash())) {
                // one move for the whole gap: the storage roots a move fetches must be at a
                // pivot peers still serve, which an intermediate pivot of a large gap is not
                return bals.fetch(upToTarget, SnapV2BalFetcher.MOVE_ROUNDS)
                    .thenCompose(
                        fetched -> apply(new SnapV2PivotMove(pivot, List.of(), upToTarget)))
                    .thenApply(applied -> (Void) null);
              }
              return recoverFromReorg(pivot, upToTarget);
            });
  }

  private CompletableFuture<Void> recoverFromReorg(
      final BlockHeader oldPivot, final List<BlockHeader> canonicalUpToTarget) {
    final long lowest = Math.max(1, oldPivot.getNumber() - MAX_REORG_DEPTH);
    final BlockHeader newPivot = canonicalUpToTarget.getLast();
    return headers
        .chainDown(canonicalUpToTarget.getFirst(), lowest)
        .thenCombine(
            orphanedChain(oldPivot, lowest),
            (lowerCanonical, orphaned) -> {
              // both branches from `lowest` up, ascending; the ancestor is the highest height at
              // which they share a hash (cf. snap-new2 SyncAttempt.commonAncestor)
              final List<BlockHeader> canonical = new ArrayList<>(lowerCanonical);
              canonical.addAll(canonicalUpToTarget.subList(1, canonicalUpToTarget.size()));
              final int ancestorIndex = commonAncestor(canonical, orphaned);
              if (ancestorIndex < 0) {
                throw new ReorgUnrecoverableException(
                    "the chain reorganised deeper than "
                        + MAX_REORG_DEPTH
                        + " blocks below pivot "
                        + oldPivot.getNumber());
              }
              final BlockHeader ancestor = canonical.get(ancestorIndex);
              return new SnapV2PivotMove(
                  ancestor,
                  orphaned.stream().filter(h -> h.getNumber() > ancestor.getNumber()).toList(),
                  canonical.subList(ancestorIndex + 1, canonical.size()));
            })
        .thenCompose(
            move -> {
              LOG.warn(
                  "snap/2 pivot {} was reorged out: common ancestor {}, {} orphaned blocks, new pivot {}",
                  oldPivot.getNumber(),
                  move.ancestor().getNumber(),
                  move.orphaned().size(),
                  newPivot.getNumber());
              return bals.fetch(move.orphaned(), SnapV2BalFetcher.ORPHANED_ROUNDS)
                  .exceptionallyCompose(
                      error ->
                          CompletableFuture.failedFuture(
                              new ReorgUnrecoverableException(
                                  "orphaned block access lists are unavailable: "
                                      + rootMessage(error))))
                  .thenCompose(
                      ignored -> bals.fetch(move.canonical(), SnapV2BalFetcher.MOVE_ROUNDS))
                  .thenCompose(ignored -> apply(move))
                  .thenApply(applied -> (Void) null);
            })
        .exceptionallyCompose(
            error -> {
              // the state cannot be brought to the new branch: end the download (cf. snap-new2
              // RestartException); any other failure is a retried move
              final Throwable cause = unwrap(error);
              if (cause instanceof ReorgUnrecoverableException) {
                target.abort(cause);
                return done();
              }
              return CompletableFuture.failedFuture(cause);
            });
  }

  // index of the highest canonical header that is also on the orphaned branch, -1 if none
  private static int commonAncestor(
      final List<BlockHeader> canonical, final List<BlockHeader> orphaned) {
    final Map<Long, Hash> orphanedHashes = new HashMap<>();
    orphaned.forEach(header -> orphanedHashes.put(header.getNumber(), header.getHash()));
    for (int i = canonical.size() - 1; i >= 0; i--) {
      final BlockHeader header = canonical.get(i);
      if (header.getHash().equals(orphanedHashes.get(header.getNumber()))) {
        return i;
      }
    }
    return -1;
  }

  private static Throwable unwrap(final Throwable error) {
    Throwable cause = error;
    while ((cause instanceof java.util.concurrent.CompletionException
            || cause instanceof java.util.concurrent.ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  // the old branch from the pivot down to lowest, ascending: remembered headers first, else peers
  private CompletableFuture<List<BlockHeader>> orphanedChain(
      final BlockHeader oldPivot, final long lowest) {
    final List<BlockHeader> descending = new ArrayList<>();
    BlockHeader cursor = oldPivot;
    synchronized (remembered) {
      while (cursor != null && cursor.getNumber() >= lowest) {
        descending.add(cursor);
        cursor = cursor.getNumber() == lowest ? null : remembered.get(cursor.getParentHash());
      }
    }
    if (descending.getLast().getNumber() == lowest) {
      Collections.reverse(descending);
      return CompletableFuture.completedFuture(descending);
    }
    return headers.chainDown(oldPivot, lowest);
  }

  // runs the apply off the I/O threads; false if the download finished or the mover stopped
  private CompletableFuture<Boolean> apply(final SnapV2PivotMove move) {
    if (inactive()) {
      return CompletableFuture.completedFuture(false);
    }
    return CompletableFuture.supplyAsync(() -> !inactive() && target.applyMove(move), applyExecutor)
        .thenApply(
            applied -> {
              if (applied) {
                move.canonical().forEach(this::remember);
                listener.onPivotUpdated(move.newPivot());
                target.checkCompletion(move.newPivot());
              }
              return applied;
            });
  }

  private void remember(final BlockHeader header) {
    synchronized (remembered) {
      remembered.put(header.getHash(), header);
      final long floor = header.getNumber() - REMEMBERED_HEADERS;
      remembered.values().removeIf(h -> h.getNumber() < floor);
    }
  }

  private boolean inactive() {
    return stopped || target.isFinished();
  }

  private static CompletableFuture<Void> done() {
    return CompletableFuture.completedFuture(null);
  }

  private static String rootMessage(final Throwable error) {
    Throwable cause = error;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
  }
}
