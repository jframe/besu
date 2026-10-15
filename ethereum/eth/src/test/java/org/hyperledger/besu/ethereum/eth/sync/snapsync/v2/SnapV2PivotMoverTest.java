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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class SnapV2PivotMoverTest {

  /** Headers (parent, to], each the parent of the next; {@code fork} makes the hashes differ. */
  private static List<BlockHeader> extend(final BlockHeader parent, final long to, final int fork) {
    final List<BlockHeader> headers = new ArrayList<>();
    BlockHeader cursor = parent;
    for (long i = parent.getNumber() + 1; i <= to; i++) {
      cursor =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(cursor.getHash())
              .extraData(Bytes.of(fork))
              .buildHeader();
      headers.add(cursor);
    }
    return headers;
  }

  /** Headers 0..n; index i is block i. */
  private static List<BlockHeader> chain(final long n) {
    final List<BlockHeader> headers = new ArrayList<>();
    headers.add(new BlockHeaderTestFixture().number(0).buildHeader());
    headers.addAll(extend(headers.getFirst(), n, 0));
    return headers;
  }

  /** Fake world state: records the moves it applies. */
  private static final class FakeTarget implements SnapV2MoveTarget {
    BlockHeader pivot;
    boolean finished;
    Throwable abortedWith;
    RuntimeException failApplyWith;
    final List<SnapV2PivotMove> applied = new ArrayList<>();
    final List<BlockHeader> completionChecks = new ArrayList<>();

    FakeTarget(final BlockHeader pivot) {
      this.pivot = pivot;
    }

    @Override
    public BlockHeader currentPivot() {
      return pivot;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public boolean applyMove(final SnapV2PivotMove move) {
      if (failApplyWith != null) {
        throw failApplyWith;
      }
      if (finished) {
        return false;
      }
      applied.add(move);
      pivot = move.newPivot();
      return true;
    }

    @Override
    public boolean checkCompletion(final BlockHeader header) {
      completionChecks.add(header);
      return false;
    }

    @Override
    public void abort(final Throwable cause) {
      abortedWith = cause;
      finished = true;
    }
  }

  private final Map<Hash, BlockHeader> network = new HashMap<>();
  private final List<BlockHeader> pivotUpdates = new ArrayList<>();
  private final AtomicBoolean balsFail = new AtomicBoolean();
  private final List<List<BlockHeader>> balRequests = new ArrayList<>();

  private void serve(final List<BlockHeader> headers) {
    headers.forEach(h -> network.put(h.getHash(), h));
  }

  private SnapV2HeaderFetcher headerFetcher() {
    return new SnapV2HeaderFetcher(
        (startHash, startNumber, count) -> {
          final List<BlockHeader> out = new ArrayList<>();
          BlockHeader cursor = network.get(startHash);
          while (cursor != null && out.size() < count) {
            out.add(cursor);
            cursor = network.get(cursor.getParentHash());
          }
          return CompletableFuture.completedFuture(out);
        },
        192);
  }

  private SnapV2PivotMover.BalSource balSource() {
    return (headers, rounds) -> {
      balRequests.add(headers);
      return balsFail.get()
          ? CompletableFuture.failedFuture(new SnapV2MoveFailedException("no BALs"))
          : CompletableFuture.completedFuture(null);
    };
  }

  private SnapV2PivotMover mover(final FakeTarget target, final BlockHeader head) {
    return mover(target, head, balSource());
  }

  private SnapV2PivotMover mover(
      final FakeTarget target, final BlockHeader head, final SnapV2PivotMover.BalSource bals) {
    return new SnapV2PivotMover(
        () -> CompletableFuture.completedFuture(Optional.ofNullable(head)),
        headerFetcher(),
        bals,
        target,
        pivotUpdates::add,
        Runnable::run);
  }

  @Test
  void noMoveBelowTheThreshold() {
    final List<BlockHeader> chain = chain(195);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));

    mover(target, chain.get(195)).tick().join(); // lag 95

    assertThat(target.applied).isEmpty();
    assertThat(balRequests).isEmpty();
  }

  @Test
  void noMoveWhenTheHeadIsBehindThePivot() {
    final List<BlockHeader> chain = chain(200);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(200));

    mover(target, chain.get(150)).tick().join();

    assertThat(target.applied).isEmpty();
  }

  @Test
  void noMoveWithoutAHead() {
    final List<BlockHeader> chain = chain(200);
    final FakeTarget target = new FakeTarget(chain.get(100));

    mover(target, null).tick().join();

    assertThat(target.applied).isEmpty();
  }

  @Test
  void movesToSixtyFourBelowTheHeadAtTheThreshold() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));

    mover(target, chain.get(196)).tick().join(); // lag 96

    assertThat(target.applied).hasSize(1);
    final SnapV2PivotMove step = target.applied.getFirst();
    assertThat(step.isReorg()).isFalse();
    assertThat(step.ancestor()).isEqualTo(chain.get(100));
    assertThat(step.canonical()).containsExactlyElementsOf(chain.subList(101, 133));
    assertThat(pivotUpdates).containsExactly(chain.get(132));
    assertThat(target.completionChecks).containsExactly(chain.get(132));
  }

  @Test
  void aLargeGapIsAppliedAsOneMoveToTheTarget() {
    // intermediate pivots would be outside the peers' serving window, so a gap is never split:
    // the storage roots a move fetches must be at a pivot peers still serve
    final List<BlockHeader> chain = chain(464);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));

    mover(target, chain.get(464)).tick().join(); // target 400: 300 blocks

    assertThat(target.applied).hasSize(1);
    assertThat(target.applied.getFirst().canonical())
        .containsExactlyElementsOf(chain.subList(101, 401));
    assertThat(pivotUpdates).containsExactly(chain.get(400));
  }

  @Test
  void aFailedMoveRechecksCompletionAtTheCurrentPivot() {
    // a task that completed during the move's drain had its completion check skipped
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    balsFail.set(true);

    mover(target, chain.get(196)).tick().join();

    assertThat(target.completionChecks).containsExactly(chain.get(100));
  }

  @Test
  void anUnrecoverableApplyAbortsTheDownload() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    target.failApplyWith = new ReorgUnrecoverableException("below BAL activation");

    mover(target, chain.get(196)).tick().join();

    assertThat(target.abortedWith).isSameAs(target.failApplyWith);
  }

  @Test
  void aSynchronousFailureDoesNotStopLaterTicks() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    final SnapV2PivotMover mover =
        new SnapV2PivotMover(
            () -> {
              if (firstCall.getAndSet(false)) {
                throw new IllegalStateException("head lookup failed");
              }
              return CompletableFuture.completedFuture(Optional.of(chain.get(196)));
            },
            headerFetcher(),
            balSource(),
            target,
            pivotUpdates::add,
            Runnable::run);

    mover.tick().join();
    mover.tick().join();

    assertThat(target.pivot).isEqualTo(chain.get(132));
  }

  @Test
  void aFailedBalFetchLeavesThePivotAndTheNextTickRetries() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    final SnapV2PivotMover mover = mover(target, chain.get(196));

    balsFail.set(true);
    mover.tick().join();
    assertThat(target.applied).isEmpty();
    assertThat(target.pivot).isEqualTo(chain.get(100));
    assertThat(target.abortedWith).isNull();

    balsFail.set(false);
    mover.tick().join();
    assertThat(target.pivot).isEqualTo(chain.get(132));
  }

  @Test
  void noApplyWhenTheTargetFinishedDuringTheFetch() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    final SnapV2PivotMover mover =
        mover(
            target,
            chain.get(196),
            (headers, rounds) -> {
              target.finished = true; // completes while the BALs are fetched
              return CompletableFuture.completedFuture(null);
            });

    mover.tick().join();

    assertThat(target.applied).isEmpty();
    assertThat(pivotUpdates).isEmpty();
  }

  @Test
  void stopDuringAFetchPreventsTheApply() {
    final List<BlockHeader> chain = chain(196);
    serve(chain);
    final FakeTarget target = new FakeTarget(chain.get(100));
    final SnapV2PivotMover[] holder = new SnapV2PivotMover[1];
    holder[0] =
        mover(
            target,
            chain.get(196),
            (headers, rounds) -> {
              holder[0].stop();
              return CompletableFuture.completedFuture(null);
            });

    holder[0].tick().join();

    assertThat(target.applied).isEmpty();
  }

  @Test
  void aReorgOfThePivotBuildsASegmentFromTheCommonAncestor() {
    final List<BlockHeader> base = chain(98);
    final List<BlockHeader> orphaned = extend(base.get(98), 100, 1); // 99s, 100s
    final List<BlockHeader> canonical = extend(base.get(98), 196, 2); // 99c .. 196c
    serve(base);
    serve(orphaned);
    serve(canonical);
    final FakeTarget target = new FakeTarget(orphaned.getLast());

    mover(target, canonical.getLast()).tick().join();

    assertThat(target.applied).hasSize(1);
    final SnapV2PivotMove move = target.applied.getFirst();
    assertThat(move.isReorg()).isTrue();
    assertThat(move.ancestor()).isEqualTo(base.get(98));
    assertThat(move.orphaned()).containsExactlyElementsOf(orphaned);
    assertThat(move.newPivot().getNumber()).isEqualTo(132);
    assertThat(balRequests.getFirst()).containsExactlyElementsOf(orphaned); // orphaned BALs first
  }

  @Test
  void missingOrphanedBalsAbortTheDownload() {
    final List<BlockHeader> base = chain(98);
    final List<BlockHeader> orphaned = extend(base.get(98), 100, 1);
    final List<BlockHeader> canonical = extend(base.get(98), 196, 2);
    serve(base);
    serve(orphaned);
    serve(canonical);
    final FakeTarget target = new FakeTarget(orphaned.getLast());

    mover(
            target,
            canonical.getLast(),
            (headers, rounds) ->
                rounds == SnapV2BalFetcher.ORPHANED_ROUNDS
                    ? CompletableFuture.failedFuture(new SnapV2MoveFailedException("no BALs"))
                    : CompletableFuture.completedFuture(null))
        .tick()
        .join();

    assertThat(target.applied).isEmpty();
    assertThat(target.abortedWith).isNotNull();
  }

  @Test
  void aReorgDeeperThanTheBoundAbortsTheDownload() {
    final List<BlockHeader> base = chain(1);
    final List<BlockHeader> orphaned = extend(base.get(1), 200, 1); // forks at block 1
    final List<BlockHeader> canonical = extend(base.get(1), 296, 2);
    serve(base);
    serve(orphaned);
    serve(canonical);
    final FakeTarget target = new FakeTarget(orphaned.getLast()); // pivot 200, ancestor 1

    mover(target, canonical.getLast()).tick().join();

    assertThat(target.applied).isEmpty();
    assertThat(target.abortedWith).isInstanceOf(ReorgUnrecoverableException.class);
  }
}
