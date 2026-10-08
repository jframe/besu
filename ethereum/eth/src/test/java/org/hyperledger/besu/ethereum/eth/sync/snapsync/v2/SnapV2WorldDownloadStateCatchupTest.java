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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class SnapV2WorldDownloadStateCatchupTest {

  final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
  final WorldStateStorageCoordinator coordinator =
      new WorldStateStorageCoordinator(
          new BonsaiWorldStateKeyValueStorage(
              new InMemoryKeyValueStorageProvider(),
              new NoOpMetricsSystem(),
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
  final EthContext ethContext = EthProtocolManagerTestBuilder.builder().build().ethContext();
  final List<String> events = new CopyOnWriteArrayList<>();
  final PivotUpdateListener pivotListener = h -> events.add("pivotUpdated:" + h.getNumber());

  /** Data source serving segments built from the local chain. */
  SnapV2CatchupDataSource chainSource() {
    return (current, next) -> CompletableFuture.completedFuture(b.segment(current, next));
  }

  final AtomicInteger applyCalls = new AtomicInteger();
  final AtomicLong head = new AtomicLong(0);

  /** Applier that counts BAL applications so tests can prove the world state was not touched. */
  class CountingApplier extends SnapV2BlockAccessListApplier {
    CountingApplier() {
      super(coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
    }

    @Override
    public BatchState applyBlockAccessLists(
        final List<BlockHeader> headers,
        final DownloadedAccountRangeTracker accountRangeTracker,
        final DownloadedStorageRangeTracker storageRangeTracker) {
      applyCalls.incrementAndGet();
      return super.applyBlockAccessLists(headers, accountRangeTracker, storageRangeTracker);
    }
  }

  static SnapV2ReorgHealer healer(
      final ReorgBlockchainBuilder builder, final WorldStateStorageCoordinator coordinator) {
    return new SnapV2ReorgHealer(
        builder.blockchain(),
        coordinator,
        ReorgBlockchainBuilder.balEnabledSchedule(),
        ReorgBlockchainBuilder.neverCalledFetcher());
  }

  /** Records checkCompletion calls so ordering against onPivotUpdated can be asserted. */
  class RecordingState extends SnapV2WorldDownloadState {
    final SnapSyncProcessState processState;

    RecordingState(final BlockHeader pivot, final SnapV2CatchupDataSource source) {
      this(pivot, source, pivotListener);
    }

    RecordingState(
        final BlockHeader pivot,
        final SnapV2CatchupDataSource source,
        final PivotUpdateListener listener) {
      this(pivot, source, listener, ethContext, healer(b, coordinator));
    }

    RecordingState(
        final BlockHeader pivot,
        final SnapV2CatchupDataSource source,
        final PivotUpdateListener listener,
        final EthContext context,
        final SnapV2ReorgHealer reorgHealer) {
      this(source, listener, context, reorgHealer, new SnapSyncProcessState(pivot));
    }

    private RecordingState(
        final SnapV2CatchupDataSource source,
        final PivotUpdateListener listener,
        final EthContext context,
        final SnapV2ReorgHealer reorgHealer,
        final SnapSyncProcessState processState) {
      super(
          coordinator,
          new SnapSyncStatePersistenceManager(new InMemoryKeyValueStorageProvider()),
          processState,
          new InMemoryTasksPriorityQueues<>(),
          10,
          50_000L,
          new SnapSyncMetricsManager(new NoOpMetricsSystem(), context),
          Clock.systemUTC(),
          SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
          null,
          source,
          listener,
          new CountingApplier(),
          reorgHealer,
          context,
          1000L,
          head::get);
      this.processState = processState;
    }

    BlockHeader currentPivot() {
      return processState.getPivotBlockHeader().orElseThrow();
    }

    /** Dequeues a code request so the state sees one in-flight task; complete it to drain. */
    Task<SnapDataRequest> startInflightTask() {
      enqueueRequest(
          new SnapV2BytecodeRequest(currentPivot(), Bytes32.ZERO, Bytes32.ZERO, Bytes32.ZERO));
      final Task<SnapDataRequest> task = dequeueCodeRequestBlocking();
      assertThat(task).isNotNull();
      return task;
    }

    void completeInflightTask(final Task<SnapDataRequest> task) {
      task.markCompleted();
      notifyTaskAvailable();
    }

    @Override
    public synchronized boolean checkCompletion(final BlockHeader header) {
      events.add("checkCompletion:" + header.getNumber());
      return super.checkCompletion(header);
    }
  }

  @Test
  void successfulCatchupNotifiesChainDownloaderBeforeCompletionCheck() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state = new RecordingState(b.header(3), chainSource());

    state.startPivotCatchup(p1);

    assertThat(events).containsSubsequence("pivotUpdated:5", "checkCompletion:5");
  }

  @Test
  void lateCatchupDataAfterDownloadCancelledIsIgnored() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state = new RecordingState(b.header(3), (c, n) -> pending);

    state.startPivotCatchup(p1);
    state.getDownloadFuture().cancel(true);
    pending.complete(b.segment(b.header(3), p1));

    assertThat(events).doesNotContain("pivotUpdated:5");
  }

  /** State whose storage-root refetch always fails, as when peers no longer serve the pivot. */
  class RootFetchFailingState extends RecordingState {
    RootFetchFailingState(final BlockHeader pivot, final SnapV2CatchupDataSource source) {
      super(pivot, source);
    }

    @Override
    CompletableFuture<Map<Hash, Bytes32>> fetchCorrectStorageRoots(
        final List<BlockHeader> headers, final BlockHeader newPivot) {
      return CompletableFuture.failedFuture(new IllegalStateException("peers gone"));
    }
  }

  static SnapV2CatchupDataSource failingSource() {
    return (c, n) -> CompletableFuture.failedFuture(new IllegalStateException("no peers"));
  }

  @Test
  void fetchFailureAbandonsWithoutFailingTheDownload() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state = new RecordingState(b.header(3), failingSource());

    state.startPivotCatchup(p1);

    assertThat(state.getDownloadFuture()).isNotDone();
    assertThat(state.isPivotCatchupInProgress()).isFalse();
    assertThat(events).doesNotContain("pivotUpdated:5");
  }

  @Test
  void dataSourceThrowingAbandonsWithoutFailingTheDownload() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state =
        new RecordingState(
            b.header(3),
            (c, n) -> {
              throw new IllegalStateException("boom");
            });

    state.startPivotCatchup(p1);

    assertThat(state.getDownloadFuture()).isNotDone();
    assertThat(state.isPivotCatchupInProgress()).isFalse();
    assertThat(events).contains("checkCompletion:3");
  }

  @Test
  void abandonedCatchupCanBeRetriedAndSucceed() {
    final BlockHeader p2 = b.appendCanonicalChain(b.header(0), 1L, 6);
    final AtomicInteger calls = new AtomicInteger();
    final SnapV2CatchupDataSource flaky =
        (c, n) ->
            calls.getAndIncrement() == 0
                ? CompletableFuture.failedFuture(new IllegalStateException("no peers"))
                : CompletableFuture.completedFuture(b.segment(c, n));
    final RecordingState state = new RecordingState(b.header(3), flaky);

    state.startPivotCatchup(b.header(5));
    state.startPivotCatchup(p2);

    assertThat(events).contains("pivotUpdated:6");
    assertThat(state.getDownloadFuture()).isNotDone();
  }

  @Test
  void rootFetchFailureAbandonsBeforeApplyingBals() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final RootFetchFailingState state = new RootFetchFailingState(b.header(3), chainSource());

    state.startPivotCatchup(p1);

    assertThat(applyCalls).hasValue(0);
    assertThat(state.isPivotCatchupInProgress()).isFalse();
    assertThat(state.getDownloadFuture()).isNotDone();
    assertThat(events).doesNotContain("pivotUpdated:5");
    assertThat(events).doesNotContain("checkCompletion:5");
    assertThat(events).contains("checkCompletion:3");
  }

  @Test
  void threeConsecutiveAbandonsFailTheDownload() {
    b.appendCanonicalChain(b.header(0), 1L, 8);
    final RecordingState state = new RecordingState(b.header(3), failingSource());

    state.startPivotCatchup(b.header(5));
    state.startPivotCatchup(b.header(6));
    assertThat(state.getDownloadFuture()).isNotDone();
    state.startPivotCatchup(b.header(7));

    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThatThrownBy(() -> state.getDownloadFuture().join())
        .hasCauseInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("3 consecutive");
  }

  @Test
  void balHashMismatchIsFatalOnFirstAttempt() {
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendCanonicalWithMismatchedBal(
            block1.getHeader(),
            b.balWithBalances(Map.of(Address.fromHexString("0xaa"), Wei.of(80))),
            b.balWithBalances(Map.of(Address.fromHexString("0xbb"), Wei.ONE)),
            2L);
    final RecordingState state = new RecordingState(block1.getHeader(), chainSource());

    state.startPivotCatchup(block2.getHeader());

    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThat(events).doesNotContain("pivotUpdated:2");
  }

  @Test
  void successResetsTheAbandonCounter() {
    b.appendCanonicalChain(b.header(0), 1L, 10);
    final AtomicBoolean fail = new AtomicBoolean(true);
    final SnapV2CatchupDataSource source =
        (c, n) ->
            fail.get()
                ? CompletableFuture.failedFuture(new IllegalStateException("no peers"))
                : CompletableFuture.completedFuture(b.segment(c, n));
    final RecordingState state = new RecordingState(b.header(3), source);

    state.startPivotCatchup(b.header(4)); // abandon 1
    state.startPivotCatchup(b.header(5)); // abandon 2
    fail.set(false);
    state.startPivotCatchup(b.header(6)); // success, counter reset
    fail.set(true);
    state.startPivotCatchup(b.header(7)); // abandon 1
    state.startPivotCatchup(b.header(8)); // abandon 2

    assertThat(state.getDownloadFuture()).isNotDone();
  }

  @Test
  void pivotListenerFailureStillRunsCompletionCheck() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state =
        new RecordingState(
            b.header(3),
            chainSource(),
            h -> {
              throw new IllegalStateException("listener broke");
            });

    state.startPivotCatchup(p1);

    assertThat(events).contains("checkCompletion:5");
  }

  @Test
  void pausesOnlyWhilePivotIsBeyondServingWindowDuringCatchup() {
    b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state =
        new RecordingState(b.header(3), (c, n) -> new CompletableFuture<>()); // never completes

    head.set(3 + 127);
    assertThat(state.isDequeueBlocked()).isFalse(); // no catch-up yet
    state.startPivotCatchup(b.header(5));
    assertThat(state.isDequeueBlocked()).isFalse(); // 127 < 128
    head.set(3 + 128);
    assertThat(state.isDequeueBlocked()).isTrue();
  }

  @Test
  void doesNotPauseWithoutCatchupEvenIfStale() {
    final BlockHeader pivot = b.appendCanonicalChain(b.header(0), 1L, 3);
    final RecordingState state = new RecordingState(pivot, (c, n) -> new CompletableFuture<>());
    head.set(10_000);
    assertThat(state.isDequeueBlocked()).isFalse();
  }

  @Test
  void doesNotPauseWhenHeadUnknownOrBehindPivot() {
    b.appendCanonicalChain(b.header(0), 1L, 5);
    final RecordingState state =
        new RecordingState(b.header(3), (c, n) -> new CompletableFuture<>());
    state.startPivotCatchup(b.header(5));

    head.set(0);
    assertThat(state.isDequeueBlocked()).isFalse();
    head.set(1); // CL behind the pivot
    assertThat(state.isDequeueBlocked()).isFalse();
  }

  @Test
  void pauseEndsWhenCatchupIsAbandoned() {
    b.appendCanonicalChain(b.header(0), 1L, 5);
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state = new RecordingState(b.header(3), (c, n) -> pending);
    state.startPivotCatchup(b.header(5));
    head.set(3 + 200);
    assertThat(state.isDequeueBlocked()).isTrue();

    pending.completeExceptionally(new IllegalStateException("no peers"));

    assertThat(state.isDequeueBlocked()).isFalse();
  }

  // ---- C1: finish runs on a service worker, never on the thread completing the data ----

  @Test
  void finishIsDispatchedToAServiceWorkerNotRunOnTheCompletingThread() throws Exception {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final SnapV2ChainSegment segment = b.segment(b.header(3), p1);
    final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();
    final EthContext context =
        EthProtocolManagerTestBuilder.builder().setEthScheduler(scheduler).build().ethContext();
    final List<Thread> listenerThreads = new CopyOnWriteArrayList<>();
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state =
        new RecordingState(
            b.header(3),
            (c, n) -> pending,
            h -> {
              listenerThreads.add(Thread.currentThread());
              events.add("pivotUpdated:" + h.getNumber());
            },
            context,
            healer(b, coordinator));
    state.startPivotCatchup(p1);
    scheduler.mockServiceExecutor().setAutoRun(false);
    final long queuedBefore = scheduler.mockServiceExecutor().getPendingFuturesCount();

    // Stands in for the Netty event loop delivering the last BAL response.
    final Thread completer = new Thread(() -> pending.complete(segment), "netty-io-stand-in");
    completer.start();
    completer.join(5_000);

    assertThat(events).doesNotContain("pivotUpdated:5");
    assertThat(applyCalls).hasValue(0);
    assertThat(scheduler.mockServiceExecutor().getPendingFuturesCount())
        .isEqualTo(queuedBefore + 1);

    scheduler.runPendingFutures();

    assertThat(events).containsSubsequence("pivotUpdated:5", "checkCompletion:5");
    assertThat(listenerThreads).isNotEmpty().doesNotContain(completer);
  }

  // ---- C2: an abandon with no remaining tasks must still run the completion check ----

  @Test
  void abandonWithNoRemainingTasksCompletesTheDownloadAtTheCurrentPivot() {
    final BlockHeader p0 =
        b.appendCanonical(b.header(0), b.emptyBal(), 1L, Hash.EMPTY_TRIE_HASH).getHeader();
    final BlockHeader p1 = b.appendCanonicalChain(p0, 2L, 2);
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state = new RecordingState(p0, (c, n) -> pending);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, RangeManager.MAX_RANGE, 0);
    state.startPivotCatchup(p1);
    // The last task completed while the catch-up was running, so its completion check declined.
    assertThat(state.checkCompletion(p0)).isFalse();
    events.clear();

    pending.completeExceptionally(new IllegalStateException("no peers"));

    assertThat(events).contains("checkCompletion:1");
    assertThat(state.getDownloadFuture()).isCompleted();
    assertThat(state.getDownloadFuture()).isNotCompletedExceptionally();
  }

  @Test
  void fatalAbandonDoesNotRunTheCompletionCheck() {
    b.appendCanonicalChain(b.header(0), 1L, 8);
    final RecordingState state = new RecordingState(b.header(3), failingSource());

    state.startPivotCatchup(b.header(5));
    state.startPivotCatchup(b.header(6));
    assertThat(events).filteredOn("checkCompletion:3"::equals).hasSize(2);
    state.startPivotCatchup(b.header(7));

    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThat(events).filteredOn("checkCompletion:3"::equals).hasSize(2);
  }

  // ---- I1: cancellation while finish waits for the in-flight drain ----

  @Test
  void cancellationWhileWaitingForInflightDrainAppliesNothing() throws Exception {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final SnapV2ChainSegment segment = b.segment(b.header(3), p1);
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state = new RecordingState(b.header(3), (c, n) -> pending);
    state.startPivotCatchup(p1); // nothing in flight: drained immediately
    // Dequeueing continues while the data is fetched, so a task is in flight when it arrives.
    final Task<SnapDataRequest> task = state.startInflightTask();

    final Thread finisher = new Thread(() -> pending.complete(segment), "finisher");
    finisher.start();
    awaitWaiting(finisher);
    state.getDownloadFuture().cancel(true);
    state.completeInflightTask(task);
    finisher.join(5_000);

    assertThat(finisher.isAlive()).isFalse();
    assertThat(applyCalls).hasValue(0);
    assertThat(events).doesNotContain("pivotUpdated:5");
    assertThat(state.currentPivot()).isEqualTo(b.header(3));
  }

  private static void awaitWaiting(final Thread thread) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 5_000;
    while (thread.getState() != Thread.State.WAITING) {
      assertThat(System.currentTimeMillis()).isLessThan(deadline);
      Thread.sleep(5);
    }
  }

  // ---- I2: paths through the state ----

  @Test
  void finishWaitsForBothDataAndInflightDrainThenRunsOnce() {
    final BlockHeader p1 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final CompletableFuture<SnapV2ChainSegment> pending = new CompletableFuture<>();
    final RecordingState state = new RecordingState(b.header(3), (c, n) -> pending);
    final Task<SnapDataRequest> task = state.startInflightTask();
    state.startPivotCatchup(p1);

    pending.complete(b.segment(b.header(3), p1)); // data arrives while the task is in flight

    assertThat(events).doesNotContain("pivotUpdated:5");
    assertThat(applyCalls).hasValue(0);
    assertThat(state.isPivotCatchupInProgress()).isTrue();

    state.completeInflightTask(task);

    assertThat(events).filteredOn("pivotUpdated:5"::equals).hasSize(1);
    assertThat(events).filteredOn("checkCompletion:5"::equals).hasSize(1);
    assertThat(applyCalls).hasValue(1);
    assertThat(state.currentPivot()).isEqualTo(p1);
  }

  @Test
  void abandonedCatchupLateSignalsDoNotAffectTheNextCatchup() {
    b.appendCanonicalChain(b.header(0), 1L, 6);
    final CompletableFuture<SnapV2ChainSegment> first = new CompletableFuture<>();
    final CompletableFuture<SnapV2ChainSegment> second = new CompletableFuture<>();
    final List<CompletableFuture<SnapV2ChainSegment>> futures = List.of(first, second);
    final AtomicInteger calls = new AtomicInteger();
    final RecordingState state =
        new RecordingState(b.header(3), (c, n) -> futures.get(calls.getAndIncrement()));
    final Task<SnapDataRequest> task = state.startInflightTask(); // first drain stays pending
    state.startPivotCatchup(b.header(5));

    first.completeExceptionally(new IllegalStateException("no peers"));
    assertThat(state.isPivotCatchupInProgress()).isFalse();
    state.startPivotCatchup(b.header(6));
    assertThat(state.isPivotCatchupInProgress()).isTrue();

    // Late signals of the first catch-up: its in-flight task drains, its data future is retried.
    state.completeInflightTask(task);
    first.complete(b.segment(b.header(3), b.header(5)));
    first.completeExceptionally(new IllegalStateException("late"));

    assertThat(state.isPivotCatchupInProgress()).isTrue();
    assertThat(events).filteredOn(e -> e.startsWith("pivotUpdated")).isEmpty();

    second.complete(b.segment(b.header(3), b.header(6)));

    assertThat(events)
        .filteredOn(e -> e.startsWith("pivotUpdated"))
        .containsExactly("pivotUpdated:6");
    assertThat(applyCalls).hasValue(1);
    assertThat(state.currentPivot()).isEqualTo(b.header(6));
    assertThat(state.getDownloadFuture()).isNotDone();
  }

  /** Healer that records reorg recoveries. */
  class RecordingHealer extends SnapV2ReorgHealer {
    final AtomicInteger recoveries = new AtomicInteger();

    RecordingHealer() {
      super(
          b.blockchain(),
          coordinator,
          ReorgBlockchainBuilder.balEnabledSchedule(),
          ReorgBlockchainBuilder.neverCalledFetcher());
    }

    @Override
    public ReorgRecoveryResult recoverFromReorg(
        final SnapV2ChainSegment segment,
        final DownloadedAccountRangeTracker accountRangeTracker,
        final DownloadedStorageRangeTracker storageRangeTracker) {
      recoveries.incrementAndGet();
      events.add("recoverFromReorg:" + segment.newPivot().getNumber());
      return super.recoverFromReorg(segment, accountRangeTracker, storageRangeTracker);
    }
  }

  @Test
  void reorgCatchupRecoversThroughTheHealerAndAdvancesThePivot() {
    final BlockHeader stale3 = b.appendStaleChain(b.header(0), 1L, 3);
    final Block c2 = b.appendCanonical(b.header(1), b.emptyBal(), 2L);
    final Block c3 = b.appendCanonical(c2.getHeader(), b.emptyBal(), 3L);
    final BlockHeader c4 = b.appendCanonical(c3.getHeader(), b.emptyBal(), 4L).getHeader();
    assertThat(b.segment(stale3, c4).isReorg()).isTrue();
    final RecordingHealer healer = new RecordingHealer();
    final RecordingState state =
        new RecordingState(stale3, chainSource(), pivotListener, ethContext, healer);

    state.startPivotCatchup(c4);

    assertThat(healer.recoveries).hasValue(1);
    assertThat(applyCalls).hasValue(0); // the same-chain apply path is not taken
    assertThat(events)
        .containsSubsequence("recoverFromReorg:4", "pivotUpdated:4", "checkCompletion:4");
    assertThat(state.currentPivot()).isEqualTo(c4);
    assertThat(state.isPivotCatchupInProgress()).isFalse();
    assertThat(state.getDownloadFuture()).isNotDone();
  }

  @Test
  void reorgRecoveryFailureIsFatal() {
    final Block s1 = b.appendStaleWithoutStoringBal(b.header(0), b.emptyBal(), 1L);
    final Block s2 = b.appendStale(s1.getHeader(), b.emptyBal(), 2L);
    final Block c1 = b.appendCanonical(b.header(0), b.emptyBal(), 1L);
    final Block c2 = b.appendCanonical(c1.getHeader(), b.emptyBal(), 2L);
    final BlockHeader c3 = b.appendCanonical(c2.getHeader(), b.emptyBal(), 3L).getHeader();
    final RecordingHealer healer = new RecordingHealer();
    final RecordingState state =
        new RecordingState(s2.getHeader(), chainSource(), pivotListener, ethContext, healer);

    state.startPivotCatchup(c3); // the orphaned BAL of block 1 is not retained: unrecoverable

    assertThat(healer.recoveries).hasValue(1);
    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThatThrownBy(() -> state.getDownloadFuture().join())
        .hasCauseInstanceOf(ReorgUnrecoverableException.class);
    assertThat(events).doesNotContain("pivotUpdated:3");
    assertThat(state.currentPivot()).isEqualTo(s2.getHeader());
  }
}
