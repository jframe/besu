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
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

  /** Records checkCompletion calls so ordering against onPivotUpdated can be asserted. */
  class RecordingState extends SnapV2WorldDownloadState {
    RecordingState(final BlockHeader pivot, final SnapV2CatchupDataSource source) {
      this(pivot, source, pivotListener);
    }

    RecordingState(
        final BlockHeader pivot,
        final SnapV2CatchupDataSource source,
        final PivotUpdateListener listener) {
      super(
          coordinator,
          new SnapSyncStatePersistenceManager(new InMemoryKeyValueStorageProvider()),
          new SnapSyncProcessState(pivot),
          new InMemoryTasksPriorityQueues<>(),
          10,
          50_000L,
          new SnapSyncMetricsManager(new NoOpMetricsSystem(), ethContext),
          Clock.systemUTC(),
          SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
          null,
          source,
          listener,
          new CountingApplier(),
          new SnapV2ReorgHealer(
              b.blockchain(),
              coordinator,
              ReorgBlockchainBuilder.balEnabledSchedule(),
              ReorgBlockchainBuilder.neverCalledFetcher()),
          b.blockchain(),
          ethContext,
          1000L);
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
}
