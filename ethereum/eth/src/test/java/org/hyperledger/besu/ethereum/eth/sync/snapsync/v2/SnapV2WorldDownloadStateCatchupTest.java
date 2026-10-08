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

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

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

  /** Records checkCompletion calls so ordering against onPivotUpdated can be asserted. */
  class RecordingState extends SnapV2WorldDownloadState {
    RecordingState(final BlockHeader pivot, final SnapV2CatchupDataSource source) {
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
          pivotListener,
          new SnapV2BlockAccessListApplier(
              coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule()),
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
}
