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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapV2WorldDownloadStateMoveTest {

  private final SnapV2BlockAccessListApplier applier = mock(SnapV2BlockAccessListApplier.class);
  private final SnapV2ReorgHealer healer = mock(SnapV2ReorgHealer.class);
  private final BlockHeader oldPivot = new BlockHeaderTestFixture().number(100).buildHeader();
  private final BlockHeader newPivot =
      new BlockHeaderTestFixture().number(101).parentHash(oldPivot.getHash()).buildHeader();
  private final SnapSyncProcessState snapSyncState = new SnapSyncProcessState(oldPivot);
  private SnapV2WorldDownloadState state;

  @BeforeEach
  void setUp() {
    final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);
    when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());
    state =
        spy(
            new SnapV2WorldDownloadState(
                mock(WorldStateStorageCoordinator.class),
                mock(SnapSyncStatePersistenceManager.class),
                snapSyncState,
                new InMemoryTasksPriorityQueues<SnapDataRequest>(),
                1000,
                60_000,
                metricsManager,
                Clock.systemUTC(),
                SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
                mock(WorldStateHealFinishedListener.class),
                applier,
                healer,
                mock(EthContext.class),
                1000));
    when(applier.collectPendingStorageAffected(any(), any())).thenReturn(Set.of());
  }

  private SnapV2PivotMove sameChainStep() {
    return new SnapV2PivotMove(oldPivot, List.of(), List.of(newPivot));
  }

  @Test
  void sameChainMoveAppliesTheBalsAndAdvancesThePivot() {
    final SnapV2BlockAccessListApplier.BatchState batch =
        mock(SnapV2BlockAccessListApplier.BatchState.class);
    doReturn(CompletableFuture.completedFuture(Map.of()))
        .when(state)
        .fetchAccountStorageRoots(any(), any());
    when(applier.applyBlockAccessLists(anyList(), any(), any())).thenReturn(batch);

    assertThat(state.applyMove(sameChainStep())).isTrue();

    verify(batch).commit();
    assertThat(state.currentPivot()).isEqualTo(newPivot);
    assertThat(state.isFinished()).isFalse();
  }

  @Test
  void rootsFetchFailureLeavesNoTrace() {
    doReturn(CompletableFuture.failedFuture(new RuntimeException("timeout")))
        .when(state)
        .fetchAccountStorageRoots(any(), any());

    assertThatThrownBy(() -> state.applyMove(sameChainStep()))
        .isInstanceOf(SnapV2MoveFailedException.class);

    verify(applier, never()).applyBlockAccessLists(anyList(), any(), any());
    assertThat(state.currentPivot()).isEqualTo(oldPivot);
    assertThat(state.isFinished()).isFalse();
  }

  @Test
  void reorgPrepareFailureLeavesNoTrace() {
    // the current pivot (oldPivot, block 100) is the orphaned branch's tip
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(99).buildHeader();
    final SnapV2PivotMove reorg =
        new SnapV2PivotMove(ancestor, List.of(oldPivot), List.of(newPivot));
    when(healer.prepareReorg(any(), any(), any())).thenThrow(new RuntimeException("timeout"));

    assertThatThrownBy(() -> state.applyMove(reorg)).isInstanceOf(SnapV2MoveFailedException.class);

    verify(healer, never()).applyReorg(any(), any(), any());
    assertThat(state.currentPivot()).isEqualTo(oldPivot);
    assertThat(state.isFinished()).isFalse();
  }

  @Test
  void aWriteFailureFailsTheWorldStateDownload() {
    final SnapV2BlockAccessListApplier.BatchState batch =
        mock(SnapV2BlockAccessListApplier.BatchState.class);
    doReturn(CompletableFuture.completedFuture(Map.of()))
        .when(state)
        .fetchAccountStorageRoots(any(), any());
    when(applier.applyBlockAccessLists(anyList(), any(), any())).thenReturn(batch);
    doThrow(new IllegalStateException("disk")).when(batch).commit();

    assertThatThrownBy(() -> state.applyMove(sameChainStep()))
        .isInstanceOf(IllegalStateException.class);

    assertThat(state.isFinished()).isTrue();
  }

  @Test
  void abortFailsTheWorldStateDownload() {
    state.abort(new ReorgUnrecoverableException("too deep"));

    assertThat(state.isFinished()).isTrue();
    assertThat(state.applyMove(sameChainStep())).isFalse();
  }

  @Test
  void aCompletionCheckForAStalePivotDoesNothing() {
    // a pipeline task that read the pivot before a move must not verify the old root against
    // the moved state
    state
        .getAccountRangeTracker()
        .registerPending(Bytes32.ZERO, Bytes32.fromHexString("0x" + "ff".repeat(32)), 0);
    snapSyncState.setCurrentHeader(newPivot);

    assertThat(state.checkCompletion(oldPivot)).isFalse();
    assertThat(state.isFinished()).isFalse();
  }

  @Test
  void aFailedPurgeAfterTheReorgWritesFailsTheDownload() {
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(99).buildHeader();
    final SnapV2PivotMove reorg =
        new SnapV2PivotMove(ancestor, List.of(oldPivot), List.of(newPivot));
    when(healer.prepareReorg(any(), any(), any())).thenReturn(null);
    // a null set makes the purge throw after the reorg writes
    when(healer.applyReorg(any(), any(), any()))
        .thenReturn(new ReorgRecoveryResult(null, Map.of()));

    assertThatThrownBy(() -> state.applyMove(reorg))
        .isNotInstanceOf(SnapV2MoveFailedException.class);

    assertThat(state.isFinished()).isTrue();
  }

  @Test
  void anUnrecoverableReorgIsNotRetried() {
    final BlockHeader ancestor = new BlockHeaderTestFixture().number(99).buildHeader();
    final SnapV2PivotMove reorg =
        new SnapV2PivotMove(ancestor, List.of(oldPivot), List.of(newPivot));
    when(healer.prepareReorg(any(), any(), any()))
        .thenThrow(new ReorgUnrecoverableException("below BAL activation"));

    assertThatThrownBy(() -> state.applyMove(reorg))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }
}
