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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Forest-specific tests for {@link SnapV2WorldDownloadState} construction — verifies that the
 * Forest account-trie root pointer is seeded from the initial pivot's state root at construction
 * time, giving Forest the same self-tracking property Bonsai has via its empty-path node.
 */
class SnapV2WorldDownloadStateForestTest {

  private static final Hash SOME_ROOT = Hash.fromHexString("0x" + "ab".repeat(32));

  @Test
  void forestInitialisesTrackedRootToInitialPivotStateRoot() {
    final ForestWorldStateKeyValueStorage forest =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(forest);
    final BlockHeader pivot = headerWithStateRoot(SOME_ROOT);

    newForestDownloadState(coordinator, pivot); // constructor seeds the pointer

    assertThat(forest.getAccountTrieRoot()).contains(Bytes32.wrap(SOME_ROOT.getBytes()));
  }

  @Test
  void forestDoesNotOverwriteExistingTrackedRoot() {
    final ForestWorldStateKeyValueStorage forest =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(forest);

    // Pre-seed a root that differs from the pivot's state root.
    final Bytes32 existingRoot = Bytes32.fromHexString("0x" + "cc".repeat(32));
    final ForestWorldStateKeyValueStorage.Updater u =
        (ForestWorldStateKeyValueStorage.Updater) coordinator.updater();
    u.putAccountTrieRoot(existingRoot);
    u.commit();

    // Construct a download state with a different pivot — the constructor must NOT overwrite the
    // already-persisted pointer.
    final Hash differentRoot = Hash.fromHexString("0x" + "dd".repeat(32));
    newForestDownloadState(coordinator, headerWithStateRoot(differentRoot));

    assertThat(forest.getAccountTrieRoot()).contains(existingRoot);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static BlockHeader headerWithStateRoot(final Hash stateRoot) {
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getStateRoot()).thenReturn(stateRoot);
    return header;
  }

  @SuppressWarnings("unchecked")
  private static SnapV2WorldDownloadState newForestDownloadState(
      final WorldStateStorageCoordinator coordinator, final BlockHeader pivot) {
    final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(pivot));

    final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);
    when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());

    return new SnapV2WorldDownloadState(
        coordinator,
        mock(SnapSyncStatePersistenceManager.class),
        snapSyncState,
        new InMemoryTasksPriorityQueues<>(),
        /* maxRequestsWithoutProgress= */ 1,
        /* minMillisBeforeStalling= */ 1_000L,
        metricsManager,
        Clock.systemUTC(),
        SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
        mock(WorldStateHealFinishedListener.class),
        mock(SnapV2PivotCatchupListener.class),
        mock(SnapV2BlockAccessListApplier.class),
        mock(SnapV2ReorgHealer.class),
        mock(Blockchain.class),
        mock(EthContext.class),
        /* storagePipelineInFlightCapacity= */ 16L);
  }
}
