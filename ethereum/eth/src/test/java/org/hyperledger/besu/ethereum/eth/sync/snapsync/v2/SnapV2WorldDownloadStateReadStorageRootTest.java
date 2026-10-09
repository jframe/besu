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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * During a pivot catch-up the queued storage requests are retargeted to the new pivot using each
 * account's current storage root. That lookup used to be Bonsai-only and always failed on Forest,
 * restarting the whole world state download at the first catch-up that had queued storage work.
 */
class SnapV2WorldDownloadStateReadStorageRootTest {

  private static final Address ALICE =
      Address.fromHexString("0x00000000000000000000000000000000000000a1");
  private static final Address UNKNOWN =
      Address.fromHexString("0x00000000000000000000000000000000000000ff");
  private static final Hash STORAGE_ROOT = Hash.hash(Bytes.of(1, 2, 3));

  static Stream<WorldStateStorageHarness> storage() {
    return Stream.of(new BonsaiWorldStateStorageHarness(), new ForestWorldStateStorageHarness());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void readsTheStorageRootOfAPersistedAccount(final WorldStateStorageHarness storage) {
    storage.seedAccount(ALICE, 1L, Wei.of(10), STORAGE_ROOT, Hash.EMPTY);

    final SnapV2WorldDownloadState state = newDownloadState(storage.coordinator());

    assertThat(state.readStorageRoot(ALICE.addressHash()))
        .isEqualTo(Bytes32.wrap(STORAGE_ROOT.getBytes()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void readsTheRootOfTheAccountThatMatchesNotItsNeighbours(final WorldStateStorageHarness storage) {
    final Hash otherRoot = Hash.hash(Bytes.of(9, 9, 9));
    final Address bob = Address.fromHexString("0x00000000000000000000000000000000000000b2");
    storage.seedAccount(ALICE, 1L, Wei.of(10), STORAGE_ROOT, Hash.EMPTY);
    storage.seedAccount(bob, 2L, Wei.of(20), otherRoot, Hash.EMPTY);

    final SnapV2WorldDownloadState state = newDownloadState(storage.coordinator());

    assertThat(state.readStorageRoot(ALICE.addressHash()))
        .isEqualTo(Bytes32.wrap(STORAGE_ROOT.getBytes()));
    assertThat(state.readStorageRoot(bob.addressHash()))
        .isEqualTo(Bytes32.wrap(otherRoot.getBytes()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("storage")
  void failsForAnAccountThatIsNotPersisted(final WorldStateStorageHarness storage) {
    storage.seedAccount(ALICE, 1L, Wei.of(10), STORAGE_ROOT, Hash.EMPTY);

    final SnapV2WorldDownloadState state = newDownloadState(storage.coordinator());

    assertThatThrownBy(() -> state.readStorageRoot(UNKNOWN.addressHash()))
        .isInstanceOf(WorldStateDownloaderException.class)
        .hasMessageContaining("Storage root not found");
  }

  @SuppressWarnings("unchecked")
  private static SnapV2WorldDownloadState newDownloadState(
      final WorldStateStorageCoordinator coordinator) {
    final BlockHeader pivot = mock(BlockHeader.class);
    when(pivot.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
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
