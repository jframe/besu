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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StubTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.Task;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

class SnapV2PersistDataStepTest {

  private final WorldStateKeyValueStorage worldStateKeyValueStorage =
      spy(
          new InMemoryKeyValueStorageProvider()
              .createWorldStateStorage(DataStorageConfiguration.DEFAULT_CONFIG));

  private final SnapV2PersistDataStep persistDataStep =
      new SnapV2PersistDataStep(
          mock(SnapSyncProcessState.class),
          new WorldStateStorageCoordinator(worldStateKeyValueStorage),
          mock(SnapRequestContext.class),
          mock(SnapSyncConfiguration.class),
          mock(DownloadedAccountRangeTracker.class),
          mock(DownloadedStorageRangeTracker.class));

  @Test
  void rollsBackUpdaterAndClearsTasksOnRetryableErrorBeforeCommit() {
    final WorldStateKeyValueStorage.Updater updater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    when(worldStateKeyValueStorage.updater()).thenReturn(updater);
    final SnapDataRequest request =
        failingRequest(
            new StorageException(
                new RocksDBException(
                    new Status(Status.Code.TimedOut, Status.SubCode.LockTimeout, "lock timeout"))));

    final List<Task<SnapDataRequest>> tasks = List.of(new StubTask(request));
    final List<Task<SnapDataRequest>> result = persistDataStep.persist(tasks);

    assertThat(result).isSameAs(tasks);
    verify(request).clear();
    verify(updater).rollback();
    verify(updater, never()).commit();
  }

  @Test
  void rollsBackUpdaterWhenTheErrorIsNotRetryable() {
    final WorldStateKeyValueStorage.Updater updater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    when(worldStateKeyValueStorage.updater()).thenReturn(updater);
    final SnapDataRequest request =
        failingRequest(new StorageException(new IllegalStateException("not a RocksDB error")));

    assertThatThrownBy(() -> persistDataStep.persist(List.of(new StubTask(request))))
        .isInstanceOf(StorageException.class);

    verify(updater).rollback();
    verify(updater, never()).commit();
  }

  @Test
  void rollsBackUpdaterWhenPersistingFailsWithAnUnexpectedException() {
    final WorldStateKeyValueStorage.Updater updater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    when(worldStateKeyValueStorage.updater()).thenReturn(updater);
    final SnapDataRequest request = failingRequest(new IllegalStateException("boom"));

    assertThatThrownBy(() -> persistDataStep.persist(List.of(new StubTask(request))))
        .isInstanceOf(IllegalStateException.class);

    verify(updater).rollback();
    verify(updater, never()).commit();
  }

  @Test
  void commitsWithoutRollingBackWhenNothingFails() {
    final WorldStateKeyValueStorage.Updater updater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    when(worldStateKeyValueStorage.updater()).thenReturn(updater);
    final SnapDataRequest request = mock(SnapV2DataRequest.class);
    when(request.isResponseReceived()).thenReturn(false);

    persistDataStep.persist(List.of(new StubTask(request)));

    verify(updater).commit();
    verify(updater, never()).rollback();
  }

  private static SnapDataRequest failingRequest(final RuntimeException failure) {
    final SnapDataRequest request = mock(SnapV2DataRequest.class);
    when(request.isResponseReceived()).thenReturn(true);
    when(request.getChildRequests(any(), any(), any())).thenReturn(Stream.empty());
    when(request.persist(any(), any(), any(), any(), any())).thenThrow(failure);
    return request;
  }
}
