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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MigrationPausingEngineCallListenerTest {

  @Mock private EngineCallListener delegate;
  @Mock private BonsaiFlatDbToArchiveMigrator migrator;

  @Test
  public void delegatesExecutionEngineCalled() {
    final MigrationPausingEngineCallListener listener =
        new MigrationPausingEngineCallListener(delegate, migrator);

    listener.executionEngineCalled();

    verify(delegate).executionEngineCalled();
    verifyNoMoreInteractions(migrator);
  }

  @Test
  public void delegatesStop() {
    final MigrationPausingEngineCallListener listener =
        new MigrationPausingEngineCallListener(delegate, migrator);

    listener.stop();

    verify(delegate).stop();
    verifyNoMoreInteractions(migrator);
  }

  @Test
  public void pausesMigratorOnEngineApiCallStart() {
    final MigrationPausingEngineCallListener listener =
        new MigrationPausingEngineCallListener(delegate, migrator);

    listener.onEngineApiCallStart();

    verify(migrator).onEngineApiCallStart();
    verifyNoMoreInteractions(delegate);
  }

  @Test
  public void resumesMigratorOnEngineApiCallEnd() {
    final MigrationPausingEngineCallListener listener =
        new MigrationPausingEngineCallListener(delegate, migrator);

    listener.onEngineApiCallEnd();

    verify(migrator).onEngineApiCallEnd();
    verifyNoMoreInteractions(delegate);
  }
}
