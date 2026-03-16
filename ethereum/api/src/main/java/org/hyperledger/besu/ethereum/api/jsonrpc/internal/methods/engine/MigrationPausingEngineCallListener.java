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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator;

/**
 * Composite EngineCallListener that wraps an existing listener and integrates with the Bonsai
 * archive migration. Pauses the migration when engine API calls are active to reduce write
 * contention, then resumes on completion.
 */
public class MigrationPausingEngineCallListener implements EngineCallListener {

  private final EngineCallListener delegate;
  private final BonsaiFlatDbToArchiveMigrator migrator;

  /**
   * Creates a new composite listener.
   *
   * @param delegate the original engine call listener to delegate to
   * @param migrator the Bonsai archive migrator to pause/resume
   */
  public MigrationPausingEngineCallListener(
      final EngineCallListener delegate, final BonsaiFlatDbToArchiveMigrator migrator) {
    this.delegate = delegate;
    this.migrator = migrator;
  }

  @Override
  public void executionEngineCalled() {
    delegate.executionEngineCalled();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void onEngineApiCallStart() {
    migrator.onEngineApiCallStart();
  }

  @Override
  public void onEngineApiCallEnd() {
    migrator.onEngineApiCallEnd();
  }
}
