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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the Bonsai archive's background components so the controller builder need not. */
public final class BonsaiArchiveFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveFactory.class);

  private BonsaiArchiveFactory() {}

  /**
   * Builds the Bonsai archive components for {@code worldStateArchive}. Must be called before the
   * genesis state is written: when archive proofs are enabled it installs the trie-node capture
   * strategy so block 0 is captured.
   *
   * <p>For any format other than {@code X_BONSAI_ARCHIVE} returns a handle that only shuts down the
   * given executors. The returned handle owns the executors.
   */
  public static BonsaiArchive create(
      final DataStorageConfiguration dataStorageConfiguration,
      final WorldStateArchive worldStateArchive,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Blockchain blockchain,
      final MetricsSystem metricsSystem,
      final ExecutorService trieCapturePool,
      final ScheduledExecutorService migrationExecutor,
      final ScheduledExecutorService rollerExecutor) {
    if (!DataStorageFormat.X_BONSAI_ARCHIVE.equals(dataStorageConfiguration.getDataStorageFormat())
        || !(worldStateArchive instanceof BonsaiArchiveWorldStateProvider provider)) {
      return new NoOpBonsaiArchive(trieCapturePool, migrationExecutor, rollerExecutor);
    }
    final ExtraStorageConfiguration extraStorage =
        dataStorageConfiguration.getExtraStorageConfiguration();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);

    Optional<ArchiveTrieNodeStrategy> trieNodeStrategy = Optional.empty();
    if (extraStorage.getUnstable().getBonsaiArchiveStateProofsEnabled()) {
      final ArchiveTrieNodeStrategy strategy =
          ArchiveTrieNodeStrategy.createArchiveStrategy(
              worldStateStorage.getComposedWorldStateStorage(),
              trieCapturePool,
              extraStorage.getUnstable().getBonsaiArchiveShallowCheckpointInterval(),
              extraStorage.getUnstable().getBonsaiArchiveDeepCheckpointInterval());
      worldStateStorage.setTrieNodeStrategy(strategy);
      LOG.info("Bonsai archive proofs enabled (--Xbonsai-archive-state-proofs-enabled)");
      trieNodeStrategy = Optional.of(strategy);
    }
    return new DefaultBonsaiArchive(
        provider,
        worldStateStorage,
        blockchain,
        metricsSystem,
        extraStorage.getMaxLayersToLoad(),
        trieNodeStrategy,
        trieCapturePool,
        migrationExecutor,
        rollerExecutor);
  }
}
