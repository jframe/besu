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
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Bonsai archive's background components for an {@code X_BONSAI_ARCHIVE} node. */
final class DefaultBonsaiArchive implements BonsaiArchive {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBonsaiArchive.class);

  private final BonsaiArchiveWorldStateProvider provider;
  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final Blockchain blockchain;
  private final MetricsSystem metricsSystem;
  private final long maxLayersToLoad;
  private final Optional<ArchiveTrieNodeStrategy> trieNodeStrategy;
  private final ExecutorService trieCapturePool;
  private final ScheduledExecutorService migrationExecutor;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private volatile BonsaiFlatDbToArchiveMigrator migrator;

  DefaultBonsaiArchive(
      final BonsaiArchiveWorldStateProvider provider,
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final MetricsSystem metricsSystem,
      final long maxLayersToLoad,
      final Optional<ArchiveTrieNodeStrategy> trieNodeStrategy,
      final ExecutorService trieCapturePool,
      final ScheduledExecutorService migrationExecutor) {
    this.provider = provider;
    this.worldStateStorage = worldStateStorage;
    this.blockchain = blockchain;
    this.metricsSystem = metricsSystem;
    this.maxLayersToLoad = maxLayersToLoad;
    this.trieNodeStrategy = trieNodeStrategy;
    this.trieCapturePool = trieCapturePool;
    this.migrationExecutor = migrationExecutor;
  }

  @Override
  public void start(final Synchronizer synchronizer, final LongSupplier bestChainHeight) {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Bonsai archive already started");
    }
    trieNodeStrategy.ifPresent(
        strategy -> {
          strategy.setHasChainEstimate(() -> synchronizer.getBestPeerChainHead().isPresent());
          synchronizer.subscribeInSync(strategy, maxLayersToLoad);
        });
    startMigration(synchronizer);
  }

  private void startMigration(final Synchronizer synchronizer) {
    final BonsaiFlatDbToArchiveMigrator archiveMigrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            provider.getTrieLogManager(),
            blockchain,
            migrationExecutor,
            metricsSystem,
            new BonsaiArchiveFlatDbStrategy(
                metricsSystem, worldStateStorage.getFlatDbStrategy().getCodeStorageStrategy()));
    provider.setArchiveMigrationProgressSupplier(archiveMigrator::getMigratedBlockNumber);
    migrator = archiveMigrator;

    final FlatDbMode flatDbMode = worldStateStorage.getFlatDbMode();
    if (flatDbMode == FlatDbMode.FULL || flatDbMode == FlatDbMode.PARTIAL) {
      final AtomicBoolean migrationStarted = new AtomicBoolean(false);
      final AtomicLong syncSubscriptionId = new AtomicLong();
      syncSubscriptionId.set(
          synchronizer.subscribeInSync(
              inSync -> {
                if (inSync && migrationStarted.compareAndSet(false, true)) {
                  synchronizer.unsubscribeInSync(syncSubscriptionId.get());
                  LOG.info("Node is in sync, starting Bonsai archive migration");
                  archiveMigrator
                      .migrate()
                      .exceptionally(
                          error -> {
                            LOG.error(
                                "Archive migration failed, archiver will remain disabled until restart",
                                error);
                            return null;
                          });
                }
              },
              0));
    } else {
      // Already in ARCHIVE mode (restart after migration): register ongoing migration
      archiveMigrator.startOngoingMigration();
    }
  }

  @Override
  public void close() {
    // Migrator first so its in-flight callback finishes before storage closes, then the capture
    // strategy (drains and shuts down the trie-capture pool). shutdownNow afterwards is idempotent
    // and covers components that were never started.
    if (migrator != null) {
      migrator.close();
    }
    trieNodeStrategy.ifPresent(ArchiveTrieNodeStrategy::close);
    trieCapturePool.shutdownNow();
    migrationExecutor.shutdownNow();
  }
}
