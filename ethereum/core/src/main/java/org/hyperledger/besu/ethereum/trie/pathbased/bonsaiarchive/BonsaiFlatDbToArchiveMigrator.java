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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VARIABLES;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Migrates a Bonsai storage to Bonsai archive storage format. */
public class BonsaiFlatDbToArchiveMigrator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);
  private static final int LOG_INTERVAL_SECONDS = 60;

  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final long archiveBoundary;
  private final Counter migratedBlocksCounter;
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicBoolean migrationRunning = new AtomicBoolean(false);
  @VisibleForTesting final AtomicLong blockObserverId = new AtomicLong(-1);
  private final AtomicBoolean initialMigrationComplete = new AtomicBoolean(false);

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   * @param archiveBoundary the number of recent blocks to keep in Bonsai (not archived)
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy,
      final long archiveBoundary) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    this.archiveBoundary = archiveBoundary;
    this.migratedBlocksCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_block",
            "Bonsai archive migration head block");
  }

  /**
   * Migrates Bonsai flat DB to Bonsai archive format by processing trie logs sequentially. Resumes
   * from saved progress if available, otherwise starts from block 0. The migration target is
   * chainHead - archiveBoundary; blocks within the boundary remain as Bonsai [hash] keys. After the
   * initial migration the block observer stays registered to archive new blocks as they cross the
   * boundary.
   *
   * @return a CompletableFuture that completes when the initial migration finishes
   */
  public CompletableFuture<Void> migrate() {
    if (!migrationRunning.compareAndSet(false, true)) {
      LOG.warn("Bonsai migration already in progress, ignoring");
      return CompletableFuture.completedFuture(null);
    }

    final AtomicLong target =
        new AtomicLong(Math.max(0, blockchain.getChainHeadBlockNumber() - archiveBoundary));
    blockObserverId.set(
        blockchain.observeBlockAdded(
            event -> {
              final long n = event.getHeader().getNumber();
              if (!initialMigrationComplete.get()) {
                // During initial migration: chase the archive boundary
                target.set(Math.max(0, n - archiveBoundary));
              } else {
                // After initial migration: archive the block that just crossed the boundary
                // off the block-import critical path.
                final long archiveBlock = n - archiveBoundary;
                if (archiveBlock > 0) {
                  executorService.submit(() -> processBlockFromObserver(archiveBlock));
                }
              }
            }));
    return CompletableFuture.runAsync(() -> migrateBlocks(target), executorService);
    // NOTE: no .whenComplete() — observer stays registered for ongoing archiving
    // NOTE: initialMigrationComplete is set inside migrateBlocks() to close the race window
    // between loop exit and flag transition (see migrateBlocks for details)
  }

  /**
   * Registers a block observer to write archive entries for ongoing blocks as they cross the
   * archive boundary. Called on restart when the DB is already in ARCHIVE mode (migration already
   * complete).
   */
  public void startOngoingArchiving() {
    blockObserverId.set(
        blockchain.observeBlockAdded(
            event -> {
              // Dispatch off the block-import critical path.
              final long archiveBlock = event.getHeader().getNumber() - archiveBoundary;
              if (archiveBlock > 0) {
                executorService.submit(() -> processBlockFromObserver(archiveBlock));
              }
            }));
  }

  /** Removes the block observer. Called during node shutdown to clean up. */
  public void stop() {
    final long id = blockObserverId.getAndSet(-1);
    if (id >= 0) {
      blockchain.removeObserver(id);
    }
  }

  private void processBlockFromObserver(final long blockNumber) {
    blockchain
        .getBlockHeader(blockNumber)
        .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()))
        .ifPresentOrElse(
            trieLog -> {
              final SegmentedKeyValueStorage storage =
                  worldStateStorage.getComposedWorldStateStorage();
              final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
              try {
                processBlock(trieLog, blockNumber, tx);
                saveProgress(blockNumber, tx);
                tx.commit();
              } catch (final Exception e) {
                LOG.error("Failed to write ongoing archive entry for block {}", blockNumber, e);
                tx.rollback();
              }
            },
            () -> LOG.error("No trie log found for ongoing archive block {}", blockNumber));
  }

  private void migrateBlocks(final AtomicLong target) {
    try {
      final Instant migrationStartTime = Instant.now();

      final long lastProcessedBlock = getMigrationProgress().orElse(-1L);
      final long startBlock = lastProcessedBlock + 1;
      final SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
      LOG.info("Starting Bonsai Archive migration from block {}", startBlock);
      long processedUpTo = startBlock - 1;
      for (long blockNumber = startBlock; blockNumber <= target.get(); blockNumber++) {

        final Optional<TrieLog> maybeTrieLog =
            blockchain
                .getBlockHeader(blockNumber)
                .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()));

        final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
        try {
          if (maybeTrieLog.isPresent()) {
            processBlock(maybeTrieLog.get(), blockNumber, tx);
            migratedBlocksCounter.inc();
          } else if (blockNumber > 0) {
            throw new IllegalStateException("No trie log found for block " + blockNumber);
          }
          // Always save progress, even for blocks with no trie log
          saveProgress(blockNumber, tx);
          tx.commit();
        } catch (final Exception e) {
          LOG.error("Failed to process block {}, rolling back transaction", blockNumber, e);
          try {
            tx.rollback();
          } catch (final Exception rollbackException) {
            LOG.error(
                "Failed to rollback transaction for block {}", blockNumber, rollbackException);
          }
          throw new IllegalStateException(
              "Migration failed at block " + blockNumber + ": " + e.getMessage(), e);
        }

        processedUpTo = blockNumber;
        logProgress(blockNumber, startBlock, target.get());
      }

      // Transition to ongoing-archiving mode. After this flag is set, the block observer
      // will call processBlockFromObserver() for new blocks as they cross the boundary.
      //
      // Race window: the observer may have updated `target` between the loop's final
      // condition evaluation and this set. Those blocks neither ran through the loop nor
      // triggered processBlockFromObserver() (because the flag was still false). Drain
      // them now. Any overlap with a concurrent observer call is safe — both paths write
      // the same key with the same value (idempotent).
      onBeforeInitialMigrationComplete(target);
      initialMigrationComplete.set(true);
      for (long b = processedUpTo + 1; b <= target.get(); b++) {
        LOG.debug("Draining gap block {} missed at migration boundary", b);
        processBlockFromObserver(b);
      }

      worldStateStorage.upgradeToArchiveFlatDbMode();
      logCompletion(startBlock, processedUpTo, migrationStartTime);

    } catch (final Exception e) {
      LOG.error("Bonsai to Bonsai archive migration failed", e);
      throw new RuntimeException(e);
    } finally {
      migrationRunning.set(false);
    }
  }

  @Override
  public void close() {
    stop();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Migration executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Called after the initial migration loop exits and before the completion flag is set. No-op in
   * production; overridden in tests to simulate blocks arriving in the race window.
   */
  @VisibleForTesting
  protected void onBeforeInitialMigrationComplete(final AtomicLong target) {}

  @VisibleForTesting
  protected Optional<Long> getMigrationProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(VARIABLES, MIGRATION_PROGRESS_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  private void logProgress(final long blockNumber, final long startBlock, final long endBlock) {
    final long totalBlocks = endBlock - startBlock;
    LogUtil.throttledLog(
        () -> {
          long progressPercent =
              totalBlocks > 0 ? ((blockNumber - startBlock) * 100) / totalBlocks : 100;
          LOG.info(
              "Bonsai Archive migration progress: {}% (block {}/{})",
              progressPercent, blockNumber, endBlock);
        },
        shouldLogProgress,
        LOG_INTERVAL_SECONDS);
  }

  private void logCompletion(
      final long startBlock, final long endBlock, final Instant migrationStartTime) {
    final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
    final String formattedDuration =
        DurationFormatUtils.formatDurationWords(migrationDuration.toMillis(), true, true);
    LOG.info(
        "Bonsai Archive migration completed. Processed {} blocks in {}.",
        endBlock - startBlock + 1,
        formattedDuration);
  }

  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    processAccountChanges(trieLog, blockNumber, tx);
    processStorageChanges(trieLog, blockNumber, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiContext context = new BonsaiContext(blockNumber);
    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getUpdated() != null) {
                final BytesValueRLPOutput out = new BytesValueRLPOutput();
                accountChange.getUpdated().writeTo(out);
                archiveStrategy.putFlatAccount(context, tx, address.addressHash(), out.encoded());
              } else {
                archiveStrategy.removeFlatAccount(context, tx, address.addressHash());
              }
            });
  }

  private void processStorageChanges(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiContext context = new BonsaiContext(blockNumber);
    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageMap) ->
                storageMap.forEach(
                    (slotKey, storageChange) -> {
                      if (storageChange.getUpdated() != null) {
                        archiveStrategy.putFlatAccountStorageValueByStorageSlotHash(
                            context,
                            tx,
                            address.addressHash(),
                            slotKey.getSlotHash(),
                            storageChange.getUpdated().toBytes());
                      } else {
                        archiveStrategy.removeFlatAccountStorageValueByStorageSlotHash(
                            context, tx, address.addressHash(), slotKey.getSlotHash());
                      }
                    }));
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(VARIABLES, MIGRATION_PROGRESS_KEY, Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }
}
