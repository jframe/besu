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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.util.Subscribers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai FULL flat database to ARCHIVE format.
 *
 * <p>This migrator processes trie logs from genesis to head, adding block number suffixes to all
 * state keys to create a versioned archive. The migration uses a pipeline-based approach with
 * prefetching for optimal performance.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Uses a pipeline to prefetch trie logs asynchronously while processing
 *   <li>Batches writes for performance (default 10,000 operations)
 *   <li>Checkpoints progress every 10,000 blocks for resumability
 *   <li>Updates FLAT_DB_MODE to ARCHIVE on completion
 * </ul>
 */
public class BonsaiFlatDbToArchiveMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);

  private static final int BATCH_SIZE = 10_000;
  private static final int CHECKPOINT_INTERVAL = 10_000;
  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  // Pipeline configuration
  private static final int PREFETCH_BUFFER_SIZE = 100;
  private static final int PREFETCH_CONCURRENCY = 4;

  /** Holds prefetched block data for the migration pipeline. */
  private record PrefetchedBlock(
      long blockNumber, Optional<BlockHeader> header, Optional<TrieLog> trieLog) {}

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final Subscribers<MigrationCompletionListener> completionListeners = Subscribers.create();

  /** Listener interface for migration completion events. */
  public interface MigrationCompletionListener {
    /**
     * Called when the archive migration completes successfully.
     *
     * @param startBlock the starting block number of the migration
     * @param endBlock the ending block number of the migration
     */
    void onMigrationComplete(long startBlock, long endBlock);

    /**
     * Called when the archive migration fails with an error.
     *
     * @param startBlock the starting block number of the migration
     * @param endBlock the ending block number of the migration
     * @param error the error that caused the failure
     */
    void onMigrationFailed(long startBlock, long endBlock, Throwable error);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy());
  }

  /**
   * Subscribe to migration completion events.
   *
   * @param listener the listener to notify on migration completion
   * @return the subscription ID that can be used to unsubscribe
   */
  public long subscribe(final MigrationCompletionListener listener) {
    return completionListeners.subscribe(listener);
  }

  /**
   * Unsubscribe from migration completion events.
   *
   * @param subscriptionId the subscription ID returned from subscribe
   * @return true if the listener was found and removed
   */
  public boolean unsubscribe(final long subscriptionId) {
    return completionListeners.unsubscribe(subscriptionId);
  }

  /**
   * Migrates FULL flat DB to ARCHIVE format by processing trie logs from startBlock to endBlock.
   * Resumes from saved progress if available.
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate(final long startBlock, final long endBlock) {
    return migrate(startBlock, endBlock, false);
  }

  /**
   * Migrates FULL flat DB to ARCHIVE format using a pipeline-based approach. This method uses a
   * Besu Pipeline to prefetch trie logs asynchronously while processing, providing optimal
   * performance by overlapping I/O with CPU work.
   *
   * <p>Pipeline stages:
   *
   * <ol>
   *   <li>Source: Stream of block numbers
   *   <li>Prefetch: Async fetch of block headers and trie logs
   *   <li>Process: Write archive keys and batch commits
   * </ol>
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @param resetProgress if true, ignores any saved progress and starts from startBlock
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate(
      final long startBlock, final long endBlock, final boolean resetProgress) {
    return CompletableFuture.runAsync(
        () -> {
          final ExecutorService pipelineExecutor =
              Executors.newFixedThreadPool(
                  PREFETCH_CONCURRENCY + 2,
                  r -> {
                    Thread t = new Thread(r, "archive-migration-pipeline");
                    t.setDaemon(true);
                    return t;
                  });

          try {
            final Instant migrationStartTime = Instant.now();
            LOG.info("Starting archive migration from block {} to {}", startBlock, endBlock);

            worldStateStorage.upgradeToArchiveDbMode();

            long currentBlock;
            if (resetProgress) {
              currentBlock = startBlock;
              LOG.info("Resetting migration progress, starting from block {}", startBlock);
            } else {
              currentBlock = loadProgress().orElse(startBlock);
              if (currentBlock > startBlock) {
                LOG.info(
                    "Resuming migration from block {} (previously started at {})",
                    currentBlock,
                    startBlock);
              }
            }

            final MetricsSystem metricsSystem = new NoOpMetricsSystem();
            final LabelledMetric<Counter> outputCounter =
                metricsSystem.createLabelledCounter(
                    BesuMetricCategory.SYNCHRONIZER,
                    "archive_migration_pipeline",
                    "Pipeline blocks for archive migration",
                    "stage",
                    "action");

            // Tracking state for batch commits
            final SegmentedKeyValueStorage storage =
                worldStateStorage.getComposedWorldStateStorage();
            final AtomicInteger batchCount = new AtomicInteger(0);
            final AtomicLong lastCheckpointBlock = new AtomicLong(currentBlock);
            final Object txLock = new Object();
            final SegmentedKeyValueStorageTransaction[] currentTx =
                new SegmentedKeyValueStorageTransaction[] {storage.startTransaction()};

            final long finalCurrentBlock = currentBlock;
            final long totalBlocks = endBlock - startBlock;

            // Create and run pipeline
            PipelineBuilder.createPipelineFrom(
                    "block-numbers",
                    LongStream.rangeClosed(finalCurrentBlock, endBlock).boxed().iterator(),
                    PREFETCH_BUFFER_SIZE,
                    outputCounter,
                    false,
                    "archive-migration")
                .thenProcessAsyncOrdered(
                    "fetch-trielog",
                    blockNumber ->
                        CompletableFuture.supplyAsync(
                            () -> prefetchBlock(blockNumber), pipelineExecutor),
                    PREFETCH_CONCURRENCY)
                .andFinishWith(
                    "process-block",
                    prefetchedBlock -> {
                      if (prefetchedBlock.header().isEmpty()
                          || prefetchedBlock.trieLog().isEmpty()) {
                        LOG.warn(
                            "Missing data for block {}, skipping", prefetchedBlock.blockNumber());
                        return;
                      }

                      // Process the block synchronously in order
                      synchronized (txLock) {
                        processBlock(
                            prefetchedBlock.trieLog().get(),
                            prefetchedBlock.blockNumber(),
                            currentTx[0]);
                        int count = batchCount.incrementAndGet();

                        if (count >= BATCH_SIZE) {
                          currentTx[0].commit();
                          currentTx[0] = storage.startTransaction();
                          batchCount.set(0);
                        }

                        // Checkpoint progress
                        if (prefetchedBlock.blockNumber() % CHECKPOINT_INTERVAL == 0
                            && prefetchedBlock.blockNumber() > lastCheckpointBlock.get()) {
                          saveProgress(prefetchedBlock.blockNumber());
                          lastCheckpointBlock.set(prefetchedBlock.blockNumber());
                          long progressPercent =
                              totalBlocks > 0
                                  ? ((prefetchedBlock.blockNumber() - startBlock) * 100)
                                      / totalBlocks
                                  : 100;
                          LOG.info(
                              "Archive migration progress: {}% (block {}/{})",
                              progressPercent,
                              prefetchedBlock.blockNumber(),
                              endBlock);
                        }
                      }
                    })
                .start(pipelineExecutor)
                .get();

            // Commit any remaining batch
            synchronized (txLock) {
              if (batchCount.get() > 0) {
                currentTx[0].commit();
              }
            }

            saveProgress(endBlock);

            final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
            LOG.info(
                "Archive migration completed. Processed {} blocks in {}.",
                endBlock - startBlock + 1,
                formatDuration(migrationDuration));

            completionListeners.forEach(
                listener -> listener.onMigrationComplete(startBlock, endBlock));
          } catch (final Exception e) {
            LOG.error("Archive migration failed", e);
            completionListeners.forEach(
                listener -> listener.onMigrationFailed(startBlock, endBlock, e));
            throw new RuntimeException(e);
          } finally {
            pipelineExecutor.shutdownNow();
          }
        },
        executorService);
  }

  /**
   * Prefetches a block's header and trie log.
   *
   * @param blockNumber the block number to prefetch
   * @return the prefetched block data
   */
  private PrefetchedBlock prefetchBlock(final long blockNumber) {
    Optional<BlockHeader> header = blockchain.getBlockHeader(blockNumber);
    Optional<TrieLog> trieLog =
        header.map(BlockHeader::getHash).flatMap(trieLogManager::getTrieLogLayer);
    return new PrefetchedBlock(blockNumber, header, trieLog);
  }

  /**
   * Processes a single block's trie log, writing archive keys for all state changes.
   *
   * @param trieLog the trie log containing state changes
   * @param blockNumber the block number for versioning
   * @param tx the transaction to write to
   */
  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {

    BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  /**
   * Processes account changes from a trie log, writing archive keys for historical account state.
   *
   * @param trieLog the trie log containing account changes
   * @param context the Bonsai context with block number for versioning
   * @param tx the transaction to write to
   */
  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {

    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getUpdated() != null) {
                Bytes accountBytes = RLP.encode(accountChange.getUpdated()::writeTo);
                BonsaiArchiveFlatDbStrategy.putFlatAccountWithContext(
                    tx, context, address.addressHash(), accountBytes);
              } else {
                // Account was deleted - use the remove method with explicit context
                archiveStrategy.removeFlatAccountWithContext(tx, context, address.addressHash());
              }
            });
  }

  /**
   * Processes storage changes from a trie log, writing archive keys for historical storage state.
   *
   * @param trieLog the trie log containing storage changes
   * @param context the Bonsai context with block number for versioning
   * @param tx the transaction to write to
   */
  private void processStorageChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {

    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageMap) -> {
              storageMap.forEach(
                  (slotKey, storageChange) -> {
                    if (storageChange.getUpdated() != null) {
                      BonsaiArchiveFlatDbStrategy.putFlatAccountStorageValueWithContext(
                          tx,
                          context,
                          address.addressHash(),
                          slotKey.getSlotHash(),
                          storageChange.getUpdated().toBytes());
                    } else {
                      // Storage was deleted - use the remove method with explicit context
                      archiveStrategy.removeFlatAccountStorageValueByStorageSlotHashWithContext(
                          tx, context, address.addressHash(), slotKey.getSlotHash());
                    }
                  });
            });
  }

  /**
   * Loads the migration progress from storage.
   *
   * @return the last processed block number, or empty if no progress exists
   */
  private Optional<Long> loadProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(TRIE_BRANCH_STORAGE, MIGRATION_PROGRESS_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  /**
   * Saves the migration progress to storage.
   *
   * @param blockNumber the last successfully processed block number
   */
  private void saveProgress(final long blockNumber) {
    SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  /**
   * Formats a duration into a human-readable string.
   *
   * @param duration the duration to format
   * @return formatted string like "4h 16m 36s" or "5m 30s" or "45s"
   */
  private static String formatDuration(final Duration duration) {
    long hours = duration.toHours();
    long minutes = duration.toMinutesPart();
    long seconds = duration.toSecondsPart();

    if (hours > 0) {
      return String.format("%dh %dm %ds", hours, minutes, seconds);
    } else if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    } else {
      return String.format("%ds", seconds);
    }
  }
}
