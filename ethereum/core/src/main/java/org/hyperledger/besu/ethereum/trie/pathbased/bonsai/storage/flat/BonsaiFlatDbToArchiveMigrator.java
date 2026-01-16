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
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
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
 *   <li>Processes blocks in parallel batches for improved throughput
 *   <li>Saves progress after each block for resumability
 *   <li>Updates FLAT_DB_MODE to ARCHIVE on completion
 * </ul>
 */
public class BonsaiFlatDbToArchiveMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);

  /** Record to hold a block number and its associated trie log through the pipeline. */
  private record PrefetchedTrieLog(long blockNumber, Optional<TrieLog> trieLog) {}

  private static final int LOG_INTERVAL = 10_000;
  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  // Pipeline configuration
  private static final int PREFETCH_BUFFER_SIZE = 200;
  private static final int PREFETCH_CONCURRENCY = 8;

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final MetricsSystem metricsSystem;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final Subscribers<MigrationCompletionListener> completionListeners = Subscribers.create();

  /** Listener interface for migration completion events. */
  public interface MigrationCompletionListener {
    /** Called when the archive migration completes successfully. */
    void onMigrationComplete();

    /**
     * Called when the archive migration fails with an error.
     *
     * @param error the error that caused the failure
     */
    void onMigrationFailed(Throwable error);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.metricsSystem = metricsSystem;
    this.archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
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
   *   <li>Prefetch: Async fetch of trie logs in parallel
   *   <li>Process: Write archive keys in batches
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

            final LabelledMetric<Counter> outputCounter =
                metricsSystem.createLabelledCounter(
                    BesuMetricCategory.SYNCHRONIZER,
                    "archive_migration_pipeline",
                    "Pipeline blocks for archive migration",
                    "stage",
                    "action");

            final SegmentedKeyValueStorage storage =
                worldStateStorage.getComposedWorldStateStorage();
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
                            () ->
                                new PrefetchedTrieLog(blockNumber, fetchTrieLog(blockNumber)),
                            pipelineExecutor),
                    PREFETCH_CONCURRENCY)
                .andFinishWith(
                    "process-block",
                    prefetched -> {
                      if (prefetched.trieLog().isEmpty()) {
                        return;
                      }

                      final TrieLog trieLog = prefetched.trieLog().get();
                      final long blockNumber = prefetched.blockNumber();

                      // Process and commit each block individually
                      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
                      processBlock(trieLog, blockNumber, tx);
                      saveProgress(blockNumber, tx);
                      tx.commit();

                      // Log progress periodically
                      if (blockNumber % LOG_INTERVAL == 0) {
                        long progressPercent =
                            totalBlocks > 0
                                ? ((blockNumber - startBlock) * 100) / totalBlocks
                                : 100;
                        LOG.info(
                            "Archive migration progress: {}% (block {}/{})",
                            progressPercent,
                            blockNumber,
                            endBlock);
                      }
                    })
                .start(pipelineExecutor)
                .get();

            final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
            LOG.info(
                "Archive migration completed. Processed {} blocks in {}.",
                endBlock - startBlock + 1,
                formatDuration(migrationDuration));

            completionListeners.forEach(MigrationCompletionListener::onMigrationComplete);
          } catch (final Exception e) {
            LOG.error("Archive migration failed", e);
            completionListeners.forEach(listener -> listener.onMigrationFailed(e));
            throw new RuntimeException(e);
          } finally {
            pipelineExecutor.shutdownNow();
          }
        },
        executorService);
  }

  /**
   * Fetches the trie log for a block.
   *
   * @param blockNumber the block number to fetch
   * @return the trie log, or empty if not found
   */
  private Optional<TrieLog> fetchTrieLog(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()));
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
    final BonsaiContext context = new BonsaiContext(blockNumber);
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
                final Bytes accountBytes = RLP.encode(accountChange.getUpdated()::writeTo);
                BonsaiArchiveFlatDbStrategy.putFlatAccountWithContext(
                    tx, context, address.addressHash(), accountBytes);
              } else {
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
   * Saves the migration progress to storage within the given transaction.
   *
   * @param blockNumber the last successfully processed block number
   * @param tx the transaction to write to
   */
  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        TRIE_BRANCH_STORAGE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
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
