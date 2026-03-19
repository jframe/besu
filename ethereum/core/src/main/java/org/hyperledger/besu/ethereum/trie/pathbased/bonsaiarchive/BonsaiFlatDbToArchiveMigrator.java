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
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
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
  private final Counter migratedBlocksCounter;
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicBoolean migrationRunning = new AtomicBoolean(false);

  private final RateLimiter rateLimiter;
  private final int maxBlocksPerBatch;
  private final int maxWritesPerBatch;

  // Engine API pause/resume for migration
  private volatile Thread migrationThread;
  private final AtomicBoolean engineApiActive = new AtomicBoolean(false);

  // Metrics
  private final Counter writesCounter;
  private final Counter batchesCounter;
  private final Counter rateLimiterWaitMsCounter;
  private final LabelledMetric<Counter> batchFlushReasonCounter;
  private final AtomicLong lastBatchWrites = new AtomicLong(0);
  private final AtomicLong lastBatchBlocks = new AtomicLong(0);
  private final AtomicLong lastCommitDurationMs = new AtomicLong(0);

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   * @param maxWritesPerSecond the maximum number of RocksDB writes per second (rate limiter)
   * @param maxBlocksPerBatch the maximum number of blocks to accumulate before committing a batch
   * @param maxWritesPerBatch the maximum number of estimated writes before committing a batch
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy,
      final int maxWritesPerSecond,
      final int maxBlocksPerBatch,
      final int maxWritesPerBatch) {

    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    this.rateLimiter = RateLimiter.create(maxWritesPerSecond);
    this.maxBlocksPerBatch = maxBlocksPerBatch;
    this.maxWritesPerBatch = maxWritesPerBatch;
    this.migratedBlocksCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_blocks_total",
            "Total blocks processed by the archive migrator");
    this.writesCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_writes_total",
            "Total estimated RocksDB writes performed by the archive migrator");
    this.batchesCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_batches_total",
            "Total number of batch commits performed by the archive migrator");
    this.rateLimiterWaitMsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_rate_limiter_wait_ms_total",
            "Total milliseconds spent waiting in the archive migration rate limiter");
    this.batchFlushReasonCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_archive_migration_batch_flush_total",
            "Number of archive migration batch flushes by trigger reason",
            "reason");
    metricsSystem.createGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_last_batch_writes",
        "Write count of the most recently committed archive migration batch",
        () -> (double) lastBatchWrites.get());
    metricsSystem.createGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_last_batch_blocks",
        "Block count of the most recently committed archive migration batch",
        () -> (double) lastBatchBlocks.get());
    metricsSystem.createGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_last_commit_duration_ms",
        "Duration in milliseconds of the most recent archive migration batch commit",
        () -> (double) lastCommitDurationMs.get());
  }

  /**
   * Migrates Bonsai flat DB to Bonsai archive format by processing trie logs sequentially. Resumes
   * from saved progress if available, otherwise starts from block 0. The target block is
   * continuously updated as new blocks are imported, so the migrator chases the chain head until it
   * converges.
   *
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate() {
    if (!migrationRunning.compareAndSet(false, true)) {
      LOG.warn("Bonsai migration already in progress, ignoring");
      return CompletableFuture.completedFuture(null);
    }

    final AtomicLong target = new AtomicLong(blockchain.getChainHeadBlockNumber());
    final long blockObserverId =
        blockchain.observeBlockAdded(event -> target.set(event.getHeader().getNumber()));
    return CompletableFuture.runAsync(() -> migrateBlocks(target), executorService)
        .whenComplete((result, ex) -> blockchain.removeObserver(blockObserverId));
  }

  private void migrateBlocks(final AtomicLong target) {
    try {
      migrationThread = Thread.currentThread();
      final Instant migrationStartTime = Instant.now();

      final long lastProcessedBlock = getMigrationProgress().orElse(-1L);
      final long startBlock = lastProcessedBlock + 1;
      final SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
      LOG.info("Starting Bonsai Archive migration from block {}", startBlock);

      int batchBlockCount = 0;
      int batchWrites = 0;
      long currentBlock = startBlock - 1;
      SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

      for (long blockNumber = startBlock; blockNumber <= target.get(); blockNumber++) {
        pauseDuringEngineApi();

        currentBlock = blockNumber;
        final Optional<TrieLog> maybeTrieLog =
            blockchain
                .getBlockHeader(blockNumber)
                .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()));

        try {
          int blockWrites = 0;
          if (maybeTrieLog.isPresent()) {
            blockWrites = estimateWrites(maybeTrieLog.get());
            batchWrites += blockWrites;
            processBlock(maybeTrieLog.get(), blockNumber, tx);
            migratedBlocksCounter.inc();
          } else if (blockNumber > 0) {
            throw new IllegalStateException("No trie log found for block " + blockNumber);
          }
          batchBlockCount++;

          if (batchBlockCount >= maxBlocksPerBatch || batchWrites >= maxWritesPerBatch) {
            pauseDuringEngineApi();
            final String flushReason =
                batchWrites >= maxWritesPerBatch ? "write_ceiling" : "block_ceiling";
            saveProgress(blockNumber, tx);
            final long commitStart = System.currentTimeMillis();
            tx.commit();
            lastCommitDurationMs.set(System.currentTimeMillis() - commitStart);

            if (batchWrites > 0) {
              final long waitStart = System.currentTimeMillis();
              rateLimiter.acquire(batchWrites);
              rateLimiterWaitMsCounter.inc(System.currentTimeMillis() - waitStart);
            }

            writesCounter.inc(batchWrites);
            batchesCounter.inc();
            batchFlushReasonCounter.labels(flushReason).inc();
            lastBatchWrites.set(batchWrites);
            lastBatchBlocks.set(batchBlockCount);

            tx = storage.startTransaction();
            batchBlockCount = 0;
            batchWrites = 0;
          }
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

        logProgress(blockNumber, startBlock, target.get());
      }

      // Flush remaining batch
      if (batchBlockCount > 0) {
        try {
          saveProgress(currentBlock, tx);
          final long commitStart = System.currentTimeMillis();
          tx.commit();
          lastCommitDurationMs.set(System.currentTimeMillis() - commitStart);
          writesCounter.inc(batchWrites);
          batchesCounter.inc();
          lastBatchWrites.set(batchWrites);
          lastBatchBlocks.set(batchBlockCount);
        } catch (final Exception e) {
          LOG.error("Failed to commit final migration batch, rolling back", e);
          try {
            tx.rollback();
          } catch (final Exception rollbackException) {
            LOG.error("Failed to rollback final migration batch", rollbackException);
          }
          throw new IllegalStateException("Migration failed at final batch: " + e.getMessage(), e);
        }
      } else {
        tx.rollback();
      }

      worldStateStorage.upgradeToArchiveFlatDbMode();
      logCompletion(startBlock, currentBlock, migrationStartTime);

    } catch (final Exception e) {
      LOG.error("Bonsai to Bonsai archive migration failed", e);
      throw new RuntimeException(e);
    } finally {
      migrationRunning.set(false);
    }
  }

  private void pauseDuringEngineApi() {
    while (engineApiActive.get()) {
      LockSupport.park();
    }
  }

  private int estimateWrites(final TrieLog trieLog) {
    return trieLog.getAccountChanges().size()
        + trieLog.getStorageChanges().values().stream().mapToInt(Map::size).sum();
  }

  @Override
  public void close() {
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
   * Called when an engine API call (newPayload, forkchoiceUpdated, etc.) starts. Signals the
   * migration thread to pause processing to reduce write contention.
   */
  public void onEngineApiCallStart() {
    engineApiActive.set(true);
  }

  /** Called when an engine API call completes. Resumes the migration thread by unparking it. */
  public void onEngineApiCallEnd() {
    engineApiActive.set(false);
    final Thread thread = migrationThread;
    if (thread != null) {
      LockSupport.unpark(thread);
    }
  }

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
              pauseDuringEngineApi();
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
                      pauseDuringEngineApi();
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
