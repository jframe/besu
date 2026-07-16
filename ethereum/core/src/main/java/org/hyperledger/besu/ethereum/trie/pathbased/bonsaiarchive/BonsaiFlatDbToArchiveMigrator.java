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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReaderV2;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai flat DB node to Bonsai archive format without requiring a full resync.
 *
 * <p>Migration replays trie logs sequentially from block 0 (or the last saved checkpoint) to the
 * current chain head, writing archive-keyed entries into the archive column families. Progress is
 * persisted atomically with each block's data so the migration can safely resume after a restart.
 *
 * <p>The chain head target is updated in real time as new blocks arrive, so the migrator chases the
 * head until it converges. Once all blocks are processed, the flat DB mode is atomically switched
 * to {@link org.hyperledger.besu.ethereum.worldstate.FlatDbMode#ARCHIVE}.
 */
public class BonsaiFlatDbToArchiveMigrator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);
  private static final int LOG_INTERVAL_SECONDS = 60;
  private static final long CATCHUP_LOG_THRESHOLD = 32;
  private static final Executor PREFETCH_POOL = Executors.newVirtualThreadPerTaskExecutor();

  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  @VisibleForTesting static final int MAX_BLOCKS_PER_BATCH = 256;
  @VisibleForTesting static final long MAX_BATCH_BYTES = 256L * 1024 * 1024;

  /** Maximum number of concurrently in-flight trie-node prefetch tasks (Design-5 part 2a). */
  @VisibleForTesting static final int PREFETCH_MAX_IN_FLIGHT = 6;

  /** Maximum trie-node location depth warmed by trie-node prefetch (Design-5 part 2a). */
  @VisibleForTesting static final int PREFETCH_MAX_DEPTH = 12;

  private int maxBlocksPerBatch = MAX_BLOCKS_PER_BATCH;
  private long maxBatchBytes = MAX_BATCH_BYTES;

  /**
   * Toggle for trie-node path prefetch (Design-5 part 2a). Defaults to enabled; can be disabled via
   * the {@code besu.bonsaiArchiveMigrationPrefetch=false} system property.
   */
  private boolean prefetchEnabled =
      !"false".equalsIgnoreCase(System.getProperty("besu.bonsaiArchiveMigrationPrefetch"));

  /**
   * Best-effort background trie-node prefetcher; non-null only when {@link #prefetchEnabled} was
   * true at the time {@link #initArchiveTrieBuilder} ran. Volatile because {@link #close()} may
   * race with a concurrent migration batch reading it.
   */
  private volatile MigrationPrefetcher prefetcher;

  /**
   * Executor used for trie-node prefetch (Design-5 part 2a). Defaults to {@link #PREFETCH_POOL};
   * overridable via {@link #setPrefetchExecutorForTesting} so tests can inject a synchronous
   * executor and observe prefetch effects deterministically. Volatile because it is read from
   * {@link #initArchiveTrieBuilder} and written from test code on a different thread before {@link
   * #migrate()} starts the migration thread.
   */
  private volatile Executor prefetchExecutor = PREFETCH_POOL;

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicLong migratedBlockNumber = new AtomicLong(0);
  protected final AtomicBoolean migrationRunning = new AtomicBoolean(false);
  protected final AtomicLong ongoingTarget = new AtomicLong(0);
  protected final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  private volatile boolean catchUpFailed = false;
  protected volatile OptionalLong blockObserverId = OptionalLong.empty();
  private boolean closed = false;

  /**
   * The append-only trie history builder; non-null when trie-node history capture is enabled (i.e.
   * the node was started with the archive trie index flag). {@code null} in flat-archive-only mode.
   *
   * <p>Non-final so that {@link #resetArchiveTrieBuilder()} can reinitialize it after a failed or
   * aborted batch commit by re-anchoring from the last committed progress in storage.
   */
  private ArchiveTrieBuilder archiveTrieBuilder;

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator without trie-node history capture.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    this.archiveTrieBuilder = null;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_block",
        "The current block the Bonsai archive migration has reached",
        migratedBlockNumber::get);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator with optional trie-node history capture via {@link
   * ArchiveTrieBuilder}.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   * @param trieNodeHistoryEnabled whether to capture trie-node history into {@code
   *     TRIE_NODE_HISTORY_ARCHIVE_V2} via {@link ArchiveTrieBuilder}
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy,
      final boolean trieNodeHistoryEnabled) {
    this(
        worldStateStorage,
        trieLogManager,
        blockchain,
        executorService,
        metricsSystem,
        archiveStrategy);
    if (trieNodeHistoryEnabled) {
      this.archiveTrieBuilder = initArchiveTrieBuilder();
    }
  }

  /**
   * Convenience constructor for tests that want trie-node history capture enabled without injecting
   * an executor or metrics system.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param blockchain the blockchain for reading block headers
   * @param trieLogManager the trie log manager for reading trie logs
   * @param trieNodeHistoryEnabled whether to enable trie-node history capture
   */
  @VisibleForTesting
  BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final MutableBlockchain blockchain,
      final TrieLogManager trieLogManager,
      final boolean trieNodeHistoryEnabled) {
    this(
        worldStateStorage,
        trieLogManager,
        blockchain,
        Executors.newScheduledThreadPool(1),
        new NoOpMetricsSystem(),
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy()),
        trieNodeHistoryEnabled);
  }

  /**
   * Index components replaced by {@link ArchiveTrieBuilder} in Task 11; use the constructor with
   * {@code trieNodeHistoryEnabled=true} instead. The {@code historyStore}, {@code changeIndex}, and
   * {@code progress} arguments are accepted but ignored. Kept for backward compile compatibility
   * only; callers should migrate to the 7-arg constructor.
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex,
      final TrieNodeIndexProgress progress) {
    // index machinery replaced by ArchiveTrieBuilder; historyStore/changeIndex/progress ignored
    this(
        worldStateStorage,
        trieLogManager,
        blockchain,
        executorService,
        metricsSystem,
        archiveStrategy);
  }

  @VisibleForTesting
  void setMaxBlocksPerBatchForTesting(final int n) {
    maxBlocksPerBatch = n;
  }

  @VisibleForTesting
  void setMaxBatchBytesForTesting(final long n) {
    maxBatchBytes = n;
  }

  /**
   * Enables or disables trie-node prefetch for testing. Must be called before {@link #migrate()}.
   *
   * <p>If a prefetcher was already created (e.g. {@link #initArchiveTrieBuilder} ran eagerly from
   * the trieNodeHistoryEnabled constructor before this setter could run), disabling here also
   * closes and discards it so that {@code migrateBlocks} sees {@code prefetcher == null} and
   * genuinely stops submitting prefetch tasks.
   *
   * @param enabled whether trie-node prefetch should run during migration
   */
  @VisibleForTesting
  void setPrefetchEnabledForTesting(final boolean enabled) {
    this.prefetchEnabled = enabled;
    if (!enabled && prefetcher != null) {
      prefetcher.close();
      prefetcher = null;
    }
  }

  /**
   * Overrides the executor used for trie-node prefetch (2a) for testing. Must be called before
   * {@link #migrate()}.
   *
   * @param executor the executor to use for background prefetch tasks
   */
  @VisibleForTesting
  void setPrefetchExecutorForTesting(final Executor executor) {
    this.prefetchExecutor = executor;
    if (prefetchEnabled && prefetcher != null) {
      prefetcher.close();
      prefetcher =
          new MigrationPrefetcher(
              worldStateStorage.getComposedWorldStateStorage(),
              prefetchExecutor,
              PREFETCH_MAX_IN_FLIGHT,
              PREFETCH_MAX_DEPTH);
    }
  }

  /**
   * Returns the number of trie-node prefetch tasks submitted so far.
   *
   * @return 0 when prefetch is disabled or the prefetcher has not been created; otherwise the
   *     prefetcher's submitted-task count
   */
  @VisibleForTesting
  long prefetchTasksSubmittedForTesting() {
    final MigrationPrefetcher p = prefetcher;
    return p == null ? 0L : p.submittedTaskCount();
  }

  /**
   * Index prefetch replaced by {@link ArchiveTrieBuilder}; always returns 0 in Task 11+. Kept for
   * backward compile compatibility with existing tests that assert on this value.
   */
  @VisibleForTesting
  long indexPrefetchBaseHitsForTesting() {
    return 0L;
  }

  /**
   * Migrates Bonsai flat DB to Bonsai archive format.
   *
   * @return a CompletableFuture that completes when migration finishes
   */
  public synchronized CompletableFuture<Void> migrate() {
    if (closed) {
      LOG.debug("migrate called after close; skipping");
      return CompletableFuture.completedFuture(null);
    }
    if (!migrationRunning.compareAndSet(false, true)) {
      LOG.warn("Bonsai migration already in progress, ignoring");
      return CompletableFuture.completedFuture(null);
    }

    final Instant migrationStartTime = Instant.now();
    final long lastProcessedBlock = getMigrationProgress().orElse(-1L);
    final long startBlock = lastProcessedBlock + 1;
    migratedBlockNumber.set(Math.max(0, lastProcessedBlock));

    final AtomicLong target = new AtomicLong(archiveTarget(blockchain.getChainHeadBlockNumber()));
    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    final long newTarget = archiveTarget(event.getHeader().getNumber());
                    target.updateAndGet(current -> Math.max(current, newTarget));
                  }
                }));

    LOG.info("Starting Bonsai Archive migration from block {}", startBlock);
    try {
      return CompletableFuture.runAsync(
          () -> {
            try {
              migrateBlocks(startBlock, target, true);
              worldStateStorage.upgradeToArchiveFlatDbMode();
              logCompletion(startBlock, target.get(), migrationStartTime);
              // Hand off observers without a gap: register the ongoing observer first, then
              // remove the bulk observer. A block arriving mid-handoff still reaches the ongoing
              // observer; removing the bulk one first would drop any event landing in between.
              final OptionalLong bulkObserverId = blockObserverId;
              blockObserverId = OptionalLong.empty();
              startOngoingMigration();
              bulkObserverId.ifPresent(blockchain::removeObserver);
            } catch (final RuntimeException ex) {
              blockObserverId.ifPresent(blockchain::removeObserver);
              blockObserverId = OptionalLong.empty();
              LOG.error("Bonsai to Bonsai archive migration failed", ex);
              throw ex;
            } finally {
              migrationRunning.set(false);
            }
          },
          executorService);
    } catch (final RejectedExecutionException e) {
      blockObserverId.ifPresent(blockchain::removeObserver);
      blockObserverId = OptionalLong.empty();
      migrationRunning.set(false);
      LOG.warn("Bonsai migration executor rejected scheduling", e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private static final int MAX_BATCH_RETRIES = 5;

  private static boolean isOptimisticConflictError(final StorageException e) {
    Throwable t = e;
    while (t != null) {
      if (t.getMessage() != null
          && t.getMessage().contains("MemTable only contains changes newer")) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private void migrateBlocks(
      final long startBlock, final AtomicLong target, final boolean shouldLog) {
    CompletableFuture<Optional<TrieLog>> prefetched = prefetchTrieLog(startBlock);
    long blockNumber = startBlock;
    int consecutiveRetries = 0;
    while (blockNumber <= target.get()) {
      // Drop decoded trie-node objects from the previous successful batch to bound heap usage;
      // re-root lazily on the next applyBlock via HistoryNodeCache (backed by committed history).
      if (archiveTrieBuilder != null) {
        archiveTrieBuilder.resetBatchState();
      }
      final long batchStartBlock = blockNumber;
      final SegmentedKeyValueStorageTransaction tx =
          worldStateStorage
              .getComposedWorldStateStorage()
              .startNormalPriorityWriteBatchTransaction();
      long lastInBatch = -1;
      boolean committed = false;
      boolean retry = false;
      // Running byte counter for the batch-size guard (rough approximation based on change counts).
      long batchBytes = 0L;
      try {
        int blocksInBatch = 0;
        for (; blockNumber <= target.get() && blocksInBatch < maxBlocksPerBatch; blockNumber++) {
          final Optional<TrieLog> maybeTrieLog = prefetched.join();
          prefetched = prefetchTrieLog(blockNumber + 1);
          if (prefetcher != null) {
            maybeTrieLog.ifPresent(prefetcher::prefetchTrieNodes);
          }
          if (maybeTrieLog.isEmpty()) {
            if (blockNumber > 0) {
              throw new IllegalStateException("No trie log found for block " + blockNumber);
            }
            continue;
          }
          if (archiveTrieBuilder != null) {
            migrateTrieBlock(maybeTrieLog.get(), blockNumber, tx);
          }
          processBlock(maybeTrieLog.get(), blockNumber, tx);
          lastInBatch = blockNumber;
          blocksInBatch++;
          final TrieLog tl = maybeTrieLog.get();
          batchBytes +=
              (long) tl.getAccountChanges().size() * 200
                  + tl.getStorageChanges().values().stream().mapToInt(m -> m.size()).sum() * 100L;
          if (batchBytes >= maxBatchBytes) {
            blockNumber++;
            break;
          }
        }
        if (lastInBatch >= 0) {
          final long overlayBytes = batchBytes;
          saveProgress(lastInBatch, tx);
          try {
            tx.commit();
            committed = true;
            consecutiveRetries = 0;
          } catch (final StorageException e) {
            if (isOptimisticConflictError(e) && consecutiveRetries < MAX_BATCH_RETRIES) {
              consecutiveRetries++;
              retry = true;
              LOG.warn(
                  "Migration batch commit failed due to OptimisticTransaction conflict "
                      + "(attempt {}/{}), retrying from block {}",
                  consecutiveRetries,
                  MAX_BATCH_RETRIES,
                  batchStartBlock);
            } else {
              throw e;
            }
          }
          if (committed) {
            migratedBlockNumber.set(lastInBatch);
            LOG.atDebug()
                .setMessage("Migration batch committed: {} blocks, last={}, overlayBytes={}")
                .addArgument(blocksInBatch)
                .addArgument(lastInBatch)
                .addArgument(overlayBytes)
                .log();
            if (shouldLog) {
              logProgress(lastInBatch, target.get());
            }
          }
        }
      } finally {
        if (!committed && archiveTrieBuilder != null) {
          resetArchiveTrieBuilder();
        }
        if (!committed) {
          try {
            tx.rollback();
          } catch (final IllegalStateException rollbackEx) {
            // SegmentedKeyValueStorageTransactionValidatorDecorator sets active=false before
            // calling the underlying RocksDB commit; if that commit threw, active is already
            // false and rollback is impossible. The original exception propagates cleanly.
            LOG.debug(
                "Batch tx already inactive during rollback (commit may have failed)", rollbackEx);
          }
        }
      }
      if (retry) {
        blockNumber = batchStartBlock;
        prefetched = prefetchTrieLog(batchStartBlock);
        if (archiveTrieBuilder != null) {
          resetArchiveTrieBuilder();
        }
      }
    }
  }

  private CompletableFuture<Optional<TrieLog>> prefetchTrieLog(final long blockNumber) {
    return CompletableFuture.supplyAsync(
        () ->
            blockchain
                .getBlockHeader(blockNumber)
                .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash())),
        PREFETCH_POOL);
  }

  /**
   * Starts the ongoing migration of blocks to bonsai archive. This should only be called after the
   * initial migration has been completed.
   */
  public synchronized void startOngoingMigration() {
    if (closed) {
      LOG.debug("startOngoingMigration called after close; skipping");
      return;
    }
    if (blockObserverId.isPresent()) {
      LOG.debug("startOngoingMigration called while an observer is already registered; skipping");
      return;
    }
    migratedBlockNumber.set(getMigrationProgress().orElse(0L));
    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (!event.isNewCanonicalHead()) {
                    return;
                  }
                  final long newTarget = archiveTarget(event.getHeader().getNumber());
                  if (newTarget <= 0) {
                    return;
                  }
                  ongoingTarget.accumulateAndGet(newTarget, Math::max);
                  scheduleCatchUpIfNeeded();
                }));
  }

  private void scheduleCatchUpIfNeeded() {
    if (catchUpFailed) {
      return;
    }
    if (!catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug(
          "Bonsai migrator executor shut down; skipping migration up to block {}",
          ongoingTarget.get());
    }
  }

  private void catchUp() {
    boolean failed = false;
    try {
      final long startBlock = migratedBlockNumber.get() + 1;
      final long initialTarget = ongoingTarget.get();
      if (startBlock > initialTarget) {
        return;
      }
      final long blocksToMigrate = initialTarget - startBlock + 1;
      final boolean shouldLog = blocksToMigrate >= CATCHUP_LOG_THRESHOLD;
      final Instant catchUpStart = shouldLog ? Instant.now() : null;
      LOG.debug(
          "Bonsai archive catch-up starting: {} blocks from {} to {}",
          blocksToMigrate,
          startBlock,
          initialTarget);
      migrateBlocks(startBlock, ongoingTarget, shouldLog);
      if (shouldLog) {
        final Duration duration = Duration.between(catchUpStart, Instant.now());
        LOG.info(
            "Bonsai archive catch-up complete: {} blocks in {}",
            (migratedBlockNumber.get() - startBlock + 1),
            DurationFormatUtils.formatDurationWords(duration.toMillis(), true, true));
      }
    } catch (final RuntimeException ex) {
      failed = true;
      catchUpFailed = true;
      LOG.error(
          "Bonsai archive catch-up failed at block {} — archive proofs will be unavailable until restart: {}",
          migratedBlockNumber.get() + 1,
          ex.getMessage(),
          ex);
    } finally {
      catchUpRunning.set(false);
      // Do not reschedule on failure — a persistent error would otherwise create a
      // tight log-spam loop. The migration will resume on the next node restart.
      if (!failed && migratedBlockNumber.get() < ongoingTarget.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  private long archiveTarget(final long blockNumber) {
    return Math.max(0, blockNumber - trieLogManager.getMaxLayersToLoad());
  }

  /**
   * Returns the current migrated block.
   *
   * @return the highest block number that bonsai archive has migrated to
   */
  public long getMigratedBlockNumber() {
    return migratedBlockNumber.get();
  }

  @Override
  public synchronized void close() {
    closed = true;
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    if (prefetcher != null) {
      prefetcher.close();
    }
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Migration executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void logProgress(final long blockNumber, final long endBlock) {
    LogUtil.throttledLog(
        () -> {
          long progressPercent = endBlock > 0 ? (blockNumber * 100) / endBlock : 0;
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
    final BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
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
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
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

  @VisibleForTesting
  protected Optional<Long> getMigrationProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(ACCOUNT_INFO_STATE_ARCHIVE, MIGRATION_PROGRESS_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        ACCOUNT_INFO_STATE_ARCHIVE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }

  /**
   * Builds a fresh {@link ArchiveTrieBuilder} anchored at the last <em>committed</em> progress.
   * Used both at startup and after any failed/aborted batch — both cases reduce to "trust storage,
   * discard memory", because progress and history always commit together in the same {@code
   * WriteBatch} (see {@link #saveProgress}), so "last committed progress" and "last committed
   * history" can never disagree.
   *
   * <p>For fresh migrations the progress is absent and the builder starts at {@link
   * Hash#EMPTY_TRIE_HASH}. For resumed migrations, reads the account root from {@code
   * TRIE_NODE_HISTORY_ARCHIVE_V2} at the last committed block.
   */
  private ArchiveTrieBuilder initArchiveTrieBuilder() {
    final long lastMigratedBlock = getMigrationProgress().orElse(-1L);
    final Hash startingRoot = resolveAccountRootAt(lastMigratedBlock);
    if (lastMigratedBlock >= 0) {
      LOG.info(
          "Resuming archive trie migration from block {} with account root {}",
          lastMigratedBlock,
          startingRoot);
    }
    if (prefetchEnabled) {
      prefetcher =
          new MigrationPrefetcher(
              worldStateStorage.getComposedWorldStateStorage(),
              prefetchExecutor,
              PREFETCH_MAX_IN_FLIGHT,
              PREFETCH_MAX_DEPTH);
    }
    return new ArchiveTrieBuilder(
        worldStateStorage.getComposedWorldStateStorage(),
        Math.max(lastMigratedBlock, 0L),
        startingRoot);
  }

  /**
   * Reinitializes the {@link ArchiveTrieBuilder} from the last committed migration progress after a
   * failed or aborted batch. Intentionally delegates to {@link #initArchiveTrieBuilder()} — the
   * whole point of design §4.3 is that there is nothing else to recover: progress and history
   * commit atomically in the same write-batch, so "re-anchor from committed storage" is the
   * complete recovery procedure for any uncommitted batch, whether it failed due to an optimistic
   * conflict, an exception, or a deliberate {@code close()}.
   */
  private void resetArchiveTrieBuilder() {
    this.archiveTrieBuilder = initArchiveTrieBuilder();
  }

  /**
   * Resolves the account trie root hash at the given block by reading the root node from {@code
   * TRIE_NODE_HISTORY_ARCHIVE_V2}. Returns {@link Hash#EMPTY_TRIE_HASH} if no history is present
   * (e.g. fresh migration or pre-v2 data).
   */
  private Hash resolveAccountRootAt(final long block) {
    if (block < 0) {
      return Hash.EMPTY_TRIE_HASH;
    }
    final var reader =
        new TrieNodeHistoryReaderV2(worldStateStorage.getComposedWorldStateStorage());
    return reader
        .nodeAt(HistoryKey.DOMAIN_ACCOUNT, Bytes.EMPTY, block)
        .map(Hash::hash)
        .orElse(Hash.EMPTY_TRIE_HASH);
  }

  /**
   * Applies one block's trie-log to the {@link ArchiveTrieBuilder}, capturing every dirty trie node
   * as a write-once entry in {@code TRIE_NODE_HISTORY_ARCHIVE_V2}.
   */
  private void migrateTrieBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    blockchain
        .getBlockHeader(blockNumber)
        .ifPresent(header -> archiveTrieBuilder.applyBlock(trieLog, header, tx));
  }
}
