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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_FRONTIER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveKeyUtil;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveMigrationTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
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

  private static final byte[] FRONTIER_TOMBSTONE = new byte[0];

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
   * true at the time {@link #initMigrationWorldState} ran. Volatile because {@link #close()} may
   * race with a concurrent migration batch reading it.
   */
  private volatile MigrationPrefetcher prefetcher;

  /**
   * Executor used for both trie-node prefetch (Design-5 part 2a) and index base-value prefetch
   * (part 2b). Defaults to {@link #PREFETCH_POOL}; overridable via {@link
   * #setPrefetchExecutorForTesting} so tests can inject a synchronous executor and observe prefetch
   * effects deterministically instead of racing a background thread. Volatile because it is read
   * from {@link #initMigrationWorldState} and written from test code on a different thread before
   * {@link #migrate()} starts the migration thread.
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

  private BonsaiWorldState migrationWorldState;
  private MigrationTrieStorage migrationTrieStorage;
  private BonsaiCachedMerkleTrieLoader migrationTrieLoader;
  private BonsaiWorldStateKeyValueStorage migrationKvStorage;

  // Optional trie-node differential-index components (null when index is disabled).
  private final TrieNodeHistoryStore migrationHistoryStore;
  private final TrieNodeChangeIndex migrationChangeIndex;
  private final TrieNodeIndexProgress migrationIndexProgress;
  // The migration strategy reference — retained so we can call advanceIndexProgress after
  // persist().
  private BonsaiArchiveMigrationTrieNodeStrategy migrationTrieNodeStrategy;

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator without trie-node differential index support.
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
    this(
        worldStateStorage,
        trieLogManager,
        blockchain,
        executorService,
        metricsSystem,
        archiveStrategy,
        null,
        null,
        null);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator with trie-node differential index support.
   *
   * <p>When {@code historyStore} and {@code changeIndex} are both non-null (and {@code
   * archiveStrategy} has a non-null checkpoint interval), each checkpoint's trie-node writes are
   * also captured into the differential index so that migrated blocks gain fast historical proofs.
   * {@code progress} is optional (may be null) even when the other two are supplied.
   *
   * <p>Partial injection — exactly one of {@code historyStore} / {@code changeIndex} non-null — is
   * rejected with {@link IllegalArgumentException}: both must be null (index disabled) or both
   * non-null (index enabled).
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   * @param historyStore the diff-entry store to write history entries to; must be non-null when
   *     {@code changeIndex} is non-null, null when {@code changeIndex} is null
   * @param changeIndex the change-block index to record mutations in; must be non-null when {@code
   *     historyStore} is non-null, null when {@code historyStore} is null
   * @param progress the coverage-progress tracker to advance after each block; may be null
   * @throws IllegalArgumentException if exactly one of {@code historyStore} / {@code changeIndex}
   *     is non-null
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
    if ((historyStore == null) != (changeIndex == null)) {
      throw new IllegalArgumentException(
          "historyStore and changeIndex must both be null (index disabled) or both non-null"
              + " (index enabled); got historyStore="
              + (historyStore == null ? "null" : "non-null")
              + ", changeIndex="
              + (changeIndex == null ? "null" : "non-null"));
    }
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    this.migrationHistoryStore = historyStore;
    this.migrationChangeIndex = changeIndex;
    this.migrationIndexProgress = progress;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_block",
        "The current block the Bonsai archive migration has reached",
        migratedBlockNumber::get);
    if (migrationHistoryStore != null) {
      initMigrationWorldState(metricsSystem);
      recoverTrieState();
    }
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
   * <p>If a prefetcher was already created (e.g. {@link #initMigrationWorldState} ran eagerly from
   * the index-enabled constructor before this setter could run), disabling here also closes and
   * discards it so that {@code migrateBlocks} sees {@code prefetcher == null} and genuinely stops
   * submitting prefetch tasks — not just flips a flag that arrived too late.
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
    if (!enabled && migrationChangeIndex != null) {
      // The index-enabled constructor calls initMigrationWorldState eagerly, which may have already
      // called migrationChangeIndex.enablePrefetch(...) if prefetchEnabled was true at construction
      // time (i.e. this setter arrived too late to prevent that call). Re-invoking enablePrefetch
      // with a null executor/semaphore genuinely disables index base-value prefetch (Design-5 part
      // 2b): TrieNodeChangeIndex treats a null prefetchExecutor exactly as if enablePrefetch had
      // never been called, so no background drains are submitted and prefetchBaseHits() stays 0.
      migrationChangeIndex.enablePrefetch(null, null);
    }
  }

  /**
   * Overrides the executor used for trie-node prefetch (2a) and index base-value prefetch (2b) for
   * testing. Must be called before {@link #migrate()}.
   *
   * <p>The index-enabled constructor calls {@link #initMigrationWorldState} eagerly, so by the time
   * this setter runs, the 2a {@link MigrationPrefetcher} and the 2b {@link
   * TrieNodeChangeIndex#enablePrefetch} wiring may already have captured the default {@link
   * #PREFETCH_POOL} executor — this setter would otherwise arrive too late to have any effect.
   * Mirroring {@link #setPrefetchEnabledForTesting}'s approach of re-applying wiring to
   * already-created components, this setter — when prefetch is enabled — closes and recreates the
   * 2a prefetcher with the new executor and re-invokes {@link TrieNodeChangeIndex#enablePrefetch}
   * with the new executor (and a fresh semaphore), so the injected executor genuinely takes effect.
   * A test injecting a synchronous ({@code Runnable::run}) executor here makes the background
   * {@code multiGet} run inline before it is consulted, eliminating the race with the fire-and-
   * forget {@code drainPrefetch}/{@code flushBuffer} design.
   *
   * @param executor the executor to use for background prefetch tasks
   */
  @VisibleForTesting
  void setPrefetchExecutorForTesting(final Executor executor) {
    this.prefetchExecutor = executor;
    if (prefetchEnabled) {
      if (prefetcher != null) {
        prefetcher.close();
        prefetcher =
            new MigrationPrefetcher(
                worldStateStorage.getComposedWorldStateStorage(),
                prefetchExecutor,
                PREFETCH_MAX_IN_FLIGHT,
                PREFETCH_MAX_DEPTH);
      }
      if (migrationChangeIndex != null) {
        migrationChangeIndex.enablePrefetch(
            prefetchExecutor, new Semaphore(PREFETCH_MAX_IN_FLIGHT));
      }
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
   * Returns the number of index base-value reads (Design-5 part 2b) that {@link
   * TrieNodeChangeIndex#flushBuffer} consumed directly from its background prefetch staging map.
   *
   * @return 0 when the trie-node differential index is disabled or prefetch never produced a usable
   *     staged value; otherwise the change index's cumulative prefetch-hit count
   */
  @VisibleForTesting
  long indexPrefetchBaseHitsForTesting() {
    return migrationChangeIndex == null ? 0L : migrationChangeIndex.prefetchBaseHits();
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
      final long batchStartBlock = blockNumber;
      final SegmentedKeyValueStorageTransaction tx =
          worldStateStorage.getComposedWorldStateStorage().startWriteBatchTransaction();
      long lastInBatch = -1;
      boolean committed = false;
      boolean retry = false;
      try {
        if (migrationWorldState != null) {
          migrationTrieStorage.beginBatch(tx);
          migrationChangeIndex.beginBuffered();
        }
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
          if (migrationWorldState != null) {
            migrateTrieBlock(maybeTrieLog.get(), blockNumber, tx);
          }
          final SegmentedKeyValueStorageTransaction processTx =
              migrationTrieStorage != null ? new FlatCapturingTx(tx, migrationTrieStorage) : tx;
          processBlock(maybeTrieLog.get(), blockNumber, processTx);
          lastInBatch = blockNumber;
          blocksInBatch++;
          if (migrationTrieStorage != null
              && migrationTrieStorage.batchByteSize() >= maxBatchBytes) {
            blockNumber++;
            break;
          }
        }
        if (lastInBatch >= 0) {
          if (migrationChangeIndex != null) {
            migrationChangeIndex.flushBuffer(tx);
          }
          final long overlayBytes =
              migrationTrieStorage != null ? migrationTrieStorage.batchByteSize() : 0L;
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
        if (migrationWorldState != null) {
          if (committed) {
            migrationTrieStorage.endBatch();
          } else {
            // Batch failed or is being retried. Cancel without promoting uncommitted trie-node
            // values into the read cache. If flushBuffer was called (lastInBatch >= 0) it may
            // have updated indexCache with values that were never persisted — clear it so the
            // retry reads the correct base state from committed storage.
            if (migrationChangeIndex != null) {
              migrationChangeIndex.discardBuffer();
              if (lastInBatch >= 0) {
                migrationChangeIndex.clearIndexCache();
              }
            }
            migrationTrieStorage.cancelBatch();
          }
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
        if (migrationWorldState != null) {
          // The failed persist() advanced migrationWorldState's internal root past the rolled-back
          // batch. Reset to a fresh world state that reads the correct frontier from storage.
          resetMigrationWorldState();
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

  private void initMigrationWorldState(final MetricsSystem metricsSystem) {
    migrationTrieStorage =
        new MigrationTrieStorage(worldStateStorage.getComposedWorldStateStorage());
    final StaticArchiveFlatDbStrategyProvider provider =
        new StaticArchiveFlatDbStrategyProvider(metricsSystem);
    provider.loadFlatDbStrategy(migrationTrieStorage);
    migrationTrieLoader = new NoopBonsaiCachedMerkleTrieLoader();
    migrationTrieNodeStrategy =
        new BonsaiArchiveMigrationTrieNodeStrategy(
            migrationTrieLoader,
            migrationHistoryStore,
            migrationChangeIndex,
            migrationIndexProgress);
    migrationKvStorage =
        new BonsaiWorldStateKeyValueStorage(
            provider,
            migrationTrieStorage,
            new InMemoryKeyValueStorage(),
            CacheManager.NO_OP_CACHE,
            0L,
            migrationTrieNodeStrategy);
    if (prefetchEnabled) {
      prefetcher =
          new MigrationPrefetcher(
              worldStateStorage.getComposedWorldStateStorage(),
              prefetchExecutor,
              PREFETCH_MAX_IN_FLIGHT,
              PREFETCH_MAX_DEPTH);
    }
    if (prefetchEnabled && migrationChangeIndex != null) {
      // Design-5 part 2b: background prefetch of committed TRIE_NODE_INDEX_ARCHIVE base values.
      // Shares the same bounded executor as the part-2a trie-node prefetcher above, with its own
      // semaphore so the two prefetchers' in-flight limits are independent.
      migrationChangeIndex.enablePrefetch(prefetchExecutor, new Semaphore(PREFETCH_MAX_IN_FLIGHT));
    }
    resetMigrationWorldState();
  }

  /**
   * Creates a fresh {@link BonsaiWorldState} backed by the existing {@link #migrationKvStorage} and
   * {@link #migrationTrieStorage}. Call this after a failed batch commit to discard the in-memory
   * trie state that was rolled back with the transaction, while preserving the cross-batch {@link
   * MigrationTrieStorage#blockReadCache} which still holds valid committed entries.
   */
  private void resetMigrationWorldState() {
    final CodeCache codeCache = new CodeCache();
    migrationWorldState =
        new BonsaiWorldState(
            migrationKvStorage,
            migrationTrieLoader,
            new NoOpBonsaiCachedWorldStorageManager(
                migrationKvStorage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
                .parallelStateRootComputationEnabled(false)
                .build(),
            codeCache);
  }

  private void recoverTrieState() {
    final Optional<Long> progress = getMigrationProgress();
    if (progress.isEmpty()) {
      migrationChangeIndex.enableFreshMigrationMode();
    } else {
      LOG.info("Resuming archive migration from block {} (frontier CF available)", progress.get());
    }
  }

  private void migrateTrieBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater();
    accumulator.setSkipCodeRoll(true);
    accumulator.rollForward(trieLog);

    if (migrationHistoryStore != null) {
      // Index mode: persist every block so the differential index gets per-block entries. persist()
      // writes the frontier + diff-index into the shared per-block transaction (via the deferred
      // MigrationTransaction) and does not commit it — the migrator commits once in migrateBlocks.
      blockchain
          .getBlockHeader(blockNumber)
          .ifPresent(
              header -> {
                migrationWorldState.persist(header);
                // flushIndexIfEnabled() inside persist() already advanced migrationIndexProgress to
                // blockNumber (it reads WORLD_BLOCK_NUMBER_KEY before commit, sees the previous
                // block's number and adds 1). Persist the already-correct progress into the same
                // shared transaction so it commits atomically with the frontier — note save() must
                // hit real TRIE_BRANCH_STORAGE, so it is written directly on tx (not via the
                // MigrationTransaction, which would redirect it to TRIE_BRANCH_FRONTIER).
                if (migrationIndexProgress != null) {
                  migrationIndexProgress.save(tx);
                }
              });
      return;
    }
  }

  static final class MigrationTrieStorage extends LayeredKeyValueStorage {
    private final SegmentedKeyValueStorage real;

    /**
     * Maximum entries in {@link #blockReadCache}. At ~200–500 bytes per trie node plus key
     * overhead, 500 K entries ≈ 150–250 MB heap — acceptable for a long-running archive migration.
     */
    private static final int MAX_TRIE_NODE_CACHE_ENTRIES = 500_000;

    /**
     * Cross-batch LRU read cache for TRIE_BRANCH_STORAGE lookups. During migration, committed
     * TRIE_BRANCH_STORAGE is stable — all writes redirect to TRIE_BRANCH_FRONTIER — so reads cached
     * in one batch remain valid in later batches. Nodes written in the current batch are refreshed
     * into this cache by {@link #endBatch} (from {@link #batchOverlay}) so the next batch's {@code
     * captureTrieNodeDiff} prior-node reads hit the cache instead of disk.
     *
     * <p>Single-threaded migration ({@code archive-migrator-0}, parallel state-root disabled), so
     * {@link LinkedHashMap} (access-order) is safe without synchronisation.
     */
    @SuppressWarnings("serial")
    private final Map<Bytes, Optional<byte[]>> blockReadCache =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<Bytes, Optional<byte[]>> eldest) {
            return size() > MAX_TRIE_NODE_CACHE_ENTRIES;
          }
        };

    /**
     * Write-back overlay for reads within a batch. Populated by {@link MigrationTransaction}
     * put/remove calls during the batch; consulted by {@link #get} before the read cache and
     * committed storage. Keyed by the logical TRIE_BRANCH_STORAGE key (not the physical frontier
     * key). A {@link Optional#empty()} value represents a tombstone (deleted node).
     */
    private final Map<Bytes, Optional<byte[]>> batchOverlay = new HashMap<>();

    /** Running sum of value bytes written to {@link #batchOverlay}. Used for batch-size limit. */
    private long batchOverlayBytes = 0L;

    /**
     * Per-batch overlay for flat account writes (ACCOUNT_INFO_STATE_ARCHIVE). Keyed by archive key
     * ({@code addressHash + blockSuffix}), sorted lexicographically to support floor-entry queries.
     * Populated by {@link FlatCapturingTx} so that block N's accounts are visible to block N+1's
     * {@code rollForward} via {@link #getNearestBefore} without requiring an interim commit.
     */
    private final TreeMap<Bytes, Optional<byte[]>> flatAccountOverlay = new TreeMap<>();

    /**
     * Per-batch overlay for flat storage writes (ACCOUNT_STORAGE_ARCHIVE). Same motivation as
     * {@link #flatAccountOverlay}.
     */
    private final TreeMap<Bytes, Optional<byte[]>> flatStorageOverlay = new TreeMap<>();

    /**
     * When non-null, {@link #startTransaction()} returns a {@link MigrationTransaction} that writes
     * into this migrator-owned transaction and defers commit/rollback to the migrator. This lets a
     * batch's {@code persist()} (frontier + diff-index) share the same atomic transaction as the
     * flat-state and progress writes.
     */
    private SegmentedKeyValueStorageTransaction activeSharedTransaction;

    MigrationTrieStorage(final SegmentedKeyValueStorage real) {
      super(real);
      this.real = real;
    }

    /**
     * Clears the per-batch read cache. Kept for backward compatibility with existing unit tests.
     */
    void resetBlockCache() {
      blockReadCache.clear();
    }

    /**
     * Starts a new batch: sets the shared transaction and clears the write-back overlays.
     *
     * <p>{@link #blockReadCache} is intentionally NOT cleared here. Committed TRIE_BRANCH_STORAGE
     * is stable throughout migration (all writes redirect to TRIE_BRANCH_FRONTIER), so cache
     * entries from prior batches remain correct. Nodes written in the previous batch were refreshed
     * into the cache by {@link #endBatch}.
     */
    void beginBatch(final SegmentedKeyValueStorageTransaction tx) {
      this.activeSharedTransaction = tx;
      batchOverlay.clear();
      batchOverlayBytes = 0L;
      flatAccountOverlay.clear();
      flatStorageOverlay.clear();
    }

    /**
     * Ends the current batch: refreshes the read cache with all nodes written in this batch (so the
     * next batch's prior-node reads hit the cache), then clears overlays.
     */
    void endBatch() {
      this.activeSharedTransaction = null;
      // Push final batchOverlay values into blockReadCache so that the next batch's
      // captureTrieNodeDiff prior-node reads find the correct post-commit values in cache
      // instead of falling through to disk.
      blockReadCache.putAll(batchOverlay);
      batchOverlay.clear();
      batchOverlayBytes = 0L;
      flatAccountOverlay.clear();
      flatStorageOverlay.clear();
    }

    /**
     * Cancels the current batch without promoting {@link #batchOverlay} into {@link
     * #blockReadCache}. Call this instead of {@link #endBatch} when the batch's transaction commit
     * failed so that uncommitted trie-node values are not visible to the next batch's prior-node
     * reads.
     */
    void cancelBatch() {
      this.activeSharedTransaction = null;
      batchOverlay.clear();
      batchOverlayBytes = 0L;
      flatAccountOverlay.clear();
      flatStorageOverlay.clear();
    }

    /** Returns the approximate byte size of frontier writes accumulated in the batch overlay. */
    long batchByteSize() {
      return batchOverlayBytes;
    }

    @Override
    public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Batch overlay is checked first — it captures writes made within the current batch,
        // including metadata keys (WORLD_BLOCK_NUMBER_KEY etc.) written by earlier blocks.
        final Bytes overlayKey = Bytes.wrap(key);
        final Optional<byte[]> overlayVal = batchOverlay.get(overlayKey);
        if (overlayVal != null) {
          return overlayVal; // Optional.empty() means tombstone (deleted in this batch)
        }

        // Metadata keys (WORLD_*, ARCHIVE_PROOF_BLOCK_NUMBER_KEY) must not fall through to live
        // TRIE_BRANCH_STORAGE — live HEAD values would corrupt the migration context. These keys
        // are written to TRIE_BRANCH_FRONTIER via MigrationTransaction.
        if (isMetadataKey(key)) {
          return real.get(TRIE_BRANCH_FRONTIER, key);
        }
        final Bytes cacheKey = overlayKey;
        final Optional<byte[]> cached = blockReadCache.get(cacheKey);
        if (cached != null) {
          return cached;
        }
        final Optional<byte[]> result;
        final Optional<byte[]> frontier = real.get(TRIE_BRANCH_FRONTIER, key);
        if (frontier.isPresent()) {
          final byte[] val = frontier.get();
          // Zero-length sentinel means the node was explicitly deleted — no fallthrough.
          result = val.length == 0 ? Optional.empty() : frontier;
        } else {
          // Frontier miss: fall through to live storage.
          // Unchanged trie nodes are byte-identical at any historical state and at live HEAD.
          result = real.get(segmentId, key);
        }
        blockReadCache.put(cacheKey, result);
        return result;
      }
      if (segmentId == ACCOUNT_INFO_STATE_ARCHIVE) {
        final Bytes flatKey = Bytes.wrap(key);
        final Optional<byte[]> v = flatAccountOverlay.get(flatKey);
        if (v != null) {
          return v;
        }
      } else if (segmentId == ACCOUNT_STORAGE_ARCHIVE) {
        final Bytes flatKey = Bytes.wrap(key);
        final Optional<byte[]> v = flatStorageOverlay.get(flatKey);
        if (v != null) {
          return v;
        }
      }
      return real.get(segmentId, key);
    }

    void recordFlatWrite(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      final Bytes flatKey = Bytes.wrap(key);
      if (segmentId == ACCOUNT_INFO_STATE_ARCHIVE) {
        flatAccountOverlay.put(flatKey, Optional.of(value));
      } else if (segmentId == ACCOUNT_STORAGE_ARCHIVE) {
        flatStorageOverlay.put(flatKey, Optional.of(value));
      }
    }

    void recordFlatRemove(final SegmentIdentifier segmentId, final byte[] key) {
      final Bytes flatKey = Bytes.wrap(key);
      if (segmentId == ACCOUNT_INFO_STATE_ARCHIVE) {
        flatAccountOverlay.put(flatKey, Optional.empty());
      } else if (segmentId == ACCOUNT_STORAGE_ARCHIVE) {
        flatStorageOverlay.put(flatKey, Optional.empty());
      }
    }

    /**
     * Overrides {@code getNearestBefore} for flat archive segments so that writes made within the
     * current batch (captured in {@link #flatAccountOverlay} / {@link #flatStorageOverlay}) are
     * visible to subsequent blocks' {@code rollForward} calls without an intermediate commit.
     *
     * <p>For all other segments the default {@link LayeredKeyValueStorage} implementation is used.
     */
    @Override
    public Optional<NearestKeyValue> getNearestBefore(
        final SegmentIdentifier segmentId, final Bytes queryKey) {
      if (segmentId == ACCOUNT_INFO_STATE_ARCHIVE || segmentId == ACCOUNT_STORAGE_ARCHIVE) {
        final TreeMap<Bytes, Optional<byte[]>> overlay =
            segmentId == ACCOUNT_INFO_STATE_ARCHIVE ? flatAccountOverlay : flatStorageOverlay;
        final Optional<NearestKeyValue> overlayNearest =
            Optional.ofNullable(overlay.floorEntry(queryKey))
                .map(e -> new NearestKeyValue(e.getKey(), e.getValue()));

        // Archive keys are naturalKey (32 or 64 bytes) + blockSuffix (8 bytes). If the overlay's
        // floor entry covers the same natural key as queryKey, it is the definitive answer:
        // migration always writes blocks in chronological order, so an overlay entry for the same
        // slot has a block number >= any committed-storage entry for that slot. Skip the RocksDB
        // iterator seek entirely in this case.
        if (overlayNearest.isPresent()) {
          final int naturalKeyLen = queryKey.size() - BonsaiArchiveKeyUtil.KEY_SUFFIX_LENGTH;
          if (overlayNearest.get().key().commonPrefixLength(queryKey) >= naturalKeyLen) {
            return overlayNearest;
          }
        }

        final Optional<NearestKeyValue> realNearest = real.getNearestBefore(segmentId, queryKey);
        if (overlayNearest.isPresent() && realNearest.isPresent()) {
          // Pick the key with the longer common prefix (= more specific match).
          // On a tie in prefix length, pick the larger key (nearest-before semantics).
          final int ovCmp = overlayNearest.get().key().compareTo(queryKey);
          final int rlCmp = realNearest.get().key().compareTo(queryKey);
          if (ovCmp == 0) return overlayNearest;
          if (rlCmp == 0) return realNearest;
          final int ovLen = overlayNearest.get().key().commonPrefixLength(queryKey);
          final int rlLen = realNearest.get().key().commonPrefixLength(queryKey);
          if (ovLen != rlLen) return ovLen > rlLen ? overlayNearest : realNearest;
          return overlayNearest.get().key().compareTo(realNearest.get().key()) > 0
              ? overlayNearest
              : realNearest;
        }
        return overlayNearest.isPresent() ? overlayNearest : realNearest;
      }
      return super.getNearestBefore(segmentId, queryKey);
    }

    private static boolean isMetadataKey(final byte[] key) {
      return java.util.Arrays.equals(key, WORLD_BLOCK_NUMBER_KEY)
          || java.util.Arrays.equals(key, WORLD_BLOCK_HASH_KEY)
          || java.util.Arrays.equals(key, WORLD_ROOT_HASH_KEY)
          || java.util.Arrays.equals(key, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    }

    @Override
    public SegmentedKeyValueStorageTransaction startTransaction() {
      if (activeSharedTransaction != null) {
        // Write into the migrator's batch transaction; the migrator owns commit/rollback so
        // frontier + index + flat + progress are committed atomically. The overlay is passed so
        // writes are visible to subsequent get() calls within the same batch.
        return new MigrationTransaction(activeSharedTransaction, true, batchOverlay, this);
      }
      return new MigrationTransaction(real.startLowPriorityTransaction());
    }

    void recordOverlayBytes(final long bytes) {
      batchOverlayBytes += bytes;
    }
  }

  private static final class MigrationTransaction implements SegmentedKeyValueStorageTransaction {
    private final SegmentedKeyValueStorageTransaction realTx;

    /**
     * When {@code true}, {@link #commit()}/{@link #rollback()}/{@link #close()} are no-ops: the
     * wrapped transaction's lifecycle is owned by the migrator, which commits frontier, diff-index,
     * flat state and progress together in a single atomic per-batch transaction. This is what makes
     * the migration crash-safe — progress can never be committed ahead of (or behind) the frontier.
     */
    private final boolean deferLifecycleToOwner;

    /**
     * Write-back overlay from the owning {@link MigrationTrieStorage}. Non-null only in the
     * deferred (batch) variant; used to make frontier writes visible to same-batch reads.
     */
    private final Map<Bytes, Optional<byte[]>> overlay;

    private final MigrationTrieStorage owner;

    MigrationTransaction(final SegmentedKeyValueStorageTransaction realTx) {
      this(realTx, false, null, null);
    }

    MigrationTransaction(
        final SegmentedKeyValueStorageTransaction realTx,
        final boolean deferLifecycleToOwner,
        final Map<Bytes, Optional<byte[]>> overlay,
        final MigrationTrieStorage owner) {
      this.realTx = realTx;
      this.deferLifecycleToOwner = deferLifecycleToOwner;
      this.overlay = overlay;
      this.owner = owner;
    }

    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        realTx.put(TRIE_BRANCH_FRONTIER, key, value);
        if (overlay != null) {
          final Bytes overlayKey = Bytes.wrap(key);
          overlay.put(overlayKey, Optional.of(value));
          owner.recordOverlayBytes(value.length);
        }
      } else if (segmentId == TRIE_NODE_HISTORY_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_META_ARCHIVE
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.put(segmentId, key, value);
      }
      // flat account/storage writes dropped — processBlock() handles those separately
    }

    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Write tombstone sentinel rather than remove() — distinguishes "deleted" from "never
        // written" since RocksDB get() returns empty for both after a remove().
        realTx.put(TRIE_BRANCH_FRONTIER, key, FRONTIER_TOMBSTONE);
        if (overlay != null) {
          overlay.put(Bytes.wrap(key), Optional.empty());
        }
      } else if (segmentId == TRIE_NODE_HISTORY_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_ARCHIVE
          || segmentId == TRIE_NODE_INDEX_META_ARCHIVE
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.remove(segmentId, key);
      }
      // flat account/storage removes dropped — processBlock() handles those separately
    }

    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      // Only the trie-node index content CF is ever merged; everything else is dropped to match
      // the same allowlist discipline as put()/remove() above — see the CAS-dedup incident
      // referenced in this class's write-path javadoc for why silent, unlisted drops are
      // dangerous here.
      if (segmentId == TRIE_NODE_INDEX_ARCHIVE) {
        realTx.merge(segmentId, key, value);
      }
    }

    @Override
    public void commit() {
      if (!deferLifecycleToOwner) {
        realTx.commit();
      }
    }

    @Override
    public void rollback() {
      if (!deferLifecycleToOwner) {
        realTx.rollback();
      }
    }

    @Override
    public void close() {
      if (!deferLifecycleToOwner) {
        realTx.close();
      }
    }
  }

  /**
   * Wraps the batch transaction passed to {@link #processBlock} and mirrors flat account/storage
   * writes into {@link MigrationTrieStorage}'s per-batch overlays. This makes block N's newly
   * created/updated accounts visible to block N+1's {@code rollForward} within the same batch,
   * without requiring an intermediate commit.
   */
  private static final class FlatCapturingTx implements SegmentedKeyValueStorageTransaction {
    private final SegmentedKeyValueStorageTransaction delegate;
    private final MigrationTrieStorage trieStorage;

    FlatCapturingTx(
        final SegmentedKeyValueStorageTransaction delegate,
        final MigrationTrieStorage trieStorage) {
      this.delegate = delegate;
      this.trieStorage = trieStorage;
    }

    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      delegate.put(segmentId, key, value);
      trieStorage.recordFlatWrite(segmentId, key, value);
    }

    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      delegate.remove(segmentId, key);
      trieStorage.recordFlatRemove(segmentId, key);
    }

    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      delegate.merge(segmentId, key, value);
    }

    @Override
    public void commit() {
      // migrateBlocks owns the lifecycle of the underlying batch tx; no-op here.
    }

    @Override
    public void rollback() {
      // migrateBlocks owns the lifecycle of the underlying batch tx; no-op here.
    }

    @Override
    public void close() {
      // migrateBlocks owns the lifecycle of the underlying batch tx; no-op here.
    }
  }

  private static final class StaticArchiveFlatDbStrategyProvider
      extends BonsaiFlatDbStrategyProvider {

    StaticArchiveFlatDbStrategyProvider(final MetricsSystem metricsSystem) {
      super(metricsSystem, DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);
    }

    @Override
    protected FlatDbStrategy createFlatDbStrategy(
        final FlatDbMode flatDbMode,
        final MetricsSystem metricsSystem,
        final CodeStorageStrategy codeStorageStrategy) {
      return new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy);
    }
  }
}
