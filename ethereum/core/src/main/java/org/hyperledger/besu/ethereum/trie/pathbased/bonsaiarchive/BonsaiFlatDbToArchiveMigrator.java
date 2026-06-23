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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_FRONTIER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE;
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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveMigrationTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
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
import java.util.Map;
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

  private static final byte[] FRONTIER_TOMBSTONE = new byte[0];

  @VisibleForTesting static final int MAX_BLOCKS_PER_BATCH = 256;
  @VisibleForTesting static final long MAX_BATCH_BYTES = 256L * 1024 * 1024;

  private int maxBlocksPerBatch = MAX_BLOCKS_PER_BATCH;

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

  private void migrateBlocks(
      final long startBlock, final AtomicLong target, final boolean shouldLog) {
    CompletableFuture<Optional<TrieLog>> prefetched = prefetchTrieLog(startBlock);
    long blockNumber = startBlock;
    while (blockNumber <= target.get()) {
      final SegmentedKeyValueStorageTransaction tx =
          worldStateStorage.getComposedWorldStateStorage().startLowPriorityTransaction();
      long lastInBatch = -1;
      boolean committed = false;
      try {
        if (migrationWorldState != null) {
          migrationTrieStorage.beginBatch(tx);
          migrationChangeIndex.beginBuffered();
        }
        int blocksInBatch = 0;
        for (; blockNumber <= target.get() && blocksInBatch < maxBlocksPerBatch; blockNumber++) {
          final Optional<TrieLog> maybeTrieLog = prefetched.join();
          prefetched = prefetchTrieLog(blockNumber + 1);
          if (maybeTrieLog.isEmpty()) {
            if (blockNumber > 0) {
              throw new IllegalStateException("No trie log found for block " + blockNumber);
            }
            continue;
          }
          if (migrationWorldState != null) {
            migrateTrieBlock(maybeTrieLog.get(), blockNumber, tx);
          }
          processBlock(maybeTrieLog.get(), blockNumber, tx);
          lastInBatch = blockNumber;
          blocksInBatch++;
          if (migrationTrieStorage != null
              && migrationTrieStorage.batchByteSize() >= MAX_BATCH_BYTES) {
            blockNumber++;
            break;
          }
        }
        if (lastInBatch >= 0) {
          if (migrationChangeIndex != null) {
            migrationChangeIndex.flushBuffer(tx);
          }
          saveProgress(lastInBatch, tx);
          tx.commit();
          committed = true;
          migratedBlockNumber.set(lastInBatch);
          if (shouldLog) {
            logProgress(lastInBatch, target.get());
          }
        }
      } finally {
        if (migrationWorldState != null) {
          if (!committed && migrationChangeIndex != null) {
            migrationChangeIndex.discardBuffer();
          }
          migrationTrieStorage.endBatch();
        }
        if (!committed) {
          tx.rollback();
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
    final BonsaiArchiveFlatDbStrategy readStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    migrationTrieStorage =
        new MigrationTrieStorage(worldStateStorage.getComposedWorldStateStorage());
    final StaticArchiveFlatDbStrategyProvider provider =
        new StaticArchiveFlatDbStrategyProvider(metricsSystem, readStrategy);
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
    ((PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater()).rollForward(trieLog);

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
     * Per-batch cache of resolved TRIE_BRANCH_STORAGE reads, keyed by node location. The migration
     * runs single-threaded on archive-migrator-0 with parallel state-root computation disabled, so
     * a plain {@link HashMap} is safe. Cleared at each batch boundary via {@link #beginBatch}.
     *
     * <p>Within a batch, committed storage is stable (no per-block commits). Writes within the
     * batch go to {@link #batchOverlay}, so cached reads always reflect pre-batch committed state —
     * safe to cache for the entire batch duration.
     */
    private final Map<Bytes, Optional<byte[]>> blockReadCache = new HashMap<>();

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
     * Starts a new batch: sets the shared transaction, clears the write-back overlay, and clears
     * the read cache so the batch starts from a clean committed-storage baseline.
     */
    void beginBatch(final SegmentedKeyValueStorageTransaction tx) {
      this.activeSharedTransaction = tx;
      batchOverlay.clear();
      batchOverlayBytes = 0L;
      blockReadCache.clear();
    }

    /** Ends the current batch: clears the overlay, read cache, and shared transaction reference. */
    void endBatch() {
      this.activeSharedTransaction = null;
      batchOverlay.clear();
      batchOverlayBytes = 0L;
      blockReadCache.clear();
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
      return real.get(segmentId, key);
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
          || segmentId == TRIE_NODE_SUBBLOCK_ARCHIVE) {
        realTx.remove(segmentId, key);
      }
      // flat account/storage removes dropped — processBlock() handles those separately
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

  private static final class StaticArchiveFlatDbStrategyProvider
      extends BonsaiFlatDbStrategyProvider {
    private final BonsaiArchiveFlatDbStrategy strategy;

    StaticArchiveFlatDbStrategyProvider(
        final MetricsSystem metricsSystem, final BonsaiArchiveFlatDbStrategy strategy) {
      super(metricsSystem, DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);
      this.strategy = strategy;
    }

    @Override
    protected FlatDbStrategy createFlatDbStrategy(
        final FlatDbMode flatDbMode,
        final MetricsSystem metricsSystem,
        final CodeStorageStrategy codeStorageStrategy) {
      return strategy;
    }
  }
}
