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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.SwappableWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.Updater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates the Bonsai flat database from non-versioned format to versioned archive format after
 * initial sync completes. This allows sync to proceed at regular Bonsai performance, then converts
 * the flat DB to archive format once sync is complete.
 */
public class BonsaiArchiveFlatDbMigrator implements InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveFlatDbMigrator.class);

  // Static instance reference for debug RPC access
  private static volatile BonsaiArchiveFlatDbMigrator instance;

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final TrieLogManager trieLogManager;
  private final BonsaiArchiveFlatDbStrategy writeStrategy;
  private final AtomicBoolean isReconstructing = new AtomicBoolean(false);
  private final AtomicLong blocksProcessed = new AtomicLong(0);
  private final AtomicLong totalBlocks = new AtomicLong(0);
  private final SwappableWorldStateArchive swappableArchive;
  private final java.util.function.Supplier<BonsaiArchiveWorldStateProvider> archiveProviderFactory;
  private final AtomicLong migrationTarget = new AtomicLong(0);
  private long blockAddedObserverId;
  private final int batchSize;
  private final long startBlockOverride;

  /**
   * Creates a new BonsaiArchiveFlatDbMigrator.
   *
   * @param worldStateStorage the world state storage
   * @param blockchain the blockchain
   * @param executorService the scheduled executor service for async operations
   * @param trieLogManager the trie log manager
   * @param metricsSystem the metrics system
   * @param swappableArchive optional swappable archive for provider swap (used in deferred archive
   *     mode)
   * @param archiveProviderFactory optional factory for creating archive provider (used in deferred
   *     archive mode)
   * @param batchSize number of blocks to process in each batch
   * @param startBlockOverride override start block (-1 for auto-detect)
   */
  public BonsaiArchiveFlatDbMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem,
      final SwappableWorldStateArchive swappableArchive,
      final java.util.function.Supplier<BonsaiArchiveWorldStateProvider> archiveProviderFactory,
      final int batchSize,
      final long startBlockOverride) {
    this.worldStateStorage = worldStateStorage;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.trieLogManager = trieLogManager;
    this.swappableArchive = swappableArchive;
    this.archiveProviderFactory = archiveProviderFactory;
    this.batchSize = batchSize;
    this.startBlockOverride = startBlockOverride;

    // Create strategy for writing versioned archive entries
    final CodeHashCodeStorageStrategy codeStorageStrategy = new CodeHashCodeStorageStrategy();
    this.writeStrategy = new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy);

    // Store instance for debug RPC access
    instance = this;

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archive_migration_blocks_processed",
        "Number of blocks processed during archive migration",
        blocksProcessed::get);

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archive_migration_blocks_target",
        "Target number of blocks to process during archive migration",
        totalBlocks::get);

    LOG.info("BonsaiArchiveFlatDbMigrator configured with batchSize={}", batchSize);
  }

  /**
   * Creates a new BonsaiArchiveFlatDbMigrator without provider swapping (for testing).
   *
   * @param worldStateStorage the world state storage
   * @param blockchain the blockchain
   * @param executorService the scheduled executor service for async operations
   * @param trieLogManager the trie log manager
   * @param metricsSystem the metrics system
   * @param batchSize the number of blocks to process in each batch
   * @param startBlockOverride override start block (-1 for auto-detect)
   */
  public BonsaiArchiveFlatDbMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem,
      final int batchSize,
      final long startBlockOverride) {
    this(
        worldStateStorage,
        blockchain,
        executorService,
        trieLogManager,
        metricsSystem,
        null,
        null,
        batchSize,
        startBlockOverride);
  }

  /**
   * Creates a new BonsaiArchiveFlatDbMigrator without provider swapping and auto-detect start block
   * (for testing).
   *
   * @param worldStateStorage the world state storage
   * @param blockchain the blockchain
   * @param executorService the scheduled executor service for async operations
   * @param trieLogManager the trie log manager
   * @param metricsSystem the metrics system
   * @param batchSize the number of blocks to process in each batch
   */
  public BonsaiArchiveFlatDbMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem,
      final int batchSize) {
    this(
        worldStateStorage,
        blockchain,
        executorService,
        trieLogManager,
        metricsSystem,
        batchSize,
        -1L); // Auto-detect start block for tests
  }

  @Override
  public void onInitialSyncCompleted() {
    LOG.info(
        "Initial sync completed. Starting deferred archive migration from trielogs in background...");

    // Check if we're still in FULL mode (not yet upgraded to ARCHIVE)
    if (worldStateStorage.getFlatDbMode() == FlatDbMode.FULL) {
      startMigration();
    } else {
      LOG.info("Initial sync completed but already in ARCHIVE mode, skipping migration");
    }
  }

  /**
   * Triggers migration regardless of current flat DB mode. This is useful for testing and can be
   * called via the debug RPC. This will reset the migration progress and start from the first
   * block.
   */
  public void triggerMigration() {
    LOG.info("Migration triggered manually, resetting to start from first block");

    // Reset the latest archived flat DB block to force restart from the first block
    // Setting to 0 will cause performMigration to start from block 1 (startBlock = 0 + 1)
    worldStateStorage.setLatestArchivedFlatDbBlock(0L);

    // Reset metrics
    blocksProcessed.set(0);
    totalBlocks.set(0);

    startMigration();
  }

  @Override
  public void onInitialSyncRestart() {
    // No action needed on sync restart for deferred archive mode
    LOG.debug("Initial sync restarted, deferred archive migration will wait");
  }

  /**
   * Starts the asynchronous migration process using the provided executor. Processes all trielogs
   * from the earliest available to the current chain head, building the versioned archive state.
   */
  private void startMigration() {
    if (!isReconstructing.compareAndSet(false, true)) {
      LOG.warn("Archive migration is already in progress");
      return;
    }

    LOG.info("Starting asynchronous Bonsai archive migration from trielogs");

    // Subscribe to block added events to track chain growth during migration
    // Note: This will handle blocks added AFTER subscription starts
    blockAddedObserverId =
        blockchain.observeBlockAdded(
            event -> {
              // Update migration target when new blocks arrive
              final long newHead = event.getBlock().getHeader().getNumber();
              migrationTarget.updateAndGet(current -> Math.max(current, newHead));
              LOG.debug("Migration target updated to {} due to new block", newHead);
            });

    // Submit migration task to the executor service
    executorService.execute(
        () -> {
          try {
            performMigration();
            // Note: performMigration schedules batches asynchronously, so we don't call
            // upgradeToArchiveMode here. It will be called when the last batch completes.
          } catch (final Exception e) {
            LOG.error("Error during archive migration", e);
            blockchain.removeObserver(blockAddedObserverId);
            isReconstructing.set(false);
            throw new RuntimeException("Archive migration failed", e);
          }
        });
  }

  /** Upgrades the storage to ARCHIVE mode after migration completes. */
  private void upgradeToArchiveMode() {
    LOG.info("Archive migration complete, upgrading to ARCHIVE flat db mode");

    // Reset WORLD_BLOCK_NUMBER_KEY to the chain head so future block imports work correctly
    // During migration we set it to parent blocks, but after migration we need it at chain head
    final long chainHeadNumber = blockchain.getChainHeadBlockNumber();
    setWorldStateBlockContext(chainHeadNumber);
    LOG.info("Reset world state block context to chain head: {}", chainHeadNumber);

    worldStateStorage.upgradeToArchiveFlatDbMode();
    LOG.info("Successfully upgraded to ARCHIVE mode");

    // If we have a swappable archive and factory, swap to the archive provider
    if (swappableArchive != null && archiveProviderFactory != null) {
      LOG.info("Swapping world state provider to BonsaiArchiveWorldStateProvider...");
      final var archiveProvider = archiveProviderFactory.get();
      swappableArchive.swapProvider(archiveProvider);
      LOG.info(
          "Successfully swapped to BonsaiArchiveWorldStateProvider for full archive functionality");
    } else {
      LOG.info(
          "Provider swap not available (swappableArchive={}, archiveProviderFactory={}). This is expected if already in ARCHIVE mode or not using deferred archive.",
          swappableArchive != null ? "present" : "null",
          archiveProviderFactory != null ? "present" : "null");
    }
  }

  /** Performs the actual migration work. This method is called by the executor service. */
  private void performMigration() {
    // Determine where to start
    final long startBlock;

    if (startBlockOverride >= 0) {
      // Use override if specified (for testing)
      startBlock = startBlockOverride;
      LOG.info("Starting archive flat DB migration from block {} (override)", startBlock);
    } else {
      // Auto-detect start block
      final Optional<Long> latestArchivedFlatDbBlock =
          worldStateStorage.getLatestArchivedFlatDbBlock();

      if (latestArchivedFlatDbBlock.isPresent()) {
        // Resume from where we left off
        startBlock = latestArchivedFlatDbBlock.get() + 1;
        LOG.info(
            "Resuming archive flat DB migration from block {} (latest archived: {})",
            startBlock,
            latestArchivedFlatDbBlock.get());
      } else {
        // Start from the earliest block we have trielogs for
        // In practice, this will typically be genesis or checkpoint
        startBlock = 0L;
        LOG.info("Starting archive flat DB migration from genesis (block 0)");
      }
    }

    // Set initial migration target to current chain head
    final long initialChainHead = blockchain.getChainHeadBlockNumber();
    migrationTarget.set(initialChainHead);
    totalBlocks.set(initialChainHead + 1);
    LOG.info(
        "Archive migration will process blocks from {} to {}. Target will update as chain grows.",
        startBlock,
        initialChainHead);

    // Process first batch, which will schedule subsequent batches
    processNextBatch(startBlock);
  }

  /**
   * Processes the next batch of blocks and schedules the following batch if needed. This method
   * uses the migrationTarget which is updated by the block added observer.
   *
   * @param currentBlock the starting block for this batch
   */
  private void processNextBatch(final long currentBlock) {
    // Get the current migration target (updated by block observer)
    final long target = migrationTarget.get();

    if (currentBlock > target) {
      // Migration is complete - we've caught up with chain head
      LOG.info(
          "Bonsai archive migration completed successfully. Processed {} blocks.",
          blocksProcessed.get());

      // Unsubscribe from block events
      blockchain.removeObserver(blockAddedObserverId);

      // Upgrade to ARCHIVE mode now that migration is complete
      executorService.execute(this::upgradeToArchiveMode);

      // Clear the reconstruction flag
      isReconstructing.set(false);
      return;
    }

    // Update metrics with current target
    totalBlocks.set(target + 1);

    final long batchEnd = Math.min(currentBlock + batchSize - 1, target);
    processBlockBatch(currentBlock, batchEnd);

    // Schedule next batch immediately for maximum throughput
    final long nextBlock = batchEnd + 1;
    executorService.execute(() -> processNextBatch(nextBlock));
  }

  /**
   * Processes a batch of blocks, moving their state changes to the archive segments.
   *
   * @param startBlock the first block in the batch
   * @param endBlock the last block in the batch
   */
  /**
   * Processes a batch of blocks, moving their state changes to the archive segments. Uses a single
   * transaction for the entire batch to minimize RocksDB commit overhead. Also prefetches all
   * trielogs for the batch to minimize RocksDB read overhead.
   *
   * @param startBlock the first block in the batch
   * @param endBlock the last block in the batch (inclusive)
   */
  private void processBlockBatch(final long startBlock, final long endBlock) {
    LOG.debug("Processing block batch: {} to {}", startBlock, endBlock);

    // Prefetch all trielogs in parallel to avoid individual RocksDB reads during processing
    // Use ConcurrentHashMap for thread-safe access
    final var trieLogsByBlockNumber = new ConcurrentHashMap<Long, TrieLog>();

    // Check if we can use parallel prefetching (requires multi-threaded executor)
    final boolean useParallelPrefetch =
        !(executorService instanceof java.util.concurrent.ScheduledThreadPoolExecutor
            && ((java.util.concurrent.ScheduledThreadPoolExecutor) executorService)
                    .getCorePoolSize()
                == 1);

    if (useParallelPrefetch) {
      // Parallel prefetch for production with multi-threaded executor
      final List<CompletableFuture<Void>> prefetchFutures = new ArrayList<>();

      for (long blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
        final long blockNum = blockNumber; // Capture for lambda
        CompletableFuture<Void> future =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    final Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(blockNum);
                    if (blockHeader.isPresent()) {
                      final Hash blockHash = blockHeader.get().getHash();
                      final Optional<TrieLog> maybeTrieLog =
                          trieLogManager.getTrieLogLayer(blockHash);
                      if (maybeTrieLog.isPresent()) {
                        trieLogsByBlockNumber.put(blockNum, maybeTrieLog.get());
                      }
                    }
                  } catch (Exception e) {
                    LOG.warn(
                        "Failed to prefetch trielog for block {}: {}", blockNum, e.getMessage());
                  }
                },
                executorService);
        prefetchFutures.add(future);
      }

      // Wait for all prefetch operations to complete
      try {
        CompletableFuture.allOf(prefetchFutures.toArray(new CompletableFuture<?>[0])).join();
      } catch (Exception e) {
        LOG.warn("Some trielog prefetch operations failed, continuing with available trielogs", e);
      }
    } else {
      // Serial prefetch for single-threaded executor (tests)
      for (long blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
        final Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(blockNumber);
        if (blockHeader.isPresent()) {
          final Hash blockHash = blockHeader.get().getHash();
          final Optional<TrieLog> maybeTrieLog = trieLogManager.getTrieLogLayer(blockHash);
          if (maybeTrieLog.isPresent()) {
            trieLogsByBlockNumber.put(blockNumber, maybeTrieLog.get());
          }
        }
      }
    }

    LOG.debug(
        "Prefetched {} trielogs for batch {} to {} (parallel={})",
        trieLogsByBlockNumber.size(),
        startBlock,
        endBlock,
        useParallelPrefetch);

    // Create a single updater/transaction for the entire batch
    final Updater batchUpdater = worldStateStorage.updater();
    final var batchTransaction = batchUpdater.getWorldStateTransaction();

    try {
      long lastProcessedBlock = startBlock - 1; // Track the last block that was actually processed

      for (long blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
        if (processBlockInBatch(blockNumber, batchTransaction, trieLogsByBlockNumber)) {
          lastProcessedBlock = blockNumber;
        }
      }

      // Only commit and update if we processed at least one block
      if (lastProcessedBlock >= startBlock) {
        // Commit once for the entire batch
        batchUpdater.commit();

        // Update the latest archived block AFTER successful commit
        worldStateStorage.setLatestArchivedFlatDbBlock(lastProcessedBlock);

        LOG.debug(
            "Committed batch of {} blocks (from {} to {})",
            lastProcessedBlock - startBlock + 1,
            startBlock,
            lastProcessedBlock);
      } else {
        // No blocks were processed, rollback empty transaction
        batchUpdater.rollback();
        LOG.debug("Batch from {} to {} contained no processable blocks", startBlock, endBlock);
      }
    } catch (Exception e) {
      LOG.error(
          "Error processing block batch from {} to {}, rolling back batch",
          startBlock,
          endBlock,
          e);
      batchUpdater.rollback();
      throw e;
    }
  }

  /**
   * Processes a single block within a batch transaction, rewriting its flat DB entries to versioned
   * format without committing.
   *
   * @param blockNumber the block number to process
   * @param batchTransaction the shared transaction for the batch
   * @param prefetchedTrieLogs map of prefetched trielogs (may be empty if prefetch failed)
   * @return true if the block was processed, false if it was skipped
   */
  private boolean processBlockInBatch(
      final long blockNumber,
      final SegmentedKeyValueStorageTransaction batchTransaction,
      final java.util.Map<Long, TrieLog> prefetchedTrieLogs) {
    final Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(blockNumber);
    if (blockHeader.isEmpty()) {
      LOG.warn("Block header not found for block {}, skipping", blockNumber);
      return false;
    }

    // Try to get trielog from prefetched map first, fall back to individual fetch
    final Hash blockHash = blockHeader.get().getHash();
    Optional<TrieLog> maybeTrieLog = Optional.ofNullable(prefetchedTrieLogs.get(blockNumber));
    if (maybeTrieLog.isEmpty()) {
      // Fallback to individual fetch if not in prefetched map
      maybeTrieLog = trieLogManager.getTrieLogLayer(blockHash);
    }

    if (maybeTrieLog.isEmpty()) {
      LOG.debug("No trielog found for block {}, skipping", blockNumber);
      return false;
    }

    // Rewrite flat DB entries with versioned keys using explicit block numbers
    // (no commits inside, no need to set WORLD_BLOCK_NUMBER_KEY since we pass blockNumber
    // explicitly)
    var trieLog = maybeTrieLog.get();
    rewriteAccountChangesInBatch(trieLog, batchTransaction, blockNumber);
    rewriteStorageChangesInBatch(trieLog, batchTransaction, blockNumber);

    // Don't set latestArchivedFlatDbBlock here - it's set after batch commit
    blocksProcessed.incrementAndGet();

    logProgressIfNeeded(blockNumber);
    return true;
  }

  /**
   * Processes a single block, rewriting its flat DB entries to versioned format. This method
   * creates its own transaction and commits. Used for non-batch processing.
   *
   * <p>Note: This method is kept for reference/compatibility but batch processing via
   * processBlockBatch is preferred for performance.
   *
   * @param blockNumber the block number to process
   */
  @SuppressWarnings("unused")
  private void processBlock(final long blockNumber) {
    final Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(blockNumber);
    if (blockHeader.isEmpty()) {
      LOG.warn("Block header not found for block {}, skipping", blockNumber);
      return;
    }

    final Hash blockHash = blockHeader.get().getHash();
    final Optional<TrieLog> maybeTrieLog = trieLogManager.getTrieLogLayer(blockHash);

    if (maybeTrieLog.isEmpty()) {
      LOG.debug("No trielog found for block {}, skipping", blockNumber);
      return;
    }

    // Set the world state block context to the parent block number
    // The archive write strategy adds +1 when writing, so setting context to blockNumber-1
    // means data is written with suffix blockNumber, representing state after blockNumber
    final long parentBlockNumber = blockNumber - 1;
    setWorldStateBlockContext(parentBlockNumber);

    // Rewrite flat DB entries with versioned keys
    var trieLog = maybeTrieLog.get();
    rewriteAccountChanges(trieLog);
    rewriteStorageChanges(trieLog);

    worldStateStorage.setLatestArchivedFlatDbBlock(blockNumber);
    blocksProcessed.incrementAndGet();

    logProgressIfNeeded(blockNumber);
  }

  /**
   * Sets the world state block context for versioned flat DB writes.
   *
   * @param blockNumber the block number to set as context
   */
  private void setWorldStateBlockContext(final long blockNumber) {
    final Updater updater = worldStateStorage.updater();
    updater
        .getWorldStateTransaction()
        .put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    updater.commit();
  }

  /**
   * Rewrites account state changes from non-versioned to versioned flat DB format within a batch
   * transaction (no commit). Uses explicit block number to avoid block context conflicts in batch.
   *
   * @param trieLog the trielog containing changes
   * @param transaction the shared transaction for the batch
   * @param blockNumber the block number to write at
   */
  private void rewriteAccountChangesInBatch(
      final TrieLog trieLog,
      final SegmentedKeyValueStorageTransaction transaction,
      final long blockNumber) {

    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final Hash accountHash = address.addressHash();

              // Get the account value from the trielog's "updated" state (state after the block)
              final AccountValue updatedAccountValue = change.getUpdated();
              if (updatedAccountValue != null) {
                // Serialize the account value to RLP
                final Bytes accountBytes = RLP.encode(updatedAccountValue::writeTo);
                // Write the account with versioned key using explicit block number
                writeStrategy.putFlatAccount(transaction, accountHash, accountBytes, blockNumber);
              } else {
                // Account was deleted in this block, so updated state is non-existent
                writeStrategy.removeFlatAccount(transaction, accountHash, blockNumber);
              }
            });
    // No commit - handled by batch
  }

  /**
   * Rewrites storage state changes from non-versioned to versioned flat DB format within a batch
   * transaction (no commit). Uses explicit block number to avoid block context conflicts in batch.
   *
   * @param trieLog the trielog containing changes
   * @param transaction the shared transaction for the batch
   * @param blockNumber the block number to write at
   */
  private void rewriteStorageChangesInBatch(
      final TrieLog trieLog,
      final SegmentedKeyValueStorageTransaction transaction,
      final long blockNumber) {

    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageChanges) -> {
              final Hash accountHash = address.addressHash();
              storageChanges.forEach(
                  (slotKey, slotValue) -> {
                    // Get the storage value from the trielog's "updated" state (state after the
                    // block)
                    final UInt256 updatedValue = slotValue.getUpdated();
                    if (updatedValue != null && !updatedValue.isZero()) {
                      // Write the storage with versioned key using explicit block number
                      writeStrategy.putFlatAccountStorageValueByStorageSlotHash(
                          transaction,
                          accountHash,
                          slotKey.getSlotHash(),
                          Bytes.wrap(updatedValue.toBytes()),
                          blockNumber);
                    } else {
                      // Storage was deleted in this block or set to zero, so updated state is
                      // non-existent
                      writeStrategy.removeFlatAccountStorageValueByStorageSlotHash(
                          transaction, accountHash, slotKey.getSlotHash(), blockNumber);
                    }
                  });
            });
    // No commit - handled by batch
  }

  /**
   * Rewrites account state changes from non-versioned to versioned flat DB format.
   *
   * @param trieLog the trielog containing changes
   */
  private void rewriteAccountChanges(final TrieLog trieLog) {
    final Updater updater = worldStateStorage.updater();
    final var transaction = updater.getWorldStateTransaction();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final Hash accountHash = address.addressHash();

              // Get the account value from the trielog's "updated" state (state after the block)
              final AccountValue updatedAccountValue = change.getUpdated();
              if (updatedAccountValue != null) {
                // Serialize the account value to RLP
                final Bytes accountBytes = RLP.encode(updatedAccountValue::writeTo);
                // Write the account with versioned key using BonsaiArchiveFlatDbStrategy
                writeStrategy.putFlatAccount(storage, transaction, accountHash, accountBytes);
              } else {
                // Account was deleted in this block, so updated state is non-existent
                writeStrategy.removeFlatAccount(storage, transaction, accountHash);
              }
            });

    updater.commit();
  }

  /**
   * Rewrites storage state changes from non-versioned to versioned flat DB format.
   *
   * @param trieLog the trielog containing changes
   */
  private void rewriteStorageChanges(final TrieLog trieLog) {
    final Updater updater = worldStateStorage.updater();
    final var transaction = updater.getWorldStateTransaction();
    final var storage = worldStateStorage.getComposedWorldStateStorage();

    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageChanges) -> {
              final Hash accountHash = address.addressHash();
              storageChanges.forEach(
                  (slotKey, slotValue) -> {
                    // Get the storage value from the trielog's "updated" state (state after the
                    // block)
                    final UInt256 updatedValue = slotValue.getUpdated();
                    if (updatedValue != null && !updatedValue.isZero()) {
                      // Write the storage with versioned key using BonsaiArchiveFlatDbStrategy
                      writeStrategy.putFlatAccountStorageValueByStorageSlotHash(
                          storage,
                          transaction,
                          accountHash,
                          slotKey.getSlotHash(),
                          Bytes.wrap(updatedValue.toBytes()));
                    } else {
                      // Storage was deleted in this block or set to zero, so updated state is
                      // non-existent
                      writeStrategy.removeFlatAccountStorageValueByStorageSlotHash(
                          storage, transaction, accountHash, slotKey.getSlotHash());
                    }
                  });
            });

    updater.commit();
  }

  /**
   * Logs migration progress if the current block is a milestone.
   *
   * @param blockNumber the current block number
   */
  private void logProgressIfNeeded(final long blockNumber) {
    if (blockNumber % 1000 == 0) {
      final long processed = blocksProcessed.get();
      final long total = totalBlocks.get();
      final double percentComplete = (total > 0) ? (100.0 * processed / total) : 0.0;
      LOG.info(
          "Archive migration progress: {}/{} blocks ({}% complete)",
          processed, total, String.format("%.2f", percentComplete));
    }
  }

  /**
   * Gets the current migrator instance, if available. This is primarily for debug RPC access.
   *
   * @return an Optional containing the migrator instance, or empty if not initialized
   */
  public static Optional<BonsaiArchiveFlatDbMigrator> getInstance() {
    return Optional.ofNullable(instance);
  }
}
