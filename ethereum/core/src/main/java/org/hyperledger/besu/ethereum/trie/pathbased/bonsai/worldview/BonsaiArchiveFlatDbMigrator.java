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
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private static final int BATCH_SIZE = 100;
  private static final long BATCH_DELAY_MS = 100; // 100ms delay between batches

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final TrieLogManager trieLogManager;
  private final BonsaiArchiveFlatDbStrategy writeStrategy;
  private final AtomicBoolean isReconstructing = new AtomicBoolean(false);
  private final AtomicLong blocksProcessed = new AtomicLong(0);
  private final AtomicLong totalBlocks = new AtomicLong(0);

  /**
   * Creates a new BonsaiArchiveFlatDbMigrator.
   *
   * @param worldStateStorage the world state storage
   * @param blockchain the blockchain
   * @param executorService the scheduled executor service for async operations
   * @param trieLogManager the trie log manager
   * @param metricsSystem the metrics system
   */
  public BonsaiArchiveFlatDbMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem) {
    this.worldStateStorage = worldStateStorage;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.trieLogManager = trieLogManager;

    // Create strategy for writing versioned archive entries
    final CodeHashCodeStorageStrategy codeStorageStrategy = new CodeHashCodeStorageStrategy();
    this.writeStrategy = new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy);

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

    // Submit migration task to the executor service
    executorService.execute(
        () -> {
          try {
            performMigration();
            LOG.info(
                "Bonsai archive migration completed successfully. Processed {} blocks.",
                blocksProcessed.get());

            // Schedule upgrade to ARCHIVE mode now that migration is complete
            executorService.execute(this::upgradeToArchiveMode);
          } catch (final Exception e) {
            LOG.error("Error during archive migration", e);
            throw new RuntimeException("Archive migration failed", e);
          } finally {
            isReconstructing.set(false);
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
  }

  /** Performs the actual migration work. This method is called by the executor service. */
  private void performMigration() {
    // Determine the range of blocks to process
    final long chainHeadNumber = blockchain.getChainHeadBlockNumber();
    final Optional<Long> latestArchivedFlatDbBlock =
        worldStateStorage.getLatestArchivedFlatDbBlock();

    final long startBlock;
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

    totalBlocks.set(chainHeadNumber - startBlock + 1);
    LOG.info(
        "Archive migration will process {} blocks (from {} to {})",
        totalBlocks.get(),
        startBlock,
        chainHeadNumber);

    // Process first batch, which will schedule subsequent batches
    processNextBatch(startBlock, chainHeadNumber);
  }

  /**
   * Processes the next batch of blocks and schedules the following batch if needed.
   *
   * @param currentBlock the starting block for this batch
   * @param chainHeadNumber the final block to process
   */
  private void processNextBatch(final long currentBlock, final long chainHeadNumber) {
    if (currentBlock > chainHeadNumber) {
      return; // All batches processed
    }

    final long batchEnd = Math.min(currentBlock + BATCH_SIZE - 1, chainHeadNumber);
    processBlockBatch(currentBlock, batchEnd);

    final long nextBlock = batchEnd + 1;
    if (nextBlock <= chainHeadNumber) {
      // Schedule next batch with a delay to avoid overwhelming the system
      executorService.schedule(
          () -> processNextBatch(nextBlock, chainHeadNumber),
          BATCH_DELAY_MS,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Processes a batch of blocks, moving their state changes to the archive segments.
   *
   * @param startBlock the first block in the batch
   * @param endBlock the last block in the batch
   */
  private void processBlockBatch(final long startBlock, final long endBlock) {
    LOG.debug("Processing block batch: {} to {}", startBlock, endBlock);

    for (long blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
      processBlock(blockNumber);
    }
  }

  /**
   * Processes a single block, rewriting its flat DB entries to versioned format.
   *
   * @param blockNumber the block number to process
   */
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
                    // Get the storage value from the trielog's "updated" state (state after the block)
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
}
