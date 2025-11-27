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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconstructs the Bonsai archive flat database from trielogs after initial sync completes. This
 * allows sync to proceed at regular Bonsai performance, then builds the versioned archive state
 * once sync is complete.
 */
@SuppressWarnings("UnusedVariable") // metricsSystem used for gauge registration
public class BonsaiArchiveFlatDbReconstructor implements InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveFlatDbReconstructor.class);
  private static final int BATCH_SIZE = 100;
  private static final long BATCH_DELAY_MS = 100; // 100ms delay between batches

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final TrieLogManager trieLogManager;
  private final AtomicBoolean isReconstructing = new AtomicBoolean(false);
  private final AtomicLong blocksProcessed = new AtomicLong(0);
  private final AtomicLong totalBlocks = new AtomicLong(0);

  /**
   * Creates a new BonsaiArchiveFlatDbReconstructor.
   *
   * @param worldStateStorage the world state storage
   * @param blockchain the blockchain
   * @param executorService the scheduled executor service for async operations
   * @param trieLogManager the trie log manager
   * @param metricsSystem the metrics system
   */
  public BonsaiArchiveFlatDbReconstructor(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem) {
    this.worldStateStorage = worldStateStorage;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.trieLogManager = trieLogManager;

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archive_reconstruction_blocks_processed",
        "Number of blocks processed during archive reconstruction",
        blocksProcessed::get);

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archive_reconstruction_blocks_target",
        "Target number of blocks to process during archive reconstruction",
        totalBlocks::get);
  }

  @Override
  public void onInitialSyncCompleted() {
    LOG.info(
        "Initial sync completed. Starting deferred archive reconstruction from trielogs in background...");

    // Check if we're still in FULL mode (not yet upgraded to ARCHIVE)
    if (worldStateStorage.getFlatDbMode() == FlatDbMode.FULL) {
      startReconstruction();
    } else {
      LOG.info("Initial sync completed but already in ARCHIVE mode, skipping reconstruction");
    }
  }

  @Override
  public void onInitialSyncRestart() {
    // No action needed on sync restart for deferred archive mode
    LOG.debug("Initial sync restarted, deferred archive reconstruction will wait");
  }

  /**
   * Starts the asynchronous reconstruction process using the provided executor. Processes all
   * trielogs from the earliest available to the current chain head, building the versioned archive
   * state.
   */
  private void startReconstruction() {
    if (!isReconstructing.compareAndSet(false, true)) {
      LOG.warn("Archive reconstruction is already in progress");
      return;
    }

    LOG.info("Starting asynchronous Bonsai archive reconstruction from trielogs");

    // Submit reconstruction task to the executor service
    executorService.execute(
        () -> {
          try {
            performReconstruction();
            LOG.info(
                "Bonsai archive reconstruction completed successfully. Processed {} blocks.",
                blocksProcessed.get());

            // Schedule upgrade to ARCHIVE mode now that reconstruction is complete
            executorService.execute(this::upgradeToArchiveMode);
          } catch (final Exception e) {
            LOG.error("Error during archive reconstruction", e);
            throw new RuntimeException("Archive reconstruction failed", e);
          } finally {
            isReconstructing.set(false);
          }
        });
  }

  /** Upgrades the storage to ARCHIVE mode after reconstruction completes. */
  private void upgradeToArchiveMode() {
    LOG.info("Archive reconstruction complete, upgrading to ARCHIVE flat db mode");
    worldStateStorage.upgradeToArchiveFlatDbMode();
    LOG.info("Successfully upgraded to ARCHIVE mode");
  }

  /** Performs the actual reconstruction work. This method is called by the executor service. */
  private void performReconstruction() {
    // Determine the range of blocks to process
    final long chainHeadNumber = blockchain.getChainHeadBlockNumber();
    final Optional<Long> latestArchivedFlatDbBlock =
        worldStateStorage.getLatestArchivedFlatDbBlock();

    final long startBlock;
    if (latestArchivedFlatDbBlock.isPresent()) {
      // Resume from where we left off
      startBlock = latestArchivedFlatDbBlock.get() + 1;
      LOG.info(
          "Resuming archive flat DB reconstruction from block {} (latest archived: {})",
          startBlock,
          latestArchivedFlatDbBlock.get());
    } else {
      // Start from the earliest block we have trielogs for
      // In practice, this will typically be genesis or checkpoint
      startBlock = 0L;
      LOG.info("Starting archive flat DB reconstruction from genesis (block 0)");
    }

    totalBlocks.set(chainHeadNumber - startBlock + 1);
    LOG.info(
        "Archive reconstruction will process {} blocks (from {} to {})",
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
   * Processes a single block, archiving its state changes.
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
    final Optional<TrieLog> trieLog = trieLogManager.getTrieLogLayer(blockHash);

    if (trieLog.isEmpty()) {
      LOG.debug("No trielog found for block {}, skipping", blockNumber);
      return;
    }

    archiveAccountChanges(blockNumber, blockHeader.get(), trieLog.get());
    archiveStorageChanges(blockNumber, blockHeader.get(), trieLog.get());

    worldStateStorage.setLatestArchivedFlatDbBlock(blockNumber);
    blocksProcessed.incrementAndGet();

    logProgressIfNeeded(blockNumber);
  }

  /**
   * Archives account state changes from a trielog.
   *
   * @param blockNumber the current block number
   * @param blockHeader the current block header
   * @param trieLog the trielog containing changes
   */
  private void archiveAccountChanges(
      final long blockNumber, final BlockHeader blockHeader, final TrieLog trieLog) {
    if (blockNumber == 0) {
      return; // No parent block to archive for genesis
    }

    final Optional<BlockHeader> parentHeader =
        blockchain.getBlockHeader(blockHeader.getParentHash());
    if (parentHeader.isEmpty()) {
      return;
    }

    trieLog
        .getAccountChanges()
        .forEach(
            (address, ignoredChange) ->
                worldStateStorage.archivePreviousAccountState(parentHeader, address.addressHash()));
  }

  /**
   * Archives storage state changes from a trielog.
   *
   * @param blockNumber the current block number
   * @param blockHeader the current block header
   * @param trieLog the trielog containing changes
   */
  private void archiveStorageChanges(
      final long blockNumber, final BlockHeader blockHeader, final TrieLog trieLog) {
    if (blockNumber == 0) {
      return; // No parent block to archive for genesis
    }

    final Optional<BlockHeader> parentHeader =
        blockchain.getBlockHeader(blockHeader.getParentHash());
    if (parentHeader.isEmpty()) {
      return;
    }

    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageChanges) ->
                storageChanges.forEach(
                    (slotKey, ignoredSlotValue) ->
                        worldStateStorage.archivePreviousStorageState(
                            parentHeader,
                            org.apache.tuweni.bytes.Bytes.concatenate(
                                address.addressHash(), slotKey.getSlotHash()))));
  }

  /**
   * Logs reconstruction progress if the current block is a milestone.
   *
   * @param blockNumber the current block number
   */
  private void logProgressIfNeeded(final long blockNumber) {
    if (blockNumber % 1000 == 0) {
      final long processed = blocksProcessed.get();
      final long total = totalBlocks.get();
      final double percentComplete = (total > 0) ? (100.0 * processed / total) : 0.0;
      LOG.info(
          "Archive reconstruction progress: {}/{} blocks ({}% complete)",
          processed, total, String.format("%.2f", percentComplete));
    }
  }
}
