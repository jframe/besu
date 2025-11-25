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
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the archiving of historic state that is still needed to satisfy queries but
 * doesn't need to be in the main DB segment. Doing so would degrade block-import performance over
 * time so we move state beyond a certain age (in blocks) to other DB segments, assuming there is a
 * more recent (i.e. changed) version of the state. If state is created once and never changed it
 * will remain in the primary DB segment(s).
 */
public class BonsaiArchiver implements BlockAddedObserver {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiver.class);

  private final PathBasedWorldStateKeyValueStorage rootWorldStateStorage;
  private final Blockchain blockchain;
  private final Consumer<Runnable> executeAsync;
  private static final int CATCHUP_LIMIT = 1000;
  private static final int DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE = 10;
  private final TrieLogManager trieLogManager;
  protected final MetricsSystem metricsSystem;
  private final boolean deferArchivingDuringSync;

  // For logging progress. Saves doing a DB read just to record our progress
  final AtomicLong latestArchivedBlock = new AtomicLong(0);

  // Track whether initial sync is in progress to defer archiving
  private volatile boolean initialSyncInProgress = true;
  private final AtomicInteger bulkArchivingInProgress = new AtomicInteger(0);

  public BonsaiArchiver(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final Blockchain blockchain,
      final Consumer<Runnable> executeAsync,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem,
      final boolean deferArchivingDuringSync) {
    this.rootWorldStateStorage = rootWorldStateStorage;
    this.blockchain = blockchain;
    this.executeAsync = executeAsync;
    this.trieLogManager = trieLogManager;
    this.metricsSystem = metricsSystem;
    this.deferArchivingDuringSync = deferArchivingDuringSync;

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archived_blocks_state",
        "Total number of blocks for which state has been archived",
        () -> latestArchivedBlock.get());
  }

  public void initialize() {
    // Read from the DB where we got to previously
    latestArchivedBlock.set(rootWorldStateStorage.getLatestArchivedBlock().orElse(0L));

    // Check if we're still significantly behind chain head, which indicates we're still in initial
    // sync
    long chainHead = blockchain.getChainHeadBlockNumber();
    long latestArchived = latestArchivedBlock.get();
    long blocksBehind = chainHead - latestArchived;

    // If we're very far behind (more than the catchup limit), we're likely still in initial sync
    if (blocksBehind > CATCHUP_LIMIT) {
      LOG.info(
          "Bonsai archiver is {} blocks behind chain head. Will defer archiving until sync completes.",
          blocksBehind);
      initialSyncInProgress = true;
    } else {
      // We're caught up or close to it, so initial sync must be done
      LOG.info(
          "Bonsai archiver is {} blocks behind chain head. Archiving will proceed normally.",
          blocksBehind);
      initialSyncInProgress = false;
    }
  }

  public long getPendingBlocksCount() {
    return blockchain.getChainHeadBlockNumber() - latestArchivedBlock.get();
  }

  // Move state and storage entries from their primary DB segments to their archive DB segments.
  // This is intended to maintain good performance for new block imports by keeping the primary
  // DB segments to live state only. Returns the number of state and storage entries moved.
  public int moveBlockStateToArchive() {
    final long retainAboveThisBlock =
        blockchain.getChainHeadBlockNumber() - DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE;

    if (rootWorldStateStorage.getFlatDbMode().getVersion() == Bytes.EMPTY) {
      throw new IllegalStateException("DB mode version not set");
    }

    AtomicInteger archivedAccountStateCount = new AtomicInteger();
    AtomicInteger archivedAccountStorageCount = new AtomicInteger();

    // Typically we will move all storage and state for a single block i.e. when a new block is
    // imported, move state for block-N. There are cases where we catch-up and move old state
    // for a number of blocks so we may iterate over a number of blocks archiving their state,
    // not just a single one.
    final SortedMap<Long, Hash> blocksToArchive;
    synchronized (this) {
      blocksToArchive = new TreeMap<>();

      long nextToArchive = latestArchivedBlock.get() + 1;
      while (blocksToArchive.size() <= CATCHUP_LIMIT && nextToArchive < retainAboveThisBlock) {
        blocksToArchive.put(
            nextToArchive, blockchain.getBlockByNumber(nextToArchive).get().getHash());

        if (!blockchain.blockIsOnCanonicalChain(
            blockchain.getBlockHashByNumber(nextToArchive).orElse(Hash.EMPTY))) {
          LOG.error(
              "Attempted to archive a non-canonical block: {} / {}",
              nextToArchive,
              blockchain.getBlockByNumber(nextToArchive).get().getHash());
        }

        nextToArchive++;
      }
    }

    if (blocksToArchive.size() > 0) {
      LOG.atDebug()
          .setMessage("Moving state to archive storage: {} to {} ")
          .addArgument(blocksToArchive.firstKey())
          .addArgument(blocksToArchive.lastKey())
          .log();

      // Determine which world state keys have changed in the last N blocks by looking at the
      // trie logs for the blocks. Then move the old keys to the archive segment (if and only if
      // they have changed)
      blocksToArchive
          .entrySet()
          .forEach(
              (block) -> {
                Hash blockHash = block.getValue();
                LOG.atDebug()
                    .setMessage("Archiving all account state for block {}")
                    .addArgument(block.getKey())
                    .log();
                Optional<TrieLog> trieLog = trieLogManager.getTrieLogLayer(blockHash);
                if (trieLog.isPresent()) {
                  trieLog
                      .get()
                      .getAccountChanges()
                      .forEach(
                          (address, change) -> {
                            // Move any previous state for this account
                            archivedAccountStateCount.addAndGet(
                                rootWorldStateStorage.archivePreviousAccountState(
                                    blockchain.getBlockHeader(
                                        blockchain.getBlockHeader(blockHash).get().getParentHash()),
                                    address.addressHash()));
                          });
                  LOG.atDebug()
                      .setMessage("Archiving all storage state for block {}")
                      .addArgument(block.getKey())
                      .log();
                  trieLog
                      .get()
                      .getStorageChanges()
                      .forEach(
                          (address, storageSlotKey) -> {
                            storageSlotKey.forEach(
                                (slotKey, slotValue) -> {
                                  // Move any previous state for this account
                                  archivedAccountStorageCount.addAndGet(
                                      rootWorldStateStorage.archivePreviousStorageState(
                                          blockchain.getBlockHeader(
                                              blockchain
                                                  .getBlockHeader(blockHash)
                                                  .get()
                                                  .getParentHash()),
                                          Bytes.concatenate(
                                              address.addressHash(), slotKey.getSlotHash())));
                                });
                          });
                }
                LOG.atDebug()
                    .setMessage("All account state and storage archived for block {}")
                    .addArgument(block.getKey())
                    .log();
                rootWorldStateStorage.setLatestArchivedBlock(block.getKey());

                // Update local var for logging progress
                latestArchivedBlock.set(block.getKey());
                if (latestArchivedBlock.get() % 100 == 0) {
                  // Log progress in case catching up causes there to be a large number of keys
                  // to move
                  LOG.atInfo()
                      .setMessage(
                          "archive progress: state up to block {} archived ({} behind chain head {})")
                      .addArgument(latestArchivedBlock.get())
                      .addArgument(blockchain.getChainHeadBlockNumber() - latestArchivedBlock.get())
                      .addArgument(blockchain.getChainHeadBlockNumber())
                      .log();
                }
              });

      LOG.atDebug()
          .setMessage(
              "finished moving state for blocks {} to {}. Archived {} account state entries, {} account storage entries")
          .addArgument(blocksToArchive.firstKey())
          .addArgument(latestArchivedBlock.get())
          .addArgument(archivedAccountStateCount.get())
          .addArgument(archivedAccountStorageCount.get())
          .log();
    }

    return archivedAccountStateCount.get() + archivedAccountStorageCount.get();
  }

  private final Lock archiveMutex = new ReentrantLock(true);

  @Override
  public void onBlockAdded(final BlockAddedEvent addedBlockContext) {
    // Skip archiving during initial sync to avoid performance degradation (if flag is enabled)
    if (deferArchivingDuringSync && initialSyncInProgress) {
      return;
    }

    // Skip if bulk archiving is running
    if (bulkArchivingInProgress.get() > 0) {
      return;
    }

    initialize();
    final Optional<Long> blockNumber = Optional.of(addedBlockContext.getHeader().getNumber());
    blockNumber.ifPresent(
        blockNum -> {
          // Since moving blocks can be done in batches we only want
          // one instance running at a time
          executeAsync.accept(
              () -> {
                if (archiveMutex.tryLock()) {
                  try {
                    moveBlockStateToArchive();
                  } finally {
                    archiveMutex.unlock();
                  }
                }
              });
        });
  }

  /**
   * Called when initial sync completes. Triggers bulk archiving of historical state from trielogs.
   * This avoids the performance overhead of archiving during sync by deferring all archiving work
   * until after sync is complete.
   */
  public void onInitialSyncCompleted() {
    initialSyncInProgress = false;

    if (deferArchivingDuringSync) {
      LOG.info(
          "Initial sync completed. Starting bulk archive generation from trielogs. "
              + "Deferred archiving was enabled to improve sync performance.");
      // Trigger bulk archiving asynchronously
      executeAsync.accept(this::executeBulkArchiving);
    } else {
      LOG.info("Initial sync completed. Archive generation was done incrementally during sync.");
    }
  }

  /**
   * Process all blocks from the last archived block up to the chain head, generating archive
   * entries from trielogs. This is much more efficient than archiving during sync because: - No
   * competition with block import for I/O - Can process in large batches - No seekForPrev overhead
   * during sync critical path - Trielogs already contain all the information we need
   */
  private void executeBulkArchiving() {
    if (bulkArchivingInProgress.incrementAndGet() > 1) {
      bulkArchivingInProgress.decrementAndGet();
      LOG.warn("Bulk archiving already in progress, skipping");
      return;
    }

    try {
      long startTime = System.currentTimeMillis();
      long startBlock = rootWorldStateStorage.getLatestArchivedBlock().orElse(0L) + 1;
      long endBlock =
          blockchain.getChainHeadBlockNumber() - DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE;
      long totalBlocks = endBlock - startBlock + 1;

      if (totalBlocks <= 0) {
        LOG.info("No blocks to archive");
        return;
      }

      LOG.info(
          "Starting bulk archive generation for blocks {} to {} ({} total blocks)",
          startBlock,
          endBlock,
          totalBlocks);

      long blocksProcessed = 0;
      long lastProgressLog = System.currentTimeMillis();

      // Process blocks one at a time (batching at the transaction level is handled by
      // moveBlockStateToArchive)
      for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
        // Archive this block
        Optional<Hash> blockHash = blockchain.getBlockHashByNumber(blockNum);
        if (blockHash.isEmpty()) {
          LOG.warn("Block {} not found in blockchain, skipping", blockNum);
          continue;
        }

        Optional<TrieLog> trieLog = trieLogManager.getTrieLogLayer(blockHash.get());
        if (trieLog.isEmpty()) {
          LOG.warn("TrieLog for block {} not found, skipping", blockNum);
          continue;
        }

        // Archive state for this block using existing archiving logic
        archiveBlockFromTrieLog(blockNum, blockHash.get(), trieLog.get());

        blocksProcessed++;

        // Log progress every 100 blocks or every 10 seconds
        long now = System.currentTimeMillis();
        if (blockNum % 100 == 0 || (now - lastProgressLog > 10000)) {
          long duration = now - startTime;
          double progress = (double) blocksProcessed / totalBlocks * 100;
          long blocksRemaining = totalBlocks - blocksProcessed;
          long estimatedTimeRemaining =
              blocksProcessed > 0 ? (duration * blocksRemaining / blocksProcessed) : 0;

          LOG.info(
              "Archive progress: {}/{} blocks ({}%), {} blocks behind chain head. Est. time remaining: {} seconds",
              blocksProcessed,
              totalBlocks,
              String.format("%.1f", progress),
              blockchain.getChainHeadBlockNumber() - blockNum,
              estimatedTimeRemaining / 1000);

          lastProgressLog = now;
        }
      }

      long duration = System.currentTimeMillis() - startTime;
      LOG.info(
          "Bulk archive generation complete. Processed {} blocks in {} seconds ({} blocks/sec)",
          blocksProcessed,
          duration / 1000,
          blocksProcessed > 0 ? (blocksProcessed * 1000) / duration : 0);

    } catch (Exception e) {
      LOG.error("Error during bulk archiving", e);
    } finally {
      bulkArchivingInProgress.decrementAndGet();
    }
  }

  /**
   * Archive state for a single block using its trielog. This examines what changed in the block and
   * moves the previous versions of those keys to the archive segment.
   */
  private void archiveBlockFromTrieLog(
      final long blockNum, final Hash blockHash, final TrieLog trieLog) {
    // Archive account state
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              // Move any previous state for this account
              Optional<BlockHeader> parentHeader =
                  blockchain
                      .getBlockHeader(blockHash)
                      .flatMap(header -> blockchain.getBlockHeader(header.getParentHash()));
              rootWorldStateStorage.archivePreviousAccountState(
                  parentHeader, address.addressHash());
            });

    // Archive storage state
    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageSlotKey) -> {
              storageSlotKey.forEach(
                  (slotKey, slotValue) -> {
                    // Move any previous state for this storage slot
                    Optional<BlockHeader> parentHeader =
                        blockchain
                            .getBlockHeader(blockHash)
                            .flatMap(header -> blockchain.getBlockHeader(header.getParentHash()));
                    rootWorldStateStorage.archivePreviousStorageState(
                        parentHeader,
                        Bytes.concatenate(address.addressHash(), slotKey.getSlotHash()));
                  });
            });

    // Update tracking
    rootWorldStateStorage.setLatestArchivedBlock(blockNum);
    latestArchivedBlock.set(blockNum);
  }
}
