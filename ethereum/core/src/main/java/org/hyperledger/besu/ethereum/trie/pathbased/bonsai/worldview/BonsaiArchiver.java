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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
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

  /** Maximum blocks to archive per invocation. */
  private static final int CATCHUP_LIMIT = 5_000;

  /** Entries to accumulate before committing a batch transaction. */
  private static final int BATCH_SIZE = 10_000;

  /** Log archiving progress every N blocks. */
  private static final int PROGRESS_LOG_INTERVAL = 1_000;

  private static final int DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE = 10;
  private final TrieLogManager trieLogManager;
  protected final MetricsSystem metricsSystem;

  // For logging progress. Saves doing a DB read just to record our progress
  final AtomicLong latestArchivedBlock = new AtomicLong(0);

  public BonsaiArchiver(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final Blockchain blockchain,
      final Consumer<Runnable> executeAsync,
      final TrieLogManager trieLogManager,
      final MetricsSystem metricsSystem) {
    this.rootWorldStateStorage = rootWorldStateStorage;
    this.blockchain = blockchain;
    this.executeAsync = executeAsync;
    this.trieLogManager = trieLogManager;
    this.metricsSystem = metricsSystem;

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "archived_blocks_state",
        "Total number of blocks for which state has been archived",
        () -> latestArchivedBlock.get());
  }

  public void initialize() {
    // Read from the DB where we got to previously
    long previousValue = latestArchivedBlock.get();
    long newValue = rootWorldStateStorage.getLatestArchivedBlock().orElse(0L);
    latestArchivedBlock.set(newValue);
    if (previousValue != newValue) {
      LOG.atInfo()
          .setMessage("Archiver: Initialized latestArchivedBlock from {} to {}")
          .addArgument(previousValue)
          .addArgument(newValue)
          .log();
    }
  }

  public long getPendingBlocksCount() {
    return blockchain.getChainHeadBlockNumber() - latestArchivedBlock.get();
  }

  /** Get or cache header for a block hash. Headers are small so safe to cache. */
  private Optional<BlockHeader> getCachedHeader(
      final Hash blockHash, final Map<Hash, BlockHeader> headerCache) {
    if (headerCache.containsKey(blockHash)) {
      return Optional.ofNullable(headerCache.get(blockHash));
    }
    Optional<BlockHeader> header = blockchain.getBlockHeader(blockHash);
    header.ifPresent(h -> headerCache.put(blockHash, h));
    return header;
  }

  // Move state and storage entries from their primary DB segments to their archive DB segments.
  // This is intended to maintain good performance for new block imports by keeping the primary
  // DB segments to live state only. Returns the number of state and storage entries moved.
  public int moveBlockStateToArchive() {
    final long startTime = System.nanoTime();
    final long chainHead = blockchain.getChainHeadBlockNumber();
    final long retainAboveThisBlock = chainHead - DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE;
    final long currentArchived = latestArchivedBlock.get();

    LOG.atInfo()
        .setMessage(
            "Archiver starting: chainHead={}, latestArchived={}, retainAbove={}, pendingBlocks={}")
        .addArgument(chainHead)
        .addArgument(currentArchived)
        .addArgument(retainAboveThisBlock)
        .addArgument(retainAboveThisBlock - currentArchived - 1)
        .log();

    if (rootWorldStateStorage.getFlatDbMode().getVersion() == Bytes.EMPTY) {
      LOG.atWarn().setMessage("Archiver: DB mode version not set, skipping").log();
      throw new IllegalStateException("DB mode version not set");
    }

    int archivedAccountStateCount = 0;
    int archivedAccountStorageCount = 0;
    int batchEntryCount = 0;
    int blocksProcessed = 0;
    int blocksSkipped = 0;

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

    if (blocksToArchive.isEmpty()) {
      LOG.atInfo().setMessage("Archiver: No blocks to archive (already caught up)").log();
      return 0;
    }

    LOG.atInfo()
        .setMessage("Archiver: Processing {} blocks from {} to {}")
        .addArgument(blocksToArchive.size())
        .addArgument(blocksToArchive.firstKey())
        .addArgument(blocksToArchive.lastKey())
        .log();

    // Header cache (headers are small, safe to cache)
    final Map<Hash, BlockHeader> headerCache = new HashMap<>();

    // Use holder to allow reassignment in loop
    final var txHolder =
        new Object() {
          SegmentedKeyValueStorageTransaction tx =
              rootWorldStateStorage.getComposedWorldStateStorage().startTransaction();
        };

    for (var block : blocksToArchive.entrySet()) {
      Hash blockHash = block.getValue();
      LOG.atTrace()
          .setMessage("Archiving all account state for block {}")
          .addArgument(block.getKey())
          .log();

      // Use lazy cached headers (headers are small)
      Optional<BlockHeader> blockHeaderOpt = getCachedHeader(blockHash, headerCache);
      if (blockHeaderOpt.isEmpty()) {
        LOG.atWarn()
            .setMessage("Archiver: Skipping block {} - header not found for hash {}")
            .addArgument(block.getKey())
            .addArgument(blockHash)
            .log();
        blocksSkipped++;
        continue;
      }
      BlockHeader blockHeader = blockHeaderOpt.get();
      Optional<BlockHeader> parentHeaderOpt =
          getCachedHeader(blockHeader.getParentHash(), headerCache);
      if (parentHeaderOpt.isEmpty()) {
        LOG.atWarn()
            .setMessage("Archiver: Skipping block {} - parent header not found for hash {}")
            .addArgument(block.getKey())
            .addArgument(blockHeader.getParentHash())
            .log();
        blocksSkipped++;
        continue;
      }
      BlockHeader parentHeader = parentHeaderOpt.get();

      // Fetch TrieLog on demand (not cached - too large)
      Optional<TrieLog> trieLogOpt = trieLogManager.getTrieLogLayer(blockHash);
      if (trieLogOpt.isEmpty()) {
        LOG.atWarn()
            .setMessage("Archiver: Skipping block {} - TrieLog not found for hash {}")
            .addArgument(block.getKey())
            .addArgument(blockHash)
            .log();
        blocksSkipped++;
        continue;
      }
      TrieLog trieLog = trieLogOpt.get();

      int accountChanges = trieLog.getAccountChanges().size();
      int storageChanges = trieLog.getStorageChanges().size();
      LOG.atDebug()
          .setMessage("Archiver: Block {} has {} account changes, {} storage changes")
          .addArgument(block.getKey())
          .addArgument(accountChanges)
          .addArgument(storageChanges)
          .log();
      // Process account and storage changes
      for (var entry : trieLog.getAccountChanges().entrySet()) {
        int count =
            rootWorldStateStorage.archivePreviousAccountStateBatched(
                txHolder.tx, parentHeader, entry.getKey().addressHash());
        archivedAccountStateCount += count;
        batchEntryCount += count;
      }

      LOG.atTrace()
          .setMessage("Archiving all storage state for block {}")
          .addArgument(block.getKey())
          .log();

      for (var entry : trieLog.getStorageChanges().entrySet()) {
        for (var slotEntry : entry.getValue().entrySet()) {
          int count =
              rootWorldStateStorage.archivePreviousStorageStateBatched(
                  txHolder.tx,
                  parentHeader,
                  Bytes.concatenate(
                      entry.getKey().addressHash().getBytes(),
                      slotEntry.getKey().getSlotHash().getBytes()));
          archivedAccountStorageCount += count;
          batchEntryCount += count;
        }
      }

      blocksProcessed++;

      // Commit batch if we've accumulated enough entries
      if (batchEntryCount >= BATCH_SIZE) {
        txHolder.tx.commit();
        batchEntryCount = 0;
        txHolder.tx = rootWorldStateStorage.getComposedWorldStateStorage().startTransaction();
      }

      // Update progress marker periodically
      latestArchivedBlock.set(block.getKey());
      if (latestArchivedBlock.get() % PROGRESS_LOG_INTERVAL == 0) {
        rootWorldStateStorage.setLatestArchivedBlock(block.getKey());
        LOG.atInfo()
            .setMessage("archive progress: state up to block {} archived ({} behind chain head {})")
            .addArgument(latestArchivedBlock.get())
            .addArgument(blockchain.getChainHeadBlockNumber() - latestArchivedBlock.get())
            .addArgument(blockchain.getChainHeadBlockNumber())
            .log();
      }
    }

    // Final commit for any remaining entries
    txHolder.tx.commit();
    rootWorldStateStorage.setLatestArchivedBlock(latestArchivedBlock.get());

    final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
    final int totalEntries = archivedAccountStateCount + archivedAccountStorageCount;

    LOG.atInfo()
        .setMessage(
            "Archiver complete: blocks processed={}, skipped={}, account entries={}, storage entries={}")
        .addArgument(blocksProcessed)
        .addArgument(blocksSkipped)
        .addArgument(archivedAccountStateCount)
        .addArgument(archivedAccountStorageCount)
        .log();

    if (totalEntries > 0) {
      LOG.atInfo()
          .setMessage("Archiver: {} entries in {} ms ({} entries/sec)")
          .addArgument(totalEntries)
          .addArgument(durationMs)
          .addArgument(durationMs > 0 ? (totalEntries * 1000L / durationMs) : totalEntries)
          .log();
    }

    return totalEntries;
  }

  /**
   * Manually trigger archiving process asynchronously. This is safe to call multiple times - if
   * archiving is already in progress, the new invocation will exit gracefully.
   */
  public void triggerArchiving() {
    LOG.atInfo().setMessage("Archiver: Manual trigger requested").log();
    executeAsync.accept(
        () -> {
          if (archiveMutex.tryLock()) {
            LOG.atInfo().setMessage("Archiver: Manual trigger - acquired lock, starting").log();
            try {
              moveBlockStateToArchive();
            } finally {
              archiveMutex.unlock();
            }
          } else {
            LOG.atInfo()
                .setMessage("Archiver: Manual trigger - skipped, archiving already in progress")
                .log();
          }
        });
  }

  private final Lock archiveMutex = new ReentrantLock(true);

  @Override
  public void onBlockAdded(final BlockAddedEvent addedBlockContext) {
    initialize();
    final long blockNum = addedBlockContext.getHeader().getNumber();
    LOG.atDebug()
        .setMessage("Archiver: onBlockAdded triggered for block {}")
        .addArgument(blockNum)
        .log();
    // Since moving blocks can be done in batches we only want
    // one instance running at a time
    executeAsync.accept(
        () -> {
          if (archiveMutex.tryLock()) {
            LOG.atDebug()
                .setMessage("Archiver: Block {} trigger - acquired lock, starting")
                .addArgument(blockNum)
                .log();
            try {
              moveBlockStateToArchive();
            } finally {
              archiveMutex.unlock();
            }
          } else {
            LOG.atTrace()
                .setMessage("Archiver: Block {} trigger - skipped, already in progress")
                .addArgument(blockNum)
                .log();
          }
        });
  }
}
