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

  /** Maximum blocks to archive per invocation (increased from 1,000 for performance). */
  private static final int CATCHUP_LIMIT = 50_000;

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
    latestArchivedBlock.set(rootWorldStateStorage.getLatestArchivedBlock().orElse(0L));
  }

  public long getPendingBlocksCount() {
    return blockchain.getChainHeadBlockNumber() - latestArchivedBlock.get();
  }

  /**
   * Pre-populate header and TrieLog caches for the batch of blocks to archive. This avoids repeated
   * DB lookups during archiving.
   */
  private void populateCaches(
      final SortedMap<Long, Hash> blocksToArchive,
      final Map<Hash, BlockHeader> headerCache,
      final Map<Hash, TrieLog> trieLogCache) {
    blocksToArchive.forEach(
        (blockNum, blockHash) -> {
          // Cache the block header
          blockchain
              .getBlockHeader(blockHash)
              .ifPresent(
                  header -> {
                    headerCache.put(blockHash, header);
                    // Also cache the parent header (needed for archiving)
                    blockchain
                        .getBlockHeader(header.getParentHash())
                        .ifPresent(parent -> headerCache.put(header.getParentHash(), parent));
                  });
          // Cache the TrieLog
          trieLogManager
              .getTrieLogLayer(blockHash)
              .ifPresent(log -> trieLogCache.put(blockHash, log));
        });
    LOG.atDebug()
        .setMessage("Pre-populated caches: {} headers, {} trieLogs")
        .addArgument(headerCache.size())
        .addArgument(trieLogCache.size())
        .log();
  }

  // Move state and storage entries from their primary DB segments to their archive DB segments.
  // This is intended to maintain good performance for new block imports by keeping the primary
  // DB segments to live state only. Returns the number of state and storage entries moved.
  public int moveBlockStateToArchive() {
    final long startTime = System.nanoTime();
    final long retainAboveThisBlock =
        blockchain.getChainHeadBlockNumber() - DISTANCE_FROM_HEAD_BEFORE_ARCHIVING_OLD_STATE;

    if (rootWorldStateStorage.getFlatDbMode().getVersion() == Bytes.EMPTY) {
      throw new IllegalStateException("DB mode version not set");
    }

    int archivedAccountStateCount = 0;
    int archivedAccountStorageCount = 0;
    int batchEntryCount = 0;

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
      return 0;
    }

    LOG.atDebug()
        .setMessage("Moving state to archive storage: {} to {} ")
        .addArgument(blocksToArchive.firstKey())
        .addArgument(blocksToArchive.lastKey())
        .log();

    // Pre-populate caches to avoid repeated lookups
    final Map<Hash, BlockHeader> headerCache = new HashMap<>();
    final Map<Hash, TrieLog> trieLogCache = new HashMap<>();
    populateCaches(blocksToArchive, headerCache, trieLogCache);

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

      // Use cached header instead of DB lookup
      BlockHeader blockHeader = headerCache.get(blockHash);
      BlockHeader parentHeader =
          blockHeader != null ? headerCache.get(blockHeader.getParentHash()) : null;

      // Use cached TrieLog instead of DB lookup
      TrieLog trieLog = trieLogCache.get(blockHash);
      if (trieLog != null && parentHeader != null) {
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
      }

      LOG.atTrace()
          .setMessage("All account state and storage batched for block {}")
          .addArgument(block.getKey())
          .log();

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

    LOG.atDebug()
        .setMessage(
            "finished moving state for blocks {} to {}. Archived {} account state entries, {} account storage entries")
        .addArgument(blocksToArchive.firstKey())
        .addArgument(latestArchivedBlock.get())
        .addArgument(archivedAccountStateCount)
        .addArgument(archivedAccountStorageCount)
        .log();

    if (totalEntries > 0) {
      LOG.atInfo()
          .setMessage("Archived {} entries in {} ms ({} entries/sec)")
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
  }

  private final Lock archiveMutex = new ReentrantLock(true);

  @Override
  public void onBlockAdded(final BlockAddedEvent addedBlockContext) {
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
}
