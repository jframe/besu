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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and maintains the archive state index by processing trielogs. This index tracks which
 * blocks modified which accounts and storage slots, enabling fast O(1) lookups instead of expensive
 * seekForPrev operations.
 *
 * <p>The builder processes trielogs in batches, committing changes periodically to avoid excessive
 * memory usage. It supports resume functionality to continue from the last checkpoint.
 */
public class BonsaiArchiveIndexBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveIndexBuilder.class);

  // Process blocks in batches to avoid excessive memory usage
  private static final int DEFAULT_BATCH_SIZE = 1000;

  // Log progress every N blocks
  private static final int PROGRESS_LOG_INTERVAL = 10000;

  private final SegmentedKeyValueStorage storage;
  private final BonsaiArchiveStateIndex index;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final int batchSize;

  // Track progress for logging
  private final AtomicLong blocksProcessed = new AtomicLong(0);

  /**
   * Creates a new index builder.
   *
   * @param storage the key-value storage
   * @param index the archive state index
   * @param trieLogManager the trielog manager
   * @param blockchain the blockchain
   */
  public BonsaiArchiveIndexBuilder(
      final SegmentedKeyValueStorage storage,
      final BonsaiArchiveStateIndex index,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain) {
    this(storage, index, trieLogManager, blockchain, DEFAULT_BATCH_SIZE);
  }

  /**
   * Creates a new index builder with custom batch size.
   *
   * @param storage the key-value storage
   * @param index the archive state index
   * @param trieLogManager the trielog manager
   * @param blockchain the blockchain
   * @param batchSize the batch size for processing blocks
   */
  public BonsaiArchiveIndexBuilder(
      final SegmentedKeyValueStorage storage,
      final BonsaiArchiveStateIndex index,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final int batchSize) {
    this.storage = storage;
    this.index = index;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.batchSize = batchSize;
  }

  /**
   * Builds the index for the specified block range. Processes blocks in batches and commits
   * periodically.
   *
   * @param fromBlock the starting block number (inclusive)
   * @param toBlock the ending block number (inclusive)
   */
  public void buildIndex(final long fromBlock, final long toBlock) {
    LOG.info(
        "Starting archive index build from block {} to block {} (total {} blocks)",
        fromBlock,
        toBlock,
        toBlock - fromBlock + 1);

    long startTime = System.currentTimeMillis();
    blocksProcessed.set(0);

    try {
      long currentBlock = fromBlock;

      while (currentBlock <= toBlock) {
        long batchEnd = Math.min(currentBlock + batchSize - 1, toBlock);
        processBatch(currentBlock, batchEnd);
        currentBlock = batchEnd + 1;

        // Log progress periodically
        if (blocksProcessed.get() % PROGRESS_LOG_INTERVAL == 0) {
          logProgress(fromBlock, toBlock, startTime);
        }
      }

      // Mark index as fully built
      SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      index.markIndexBuilt(tx, toBlock);
      tx.commit();

      long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
      LOG.info(
          "Archive index build complete. Indexed {} blocks in {} seconds ({} blocks/sec)",
          blocksProcessed.get(),
          elapsedSeconds,
          elapsedSeconds > 0 ? blocksProcessed.get() / elapsedSeconds : 0);

    } catch (Exception e) {
      LOG.error("Error building archive index", e);
      throw new RuntimeException("Failed to build archive index", e);
    }
  }

  /**
   * Resumes building the index from the last checkpoint to the target block.
   *
   * @param targetBlock the target block number
   */
  public void resumeBuild(final long targetBlock) {
    Optional<Long> latestIndexed = index.getLatestIndexedBlock(storage);

    if (latestIndexed.isEmpty()) {
      LOG.info("No existing index found, starting from genesis");
      buildIndex(0, targetBlock);
      return;
    }

    long startBlock = latestIndexed.get() + 1;
    if (startBlock > targetBlock) {
      LOG.info("Index already up to date at block {}", latestIndexed.get());
      return;
    }

    LOG.info("Resuming index build from block {} to block {}", startBlock, targetBlock);
    buildIndex(startBlock, targetBlock);
  }

  /**
   * Updates the index for a newly added block. This is called incrementally as new blocks are added
   * to the chain.
   *
   * @param blockNumber the block number
   * @param trieLog the trielog for the block
   */
  public void updateForNewBlock(final long blockNumber, final TrieLog trieLog) {
    try {
      SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

      if (trieLog instanceof TrieLogLayer) {
        processTrieLog(tx, blockNumber, (TrieLogLayer) trieLog);
        index.updateLatestIndexedBlock(tx, blockNumber);
        tx.commit();
      } else {
        LOG.warn(
            "Unexpected trielog type for block {}: {}",
            blockNumber,
            trieLog.getClass().getSimpleName());
      }
    } catch (Exception e) {
      LOG.error("Error updating index for block {}", blockNumber, e);
    }
  }

  /**
   * Processes a batch of blocks and commits the transaction.
   *
   * @param fromBlock the starting block number (inclusive)
   * @param toBlock the ending block number (inclusive)
   */
  private void processBatch(final long fromBlock, final long toBlock) {
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    try {
      for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
        Optional<TrieLog> trieLogOpt = getTrieLogForBlock(blockNumber);

        if (trieLogOpt.isPresent() && trieLogOpt.get() instanceof TrieLogLayer) {
          TrieLogLayer trieLog = (TrieLogLayer) trieLogOpt.get();
          processTrieLog(tx, blockNumber, trieLog);
          blocksProcessed.incrementAndGet();
        } else {
          LOG.debug("No trielog found for block {}", blockNumber);
        }
      }

      // Update latest indexed block
      index.updateLatestIndexedBlock(tx, toBlock);

      // Commit the batch
      tx.commit();

    } catch (Exception e) {
      LOG.error("Error processing batch from {} to {}", fromBlock, toBlock, e);
      tx.rollback();
      throw e;
    }
  }

  /**
   * Processes a single trielog and adds index entries for all account and storage changes.
   *
   * @param tx the storage transaction
   * @param blockNumber the block number
   * @param trieLog the trielog
   */
  private void processTrieLog(
      final SegmentedKeyValueStorageTransaction tx,
      final long blockNumber,
      final TrieLogLayer trieLog) {

    // Process account changes - we only care about which accounts changed, not their values
    for (Address address : trieLog.getAccountChanges().keySet()) {
      Hash accountHash = Hash.hash(address);
      index.addAccountModification(storage, tx, accountHash, blockNumber);
    }

    // Process storage changes - we only care about which slots changed, not their values
    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageChanges) -> {
              Hash accountHash = Hash.hash(address);
              for (StorageSlotKey slotKey : storageChanges.keySet()) {
                Hash slotHash = slotKey.getSlotHash();
                index.addStorageModification(storage, tx, accountHash, slotHash, blockNumber);
              }
            });
  }

  /**
   * Retrieves the trielog for a specific block number.
   *
   * @param blockNumber the block number
   * @return the trielog, or empty if not found
   */
  private Optional<TrieLog> getTrieLogForBlock(final long blockNumber) {
    // Get block header by number
    Optional<BlockHeader> header = blockchain.getBlockHeader(blockNumber);
    if (header.isEmpty()) {
      return Optional.empty();
    }

    // Get trielog by block hash
    Hash blockHash = header.get().getHash();
    return trieLogManager.getTrieLogLayer(blockHash);
  }

  /**
   * Logs progress of the index build.
   *
   * @param fromBlock the starting block number
   * @param toBlock the ending block number
   * @param startTime the start time in milliseconds
   */
  private void logProgress(final long fromBlock, final long toBlock, final long startTime) {
    long processed = blocksProcessed.get();
    long total = toBlock - fromBlock + 1;
    double percentComplete = (processed * 100.0) / total;
    long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
    long blocksPerSecond = elapsedSeconds > 0 ? processed / elapsedSeconds : 0;

    LOG.info(
        "Archive index build progress: {}/{} blocks ({:.2f}%) - {} blocks/sec",
        processed, total, percentComplete, blocksPerSecond);
  }

  /**
   * Gets the current build progress.
   *
   * @return the number of blocks processed
   */
  public long getBlocksProcessed() {
    return blocksProcessed.get();
  }

  /**
   * Checks if the index is complete for the current chain.
   *
   * @return true if the index is up to date with the chain head
   */
  public boolean isIndexComplete() {
    Optional<Long> latestIndexed = index.getLatestIndexedBlock(storage);
    if (latestIndexed.isEmpty()) {
      return false;
    }

    long chainHeadNumber = blockchain.getChainHeadBlockNumber();
    return latestIndexed.get() >= chainHeadNumber;
  }
}
