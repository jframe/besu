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
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BonsaiArchiveIndexBuilder builds an index of state changes from trielogs. This index enables
 * fast O(1) lookups of historical state without requiring expensive seekForPrev operations.
 *
 * <p>The builder processes trielogs sequentially and tracks which blocks modified which accounts
 * and storage slots. This approach is inspired by Geth's archive implementation.
 */
public class BonsaiArchiveIndexBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveIndexBuilder.class);

  private final BonsaiArchiveStateIndex stateIndex;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final SegmentedKeyValueStorage storage;

  private final AtomicBoolean isBuilding = new AtomicBoolean(false);
  private final AtomicLong lastIndexedBlock = new AtomicLong(0);

  // Batch size for committing index updates
  private static final int INDEX_BUILD_BATCH_SIZE = 1000;

  public BonsaiArchiveIndexBuilder(
      final BonsaiArchiveStateIndex stateIndex,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final SegmentedKeyValueStorage storage) {
    this.stateIndex = stateIndex;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.storage = storage;

    // Initialize last indexed block from the index
    stateIndex.getLatestIndexedBlock().ifPresent(lastIndexedBlock::set);
  }

  /**
   * Build the index from trielogs for a range of blocks.
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @return the number of blocks indexed
   */
  public long buildIndex(final long startBlock, final long endBlock) {
    if (!isBuilding.compareAndSet(false, true)) {
      LOG.warn("Index build already in progress");
      return 0;
    }

    try {
      LOG.info("Starting archive index build from block {} to {}", startBlock, endBlock);
      long startTime = System.currentTimeMillis();
      long blocksProcessed = 0;

      SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
      int batchCount = 0;

      for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
        Optional<Hash> blockHash = blockchain.getBlockHashByNumber(blockNum);
        if (blockHash.isEmpty()) {
          LOG.warn("Block {} not found in blockchain, skipping", blockNum);
          continue;
        }

        Optional<BlockHeader> header = blockchain.getBlockHeader(blockHash.get());
        if (header.isEmpty()) {
          LOG.warn("Header for block {} not found, skipping", blockNum);
          continue;
        }

        // Get the trielog for this block
        Optional<TrieLog> trieLogOpt = trieLogManager.getTrieLogLayer(blockHash.get());
        if (trieLogOpt.isEmpty()) {
          LOG.trace("No trielog found for block {}, skipping", blockNum);
          continue;
        }

        // Index changes from this trielog
        indexTrieLog(transaction, trieLogOpt.get(), blockNum);
        blocksProcessed++;
        batchCount++;

        // Commit in batches
        if (batchCount >= INDEX_BUILD_BATCH_SIZE) {
          transaction.commit();
          transaction = storage.startTransaction();
          batchCount = 0;
          lastIndexedBlock.set(blockNum);

          if (blocksProcessed % 10000 == 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            double blocksPerSecond = (blocksProcessed * 1000.0) / elapsed;
            LOG.info(
                "Index build progress: {} blocks indexed ({} blocks/sec), current block: {}",
                blocksProcessed,
                String.format("%.2f", blocksPerSecond),
                blockNum);
          }
        }
      }

      // Commit remaining changes
      if (batchCount > 0) {
        transaction.commit();
        lastIndexedBlock.set(endBlock);
      }

      // Mark index as built
      SegmentedKeyValueStorageTransaction markerTx = storage.startTransaction();
      stateIndex.markIndexBuilt(markerTx, endBlock);
      markerTx.commit();

      long duration = System.currentTimeMillis() - startTime;
      double blocksPerSecond = (blocksProcessed * 1000.0) / duration;
      LOG.info(
          "Archive index build complete. Indexed {} blocks in {} seconds ({} blocks/sec)",
          blocksProcessed,
          duration / 1000,
          String.format("%.2f", blocksPerSecond));

      return blocksProcessed;

    } catch (Exception e) {
      LOG.error("Error building archive index", e);
      return 0;
    } finally {
      isBuilding.set(false);
    }
  }

  /**
   * Build the index incrementally from the last indexed block to the current chain head.
   *
   * @return the number of blocks indexed
   */
  public long buildIndexIncremental() {
    long startBlock = lastIndexedBlock.get() + 1;
    long endBlock = blockchain.getChainHeadBlockNumber();

    if (startBlock > endBlock) {
      LOG.debug("Index is up to date. Last indexed block: {}", lastIndexedBlock.get());
      return 0;
    }

    return buildIndex(startBlock, endBlock);
  }

  /**
   * Index changes from a single trielog.
   *
   * @param transaction the storage transaction to use
   * @param trieLog the trielog to index
   * @param blockNumber the block number of the trielog
   */
  private void indexTrieLog(
      final SegmentedKeyValueStorageTransaction transaction,
      final TrieLog trieLog,
      final long blockNumber) {

    // Index account changes
    Map<Address, ? extends TrieLog.LogTuple<?>> accountChanges = trieLog.getAccountChanges();
    for (Address address : accountChanges.keySet()) {
      stateIndex.addAccountModification(transaction, address.addressHash(), blockNumber);
    }

    // Index storage changes
    Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<?>>> storageChanges =
        trieLog.getStorageChanges();
    for (Map.Entry<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<?>>> entry :
        storageChanges.entrySet()) {
      Address address = entry.getKey();
      Map<StorageSlotKey, ? extends TrieLog.LogTuple<?>> slots = entry.getValue();

      for (StorageSlotKey slotKey : slots.keySet()) {
        stateIndex.addStorageModification(transaction, address.addressHash(), slotKey, blockNumber);
      }
    }

    LOG.trace(
        "Indexed block {}: {} account changes, {} storage changes",
        blockNumber,
        accountChanges.size(),
        storageChanges.values().stream().mapToInt(Map::size).sum());
  }

  /**
   * Check if an index build is currently in progress.
   *
   * @return true if building, false otherwise
   */
  public boolean isBuilding() {
    return isBuilding.get();
  }

  /**
   * Get the last indexed block number.
   *
   * @return the last indexed block number
   */
  public long getLastIndexedBlock() {
    return lastIndexedBlock.get();
  }

  /**
   * Check if the index is built and up to date.
   *
   * @return true if the index is built and current, false otherwise
   */
  public boolean isIndexUpToDate() {
    return stateIndex.isIndexBuilt()
        && lastIndexedBlock.get() >= blockchain.getChainHeadBlockNumber();
  }
}
