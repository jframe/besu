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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a Bonsai FULL flat database to ARCHIVE format.
 *
 * <p>This converter processes trie logs from genesis to head, adding block number suffixes to all
 * state keys to create a versioned archive. The conversion happens in batches with periodic
 * checkpointing for resumability.
 *
 * <p>Key features: - Processes blocks sequentially from start to end - Batches writes for
 * performance (default 10,000 operations) - Checkpoints progress every 10,000 blocks - Resumable
 * from last checkpoint - Updates FLAT_DB_MODE to ARCHIVE on completion
 */
public class BonsaiFlatDbToArchiveConverter {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveConverter.class);

  private static final int BATCH_SIZE = 10_000;
  private static final int CHECKPOINT_INTERVAL = 10_000;
  private static final byte[] CONVERSION_PROGRESS_KEY =
      "ARCHIVE_CONVERSION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;

  /**
   * Creates a new BonsaiFlatDbToArchiveConverter.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running conversion on a separate thread
   */
  public BonsaiFlatDbToArchiveConverter(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
  }

  /**
   * Converts FULL flat DB to ARCHIVE format by processing trie logs from startBlock to endBlock.
   *
   * <p>The conversion runs asynchronously on the provided executor service. It: 1. Loads progress
   * or starts fresh 2. Processes blocks sequentially, writing archive keys 3. Checkpoints progress
   * periodically 4. Updates FLAT_DB_MODE to ARCHIVE on completion
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @return a CompletableFuture that completes when conversion finishes
   */
  public CompletableFuture<Void> convert(final long startBlock, final long endBlock) {
    return CompletableFuture.runAsync(
        () -> {
          LOG.info("Starting archive conversion from block {} to {}", startBlock, endBlock);

          long currentBlock = loadProgress().orElse(startBlock);

          if (currentBlock > startBlock) {
            LOG.info(
                "Resuming conversion from block {} (previously started at {})",
                currentBlock,
                startBlock);
          }

          int batchCount = 0;
          SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
          SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

          while (currentBlock <= endBlock) {
            Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(currentBlock);
            if (blockHeader.isEmpty()) {
              LOG.warn("Missing block header for block {}, skipping", currentBlock);
              currentBlock++;
              continue;
            }

            Optional<TrieLog> trieLog = trieLogManager.getTrieLogLayer(blockHeader.get().getHash());

            if (trieLog.isEmpty()) {
              LOG.warn("Missing trie log for block {}, skipping", currentBlock);
              currentBlock++;
              continue;
            }

            processBlock(trieLog.get(), currentBlock, tx);
            batchCount++;

            if (batchCount >= BATCH_SIZE) {
              tx.commit();
              tx = storage.startTransaction();
              batchCount = 0;
            }

            if (currentBlock % CHECKPOINT_INTERVAL == 0) {
              saveProgress(currentBlock);
              long progressPercent = ((currentBlock - startBlock) * 100) / (endBlock - startBlock);
              LOG.info(
                  "Archive conversion progress: {}% (block {}/{})",
                  progressPercent,
                  currentBlock,
                  endBlock);
            }

            currentBlock++;
          }

          if (batchCount > 0) {
            tx.commit();
          }

          worldStateStorage.upgradeToArchiveDbMode();
          saveProgress(endBlock);

          LOG.info("Archive conversion completed. Processed {} blocks.", endBlock - startBlock + 1);
        },
        executorService);
  }

  /**
   * Processes a single block's trie log, writing archive keys for all state changes.
   *
   * @param trieLog the trie log containing state changes
   * @param blockNumber the block number for versioning
   * @param tx the transaction to write to
   */
  private void processBlock(
      final TrieLog trieLog,
      final long blockNumber,
      final SegmentedKeyValueStorageTransaction tx) {

    BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  /**
   * Processes account changes from a trie log, writing archive keys for historical account state.
   *
   * @param trieLog the trie log containing account changes
   * @param context the Bonsai context with block number for versioning
   * @param tx the transaction to write to
   */
  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {

    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getPrior() != null) {
                Bytes accountBytes = RLP.encode(accountChange.getPrior()::writeTo);
                BonsaiArchiveFlatDbStrategy.putFlatAccountWithContext(
                    tx, context, address.addressHash(), accountBytes);
              }
            });
  }

  /**
   * Processes storage changes from a trie log, writing archive keys for historical storage state.
   *
   * @param trieLog the trie log containing storage changes
   * @param context the Bonsai context with block number for versioning
   * @param tx the transaction to write to
   */
  private void processStorageChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {

    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageMap) -> {
              storageMap.forEach(
                  (slotKey, storageChange) -> {
                    if (storageChange.getPrior() != null) {
                      BonsaiArchiveFlatDbStrategy.putFlatAccountStorageValueWithContext(
                          tx,
                          context,
                          address.addressHash(),
                          slotKey.getSlotHash(),
                          storageChange.getPrior().toBytes());
                    }
                  });
            });
  }

  /**
   * Loads the conversion progress from storage.
   *
   * @return the last processed block number, or empty if no progress exists
   */
  private Optional<Long> loadProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(TRIE_BRANCH_STORAGE, CONVERSION_PROGRESS_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  /**
   * Saves the conversion progress to storage.
   *
   * @param blockNumber the last successfully processed block number
   */
  private void saveProgress(final long blockNumber) {
    SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        CONVERSION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }
}
