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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.Subscribers;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai FULL flat database to ARCHIVE format.
 *
 * <p>This migrator processes trie logs from genesis to head, adding block number suffixes to all
 * state keys to create a versioned archive. The migration happens in batches with periodic
 * checkpointing for resumability.
 *
 * <p>Key features: - Processes blocks sequentially from start to end - Batches writes for
 * performance (default 10,000 operations) - Checkpoints progress every 10,000 blocks - Resumable
 * from last checkpoint - Updates FLAT_DB_MODE to ARCHIVE on completion
 */
public class BonsaiFlatDbToArchiveMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);

  private static final int BATCH_SIZE = 10_000;
  private static final int CHECKPOINT_INTERVAL = 10_000;
  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final Subscribers<MigrationCompletionListener> completionListeners = Subscribers.create();

  /** Listener interface for migration completion events. */
  public interface MigrationCompletionListener {
    /**
     * Called when the archive migration completes successfully.
     *
     * @param startBlock the starting block number of the migration
     * @param endBlock the ending block number of the migration
     */
    void onMigrationComplete(long startBlock, long endBlock);

    /**
     * Called when the archive migration fails with an error.
     *
     * @param startBlock the starting block number of the migration
     * @param endBlock the ending block number of the migration
     * @param error the error that caused the failure
     */
    void onMigrationFailed(long startBlock, long endBlock, Throwable error);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy());
  }

  /**
   * Subscribe to migration completion events.
   *
   * @param listener the listener to notify on migration completion
   * @return the subscription ID that can be used to unsubscribe
   */
  public long subscribe(final MigrationCompletionListener listener) {
    return completionListeners.subscribe(listener);
  }

  /**
   * Unsubscribe from migration completion events.
   *
   * @param subscriptionId the subscription ID returned from subscribe
   * @return true if the listener was found and removed
   */
  public boolean unsubscribe(final long subscriptionId) {
    return completionListeners.unsubscribe(subscriptionId);
  }

  /**
   * Migrates FULL flat DB to ARCHIVE format by processing trie logs from startBlock to endBlock.
   *
   * <p>The migration runs asynchronously on the provided executor service. It: 1. Loads progress or
   * starts fresh 2. Processes blocks sequentially, writing archive keys 3. Checkpoints progress
   * periodically 4. Updates FLAT_DB_MODE to ARCHIVE on completion
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate(final long startBlock, final long endBlock) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            LOG.info("Starting archive migration from block {} to {}", startBlock, endBlock);

            long currentBlock = loadProgress().orElse(startBlock);

            if (currentBlock > startBlock) {
              LOG.info(
                  "Resuming migration from block {} (previously started at {})",
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

              Optional<TrieLog> trieLog =
                  trieLogManager.getTrieLogLayer(blockHeader.get().getHash());

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
                long totalBlocks = endBlock - startBlock;
                long progressPercent =
                    totalBlocks > 0 ? ((currentBlock - startBlock) * 100) / totalBlocks : 100;
                LOG.info(
                    "Archive migration progress: {}% (block {}/{})",
                    progressPercent, currentBlock, endBlock);
              }

              currentBlock++;
            }

            if (batchCount > 0) {
              tx.commit();
            }

            worldStateStorage.upgradeToArchiveDbMode();
            saveProgress(endBlock);

            LOG.info(
                "Archive migration completed. Processed {} blocks.", endBlock - startBlock + 1);

            // Notify all listeners of successful completion
            completionListeners.forEach(
                listener -> listener.onMigrationComplete(startBlock, endBlock));
          } catch (final Exception e) {
            LOG.error("Archive migration failed", e);
            // Notify all listeners of failure
            completionListeners.forEach(
                listener -> listener.onMigrationFailed(startBlock, endBlock, e));
            throw e;
          }
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
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {

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
              if (accountChange.getUpdated() != null) {
                Bytes accountBytes = RLP.encode(accountChange.getUpdated()::writeTo);
                BonsaiArchiveFlatDbStrategy.putFlatAccountWithContext(
                    tx, context, address.addressHash(), accountBytes);
              } else {
                // Account was deleted - use the remove method with explicit context
                archiveStrategy.removeFlatAccountWithContext(tx, context, address.addressHash());
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
                    if (storageChange.getUpdated() != null) {
                      BonsaiArchiveFlatDbStrategy.putFlatAccountStorageValueWithContext(
                          tx,
                          context,
                          address.addressHash(),
                          slotKey.getSlotHash(),
                          storageChange.getUpdated().toBytes());
                    } else {
                      // Storage was deleted - use the remove method with explicit context
                      archiveStrategy.removeFlatAccountStorageValueByStorageSlotHashWithContext(
                          tx, context, address.addressHash(), slotKey.getSlotHash());
                    }
                  });
            });
  }

  /**
   * Loads the migration progress from storage.
   *
   * @return the last processed block number, or empty if no progress exists
   */
  private Optional<Long> loadProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(TRIE_BRANCH_STORAGE, MIGRATION_PROGRESS_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  /**
   * Saves the migration progress to storage.
   *
   * @param blockNumber the last successfully processed block number
   */
  private void saveProgress(final long blockNumber) {
    SegmentedKeyValueStorage storage = worldStateStorage.getComposedWorldStateStorage();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }
}
