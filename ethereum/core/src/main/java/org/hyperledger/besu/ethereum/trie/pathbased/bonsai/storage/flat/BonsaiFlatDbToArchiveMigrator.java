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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VARIABLES;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.Subscribers;
import org.hyperledger.besu.util.log.LogUtil;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai FULL flat database to ARCHIVE format.
 *
 * <p>This migrator processes trie logs from genesis to head, adding block number suffixes to all
 * state keys to create a versioned archive.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Processes blocks sequentially from start to end
 *   <li>Saves progress after each block for resumability
 *   <li>Updates FLAT_DB_MODE to ARCHIVE on completion
 * </ul>
 */
public class BonsaiFlatDbToArchiveMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);

  private static final int PRINT_DELAY_SECONDS = 30;
  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final Subscribers<MigrationCompletionListener> completionListeners = Subscribers.create();
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);

  /** Listener interface for migration completion events. */
  public interface MigrationCompletionListener {
    /** Called when the archive migration completes successfully. */
    void onMigrationComplete();

    /**
     * Called when the archive migration fails with an error.
     *
     * @param error the error that caused the failure
     */
    void onMigrationFailed(Throwable error);
  }

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
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
   * Resumes from saved progress if available.
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate(final long startBlock, final long endBlock) {
    return migrate(startBlock, endBlock, false);
  }

  /**
   * Migrates FULL flat DB to ARCHIVE format by processing trie logs sequentially.
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   * @param resetProgress if true, ignores any saved progress and starts from startBlock
   * @return a CompletableFuture that completes when migration finishes
   */
  public CompletableFuture<Void> migrate(
      final long startBlock, final long endBlock, final boolean resetProgress) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            final Instant migrationStartTime = Instant.now();
            LOG.info("Starting archive migration from block {} to {}", startBlock, endBlock);

            worldStateStorage.upgradeToArchiveDbMode();

            final long currentBlock = determineStartBlock(startBlock, resetProgress);
            final SegmentedKeyValueStorage storage =
                worldStateStorage.getComposedWorldStateStorage();

            for (long blockNumber = currentBlock; blockNumber <= endBlock; blockNumber++) {
              final Optional<TrieLog> maybeTrieLog = fetchTrieLog(blockNumber);
              if (maybeTrieLog.isEmpty()) {
                continue;
              }

              final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
              processBlock(maybeTrieLog.get(), blockNumber, tx);
              saveProgress(blockNumber, tx);
              tx.commit();

              logProgress(blockNumber, startBlock, endBlock);
            }

            logCompletion(startBlock, endBlock, migrationStartTime);
            completionListeners.forEach(MigrationCompletionListener::onMigrationComplete);
          } catch (final Exception e) {
            LOG.error("Archive migration failed", e);
            completionListeners.forEach(listener -> listener.onMigrationFailed(e));
            throw new RuntimeException(e);
          }
        },
        executorService);
  }

  private long determineStartBlock(final long startBlock, final boolean resetProgress) {
    if (resetProgress) {
      LOG.info("Resetting migration progress, starting from block {}", startBlock);
      return startBlock;
    }
    final long currentBlock = loadProgress().orElse(startBlock);
    if (currentBlock > startBlock) {
      LOG.info(
          "Resuming migration from block {} (previously started at {})", currentBlock, startBlock);
    }
    return currentBlock;
  }

  private void logProgress(final long blockNumber, final long startBlock, final long endBlock) {
    final long totalBlocks = endBlock - startBlock;
    LogUtil.throttledLog(
        () -> {
          long progressPercent =
              totalBlocks > 0 ? ((blockNumber - startBlock) * 100) / totalBlocks : 100;
          LOG.info(
              "Archive migration progress: {}% (block {}/{})",
              progressPercent, blockNumber, endBlock);
        },
        shouldLogProgress,
        PRINT_DELAY_SECONDS);
  }

  private void logCompletion(
      final long startBlock, final long endBlock, final Instant migrationStartTime) {
    final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
    LOG.info(
        "Archive migration completed. Processed {} blocks in {}.",
        endBlock - startBlock + 1,
        DurationFormatUtils.formatDurationWords(migrationDuration.toMillis(), false, false));
  }

  private Optional<TrieLog> fetchTrieLog(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash()));
  }

  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getUpdated() != null) {
                final Bytes accountBytes = RLP.encode(accountChange.getUpdated()::writeTo);
                BonsaiArchiveFlatDbStrategy.putFlatAccountWithContext(
                    tx, context, address.addressHash(), accountBytes);
              } else {
                archiveStrategy.removeFlatAccountWithContext(tx, context, address.addressHash());
              }
            });
  }

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
                      archiveStrategy.removeFlatAccountStorageValueByStorageSlotHashWithContext(
                          tx, context, address.addressHash(), slotKey.getSlotHash());
                    }
                  });
            });
  }

  private Optional<Long> loadProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(VARIABLES, MIGRATION_PROGRESS_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(VARIABLES, MIGRATION_PROGRESS_KEY, Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }
}
