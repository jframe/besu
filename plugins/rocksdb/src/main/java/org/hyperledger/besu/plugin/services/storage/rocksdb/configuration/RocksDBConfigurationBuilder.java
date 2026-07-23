/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services.storage.rocksdb.configuration;

import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BLOCK_SIZE;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_HARD_PENDING_COMPACTION_BYTES_LIMIT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_BACKGROUND_JOBS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_BYTES_FOR_LEVEL_MULTIPLIER;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_SUBCOMPACTIONS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_WRITE_BUFFER_NUMBER;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_RECYCLE_LOG_FILE_NUM;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_SOFT_PENDING_COMPACTION_BYTES_LIMIT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_TARGET_FILE_SIZE_BASE;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_WRITE_BUFFER_SIZE;

import java.nio.file.Path;
import java.util.Optional;

/** The RocksDb configuration builder. */
public class RocksDBConfigurationBuilder {

  private Path databaseDir;
  private String label = "blockchain";
  private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
  private long cacheCapacity = DEFAULT_CACHE_CAPACITY;
  private int backgroundThreadCount = DEFAULT_BACKGROUND_THREAD_COUNT;
  private boolean isHighSpec = DEFAULT_IS_HIGH_SPEC;
  private boolean enableReadCacheForSnapshots = DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS;
  private boolean isBlockchainGarbageCollectionEnabled = false;
  private Optional<Double> blobGarbageCollectionAgeCutoff = Optional.empty();
  private Optional<Double> blobGarbageCollectionForceThreshold = Optional.empty();
  private long writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;
  private long blockSize = DEFAULT_BLOCK_SIZE;
  private int maxWriteBufferNumber = DEFAULT_MAX_WRITE_BUFFER_NUMBER;
  private long softPendingCompactionBytesLimit = DEFAULT_SOFT_PENDING_COMPACTION_BYTES_LIMIT;
  private long hardPendingCompactionBytesLimit = DEFAULT_HARD_PENDING_COMPACTION_BYTES_LIMIT;
  private int maxBackgroundJobs = DEFAULT_MAX_BACKGROUND_JOBS;
  private int maxSubcompactions = DEFAULT_MAX_SUBCOMPACTIONS;
  private long recycleLogFileNum = DEFAULT_RECYCLE_LOG_FILE_NUM;
  private double maxBytesForLevelMultiplier = DEFAULT_MAX_BYTES_FOR_LEVEL_MULTIPLIER;
  private long targetFileSizeBase = DEFAULT_TARGET_FILE_SIZE_BASE;

  /** Instantiates a new Rocks db configuration builder. */
  public RocksDBConfigurationBuilder() {}

  /**
   * Database dir.
   *
   * @param databaseDir the database dir
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder databaseDir(final Path databaseDir) {
    this.databaseDir = databaseDir;
    return this;
  }

  /**
   * Max open files.
   *
   * @param maxOpenFiles the max open files
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder maxOpenFiles(final int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
    return this;
  }

  /**
   * Label.
   *
   * @param label the label
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder label(final String label) {
    this.label = label;
    return this;
  }

  /**
   * Cache capacity.
   *
   * @param cacheCapacity the cache capacity
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder cacheCapacity(final long cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
    return this;
  }

  /**
   * Background thread count.
   *
   * @param backgroundThreadCount the background thread count
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder backgroundThreadCount(final int backgroundThreadCount) {
    this.backgroundThreadCount = backgroundThreadCount;
    return this;
  }

  /**
   * Is high spec.
   *
   * @param isHighSpec the is high spec
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder isHighSpec(final boolean isHighSpec) {
    this.isHighSpec = isHighSpec;
    return this;
  }

  /**
   * Enables or disables read caching for snapshot access.
   *
   * @param enableReadCacheForSnapshots whether read caching should be enabled for snapshots
   * @return the RocksDB configuration builder
   */
  public RocksDBConfigurationBuilder enableReadCacheForSnapshots(
      final boolean enableReadCacheForSnapshots) {
    this.enableReadCacheForSnapshots = enableReadCacheForSnapshots;
    return this;
  }

  /**
   * Is blockchain garbage collection enabled.
   *
   * @param isBlockchainGarbageCollectionEnabled the is blockchain garbage collection enabled
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder isBlockchainGarbageCollectionEnabled(
      final boolean isBlockchainGarbageCollectionEnabled) {
    this.isBlockchainGarbageCollectionEnabled = isBlockchainGarbageCollectionEnabled;
    return this;
  }

  /**
   * Blob garbage collection age cutoff.
   *
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder blobGarbageCollectionAgeCutoff(
      final Optional<Double> blobGarbageCollectionAgeCutoff) {
    this.blobGarbageCollectionAgeCutoff = blobGarbageCollectionAgeCutoff;
    return this;
  }

  /**
   * Blob garbage collection force threshold.
   *
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder blobGarbageCollectionForceThreshold(
      final Optional<Double> blobGarbageCollectionForceThreshold) {
    this.blobGarbageCollectionForceThreshold = blobGarbageCollectionForceThreshold;
    return this;
  }

  /**
   * Write buffer size.
   *
   * @param writeBufferSize write buffer size per column family in bytes (0 = use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder writeBufferSize(final long writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  /**
   * Block size.
   *
   * @param blockSize SST block size in bytes for all column families (0 = use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder blockSize(final long blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  /**
   * Max write buffer number.
   *
   * @param maxWriteBufferNumber max write buffers per column family (0 = use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder maxWriteBufferNumber(final int maxWriteBufferNumber) {
    this.maxWriteBufferNumber = maxWriteBufferNumber;
    return this;
  }

  /**
   * Soft pending compaction bytes limit.
   *
   * @param softPendingCompactionBytesLimit soft pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder softPendingCompactionBytesLimit(
      final long softPendingCompactionBytesLimit) {
    this.softPendingCompactionBytesLimit = softPendingCompactionBytesLimit;
    return this;
  }

  /**
   * Hard pending compaction bytes limit.
   *
   * @param hardPendingCompactionBytesLimit hard pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder hardPendingCompactionBytesLimit(
      final long hardPendingCompactionBytesLimit) {
    this.hardPendingCompactionBytesLimit = hardPendingCompactionBytesLimit;
    return this;
  }

  /**
   * Max background jobs.
   *
   * @param maxBackgroundJobs maximum number of background jobs (0 = use RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder maxBackgroundJobs(final int maxBackgroundJobs) {
    this.maxBackgroundJobs = maxBackgroundJobs;
    return this;
  }

  /**
   * Max subcompactions.
   *
   * @param maxSubcompactions maximum number of subcompactions per compaction job (0 = use RocksDB
   *     default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder maxSubcompactions(final int maxSubcompactions) {
    this.maxSubcompactions = maxSubcompactions;
    return this;
  }

  /**
   * Recycle log file num.
   *
   * @param recycleLogFileNum RocksDB recycle_log_file_num (-1 = leave Besu default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder recycleLogFileNum(final long recycleLogFileNum) {
    this.recycleLogFileNum = recycleLogFileNum;
    return this;
  }

  /**
   * Max bytes for level multiplier.
   *
   * @param maxBytesForLevelMultiplier RocksDB max_bytes_for_level_multiplier per CF (0 = use
   *     RocksDB default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder maxBytesForLevelMultiplier(
      final double maxBytesForLevelMultiplier) {
    this.maxBytesForLevelMultiplier = maxBytesForLevelMultiplier;
    return this;
  }

  /**
   * Target file size base.
   *
   * @param targetFileSizeBase RocksDB target_file_size_base in bytes per CF (0 = use RocksDB
   *     default)
   * @return the rocks db configuration builder
   */
  public RocksDBConfigurationBuilder targetFileSizeBase(final long targetFileSizeBase) {
    this.targetFileSizeBase = targetFileSizeBase;
    return this;
  }

  /**
   * From.
   *
   * @param configuration the configuration
   * @return the rocks db configuration builder
   */
  public static RocksDBConfigurationBuilder from(final RocksDBFactoryConfiguration configuration) {
    return new RocksDBConfigurationBuilder()
        .backgroundThreadCount(configuration.getBackgroundThreadCount())
        .cacheCapacity(configuration.getCacheCapacity())
        .maxOpenFiles(configuration.getMaxOpenFiles())
        .isHighSpec(configuration.isHighSpec())
        .enableReadCacheForSnapshots(configuration.isReadCacheEnabledForSnapshots())
        .isBlockchainGarbageCollectionEnabled(configuration.isBlockchainGarbageCollectionEnabled())
        .blobGarbageCollectionAgeCutoff(configuration.getBlobGarbageCollectionAgeCutoff())
        .blobGarbageCollectionForceThreshold(configuration.getBlobGarbageCollectionForceThreshold())
        .writeBufferSize(configuration.getWriteBufferSize())
        .blockSize(configuration.getBlockSize())
        .maxWriteBufferNumber(configuration.getMaxWriteBufferNumber())
        .softPendingCompactionBytesLimit(configuration.getSoftPendingCompactionBytesLimit())
        .hardPendingCompactionBytesLimit(configuration.getHardPendingCompactionBytesLimit())
        .maxBackgroundJobs(configuration.getMaxBackgroundJobs())
        .maxSubcompactions(configuration.getMaxSubcompactions())
        .recycleLogFileNum(configuration.getRecycleLogFileNum())
        .maxBytesForLevelMultiplier(configuration.getMaxBytesForLevelMultiplier())
        .targetFileSizeBase(configuration.getTargetFileSizeBase());
  }

  /**
   * Build rocks db configuration.
   *
   * @return the rocks db configuration
   */
  public RocksDBConfiguration build() {
    return new RocksDBConfiguration(
        databaseDir,
        maxOpenFiles,
        backgroundThreadCount,
        cacheCapacity,
        label,
        isHighSpec,
        enableReadCacheForSnapshots,
        isBlockchainGarbageCollectionEnabled,
        blobGarbageCollectionAgeCutoff,
        blobGarbageCollectionForceThreshold,
        writeBufferSize,
        blockSize,
        maxWriteBufferNumber,
        softPendingCompactionBytesLimit,
        hardPendingCompactionBytesLimit,
        maxBackgroundJobs,
        maxSubcompactions,
        recycleLogFileNum,
        maxBytesForLevelMultiplier,
        targetFileSizeBase);
  }
}
