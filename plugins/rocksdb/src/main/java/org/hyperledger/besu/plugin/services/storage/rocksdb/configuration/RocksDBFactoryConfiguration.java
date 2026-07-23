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

import java.util.Optional;

/** The RocksDb factory configuration. */
public class RocksDBFactoryConfiguration {

  private final int maxOpenFiles;
  private final int backgroundThreadCount;
  private final long cacheCapacity;
  private final boolean isHighSpec;
  private final boolean enableReadCacheForSnapshots;
  private final boolean isBlockchainGarbageCollectionEnabled;
  private final Optional<Double> blobGarbageCollectionAgeCutoff;
  private final Optional<Double> blobGarbageCollectionForceThreshold;
  private final long writeBufferSize;
  private final long blockSize;
  private final int maxWriteBufferNumber;
  private final long softPendingCompactionBytesLimit;
  private final long hardPendingCompactionBytesLimit;
  private final int maxBackgroundJobs;
  private final int maxSubcompactions;
  private final long recycleLogFileNum;
  private final double maxBytesForLevelMultiplier;
  private final long targetFileSizeBase;

  /**
   * Instantiates a new RocksDb factory configuration.
   *
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled is garbage collection enabled for the BLOCKCHAIN
   *     column family
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   */
  public RocksDBFactoryConfiguration(
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final boolean isHighSpec,
      final boolean enableReadCacheForSnapshots,
      final boolean isBlockchainGarbageCollectionEnabled,
      final Optional<Double> blobGarbageCollectionAgeCutoff,
      final Optional<Double> blobGarbageCollectionForceThreshold) {
    this(
        maxOpenFiles,
        backgroundThreadCount,
        cacheCapacity,
        isHighSpec,
        enableReadCacheForSnapshots,
        isBlockchainGarbageCollectionEnabled,
        blobGarbageCollectionAgeCutoff,
        blobGarbageCollectionForceThreshold,
        0L,
        0L,
        0,
        0L,
        0L,
        0,
        0,
        -1L);
  }

  /**
   * Instantiates a new RocksDb factory configuration with bulk-load tuning options.
   *
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled is garbage collection enabled for the BLOCKCHAIN
   *     column family
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   * @param writeBufferSize write buffer size per column family in bytes (0 = use RocksDB default)
   * @param blockSize SST block size in bytes for all column families (0 = use RocksDB default)
   * @param maxWriteBufferNumber max write buffers per column family (0 = use RocksDB default)
   * @param softPendingCompactionBytesLimit soft pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @param hardPendingCompactionBytesLimit hard pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @param maxBackgroundJobs maximum number of background jobs (0 = use RocksDB default)
   * @param maxSubcompactions maximum number of subcompactions per compaction job (0 = use RocksDB
   *     default)
   * @param recycleLogFileNum RocksDB recycle_log_file_num (-1 = leave Besu default)
   */
  public RocksDBFactoryConfiguration(
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final boolean isHighSpec,
      final boolean enableReadCacheForSnapshots,
      final boolean isBlockchainGarbageCollectionEnabled,
      final Optional<Double> blobGarbageCollectionAgeCutoff,
      final Optional<Double> blobGarbageCollectionForceThreshold,
      final long writeBufferSize,
      final long blockSize,
      final int maxWriteBufferNumber,
      final long softPendingCompactionBytesLimit,
      final long hardPendingCompactionBytesLimit,
      final int maxBackgroundJobs,
      final int maxSubcompactions,
      final long recycleLogFileNum) {
    this(
        maxOpenFiles,
        backgroundThreadCount,
        cacheCapacity,
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
        0.0,
        0L);
  }

  /**
   * Instantiates a new RocksDb factory configuration with bulk-load tuning options including
   * leveled compaction shaping.
   *
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled is garbage collection enabled for the BLOCKCHAIN
   *     column family
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   * @param writeBufferSize write buffer size per column family in bytes (0 = use RocksDB default)
   * @param blockSize SST block size in bytes for all column families (0 = use RocksDB default)
   * @param maxWriteBufferNumber max write buffers per column family (0 = use RocksDB default)
   * @param softPendingCompactionBytesLimit soft pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @param hardPendingCompactionBytesLimit hard pending compaction bytes limit per CF in bytes (0 =
   *     use RocksDB default)
   * @param maxBackgroundJobs maximum number of background jobs (0 = use RocksDB default)
   * @param maxSubcompactions maximum number of subcompactions per compaction job (0 = use RocksDB
   *     default)
   * @param recycleLogFileNum RocksDB recycle_log_file_num (-1 = leave Besu default)
   * @param maxBytesForLevelMultiplier RocksDB max_bytes_for_level_multiplier per CF (0 = use
   *     RocksDB default)
   * @param targetFileSizeBase RocksDB target_file_size_base in bytes per CF (0 = use RocksDB
   *     default)
   */
  public RocksDBFactoryConfiguration(
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final boolean isHighSpec,
      final boolean enableReadCacheForSnapshots,
      final boolean isBlockchainGarbageCollectionEnabled,
      final Optional<Double> blobGarbageCollectionAgeCutoff,
      final Optional<Double> blobGarbageCollectionForceThreshold,
      final long writeBufferSize,
      final long blockSize,
      final int maxWriteBufferNumber,
      final long softPendingCompactionBytesLimit,
      final long hardPendingCompactionBytesLimit,
      final int maxBackgroundJobs,
      final int maxSubcompactions,
      final long recycleLogFileNum,
      final double maxBytesForLevelMultiplier,
      final long targetFileSizeBase) {
    this.backgroundThreadCount = backgroundThreadCount;
    this.maxOpenFiles = maxOpenFiles;
    this.cacheCapacity = cacheCapacity;
    this.isHighSpec = isHighSpec;
    this.enableReadCacheForSnapshots = enableReadCacheForSnapshots;
    this.isBlockchainGarbageCollectionEnabled = isBlockchainGarbageCollectionEnabled;
    this.blobGarbageCollectionAgeCutoff = blobGarbageCollectionAgeCutoff;
    this.blobGarbageCollectionForceThreshold = blobGarbageCollectionForceThreshold;
    this.writeBufferSize = writeBufferSize;
    this.blockSize = blockSize;
    this.maxWriteBufferNumber = maxWriteBufferNumber;
    this.softPendingCompactionBytesLimit = softPendingCompactionBytesLimit;
    this.hardPendingCompactionBytesLimit = hardPendingCompactionBytesLimit;
    this.maxBackgroundJobs = maxBackgroundJobs;
    this.maxSubcompactions = maxSubcompactions;
    this.recycleLogFileNum = recycleLogFileNum;
    this.maxBytesForLevelMultiplier = maxBytesForLevelMultiplier;
    this.targetFileSizeBase = targetFileSizeBase;
  }

  /**
   * Gets max open files.
   *
   * @return the max open files
   */
  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  /**
   * Gets background thread count.
   *
   * @return the background thread count
   */
  public int getBackgroundThreadCount() {
    return backgroundThreadCount;
  }

  /**
   * Gets cache capacity.
   *
   * @return the cache capacity
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Is high spec.
   *
   * @return the boolean
   */
  public boolean isHighSpec() {
    return isHighSpec;
  }

  /**
   * Indicates whether read caching is enabled for snapshot access.
   *
   * @return {@code true} if read cache is enabled for snapshots; {@code false} otherwise.
   */
  public boolean isReadCacheEnabledForSnapshots() {
    return enableReadCacheForSnapshots;
  }

  /**
   * Is garbage collection enabled for the BLOCKCHAIN column family.
   *
   * @return the boolean
   */
  public boolean isBlockchainGarbageCollectionEnabled() {
    return isBlockchainGarbageCollectionEnabled;
  }

  /**
   * Gets blob garbage collection age cutoff.
   *
   * @return the blob garbage collection age cutoff, if set
   */
  public Optional<Double> getBlobGarbageCollectionAgeCutoff() {
    return blobGarbageCollectionAgeCutoff;
  }

  /**
   * Gets blob garbage collection force threshold.
   *
   * @return the blob garbage collection force threshold, if set
   */
  public Optional<Double> getBlobGarbageCollectionForceThreshold() {
    return blobGarbageCollectionForceThreshold;
  }

  /**
   * Gets write buffer size per column family in bytes.
   *
   * @return the write buffer size (0 = use RocksDB default)
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Gets SST block size in bytes for all column families.
   *
   * @return the block size (0 = use RocksDB default)
   */
  public long getBlockSize() {
    return blockSize;
  }

  /**
   * Gets max write buffer number per column family.
   *
   * @return the max write buffer number (0 = use RocksDB default)
   */
  public int getMaxWriteBufferNumber() {
    return maxWriteBufferNumber;
  }

  /**
   * Gets soft pending compaction bytes limit per column family in bytes.
   *
   * @return the soft pending compaction bytes limit (0 = use RocksDB default)
   */
  public long getSoftPendingCompactionBytesLimit() {
    return softPendingCompactionBytesLimit;
  }

  /**
   * Gets hard pending compaction bytes limit per column family in bytes.
   *
   * @return the hard pending compaction bytes limit (0 = use RocksDB default)
   */
  public long getHardPendingCompactionBytesLimit() {
    return hardPendingCompactionBytesLimit;
  }

  /**
   * Gets maximum number of background jobs.
   *
   * @return the max background jobs (0 = use RocksDB default)
   */
  public int getMaxBackgroundJobs() {
    return maxBackgroundJobs;
  }

  /**
   * Gets maximum number of subcompactions per compaction job.
   *
   * @return the max subcompactions (0 = use RocksDB default)
   */
  public int getMaxSubcompactions() {
    return maxSubcompactions;
  }

  /**
   * Gets RocksDB recycle_log_file_num.
   *
   * @return the recycle log file num (-1 = leave Besu default)
   */
  public long getRecycleLogFileNum() {
    return recycleLogFileNum;
  }

  /**
   * Gets RocksDB max_bytes_for_level_multiplier per column family.
   *
   * @return the max bytes for level multiplier (0 = use RocksDB default)
   */
  public double getMaxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier;
  }

  /**
   * Gets RocksDB target_file_size_base in bytes per column family.
   *
   * @return the target file size base (0 = use RocksDB default)
   */
  public long getTargetFileSizeBase() {
    return targetFileSizeBase;
  }
}
