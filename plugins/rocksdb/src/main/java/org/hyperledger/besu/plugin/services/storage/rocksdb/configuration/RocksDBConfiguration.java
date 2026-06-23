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

import java.nio.file.Path;
import java.util.Optional;

/** The Rocks db configuration. */
public class RocksDBConfiguration {

  private final Path databaseDir;
  private final int maxOpenFiles;
  private final String label;
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

  /**
   * Instantiates a new RocksDb configuration.
   *
   * @param databaseDir the database dir
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param label the label
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled the garbage collection enabled for the BLOCKCHAIN
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
  public RocksDBConfiguration(
      final Path databaseDir,
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final String label,
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
    this.backgroundThreadCount = backgroundThreadCount;
    this.databaseDir = databaseDir;
    this.maxOpenFiles = maxOpenFiles;
    this.cacheCapacity = cacheCapacity;
    this.label = label;
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
  }

  /**
   * Gets database dir.
   *
   * @return the database dir
   */
  public Path getDatabaseDir() {
    return databaseDir;
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
   * Gets label.
   *
   * @return the label
   */
  public String getLabel() {
    return label;
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
   * @return {@code true} if read cache is used during snapshot reads; {@code false} otherwise.
   */
  public boolean isReadCacheEnabledForSnapshots() {
    return enableReadCacheForSnapshots;
  }

  /**
   * Is blockchain garbage collection enabled.
   *
   * @return the boolean
   */
  public boolean isBlockchainGarbageCollectionEnabled() {
    return isBlockchainGarbageCollectionEnabled;
  }

  /**
   * Gets blob garbage collection age cutoff.
   *
   * @return the blob garbage collection age cutoff
   */
  public Optional<Double> getBlobGarbageCollectionAgeCutoff() {
    return blobGarbageCollectionAgeCutoff;
  }

  /**
   * Gets blob garbage collection force threshold.
   *
   * @return the blob garbage collection force threshold
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
}
