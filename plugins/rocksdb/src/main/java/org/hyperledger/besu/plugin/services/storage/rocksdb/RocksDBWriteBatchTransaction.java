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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.function.Function;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SegmentedKeyValueStorageTransaction} backed by a RocksDB {@link WriteBatch}. Unlike
 * {@link RocksDBTransaction}, this implementation bypasses transaction overhead (no MVCC, no
 * conflict detection) and is suitable for bulk write operations such as data migration.
 */
public class RocksDBWriteBatchTransaction implements SegmentedKeyValueStorageTransaction {

  private static final Logger logger = LoggerFactory.getLogger(RocksDBWriteBatchTransaction.class);
  private static final String NO_SPACE_LEFT_ON_DEVICE = "No space left on device";

  private final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper;
  private final RocksDB db;
  private final WriteOptions options;
  private final RocksDBMetrics metrics;
  private final WriteBatch writeBatch;

  /**
   * Instantiates a new RocksDB write batch transaction.
   *
   * @param columnFamilyMapper mapper from segment identifier to column family handle
   * @param db the RocksDB instance
   * @param options the write options
   * @param metrics the metrics
   */
  public RocksDBWriteBatchTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper,
      final RocksDB db,
      final WriteOptions options,
      final RocksDBMetrics metrics) {
    this.columnFamilyMapper = columnFamilyMapper;
    this.db = db;
    this.options = options;
    this.metrics = metrics;
    this.writeBatch = new WriteBatch();
  }

  @Override
  public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      writeBatch.put(columnFamilyMapper.apply(segmentId), key, value);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        logger.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }

  @Override
  public void remove(final SegmentIdentifier segmentId, final byte[] key) {
    try (final OperationTimer.TimingContext ignored = metrics.getRemoveLatency().startTimer()) {
      writeBatch.delete(columnFamilyMapper.apply(segmentId), key);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        logger.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }

  @Override
  public void commit() throws StorageException {
    try (final OperationTimer.TimingContext ignored = metrics.getCommitLatency().startTimer()) {
      db.write(options, writeBatch);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        logger.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    } finally {
      close();
    }
  }

  @Override
  public void rollback() {
    try {
      writeBatch.clear();
    } finally {
      close();
    }
  }

  @Override
  public void close() {
    writeBatch.close();
    options.close();
  }
}
