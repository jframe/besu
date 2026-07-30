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
 * A {@link SegmentedKeyValueStorageTransaction} backed by a plain RocksDB {@link WriteBatch}.
 *
 * <p>Unlike {@link RocksDBTransaction} (which uses {@code OptimisticTransaction} with a {@code
 * WriteBatchWithIndex}), this class accumulates writes in a {@link WriteBatch} and applies them
 * atomically via {@link RocksDB#write} at commit time. There is no read-set tracking and no
 * conflict detection overhead — making this the right choice for workloads where the caller is the
 * sole writer to the affected column families (e.g., the archive migration).
 */
public class RocksDBWriteBatchTransaction implements SegmentedKeyValueStorageTransaction {
  private static final Logger logger = LoggerFactory.getLogger(RocksDBWriteBatchTransaction.class);
  private static final String NO_SPACE_LEFT_ON_DEVICE = "No space left on device";

  private final RocksDBMetrics metrics;
  private final WriteBatch writeBatch;
  private final WriteOptions writeOptions;
  private final RocksDB db;
  private final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper;

  /**
   * Instantiates a new RocksDB write-batch transaction.
   *
   * @param columnFamilyMapper mapper from segment identifier to column family handle
   * @param db the underlying RocksDB instance
   * @param writeOptions the write options (ownership transferred; closed on {@link #close()})
   * @param metrics the metrics
   */
  public RocksDBWriteBatchTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper,
      final RocksDB db,
      final WriteOptions writeOptions,
      final RocksDBMetrics metrics) {
    this.columnFamilyMapper = columnFamilyMapper;
    this.db = db;
    this.writeOptions = writeOptions;
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
  public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      writeBatch.merge(columnFamilyMapper.apply(segmentId), key, value);
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
      db.write(writeOptions, writeBatch);
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
    close();
  }

  @Override
  public void close() {
    writeBatch.close();
    writeOptions.close();
  }
}
