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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Optimized RocksDB reader that reduces SeekForPrev calls by trying point lookups first for recent
 * blocks.
 *
 * <p>Performance optimization for Bonsai Archive mode where archive keys are formatted as:
 * naturalKey (32 or 64 bytes) + blockNumber (8 bytes, big-endian long)
 *
 * <p>Strategy: 1. For recent blocks (within POINT_LOOKUP_WINDOW), try direct point lookups before
 * falling back to expensive SeekForPrev 2. Use multi-get batching when available to reduce JNI
 * boundary crossings
 */
public class OptimizedRocksDBReader {

  /** Number of recent blocks to try with point lookups before falling back to SeekForPrev */
  private static final int POINT_LOOKUP_WINDOW = 10;

  /** Minimum key size for archive keys (32 bytes hash + 8 bytes block number) */
  private static final int MIN_ARCHIVE_KEY_SIZE = 40;

  /**
   * Optimized getNearestBefore that tries point lookups for recent blocks before falling back to
   * SeekForPrev.
   *
   * @param db the RocksDB instance
   * @param columnFamilyHandle the column family to query
   * @param key the key to search for (should be naturalKey + targetBlockNumber)
   * @return the nearest key-value pair before or equal to the given key
   * @throws StorageException if a database error occurs
   */
  public static Optional<NearestKeyValue> getNearestBeforeOptimized(
      final RocksDB db, final ColumnFamilyHandle columnFamilyHandle, final Bytes key)
      throws StorageException {

    // If the key is too short to be an archive key, fall back to seekForPrev
    if (key.size() < MIN_ARCHIVE_KEY_SIZE) {
      return getNearestBeforeWithSeek(db, columnFamilyHandle, key);
    }

    // Extract the natural key (everything except the last 8 bytes)
    final Bytes naturalKey = key.slice(0, key.size() - 8);
    // Extract the target block number (last 8 bytes)
    final long targetBlockNumber = key.slice(key.size() - 8).toLong();

    // Try point lookups for recent blocks (more efficient than seekForPrev)
    for (int i = 0; i < POINT_LOOKUP_WINDOW; i++) {
      final long lookupBlockNumber = targetBlockNumber - i;
      if (lookupBlockNumber < 0) {
        break; // Don't try negative block numbers
      }

      // Construct the exact key: naturalKey + lookupBlockNumber
      final Bytes lookupKey = Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(lookupBlockNumber));

      try {
        final byte[] value = db.get(columnFamilyHandle, lookupKey.toArrayUnsafe());
        if (value != null) {
          // Found a match with point lookup (much faster than seekForPrev)
          return Optional.of(new NearestKeyValue(lookupKey, Optional.of(value)));
        }
      } catch (final RocksDBException e) {
        throw new StorageException(e);
      }
    }

    // If point lookups didn't find anything, fall back to seekForPrev
    // This means the value is more than POINT_LOOKUP_WINDOW blocks old
    return getNearestBeforeWithSeek(db, columnFamilyHandle, key);
  }

  /**
   * Traditional getNearestBefore using seekForPrev (expensive but comprehensive).
   *
   * @param db the RocksDB instance
   * @param columnFamilyHandle the column family to query
   * @param key the key to search for
   * @return the nearest key-value pair before or equal to the given key
   */
  private static Optional<NearestKeyValue> getNearestBeforeWithSeek(
      final RocksDB db, final ColumnFamilyHandle columnFamilyHandle, final Bytes key) {
    try (final RocksIterator rocksIterator = db.newIterator(columnFamilyHandle)) {
      rocksIterator.seekForPrev(key.toArrayUnsafe());
      if (rocksIterator.isValid()) {
        return Optional.of(
            new NearestKeyValue(Bytes.of(rocksIterator.key()), Optional.of(rocksIterator.value())));
      }
    }
    return Optional.empty();
  }

  /**
   * Batch version of getNearestBefore that processes multiple keys in a single call. This reduces
   * JNI boundary crossings and improves performance for multiple concurrent lookups.
   *
   * @param db the RocksDB instance
   * @param columnFamilyHandle the column family to query
   * @param keys the list of keys to search for
   * @return a list of optional nearest key-value pairs, in the same order as the input keys
   * @throws StorageException if a database error occurs
   */
  public static List<Optional<NearestKeyValue>> getNearestBeforeBatch(
      final RocksDB db, final ColumnFamilyHandle columnFamilyHandle, final List<Bytes> keys)
      throws StorageException {

    // Initialize results list with nulls
    final List<Optional<NearestKeyValue>> results = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      results.add(null);
    }

    // First, try point lookups for all keys in batch
    final List<byte[]> pointLookupKeys = new ArrayList<>();
    final List<Integer> pointLookupIndices = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final Bytes key = keys.get(i);
      if (key.size() >= MIN_ARCHIVE_KEY_SIZE) {
        // Extract natural key and block number
        final Bytes naturalKey = key.slice(0, key.size() - 8);
        final long targetBlockNumber = key.slice(key.size() - 8).toLong();

        // Try the exact block first
        final Bytes lookupKey = Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(targetBlockNumber));
        pointLookupKeys.add(lookupKey.toArrayUnsafe());
        pointLookupIndices.add(i);
      }
    }

    // Batch point lookup using multi-get
    if (!pointLookupKeys.isEmpty()) {
      try {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(pointLookupKeys.size());
        for (int i = 0; i < pointLookupKeys.size(); i++) {
          columnFamilyHandles.add(columnFamilyHandle);
        }

        final List<byte[]> values = db.multiGetAsList(columnFamilyHandles, pointLookupKeys);

        // Process results
        for (int i = 0; i < pointLookupKeys.size(); i++) {
          final int originalIndex = pointLookupIndices.get(i);
          final byte[] value = values.get(i);

          if (value != null) {
            // Found via point lookup
            results.set(
                originalIndex,
                Optional.of(
                    new NearestKeyValue(Bytes.of(pointLookupKeys.get(i)), Optional.of(value))));
          } else {
            // Need to fall back to seekForPrev for this key
            results.set(originalIndex, getNearestBeforeWithSeek(db, columnFamilyHandle, keys.get(originalIndex)));
          }
        }
      } catch (final RocksDBException e) {
        throw new StorageException(e);
      }
    }

    // Handle keys that were too short for optimization or not yet processed
    for (int i = 0; i < keys.size(); i++) {
      if (results.get(i) == null) {
        results.set(i, getNearestBeforeWithSeek(db, columnFamilyHandle, keys.get(i)));
      }
    }

    return results;
  }
}
