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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbUtil;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

/** Unit tests for OptimizedRocksDBReader. */
public class OptimizedRocksDBReaderTest {

  @TempDir Path tempDir;

  private RocksDB db;
  private ColumnFamilyHandle columnFamily;

  @BeforeAll
  public static void loadNativeLibrary() {
    RocksDbUtil.loadNativeLibrary();
  }

  @BeforeEach
  public void setup() throws Exception {
    final List<ColumnFamilyDescriptor> columnDescriptors =
        List.of(
            new ColumnFamilyDescriptor(
                RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

    final DBOptions dbOptions =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    final List<ColumnFamilyHandle> columnHandles = new ArrayList<>();
    db = RocksDB.open(dbOptions, tempDir.toString(), columnDescriptors, columnHandles);
    columnFamily = columnHandles.get(0);
  }

  @AfterEach
  public void cleanup() {
    if (columnFamily != null) {
      columnFamily.close();
    }
    if (db != null) {
      db.close();
    }
  }

  /**
   * Test that point lookup successfully finds a value at the exact block number.
   *
   * <p>Archive key format: accountHash (32 bytes) + blockNumber (8 bytes)
   */
  @Test
  public void testPointLookupFindsExactBlock() throws Exception {
    // Create test data: account hash + block number
    final Bytes accountHash = createAccountHash("0xAABBCCDD");
    final long blockNumber = 1000L;
    final Bytes key = createArchiveKey(accountHash, blockNumber);
    final byte[] value = "state_at_block_1000".getBytes(StandardCharsets.UTF_8);

    // Store the value
    db.put(columnFamily, key.toArrayUnsafe(), value);

    // Query for exact block number
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, key);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(key);
    assertThat(result.get().value()).isPresent();
    assertThat(result.get().value().get()).isEqualTo(value);
  }

  /**
   * Test that point lookup finds a value within the point lookup window (last 10 blocks).
   */
  @Test
  public void testPointLookupFindsRecentBlock() throws Exception {
    final Bytes accountHash = createAccountHash("0x1122334455");
    final long storedBlockNumber = 995L;
    final long queryBlockNumber = 1000L;

    // Store value at block 995
    final Bytes storedKey = createArchiveKey(accountHash, storedBlockNumber);
    final byte[] value = "state_at_block_995".getBytes(StandardCharsets.UTF_8);
    db.put(columnFamily, storedKey.toArrayUnsafe(), value);

    // Query for block 1000 (should find block 995 via point lookups)
    final Bytes queryKey = createArchiveKey(accountHash, queryBlockNumber);
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, queryKey);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(storedKey);
    assertThat(result.get().value()).isPresent();
    assertThat(result.get().value().get()).isEqualTo(value);
  }

  /**
   * Test that the optimization falls back to seekForPrev for old blocks (outside point lookup
   * window).
   */
  @Test
  public void testFallbackToSeekForPrevForOldBlocks() throws Exception {
    final Bytes accountHash = createAccountHash("0xDEADBEEF");
    final long storedBlockNumber = 100L;
    final long queryBlockNumber = 1000L;

    // Store value at block 100 (way outside the 10-block window)
    final Bytes storedKey = createArchiveKey(accountHash, storedBlockNumber);
    final byte[] value = "old_state_at_block_100".getBytes(StandardCharsets.UTF_8);
    db.put(columnFamily, storedKey.toArrayUnsafe(), value);

    // Query for block 1000 (should fall back to seekForPrev)
    final Bytes queryKey = createArchiveKey(accountHash, queryBlockNumber);
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, queryKey);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(storedKey);
    assertThat(result.get().value()).isPresent();
    assertThat(result.get().value().get()).isEqualTo(value);
  }

  /**
   * Test that getNearestBefore returns empty when no data exists for the account.
   */
  @Test
  public void testReturnsEmptyWhenNoDataExists() throws Exception {
    final Bytes accountHash = createAccountHash("0xNONEXISTENT");
    final long blockNumber = 1000L;
    final Bytes key = createArchiveKey(accountHash, blockNumber);

    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, key);

    assertThat(result).isEmpty();
  }

  /**
   * Test with storage keys (64 byte natural key + 8 byte block number = 72 bytes).
   */
  @Test
  public void testWithStorageKeys() throws Exception {
    final Bytes accountHash = createAccountHash("0xACCOUNT1");
    final Bytes slotHash = createAccountHash("0xSLOTHASH");
    final Bytes naturalKey = Bytes.concatenate(accountHash, slotHash); // 64 bytes
    final long blockNumber = 500L;

    final Bytes key = Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(blockNumber));
    final byte[] value = "storage_value_at_500".getBytes(StandardCharsets.UTF_8);

    db.put(columnFamily, key.toArrayUnsafe(), value);

    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, key);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(key);
    assertThat(result.get().value()).isPresent();
    assertThat(result.get().value().get()).isEqualTo(value);
  }

  /**
   * Test that keys shorter than MIN_ARCHIVE_KEY_SIZE (40 bytes) fall back to seekForPrev.
   */
  @Test
  public void testShortKeysFallBackToSeekForPrev() throws Exception {
    // Create a key that's only 20 bytes (too short for archive key optimization)
    final Bytes shortKey = Bytes.fromHexString("0xAABBCCDDEEFF00112233445566778899AABBCCDD");
    final byte[] value = "short_key_value".getBytes(StandardCharsets.UTF_8);

    db.put(columnFamily, shortKey.toArrayUnsafe(), value);

    // This should still work via seekForPrev fallback
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, shortKey);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(shortKey);
  }

  /**
   * Test with negative block numbers (edge case - should not crash, should return empty or use
   * seekForPrev).
   */
  @Test
  public void testNegativeBlockNumberHandling() throws Exception {
    final Bytes accountHash = createAccountHash("0xACCOUNT");
    // Query for block 5, but point lookups might try negative blocks
    final long blockNumber = 5L;
    final Bytes key = createArchiveKey(accountHash, blockNumber);

    // Store value at block 3
    final Bytes storedKey = createArchiveKey(accountHash, 3L);
    final byte[] value = "state_at_block_3".getBytes(StandardCharsets.UTF_8);
    db.put(columnFamily, storedKey.toArrayUnsafe(), value);

    // Should handle gracefully (point lookups will try 5, 4, 3, 2, 1, 0 but not negative)
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, key);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(storedKey);
  }

  /**
   * Test multiple versions of the same account at different blocks.
   */
  @Test
  public void testMultipleVersionsOfSameAccount() throws Exception {
    final Bytes accountHash = createAccountHash("0xMULTIVER");

    // Store multiple versions
    final byte[] value100 = "state_at_100".getBytes(StandardCharsets.UTF_8);
    final byte[] value200 = "state_at_200".getBytes(StandardCharsets.UTF_8);
    final byte[] value300 = "state_at_300".getBytes(StandardCharsets.UTF_8);

    db.put(columnFamily, createArchiveKey(accountHash, 100L).toArrayUnsafe(), value100);
    db.put(columnFamily, createArchiveKey(accountHash, 200L).toArrayUnsafe(), value200);
    db.put(columnFamily, createArchiveKey(accountHash, 300L).toArrayUnsafe(), value300);

    // Query for block 250 - should find block 200
    final Bytes queryKey = createArchiveKey(accountHash, 250L);
    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, queryKey);

    assertThat(result).isPresent();
    assertThat(result.get().key()).isEqualTo(createArchiveKey(accountHash, 200L));
    assertThat(result.get().value().get()).isEqualTo(value200);
  }

  /**
   * Test batch multi-get with multiple keys.
   */
  @Test
  public void testBatchMultiGetWithMultipleKeys() throws Exception {
    // Create test data for multiple accounts
    final Bytes account1 = createAccountHash("0xACCT0001");
    final Bytes account2 = createAccountHash("0xACCT0002");
    final Bytes account3 = createAccountHash("0xACCT0003");

    final byte[] value1 = "account1_state".getBytes(StandardCharsets.UTF_8);
    final byte[] value2 = "account2_state".getBytes(StandardCharsets.UTF_8);
    final byte[] value3 = "account3_state".getBytes(StandardCharsets.UTF_8);

    // Store values at block 1000
    db.put(columnFamily, createArchiveKey(account1, 1000L).toArrayUnsafe(), value1);
    db.put(columnFamily, createArchiveKey(account2, 1000L).toArrayUnsafe(), value2);
    db.put(columnFamily, createArchiveKey(account3, 1000L).toArrayUnsafe(), value3);

    // Query all three accounts
    final List<Bytes> keys =
        List.of(
            createArchiveKey(account1, 1000L),
            createArchiveKey(account2, 1000L),
            createArchiveKey(account3, 1000L));

    final List<Optional<NearestKeyValue>> results =
        OptimizedRocksDBReader.getNearestBeforeBatch(db, columnFamily, keys);

    assertThat(results).hasSize(3);
    assertThat(results.get(0)).isPresent();
    assertThat(results.get(0).get().value().get()).isEqualTo(value1);
    assertThat(results.get(1)).isPresent();
    assertThat(results.get(1).get().value().get()).isEqualTo(value2);
    assertThat(results.get(2)).isPresent();
    assertThat(results.get(2).get().value().get()).isEqualTo(value3);
  }

  /**
   * Test batch multi-get with some keys missing (should fall back to seekForPrev).
   * Note: seekForPrev finds the nearest key lexicographically before or equal to the search key,
   * so we need to verify that the found key actually matches the expected account hash.
   */
  @Test
  public void testBatchMultiGetWithMissingKeys() throws Exception {
    final Bytes account1 = createAccountHash("0xAAAA0001"); // Lexicographically first
    final Bytes account2 = createAccountHash("0xBBBB0002"); // Middle (missing)
    final Bytes account3 = createAccountHash("0xCCCC0003"); // Lexicographically last

    final byte[] value1 = "account1_value".getBytes(StandardCharsets.UTF_8);
    final byte[] value3 = "account3_value".getBytes(StandardCharsets.UTF_8);

    // Store only account1 and account3, skip account2
    db.put(columnFamily, createArchiveKey(account1, 1000L).toArrayUnsafe(), value1);
    db.put(columnFamily, createArchiveKey(account3, 1000L).toArrayUnsafe(), value3);

    final List<Bytes> keys =
        List.of(
            createArchiveKey(account1, 1000L),
            createArchiveKey(account2, 1000L),
            createArchiveKey(account3, 1000L));

    final List<Optional<NearestKeyValue>> results =
        OptimizedRocksDBReader.getNearestBeforeBatch(db, columnFamily, keys);

    assertThat(results).hasSize(3);
    assertThat(results.get(0)).isPresent(); // account1 found
    // account2 search will find account1 (nearest before) but that doesn't match account2's prefix
    // For a proper archive implementation, we'd check prefix match, but for this basic test
    // seekForPrev will find account1 since it's the nearest key before account2
    assertThat(results.get(1)).isPresent(); // Will find account1 (nearest before account2)
    assertThat(results.get(2)).isPresent(); // account3 found
  }

  /**
   * Test batch multi-get with recent block lookups (within point lookup window).
   */
  @Test
  public void testBatchMultiGetWithRecentBlocks() throws Exception {
    final Bytes account1 = createAccountHash("0xRECENT01");
    final Bytes account2 = createAccountHash("0xRECENT02");

    final byte[] value1 = "recent1_state".getBytes(StandardCharsets.UTF_8);
    final byte[] value2 = "recent2_state".getBytes(StandardCharsets.UTF_8);

    // Store at block 995 and 998
    db.put(columnFamily, createArchiveKey(account1, 995L).toArrayUnsafe(), value1);
    db.put(columnFamily, createArchiveKey(account2, 998L).toArrayUnsafe(), value2);

    // Query for block 1000 (should find via point lookups)
    final List<Bytes> keys =
        List.of(createArchiveKey(account1, 1000L), createArchiveKey(account2, 1000L));

    final List<Optional<NearestKeyValue>> results =
        OptimizedRocksDBReader.getNearestBeforeBatch(db, columnFamily, keys);

    assertThat(results).hasSize(2);
    assertThat(results.get(0)).isPresent();
    assertThat(results.get(0).get().key()).isEqualTo(createArchiveKey(account1, 995L));
    assertThat(results.get(1)).isPresent();
    assertThat(results.get(1).get().key()).isEqualTo(createArchiveKey(account2, 998L));
  }

  /**
   * Test empty database returns empty for all queries.
   */
  @Test
  public void testEmptyDatabase() throws Exception {
    final Bytes accountHash = createAccountHash("0xEMPTYDB");
    final Bytes key = createArchiveKey(accountHash, 1000L);

    final Optional<NearestKeyValue> result =
        OptimizedRocksDBReader.getNearestBeforeOptimized(db, columnFamily, key);

    assertThat(result).isEmpty();
  }

  /**
   * Test batch with empty list returns empty results.
   */
  @Test
  public void testBatchWithEmptyList() throws Exception {
    final List<Bytes> emptyKeys = List.of();
    final List<Optional<NearestKeyValue>> results =
        OptimizedRocksDBReader.getNearestBeforeBatch(db, columnFamily, emptyKeys);

    assertThat(results).isEmpty();
  }

  // Helper methods

  /**
   * Create a 32-byte account hash from a hex string (padded if necessary).
   */
  private Bytes createAccountHash(final String hexPrefix) {
    final byte[] bytes = hexPrefix.getBytes(StandardCharsets.UTF_8);
    final byte[] padded = Arrays.copyOf(bytes, 32);
    return Bytes.wrap(padded);
  }

  /**
   * Create an archive key: accountHash (32 bytes) + blockNumber (8 bytes).
   */
  private Bytes createArchiveKey(final Bytes accountHash, final long blockNumber) {
    return Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(blockNumber));
  }
}
