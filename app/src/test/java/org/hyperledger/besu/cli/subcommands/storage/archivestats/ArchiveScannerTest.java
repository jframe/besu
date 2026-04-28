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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

class ArchiveScannerTest {

  @TempDir Path dbPath;
  private RocksDB db;
  private final List<ColumnFamilyHandle> handles = new ArrayList<>();

  @BeforeEach
  void openWriter() throws RocksDBException {
    RocksDB.loadLibrary();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_INFO_STATE_ARCHIVE".getBytes(StandardCharsets.UTF_8),
                new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_STORAGE_ARCHIVE".getBytes(StandardCharsets.UTF_8),
                new ColumnFamilyOptions()));
    final DBOptions opts =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    db = RocksDB.open(opts, dbPath.toString(), cfDescriptors, handles);
  }

  @AfterEach
  void close() {
    handles.forEach(ColumnFamilyHandle::close);
    if (db != null) db.close();
  }

  @Test
  void iteratesAccountCfInLexOrder() throws RocksDBException, IOException {
    // CF 1 is ACCOUNT_INFO_STATE_ARCHIVE per the open above.
    final ColumnFamilyHandle accountCf = handles.get(1);
    db.put(accountCf, accountKey(0xaa, 100L), new byte[] {1});
    db.put(accountCf, accountKey(0xaa, 200L), new byte[] {1});
    db.put(accountCf, accountKey(0xbb, 50L), new byte[] {1});
    db.flush(new org.rocksdb.FlushOptions());
    handles.forEach(ColumnFamilyHandle::close);
    handles.clear();
    db.close();
    db = null;

    final List<long[]> seen = new ArrayList<>();
    try (final ArchiveScanner scanner = ArchiveScanner.openReadOnly(dbPath.toString())) {
      scanner.forEach(
          ArchiveCf.ACCOUNT,
          Long.MAX_VALUE,
          (cf, rawKey, valueLen) -> {
            final KeyDecoder.Decoded d = KeyDecoder.decode(cf, rawKey);
            seen.add(new long[] {d.prefix()[0] & 0xff, d.blockNumber()});
          });
    }

    assertThat(seen).hasSize(3);
    assertThat(seen.get(0)).containsExactly(0xaa, 100L);
    assertThat(seen.get(1)).containsExactly(0xaa, 200L);
    assertThat(seen.get(2)).containsExactly(0xbb, 50L);
  }

  @Test
  void respectsMaxKeysLimit() throws RocksDBException, IOException {
    final ColumnFamilyHandle accountCf = handles.get(1);
    for (int i = 0; i < 10; i++) {
      db.put(accountCf, accountKey(i, 1L), new byte[] {1});
    }
    db.flush(new org.rocksdb.FlushOptions());
    handles.forEach(ColumnFamilyHandle::close);
    handles.clear();
    db.close();
    db = null;

    final int[] count = {0};
    try (final ArchiveScanner scanner = ArchiveScanner.openReadOnly(dbPath.toString())) {
      scanner.forEach(ArchiveCf.ACCOUNT, 4L, (cf, rawKey, valueLen) -> count[0]++);
    }
    assertThat(count[0]).isEqualTo(4);
  }

  private static byte[] accountKey(final int firstByte, final long blockNumber) {
    final byte[] key = new byte[40];
    Arrays.fill(key, 0, 32, (byte) firstByte);
    ByteBuffer.wrap(key, 32, 8).putLong(blockNumber);
    return key;
  }
}
