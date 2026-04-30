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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

class ArchiveStatsRunnerEndToEndTest {

  @TempDir Path dbPath;
  @TempDir Path outputPath;

  @BeforeEach
  void buildFixtureArchive() throws RocksDBException {
    RocksDB.loadLibrary();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_INFO_STATE_ARCHIVE".getBytes(UTF_8), new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_STORAGE_ARCHIVE".getBytes(UTF_8), new ColumnFamilyOptions()));
    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final DBOptions opts =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    try (final RocksDB db = RocksDB.open(opts, dbPath.toString(), cfDescriptors, handles)) {
      final ColumnFamilyHandle accountCf = handles.get(1);
      // account 0xaa: 5 mods spread across 2 ranges (3 in range 0, 2 in range 1).
      // account 0xbb: 1 mod in range 0.
      db.put(accountCf, accountKey(0xaa, 100L), new byte[] {1});
      db.put(accountCf, accountKey(0xaa, 200L), new byte[] {1});
      db.put(accountCf, accountKey(0xaa, 300L), new byte[] {1});
      db.put(accountCf, accountKey(0xaa, 1_500_000L), new byte[] {1});
      db.put(accountCf, accountKey(0xaa, 1_500_001L), new byte[] {1});
      db.put(accountCf, accountKey(0xbb, 5L), new byte[] {1});

      final ColumnFamilyHandle storageCf = handles.get(2);
      // Storage layout used by the slot-fan-out test:
      // account 0xaa: slots 0x01, 0x02 in range 0 (slot 0x01 also in range 1).
      // account 0xbb: slot 0x10 in range 0.
      // -> expected slot-fan-out observations: {2 (aa,0), 1 (aa,1), 1 (bb,0)} -> 3 pairs.
      db.put(storageCf, storageKey(0xaa, 0x01, 100L), new byte[] {1});
      db.put(storageCf, storageKey(0xaa, 0x01, 1_000_500L), new byte[] {1});
      db.put(storageCf, storageKey(0xaa, 0x02, 200L), new byte[] {1});
      db.put(storageCf, storageKey(0xbb, 0x10, 5L), new byte[] {1});

      db.flush(new org.rocksdb.FlushOptions());
    } finally {
      handles.forEach(ColumnFamilyHandle::close);
    }
  }

  private void appendMalformedKey() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_INFO_STATE_ARCHIVE".getBytes(UTF_8), new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor(
                "ACCOUNT_STORAGE_ARCHIVE".getBytes(UTF_8), new ColumnFamilyOptions()));
    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (final RocksDB db =
        RocksDB.open(new DBOptions(), dbPath.toString(), cfDescriptors, handles)) {
      final ColumnFamilyHandle accountCf = handles.get(1);
      // Mirrors the BonsaiFlatDbToArchiveMigrator's metadata sentinel.
      db.put(
          accountCf,
          "ARCHIVE_MIGRATION_PROGRESS".getBytes(UTF_8),
          new byte[] {(byte) 0xde, (byte) 0xad});
      db.flush(new org.rocksdb.FlushOptions());
    } finally {
      handles.forEach(ColumnFamilyHandle::close);
    }
  }

  @Test
  void runProducesExpectedHistogramTotals() throws IOException, RocksDBException {
    final ArchiveStatsRunner.Config cfg =
        new ArchiveStatsRunner.Config(
            dbPath.toString(),
            outputPath,
            1_000_000L,
            List.of(new FpRateProjector.GridPoint(7, 1_048_576L)),
            List.of(3L, 50L, 10_000L, 1_000_000L),
            List.of(1L, 10L, 1_000L, 100_000L),
            List.of(ArchiveCf.ACCOUNT),
            Long.MAX_VALUE,
            Duration.ofSeconds(60),
            1024L);
    final StringWriter logSink = new StringWriter();
    final ArchiveStatsRunner runner = new ArchiveStatsRunner(cfg, new PrintWriter(logSink));

    final ScanResult result = runner.run();

    final CfResult acc = result.cfResults().get(ArchiveCf.ACCOUNT);
    assertThat(acc.totalEntries()).isEqualTo(6L);
    assertThat(acc.totalUniqueKeys()).isEqualTo(2L);
    // account 0xaa contributes 2 rows (range 0 and range 1); account 0xbb contributes 1 row.
    assertThat(acc.totalRows()).isEqualTo(3L);
    assertThat(result.chainHead()).isEqualTo(1_500_001L);

    new ReportWriter(outputPath).write(result);
    assertThat(Files.exists(outputPath.resolve("stats.json"))).isTrue();
  }

  @Test
  void skipsMalformedKeysAndContinues() throws IOException, RocksDBException {
    appendMalformedKey();

    final ArchiveStatsRunner.Config cfg =
        new ArchiveStatsRunner.Config(
            dbPath.toString(),
            outputPath,
            1_000_000L,
            List.of(new FpRateProjector.GridPoint(7, 1_048_576L)),
            List.of(3L, 50L, 10_000L, 1_000_000L),
            List.of(1L, 10L, 1_000L, 100_000L),
            List.of(ArchiveCf.ACCOUNT),
            Long.MAX_VALUE,
            Duration.ofSeconds(60),
            1024L);
    final StringWriter logSink = new StringWriter();
    final ArchiveStatsRunner runner = new ArchiveStatsRunner(cfg, new PrintWriter(logSink));

    final ScanResult result = runner.run();

    final CfResult acc = result.cfResults().get(ArchiveCf.ACCOUNT);
    // The 6 valid keys are still counted; the malformed sentinel is skipped.
    assertThat(acc.totalEntries()).isEqualTo(6L);
    assertThat(acc.totalUniqueKeys()).isEqualTo(2L);

    final String logged = logSink.toString();
    assertThat(logged).contains("WARN: skipping unrecognised key");
    assertThat(logged).contains("expected 40 bytes, got 26");
    // Hex of "ARCHIVE_MIGRATION_PROGRESS"
    assertThat(logged).contains("0x415243484956455f4d4947524154494f4e5f50524f4752455353");
    // First two value bytes (truncated representation includes them in hex)
    assertThat(logged).contains("0xdead");
    assertThat(logged).contains("skipped 1 unrecognised key(s)");
  }

  @Test
  void runnerEmitsStorageSlotFanOutResult() throws IOException, RocksDBException {
    final ArchiveStatsRunner.Config cfg =
        new ArchiveStatsRunner.Config(
            dbPath.toString(),
            outputPath,
            1_000_000L,
            List.of(new FpRateProjector.GridPoint(7, 1_048_576L)),
            List.of(3L, 50L, 10_000L, 1_000_000L),
            List.of(1L, 10L, 1_000L, 100_000L),
            List.of(ArchiveCf.STORAGE),
            Long.MAX_VALUE,
            Duration.ofSeconds(60),
            1024L);
    final StringWriter logSink = new StringWriter();
    final ArchiveStatsRunner runner = new ArchiveStatsRunner(cfg, new PrintWriter(logSink));

    final ScanResult result = runner.run();

    final SlotFanOutResult sfo = result.slotFanOutResults().get(ArchiveCf.STORAGE);
    assertThat(sfo).as("storage slot-fan-out result").isNotNull();
    assertThat(sfo.totalAccountRangePairs()).isEqualTo(3L);
    assertThat(sfo.histogram().max()).isEqualTo(2L);
    assertThat(sfo.histogram().total()).isEqualTo(3L);
  }

  private static byte[] accountKey(final int firstByte, final long blockNumber) {
    final byte[] key = new byte[40];
    Arrays.fill(key, 0, 32, (byte) firstByte);
    ByteBuffer.wrap(key, 32, 8).putLong(blockNumber);
    return key;
  }

  private static byte[] storageKey(
      final int accountByte, final int slotByte, final long blockNumber) {
    final byte[] key = new byte[72];
    Arrays.fill(key, 0, 32, (byte) accountByte);
    Arrays.fill(key, 32, 64, (byte) slotByte);
    ByteBuffer.wrap(key, 64, 8).putLong(blockNumber);
    return key;
  }
}
