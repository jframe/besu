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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Opens a RocksDB instance read-only and iterates the archive column families in lex order.
 *
 * <p>Single point of contact between this tool and RocksDB. Closing the scanner closes the
 * underlying database and all column-family handles.
 */
public final class ArchiveScanner implements AutoCloseable {

  /** Per-key callback. */
  @FunctionalInterface
  public interface KeyVisitor {
    /**
     * Visit one key.
     *
     * @param cf which CF the key came from
     * @param rawKey the raw key bytes (caller must not retain across calls)
     * @param value the raw value bytes (caller must not retain across calls)
     */
    void visit(ArchiveCf cf, byte[] rawKey, byte[] value);
  }

  private final RocksDB db;
  private final Map<ArchiveCf, ColumnFamilyHandle> cfByArchive;
  private final List<ColumnFamilyHandle> allHandles;

  /**
   * Open the database read-only, locating the archive CFs.
   *
   * @param dbPath filesystem path to the RocksDB instance
   * @return an open scanner; close it via {@link #close()} or try-with-resources
   * @throws RocksDBException if the DB cannot be opened or the archive CFs are missing
   */
  public static ArchiveScanner openReadOnly(final String dbPath) throws RocksDBException {
    RocksDB.loadLibrary();
    final List<byte[]> cfNames = RocksDB.listColumnFamilies(new Options(), dbPath);
    final Map<String, byte[]> available = new HashMap<>();
    for (final byte[] n : cfNames) {
      available.put(new String(n, StandardCharsets.UTF_8), n);
    }

    final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    final Map<ArchiveCf, Integer> indexByArchive = new HashMap<>();
    for (final ArchiveCf cf : ArchiveCf.values()) {
      final byte[] name = available.get(cf.segment().getName());
      if (name == null) {
        throw new RocksDBException(
            "Archive CF missing: " + cf.segment().getName() + ". Is this a Bonsai-archive node?");
      }
      indexByArchive.put(cf, descriptors.size());
      descriptors.add(new ColumnFamilyDescriptor(name));
    }

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final RocksDB db = RocksDB.openReadOnly(dbPath, descriptors, handles);
    final Map<ArchiveCf, ColumnFamilyHandle> cfMap = new HashMap<>();
    for (final var entry : indexByArchive.entrySet()) {
      cfMap.put(entry.getKey(), handles.get(entry.getValue()));
    }
    return new ArchiveScanner(db, cfMap, handles);
  }

  ArchiveScanner(
      final RocksDB db,
      final Map<ArchiveCf, ColumnFamilyHandle> cfByArchive,
      final List<ColumnFamilyHandle> allHandles) {
    this.db = db;
    this.cfByArchive = cfByArchive;
    this.allHandles = allHandles;
  }

  /**
   * Iterate {@code cf} in lex order, invoking {@code visitor} on each key, up to {@code maxKeys}.
   *
   * @param cf archive CF to iterate
   * @param maxKeys upper bound on keys visited; pass {@link Long#MAX_VALUE} for no limit.
   * @param visitor per-key callback
   */
  public void forEach(final ArchiveCf cf, final long maxKeys, final KeyVisitor visitor) {
    final ColumnFamilyHandle handle = cfByArchive.get(cf);
    long visited = 0L;
    try (final ReadOptions ro = new ReadOptions().setVerifyChecksums(false);
        final RocksIterator it = db.newIterator(handle, ro)) {
      it.seekToFirst();
      while (it.isValid() && visited < maxKeys) {
        final byte[] key = it.key();
        final byte[] value = it.value();
        visitor.visit(cf, key, value);
        visited++;
        it.next();
      }
    }
  }

  /**
   * Look up estimated key count for a CF.
   *
   * @param cf archive CF
   * @return estimated key count, or 0 if unavailable
   */
  public long estimateKeyCount(final ArchiveCf cf) {
    try {
      final String prop = db.getProperty(cfByArchive.get(cf), "rocksdb.estimate-num-keys");
      return prop == null || prop.isBlank() ? 0L : Long.parseLong(prop);
    } catch (final RocksDBException | NumberFormatException e) {
      return 0L;
    }
  }

  /**
   * Estimated total file size for a CF.
   *
   * @param cf archive CF
   * @return estimated total file size (SST + blob) in bytes; 0 if unavailable
   */
  public long estimateCfSizeBytes(final ArchiveCf cf) {
    try {
      final long sst =
          parseLongOrZero(db.getProperty(cfByArchive.get(cf), "rocksdb.total-sst-files-size"));
      final long blob =
          parseLongOrZero(db.getProperty(cfByArchive.get(cf), "rocksdb.total-blob-file-size"));
      return sst + blob;
    } catch (final RocksDBException e) {
      return 0L;
    }
  }

  private static long parseLongOrZero(final String s) {
    if (s == null || s.isBlank()) {
      return 0L;
    }
    try {
      return Long.parseLong(s);
    } catch (final NumberFormatException e) {
      return 0L;
    }
  }

  @Override
  public void close() {
    for (final ColumnFamilyHandle h : allHandles) {
      h.close();
    }
    db.close();
  }
}
