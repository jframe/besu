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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.hyperledger.besu.controller.BesuController.DATABASE_PATH;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryComposition;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryComposition.Bucket;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryComposition.Category;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Scans the {@code TRIE_NODE_HISTORY_ARCHIVE} column family read-only and reports its composition:
 * how many entries (and logical bytes) are FULL creations, FULL checkpoints, always-FULL upper-trie
 * nodes, or small DIFFs, and how the blob-file storage is attributed across those categories.
 *
 * <p>The node must be stopped so the diagnostic can take the RocksDB read lock. No migration re-run
 * is required — the breakdown is derived entirely from the on-disk entries.
 */
@Command(
    name = "x-trie-node-history-stats",
    description = "Analyze the composition of the archive TRIE_NODE_HISTORY_ARCHIVE column family",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class TrieNodeHistoryStatsSubCommand implements Runnable {

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Option(
      names = {"--min-blob-size"},
      description =
          "RocksDB min_blob_size threshold in bytes; values this size or larger are stored in blob"
              + " files (default: ${DEFAULT-VALUE})")
  private int minBlobSize = 100;

  @Option(
      names = {"--full-above-depth"},
      description =
          "FULL_ABOVE_DEPTH threshold in location bytes; nodes this shallow are always FULL"
              + " (default: ${DEFAULT-VALUE})")
  private int fullAboveDepth = 2;

  @Option(
      names = {"--max-entries"},
      description =
          "Stop after scanning this many entries (0 = scan the whole column family, default:"
              + " ${DEFAULT-VALUE}). Use a non-zero value for a quick smoke test.")
  private long maxEntries = 0;

  @Option(
      names = {"--log-interval-seconds"},
      description =
          "Print a progress line (with percentage complete against the CF's estimated key count)"
              + " roughly every N seconds; 0 disables progress (default: ${DEFAULT-VALUE})")
  private long logIntervalSeconds = 60;

  /** Default constructor. */
  public TrieNodeHistoryStatsSubCommand() {}

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    final String dbPath = parentCommand.besuCommand.dataDir().resolve(DATABASE_PATH).toString();

    final byte[] targetCfId = KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE.getId();
    final TrieNodeHistoryComposition comp =
        new TrieNodeHistoryComposition(minBlobSize, fullAboveDepth);
    final long[] onDisk = new long[2]; // [0] = total-sst-files-size, [1] = total-blob-file-size
    final boolean[] truncated = new boolean[1];

    out.println(
        "Scanning TRIE_NODE_HISTORY_ARCHIVE (min_blob_size="
            + minBlobSize
            + ", full_above_depth="
            + fullAboveDepth
            + ")...");
    out.flush();

    RocksDbHelper.forFilteredColumnFamily(
        dbPath,
        (rocksdb, cfHandle) -> {
          try {
            if (!Arrays.equals(cfHandle.getName(), targetCfId)) {
              return; // skip the default column family that forFilteredColumnFamily also opens
            }
            onDisk[0] = readSizeProperty(rocksdb, cfHandle, "rocksdb.total-sst-files-size");
            onDisk[1] = readSizeProperty(rocksdb, cfHandle, "rocksdb.total-blob-file-size");
            final long estTotal = readSizeProperty(rocksdb, cfHandle, "rocksdb.estimate-num-keys");
            truncated[0] = scan(rocksdb, cfHandle, comp, estTotal, out);
          } catch (final RocksDBException e) {
            throw new RuntimeException(e);
          }
        },
        List.of(KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE.getName()));

    printReport(out, comp, onDisk[0], onDisk[1], truncated[0]);
  }

  /**
   * Iterates the column family, feeding every entry to the accumulator.
   *
   * @param estTotal RocksDB's estimated key count for the CF, used for the progress percentage (0
   *     if unavailable)
   * @return {@code true} if the scan stopped early because of {@code --max-entries}
   */
  private boolean scan(
      final RocksDB rocksdb,
      final ColumnFamilyHandle cfHandle,
      final TrieNodeHistoryComposition comp,
      final long estTotal,
      final PrintWriter out) {
    final long intervalNanos = logIntervalSeconds * 1_000_000_000L;
    final ReadOptions readOptions = new ReadOptions();
    readOptions.setReadaheadSize(4L * 1024 * 1024).setVerifyChecksums(false).setFillCache(false);
    try (readOptions;
        final RocksIterator it = rocksdb.newIterator(cfHandle, readOptions)) {
      final long startNanos = System.nanoTime();
      long lastLogNanos = startNanos;
      long scanned = 0;
      for (it.seekToFirst(); it.isValid(); it.next()) {
        comp.record(it.key(), it.value());
        scanned++;
        // Check the wall clock only every 65536 entries to keep the hot loop cheap.
        if (intervalNanos > 0 && (scanned & 0xFFFF) == 0) {
          final long now = System.nanoTime();
          if (now - lastLogNanos >= intervalNanos) {
            logProgress(out, scanned, estTotal, startNanos, now);
            lastLogNanos = now;
          }
        }
        if (maxEntries > 0 && scanned >= maxEntries) {
          out.println("  reached --max-entries=" + maxEntries + ", stopping scan early");
          return true;
        }
      }
    }
    return false;
  }

  private static void logProgress(
      final PrintWriter out,
      final long scanned,
      final long estTotal,
      final long startNanos,
      final long nowNanos) {
    final double elapsedSec = (nowNanos - startNanos) / 1e9;
    final double rate = elapsedSec > 0 ? scanned / elapsedSec : 0;
    final StringBuilder sb =
        new StringBuilder("  progress: ").append(String.format("%,d", scanned));
    if (estTotal > 0) {
      sb.append(" / ~")
          .append(String.format("%,d", estTotal))
          .append(String.format(" (%.1f%%)", 100.0 * scanned / estTotal));
      if (rate > 0 && estTotal > scanned) {
        sb.append(", ETA ").append(formatDuration((long) ((estTotal - scanned) / rate)));
      }
    }
    sb.append(String.format(", %,.0f entries/s", rate));
    sb.append(", elapsed ").append(formatDuration((long) elapsedSec));
    out.println(sb);
    out.flush();
  }

  private static String formatDuration(final long totalSeconds) {
    final long h = totalSeconds / 3600;
    final long m = (totalSeconds % 3600) / 60;
    final long s = totalSeconds % 60;
    if (h > 0) {
      return String.format("%dh%02dm%02ds", h, m, s);
    }
    if (m > 0) {
      return String.format("%dm%02ds", m, s);
    }
    return s + "s";
  }

  private static long readSizeProperty(
      final RocksDB rocksdb, final ColumnFamilyHandle cfHandle, final String property)
      throws RocksDBException {
    final String value = rocksdb.getProperty(cfHandle, property);
    return value == null || value.isBlank() ? 0 : Long.parseLong(value);
  }

  private void printReport(
      final PrintWriter out,
      final TrieNodeHistoryComposition comp,
      final long onDiskSst,
      final long onDiskBlob,
      final boolean truncated) {
    final long totalEntries = comp.totalEntries();
    if (totalEntries == 0) {
      out.println("No entries found in TRIE_NODE_HISTORY_ARCHIVE.");
      return;
    }

    long logicalKeyBytes = 0;
    long logicalValueBytes = 0;
    long logicalBlobValueBytes = 0;
    for (final Category c : Category.values()) {
      final Bucket b = comp.bucket(c);
      logicalKeyBytes += b.keyBytes();
      logicalValueBytes += b.valueBytes();
      logicalBlobValueBytes += b.blobValueBytes();
    }
    final long logicalInlineValueBytes = logicalValueBytes - logicalBlobValueBytes;
    final long totalBlobLogical = logicalBlobValueBytes;

    out.println();
    out.println("=== TRIE_NODE_HISTORY_ARCHIVE composition ===");
    out.println("Entries scanned:        " + totalEntries);
    out.println();
    out.println("On-disk (RocksDB properties, whole column family):");
    out.println("  SST files:            " + RocksDbHelper.formatOutputSize(onDiskSst));
    out.println("  Blob files:           " + RocksDbHelper.formatOutputSize(onDiskBlob));
    out.println(
        "  Total:                " + RocksDbHelper.formatOutputSize(onDiskSst + onDiskBlob));
    out.println();
    out.println("Logical (uncompressed) bytes over scanned entries:");
    out.println("  Keys:                 " + RocksDbHelper.formatOutputSize(logicalKeyBytes));
    out.println("  Values (all):         " + RocksDbHelper.formatOutputSize(logicalValueBytes));
    out.println("  Values blob-eligible: " + RocksDbHelper.formatOutputSize(logicalBlobValueBytes));
    out.println(
        "  Values inline:        " + RocksDbHelper.formatOutputSize(logicalInlineValueBytes));

    if (truncated) {
      out.println();
      out.println(
          "WARNING: scan was truncated by --max-entries. The iterator walks keys in sorted"
              + " order (account-trie nodes first), so this prefix sample is NOT representative"
              + " of the whole CF. The ~OnDiskBlob column below is a proportional estimate that is"
              + " only meaningful for a FULL scan (omit --max-entries).");
    }

    // The on-disk blob total is real; attribute it across categories in proportion to each
    // category's share of logical blob bytes (assumes roughly uniform blob compression). This is
    // ratio-free, so it stays sane whether the scan was full or partial.
    out.println();
    out.format(
        "%-22s %14s %8s %14s %14s %14s%n",
        "Category", "Count", "%Entries", "LogicalValue", "BlobLogical", "~OnDiskBlob");
    out.println(
        "----------------------------------------------------------------------------------------------");
    for (final Category c : Category.values()) {
      final Bucket b = comp.bucket(c);
      if (b.count() == 0) {
        continue;
      }
      out.format(
          "%-22s %14d %7.2f%% %14s %14s %14s%n",
          c.name(),
          b.count(),
          100.0 * b.count() / totalEntries,
          RocksDbHelper.formatOutputSize(b.valueBytes()),
          RocksDbHelper.formatOutputSize(b.blobValueBytes()),
          RocksDbHelper.formatOutputSize(
              estOnDiskBlob(b.blobValueBytes(), totalBlobLogical, onDiskBlob)));
    }

    // Blob attribution: which lever attacks the blob files (the dominant cost).
    final long creationBlob =
        comp.bucket(Category.CREATION_BRANCH).blobValueBytes()
            + comp.bucket(Category.CREATION_SHORT).blobValueBytes()
            + comp.bucket(Category.CREATION_UNKNOWN).blobValueBytes();
    final long checkpointBlob =
        comp.bucket(Category.CHECKPOINT_BRANCH).blobValueBytes()
            + comp.bucket(Category.CHECKPOINT_SHORT).blobValueBytes()
            + comp.bucket(Category.CHECKPOINT_UNKNOWN).blobValueBytes();
    final long upperTrieBlob =
        comp.bucket(Category.UPPER_TRIE_BRANCH).blobValueBytes()
            + comp.bucket(Category.UPPER_TRIE_SHORT).blobValueBytes()
            + comp.bucket(Category.UPPER_TRIE_UNKNOWN).blobValueBytes();
    final long diffBlob =
        comp.bucket(Category.DIFF_BRANCH).blobValueBytes()
            + comp.bucket(Category.DIFF_SHORT).blobValueBytes();

    out.println();
    out.println("Estimated on-disk blob attribution (proportional share of blob files):");
    out.println(
        "  Creations (unavoidable):                 "
            + RocksDbHelper.formatOutputSize(
                estOnDiskBlob(creationBlob, totalBlobLogical, onDiskBlob)));
    out.println(
        "  Checkpoints (CHECKPOINT_INTERVAL lever):  "
            + RocksDbHelper.formatOutputSize(
                estOnDiskBlob(checkpointBlob, totalBlobLogical, onDiskBlob)));
    out.println(
        "  Upper-trie (FULL_ABOVE_DEPTH lever):      "
            + RocksDbHelper.formatOutputSize(
                estOnDiskBlob(upperTrieBlob, totalBlobLogical, onDiskBlob)));
    out.println(
        "  Diffs spilled to blob:                    "
            + RocksDbHelper.formatOutputSize(
                estOnDiskBlob(diffBlob, totalBlobLogical, onDiskBlob)));

    printDepthHistogram(out, comp);
  }

  /**
   * Allocates a share of the (real, whole-CF) on-disk blob size to a category in proportion to its
   * logical blob bytes.
   *
   * @param categoryBlobLogical logical blob bytes attributed to the category
   * @param totalBlobLogical logical blob bytes across all scanned entries
   * @param onDiskBlob measured total blob-file size for the column family
   * @return the estimated on-disk blob bytes for the category
   */
  private static long estOnDiskBlob(
      final long categoryBlobLogical, final long totalBlobLogical, final long onDiskBlob) {
    if (totalBlobLogical <= 0) {
      return 0;
    }
    return Math.round((double) onDiskBlob * categoryBlobLogical / totalBlobLogical);
  }

  private void printDepthHistogram(final PrintWriter out, final TrieNodeHistoryComposition comp) {
    final long[] hist = comp.locationDepthHistogram();
    out.println();
    out.println(
        "Location-depth histogram (bytes -> entry count; storage nodes subtract the 32B hash):");
    for (int depth = 0; depth < hist.length; depth++) {
      if (hist[depth] > 0) {
        out.format("  depth %2d: %d%n", depth, hist[depth]);
      }
    }
    out.flush();
  }
}
