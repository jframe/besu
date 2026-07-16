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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.CalibrationResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ChangeCountResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.EntrySizeTable;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.FlatDbStorageLeafCountProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistorySizeEstimate;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.StorageTrieLeafCountProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieLogChangeCounter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieShapeModel;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Estimates the on-disk size of the {@code TRIE_NODE_HISTORY_ARCHIVE} column family <em>before</em>
 * running the archive migration, by scanning every trie log on a stopped node and pricing the
 * per-depth trie-node write counts against the {@link EntrySizeTable} byte model. Prints a headline
 * estimate plus a {@code FULL_ABOVE_DEPTH} / {@code CHECKPOINT_INTERVAL} lever sweep so operators
 * can size the feature and pick lever settings without materialising the CF.
 *
 * <p>The node must be stopped so the diagnostic can take the RocksDB read lock. The scan runs in
 * two passes: pass A accumulates net account-leaf deltas per 100k-block era (cheap — only account
 * changes), which prefix-sums into the per-era leaf-count timeline that pass B needs to bound each
 * changed leaf's trie-node path depth; pass B does the full per-depth count.
 */
@Command(
    name = "x-trie-node-history-estimate",
    description =
        "Estimate the on-disk size of the archive TRIE_NODE_HISTORY_ARCHIVE column family from trie"
            + " logs, without running the migration",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class TrieNodeHistoryEstimateSubCommand implements Runnable {

  // Blocks per parallel scan chunk. Chunks are submitted to a fixed thread pool; a smaller value
  // yields finer progress reporting at the cost of more task-submission overhead.
  private static final long SCAN_CHUNK_BLOCKS = 50_000L;

  // Hoodi-derived on-disk conversion ratios (2026-07-14 trie-node-history storage analysis): SST
  // key compression and blob-file value overhead. Superseded by calibration when it lands.
  private static final double DEFAULT_SST_COMPRESSION_RATIO = 1.93;
  private static final double DEFAULT_BLOB_OVERHEAD_RATIO = 1.44;

  private static final int TRIE_RADIX = 16;

  // Per-contract storage-slot scan cap. A storage trie's expected node-path depth grows only as
  // log16(slotCount), so counting beyond this adds no meaningful depth resolution while bounding
  // worst-case scan cost for the few very large contracts. 32k slots ≈ depth ~3.5.
  private static final int STORAGE_SLOT_PROBE_CAP = 1 << 15;

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Option(
      names = {"--calibration-file"},
      description =
          "Path to a calibration file produced by x-trie-node-history-calibrate, supplying measured"
              + " entry sizes instead of the embedded hoodi defaults.")
  private Path calibrationFile = null;

  @Option(
      names = {"--threads"},
      description =
          "Number of parallel scan threads over block-range chunks (default: ${DEFAULT-VALUE})")
  private int threads = 8;

  @Option(
      names = {"--log-interval-seconds"},
      description =
          "Print a progress line roughly every N seconds; 0 disables progress (default:"
              + " ${DEFAULT-VALUE})")
  private long logIntervalSeconds = 60;

  @Option(
      names = {"--start-block"},
      description =
          "First block (inclusive) to scan (default: ${DEFAULT-VALUE}). Genesis (block 0) has no"
              + " trie log, so scanning starts at block 1; pass 0 only if you have stored a genesis"
              + " trie log.")
  long startBlock = 1;

  @Option(
      names = {"--end-block"},
      description =
          "Last block (inclusive) to scan; -1 means the chain head (default: ${DEFAULT-VALUE})")
  private long endBlock = -1;

  @Option(
      names = {"--sample-shift"},
      description =
          "Hash-sampling shift for lifetime write-count tracking; higher = fewer keys sampled"
              + " (default: ${DEFAULT-VALUE})")
  private int sampleShift = 10;

  @Option(
      names = {"--full-above-depth"},
      description =
          "FULL_ABOVE_DEPTH threshold; nodes this shallow are always FULL (default:"
              + " ${DEFAULT-VALUE})")
  private int fullAboveDepth = 2;

  @Option(
      names = {"--checkpoint-interval"},
      description = "CHECKPOINT_INTERVAL for the headline estimate (default: ${DEFAULT-VALUE})")
  private int checkpointInterval = 16;

  @Option(
      names = {"--output"},
      description = "Optional path to also write the estimate as JSON")
  private Path output = null;

  /** Default constructor. */
  public TrieNodeHistoryEstimateSubCommand() {}

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    final BesuController controller = parentCommand.besuCommand.buildController();
    final MutableBlockchain blockchain = controller.getProtocolContext().getBlockchain();
    final SegmentedKeyValueStorage trieLogStorage =
        controller.getStorageProvider().getStorageBySegmentIdentifiers(List.of(TRIE_LOG_STORAGE));
    final SegmentedKeyValueStorage storageTrieStorage =
        controller
            .getStorageProvider()
            .getStorageBySegmentIdentifiers(List.of(ACCOUNT_STORAGE_STORAGE));
    final StorageTrieLeafCountProvider storageLeafCounts =
        new FlatDbStorageLeafCountProvider(storageTrieStorage, STORAGE_SLOT_PROBE_CAP);

    final long head = blockchain.getChainHeadBlockNumber();
    final long from = Math.max(0, startBlock);
    final long toExclusive = (endBlock < 0 ? head : Math.min(endBlock, head)) + 1;
    if (from >= toExclusive) {
      out.println(
          "Nothing to scan: start-block " + from + " is beyond end block " + (toExclusive - 1));
      return;
    }

    final EntrySizeTable entrySizeTable = resolveEntrySizeTable(out, calibrationFile);
    out.println(
        "Scanning trie logs for blocks ["
            + from
            + ", "
            + (toExclusive - 1)
            + "] with "
            + Math.max(1, threads)
            + " threads (two passes)...");
    out.flush();

    // Pass A: net account-leaf deltas per era → prefix-sum into the leaf-count timeline.
    final ChangeCountResult passA =
        runParallel(
            out,
            blockchain,
            trieLogStorage,
            from,
            toExclusive,
            "pass A (account deltas)",
            (b, s, f, t) -> accountDeltaRange(b, s, f, t));
    final long[] leafCountByRange = prefixSum(passA.accountDeltaByRange());

    // Pass B: full per-depth count using the era leaf-count timeline.
    final TrieShapeModel shape = new TrieShapeModel(TRIE_RADIX);
    final TrieLogChangeCounter counter =
        new TrieLogChangeCounter(fullAboveDepth, sampleShift, shape);
    final ChangeCountResult passB =
        runParallel(
            out,
            blockchain,
            trieLogStorage,
            from,
            toExclusive,
            "pass B (full count)",
            (b, s, f, t) -> countRange(b, s, f, t, counter, leafCountByRange, storageLeafCounts));

    final HistorySizeEstimate estimate =
        new HistorySizeEstimate(
            passB,
            entrySizeTable,
            shape,
            leafCountByRange,
            DEFAULT_SST_COMPRESSION_RATIO,
            DEFAULT_BLOB_OVERHEAD_RATIO);

    out.println();
    out.printf(
        "Estimate for requested levers (FULL_ABOVE_DEPTH=%d, CHECKPOINT_INTERVAL=%d): %,d bytes%n",
        fullAboveDepth,
        checkpointInterval,
        estimate.estimatedOnDiskBytes(fullAboveDepth, checkpointInterval));
    out.print(estimate.renderText(fullAboveDepth, checkpointInterval));
    out.flush();

    if (output != null) {
      try {
        Files.writeString(
            output, estimate.renderJson(fullAboveDepth, checkpointInterval).toPrettyString());
        out.println("Wrote JSON estimate to " + output);
      } catch (final java.io.IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /**
   * Resolves the {@link EntrySizeTable} to price trie-node writes with: measured calibration data
   * when {@code calibrationFile} is given, otherwise the embedded hoodi fallback defaults. Package-
   * private so it can be tested without standing up a full controller.
   *
   * @throws IllegalArgumentException if {@code calibrationFile} is given but cannot be read (e.g.
   *     missing or malformed) — calibration is explicitly requested, so a silent fallback to hoodi
   *     defaults would be surprising and wrong.
   */
  static EntrySizeTable resolveEntrySizeTable(final PrintWriter out, final Path calibrationFile) {
    if (calibrationFile == null) {
      out.println("Using embedded hoodi fallback defaults (no --calibration-file given).");
      return EntrySizeTable.hoodiDefaults();
    }
    final CalibrationResult calibration;
    try {
      calibration = CalibrationResult.readFrom(calibrationFile);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException(
          "Failed to read calibration file "
              + calibrationFile
              + ": "
              + e.getMessage()
              + ". Check the path and that it was produced by x-trie-node-history-calibrate.",
          e);
    }
    out.println("Using calibration data from " + calibrationFile);
    return calibration.toEntrySizeTable();
  }

  /** A per-chunk scan step over {@code [fromInclusive, toExclusive)}. */
  @FunctionalInterface
  private interface RangeScan {
    ChangeCountResult scan(
        MutableBlockchain blockchain,
        SegmentedKeyValueStorage trieLogStorage,
        long fromInclusive,
        long toExclusive);
  }

  private ChangeCountResult runParallel(
      final PrintWriter out,
      final MutableBlockchain blockchain,
      final SegmentedKeyValueStorage trieLogStorage,
      final long from,
      final long toExclusive,
      final String passName,
      final RangeScan step) {
    final int nThreads = Math.max(1, threads);
    final long total = toExclusive - from;
    final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
    final LongAdder processed = new LongAdder();
    final long startNanos = System.nanoTime();
    final Thread reporter = startProgressReporter(out, passName, processed, total, startNanos);
    final List<Future<ChangeCountResult>> futures = new ArrayList<>();
    try {
      for (long chunkStart = from; chunkStart < toExclusive; chunkStart += SCAN_CHUNK_BLOCKS) {
        final long chunkEnd = Math.min(chunkStart + SCAN_CHUNK_BLOCKS, toExclusive);
        final long f = chunkStart;
        futures.add(
            pool.submit(
                () -> {
                  final ChangeCountResult local =
                      step.scan(blockchain, trieLogStorage, f, chunkEnd);
                  processed.add(chunkEnd - f);
                  return local;
                }));
      }
      final ChangeCountResult merged = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
      for (final Future<ChangeCountResult> future : futures) {
        merged.merge(future.get());
      }
      return merged;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted during " + passName, e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(cause);
    } finally {
      pool.shutdownNow();
      if (reporter != null) {
        reporter.interrupt();
      }
    }
  }

  private Thread startProgressReporter(
      final PrintWriter out,
      final String passName,
      final LongAdder processed,
      final long total,
      final long startNanos) {
    if (logIntervalSeconds <= 0) {
      return null;
    }
    final Thread t =
        new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  Thread.sleep(logIntervalSeconds * 1000L);
                  final long done = processed.sum();
                  final double elapsedSec = (System.nanoTime() - startNanos) / 1e9;
                  final double rate = elapsedSec > 0 ? done / elapsedSec : 0;
                  out.printf(
                      "  %s: %,d / %,d blocks (%.1f%%), %,.0f blocks/s%n",
                      passName, done, total, total > 0 ? 100.0 * done / total : 0.0, rate);
                  out.flush();
                }
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "history-estimate-progress");
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * Pass B step: decode every trie log in {@code [fromInclusive, toExclusive)} and count its
   * trie-node writes per depth into a fresh accumulator, using the era leaf-count timeline to bound
   * path depth. Package-private so it can be tested against an in-memory blockchain + storage.
   *
   * @throws IllegalStateException if any block in the range has no stored trie log (fail fast — a
   *     gap would silently bias the counts low)
   */
  static ChangeCountResult countRange(
      final MutableBlockchain blockchain,
      final SegmentedKeyValueStorage trieLogStorage,
      final long fromInclusive,
      final long toExclusive,
      final TrieLogChangeCounter counter,
      final long[] leafCountByRange,
      final StorageTrieLeafCountProvider storageLeafCounts) {
    final ChangeCountResult local = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    for (long n = fromInclusive; n < toExclusive; n++) {
      final TrieLog trieLog = loadTrieLog(blockchain, trieLogStorage, n);
      counter.countBlock(
          trieLog, n, leafCountForEra(n, leafCountByRange), storageLeafCounts, local);
    }
    return local;
  }

  /**
   * Pass A step: decode every trie log in {@code [fromInclusive, toExclusive)} and record only its
   * net account-leaf delta (creations {@code +1}, deletions {@code -1}) per era. Cheap — it touches
   * account changes only, not the full trie-node path walk.
   */
  static ChangeCountResult accountDeltaRange(
      final MutableBlockchain blockchain,
      final SegmentedKeyValueStorage trieLogStorage,
      final long fromInclusive,
      final long toExclusive) {
    final ChangeCountResult local = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    for (long n = fromInclusive; n < toExclusive; n++) {
      final TrieLog trieLog = loadTrieLog(blockchain, trieLogStorage, n);
      final long blockNumber = n;
      trieLog
          .getAccountChanges()
          .forEach(
              (address, change) -> {
                if (change.getPrior() == null && change.getUpdated() != null) {
                  local.recordAccountDelta(blockNumber, 1);
                } else if (change.getUpdated() == null && change.getPrior() != null) {
                  local.recordAccountDelta(blockNumber, -1);
                }
              });
    }
    return local;
  }

  private static TrieLog loadTrieLog(
      final MutableBlockchain blockchain,
      final SegmentedKeyValueStorage trieLogStorage,
      final long blockNumber) {
    final Hash blockHash =
        blockchain
            .getBlockHeader(blockNumber)
            .orElseThrow(
                () -> new IllegalStateException("No block header for block " + blockNumber))
            .getHash();
    final byte[] bytes =
        trieLogStorage
            .get(TRIE_LOG_STORAGE, blockHash.getBytes().toArrayUnsafe())
            .orElseThrow(
                () -> new IllegalStateException("No trie log found for block " + blockNumber));
    return new TrieLogFactoryImpl().deserialize(bytes);
  }

  private static long leafCountForEra(final long blockNumber, final long[] leafCountByRange) {
    if (leafCountByRange.length == 0) {
      return 0;
    }
    final int range = (int) (blockNumber / ChangeCountResult.RANGE_BLOCKS);
    return leafCountByRange[Math.min(range, leafCountByRange.length - 1)];
  }

  /** Cumulative running total of per-era account-leaf deltas (era {@code i} = sum of eras 0..i). */
  private static long[] prefixSum(final long[] deltaByRange) {
    final long[] cumulative = new long[deltaByRange.length];
    long running = 0;
    for (int i = 0; i < deltaByRange.length; i++) {
      running += deltaByRange[i];
      cumulative[i] = running;
    }
    return cumulative;
  }
}
