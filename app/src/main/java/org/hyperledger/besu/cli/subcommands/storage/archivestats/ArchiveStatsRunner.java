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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rocksdb.RocksDBException;

/** Pipeline orchestrator. Decoupled from PicoCLI so it's testable with plain constructor args. */
public final class ArchiveStatsRunner {

  private static final List<String> CLASS_LABELS =
      List.of("dormant", "active", "long-lived", "hot", "mega-hot");

  /**
   * Runtime config for the analyzer.
   *
   * @param dbPath filesystem path to the RocksDB instance
   * @param outputPath directory for report outputs
   * @param rangeSize block-range partition size
   * @param fpSweepGrid grid of (k, m) bloom-sizing points for analytical FP projection
   * @param accountClassBoundaries 4 ascending thresholds for account-CF class bins
   * @param storageClassBoundaries 4 ascending thresholds for storage-CF class bins
   * @param selectedCfs CFs to scan
   * @param maxKeysPerCf cap on keys per CF; pass {@link Long#MAX_VALUE} for no limit
   * @param progressInterval minimum interval between progress lines
   * @param memoryBudgetMb defensive HLL footprint budget
   */
  public record Config(
      String dbPath,
      Path outputPath,
      long rangeSize,
      List<FpRateProjector.GridPoint> fpSweepGrid,
      List<Long> accountClassBoundaries,
      List<Long> storageClassBoundaries,
      List<ArchiveCf> selectedCfs,
      long maxKeysPerCf,
      Duration progressInterval,
      long memoryBudgetMb) {}

  private final Config config;
  private final PrintWriter log;

  /**
   * Construct the runner.
   *
   * @param config runtime config
   * @param log writer for progress and errors
   */
  public ArchiveStatsRunner(final Config config, final PrintWriter log) {
    this.config = config;
    this.log = log;
  }

  /**
   * Run the full pipeline and return the assembled scan result.
   *
   * @return aggregated scan result
   * @throws IOException if I/O fails
   * @throws RocksDBException if RocksDB scan fails
   */
  public ScanResult run() throws IOException, RocksDBException {
    final Instant scanStart = Instant.now();
    final EnumMap<ArchiveCf, CfResult> cfResults = new EnumMap<>(ArchiveCf.class);
    final Map<ArchiveCf, Long> cfSizes = new HashMap<>();
    long chainHead = 0L;

    try (final ArchiveScanner scanner = ArchiveScanner.openReadOnly(config.dbPath())) {
      for (final ArchiveCf cf : config.selectedCfs()) {
        final ProgressReporter progress =
            ProgressReporter.wallClock(log, config.progressInterval());
        final long estimatedTotal = scanner.estimateKeyCount(cf);
        progress.beginCf(cf.name(), estimatedTotal);

        final HistogramCollector entriesPerRow = HistogramCollector.log(28);
        final HistogramCollector rowsPerKey = HistogramCollector.linear(1024);
        final HistogramCollector modsPerKey = HistogramCollector.log(34);
        final RangeStatsCollector rangeStats = new RangeStatsCollector();
        final ClassBinner classBinner = new ClassBinner(boundariesFor(cf), CLASS_LABELS);

        final long[] cfChainHead = {0L};
        final long[] totalRows = {0L};
        final long[] totalKeys = {0L};
        final long[] totalEntries = {0L};
        final StreamingAggregator agg =
            new StreamingAggregator(
                config.rangeSize(),
                row -> {
                  entriesPerRow.record(row.count());
                  totalRows[0]++;
                },
                key -> {
                  rowsPerKey.record(key.distinctRanges());
                  modsPerKey.record(key.totalModifications());
                  classBinner.record(key.totalModifications());
                  totalKeys[0]++;
                });

        scanner.forEach(
            cf,
            config.maxKeysPerCf(),
            (visitedCf, rawKey, valueLen) -> {
              final KeyDecoder.Decoded d = KeyDecoder.decode(visitedCf, rawKey);
              if (d.blockNumber() > cfChainHead[0]) {
                cfChainHead[0] = d.blockNumber();
              }
              agg.observe(d.prefix(), d.blockNumber());
              rangeStats.observe(d.blockNumber() / config.rangeSize(), d.prefix());
              totalEntries[0]++;
              progress.tick(totalEntries[0], estimatedTotal);
            });
        agg.flush();
        progress.endCf(totalEntries[0]);

        if (cfChainHead[0] > chainHead) {
          chainHead = cfChainHead[0];
        }

        final Map<Long, Long> cardByRange = new HashMap<>();
        for (final long rid : rangeStats.rangeIds()) {
          cardByRange.put(rid, rangeStats.uniqueKeys(rid));
        }
        final FpRateProjector.Result fp =
            FpRateProjector.project(config.fpSweepGrid(), cardByRange);

        cfResults.put(
            cf,
            new CfResult(
                totalEntries[0],
                totalKeys[0],
                totalRows[0],
                rowsPerKey,
                entriesPerRow,
                modsPerKey,
                classBinner.snapshot(),
                boundariesFor(cf),
                rangeStats,
                fp));
        cfSizes.put(cf, 0L); // CF size lookup added in Task 16
      }
    }

    final long totalRanges = (chainHead / config.rangeSize()) + 1;
    return new ScanResult(
        config.dbPath(),
        chainHead,
        config.rangeSize(),
        totalRanges,
        scanStart,
        Instant.now(),
        cfSizes,
        config.fpSweepGrid(),
        cfResults);
  }

  private List<Long> boundariesFor(final ArchiveCf cf) {
    return cf == ArchiveCf.ACCOUNT
        ? config.accountClassBoundaries()
        : config.storageClassBoundaries();
  }
}
