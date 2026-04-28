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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Serialises {@link ScanResult} to JSON, CSVs, and a Markdown summary. */
public final class ReportWriter {

  private final Path outputDir;
  private final ObjectMapper json;

  /**
   * Construct a writer.
   *
   * @param outputDir directory to write outputs into; will be created if missing
   */
  public ReportWriter(final Path outputDir) {
    this.outputDir = outputDir;
    this.json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  }

  /**
   * Write all output files for a scan result.
   *
   * @param result the assembled scan result
   * @throws IOException if any output file cannot be written
   */
  public void write(final ScanResult result) throws IOException {
    Files.createDirectories(outputDir);
    writeJson(result);
    for (final var entry : result.cfResults().entrySet()) {
      writeCsvs(entry.getKey(), entry.getValue());
    }
    writeSummary(result);
  }

  private void writeJson(final ScanResult result) throws IOException {
    final Map<String, Object> root = new LinkedHashMap<>();
    final Map<String, Object> scanInfo = new LinkedHashMap<>();
    scanInfo.put("dataDir", result.dataDir());
    scanInfo.put("chainHead", result.chainHead());
    scanInfo.put("rangeSize", result.rangeSize());
    scanInfo.put("totalRanges", result.totalRanges());
    scanInfo.put("scanStart", result.scanStart().toString());
    scanInfo.put("scanEnd", result.scanEnd().toString());
    scanInfo.put("rocksDbCfSizeBytes", toLabelMap(result.rocksDbCfSizeBytes()));
    scanInfo.put(
        "fpSweepGrid",
        result.fpSweepGrid().stream().map(gp -> Map.of("k", gp.k(), "m", gp.m())).toList());
    root.put("scanInfo", scanInfo);

    for (final var entry : result.cfResults().entrySet()) {
      root.put(entry.getKey().cliLabel(), serializeCf(entry.getValue()));
    }

    json.writerWithDefaultPrettyPrinter()
        .writeValue(outputDir.resolve("stats.json").toFile(), root);
  }

  private Map<String, Long> toLabelMap(final Map<ArchiveCf, Long> in) {
    final Map<String, Long> out = new LinkedHashMap<>();
    for (final var e : in.entrySet()) {
      out.put(e.getKey().segment().getName(), e.getValue());
    }
    return out;
  }

  private Map<String, Object> serializeCf(final CfResult cf) {
    final Map<String, Object> m = new LinkedHashMap<>();
    m.put("totalEntries", cf.totalEntries());
    m.put("totalUniqueKeys", cf.totalUniqueKeys());
    m.put("totalRows", cf.totalRows());
    m.put("rowsPerKey", serializeHist(cf.rowsPerKey()));
    m.put("entriesPerRow", serializeHist(cf.entriesPerRow()));
    m.put("modificationsPerKey", serializeHist(cf.modificationsPerKey()));

    final Map<String, Object> classDist = new LinkedHashMap<>();
    classDist.put("boundaries", cf.classBoundaries());
    classDist.put(
        "classes",
        cf.classBins().stream()
            .map(
                b ->
                    Map.of(
                        "label", b.label(),
                        "rangeMods", b.rangeMods(),
                        "count", b.count(),
                        "percentage", b.percentage()))
            .toList());
    m.put("classDistribution", classDist);

    final List<Map<String, Object>> perRange = new ArrayList<>();
    for (final long rangeId : new TreeSet<>(cf.rangeStats().rangeIds())) {
      final Map<String, Object> rr = new LinkedHashMap<>();
      rr.put("rangeId", rangeId);
      rr.put("entries", cf.rangeStats().entries(rangeId));
      rr.put("uniqueKeys", cf.rangeStats().uniqueKeys(rangeId));
      final Map<String, Double> fps = new LinkedHashMap<>();
      final var rangeFps = cf.fpResult().perRange().get(rangeId);
      if (rangeFps != null) {
        for (final var e : rangeFps.entrySet()) {
          fps.put(e.getKey().label(), e.getValue());
        }
      }
      rr.put("projectedFp", fps);
      perRange.add(rr);
    }
    m.put("perRange", perRange);

    final Map<String, Object> fpSummary = new LinkedHashMap<>();
    for (final var e : cf.fpResult().summaries().entrySet()) {
      fpSummary.put(
          e.getKey().label(),
          Map.of(
              "median", e.getValue().median(),
              "p95", e.getValue().p95(),
              "max", e.getValue().max(),
              "worstRangeId", e.getValue().worstRangeId()));
    }
    m.put("fpSummary", fpSummary);
    return m;
  }

  private Map<String, Object> serializeHist(final HistogramCollector h) {
    final List<Map<String, Long>> buckets = new ArrayList<>();
    final long[] lb = h.bucketLowerBounds();
    final long[] cnt = h.bucketCounts();
    for (int i = 0; i < lb.length; i++) {
      if (cnt[i] > 0) {
        buckets.add(Map.of("lowerBound", lb[i], "count", cnt[i]));
      }
    }
    return Map.of(
        "histogramBuckets", buckets,
        "p50", h.percentile(0.5),
        "p90", h.percentile(0.9),
        "p99", h.percentile(0.99),
        "max", h.max());
  }

  private void writeCsvs(final ArchiveCf cf, final CfResult res) throws IOException {
    final String prefix = cf.cliLabel() + "-";
    writeBucketCsv(prefix + "rows-per-key.csv", res.rowsPerKey());
    writeBucketCsv(prefix + "entries-per-row.csv", res.entriesPerRow());
    writeBucketCsv(prefix + "modifications-per-key.csv", res.modificationsPerKey());

    try (BufferedWriter w =
        Files.newBufferedWriter(outputDir.resolve(prefix + "class-distribution.csv"))) {
      w.write("classLabel,rangeMods,count,percentage");
      w.newLine();
      for (final ClassBinner.Bin b : res.classBins()) {
        w.write(b.label() + "," + b.rangeMods() + "," + b.count() + "," + b.percentage());
        w.newLine();
      }
    }

    try (BufferedWriter w = Files.newBufferedWriter(outputDir.resolve(prefix + "per-range.csv"))) {
      w.write("rangeId,entries,uniqueKeys");
      w.newLine();
      for (final long rangeId : new TreeSet<>(res.rangeStats().rangeIds())) {
        w.write(
            rangeId
                + ","
                + res.rangeStats().entries(rangeId)
                + ","
                + res.rangeStats().uniqueKeys(rangeId));
        w.newLine();
      }
    }

    try (BufferedWriter w = Files.newBufferedWriter(outputDir.resolve(prefix + "fp-grid.csv"))) {
      w.write("rangeId,k,m,projectedFp");
      w.newLine();
      for (final long rangeId : new TreeSet<>(res.fpResult().perRange().keySet())) {
        final var perPoint = res.fpResult().perRange().get(rangeId);
        for (final var e : perPoint.entrySet()) {
          w.write(rangeId + "," + e.getKey().k() + "," + e.getKey().m() + "," + e.getValue());
          w.newLine();
        }
      }
    }
  }

  private void writeBucketCsv(final String filename, final HistogramCollector h)
      throws IOException {
    try (BufferedWriter w = Files.newBufferedWriter(outputDir.resolve(filename))) {
      w.write("bucketLowerBound,count");
      w.newLine();
      final long[] lb = h.bucketLowerBounds();
      final long[] cnt = h.bucketCounts();
      for (int i = 0; i < lb.length; i++) {
        w.write(lb[i] + "," + cnt[i]);
        w.newLine();
      }
    }
  }

  private void writeSummary(final ScanResult result) throws IOException {
    try (BufferedWriter w = Files.newBufferedWriter(outputDir.resolve("summary.md"))) {
      w.write("# Archive Distribution Analyzer Report");
      w.newLine();
      w.newLine();
      w.write(
          "Scanned `"
              + result.dataDir()
              + "` from "
              + result.scanStart()
              + " to "
              + result.scanEnd()
              + ".");
      w.newLine();
      w.write(
          "Chain head observed: "
              + result.chainHead()
              + " (RANGE_SIZE = "
              + result.rangeSize()
              + ", "
              + result.totalRanges()
              + " ranges).");
      w.newLine();
      w.newLine();
      for (final var e : result.cfResults().entrySet()) {
        final ArchiveCf cf = e.getKey();
        final CfResult r = e.getValue();
        w.write("## " + cf.name());
        w.newLine();
        w.write("- Total entries: " + r.totalEntries());
        w.newLine();
        w.write("- Total unique keys: " + r.totalUniqueKeys());
        w.newLine();
        w.write("- Total rows: " + r.totalRows());
        w.newLine();
        w.newLine();
        w.write("### Class distribution");
        w.newLine();
        for (final ClassBinner.Bin b : r.classBins()) {
          w.write(
              "- **"
                  + b.label()
                  + "** ("
                  + b.rangeMods()
                  + "): "
                  + b.count()
                  + " ("
                  + String.format(Locale.ROOT, "%.2f", b.percentage())
                  + "%)");
          w.newLine();
        }
        w.newLine();
      }
      w.newLine();
      w.write("## RANGE_SIZE projection (account-CF rows)");
      w.newLine();
      final CfResult acc = result.cfResults().get(ArchiveCf.ACCOUNT);
      if (acc != null) {
        w.write("| RANGE_SIZE | projected rows | note |");
        w.newLine();
        w.write("|---:|---:|---|");
        w.newLine();
        for (final var row :
            RangeSizeProjection.project(
                acc.totalRows(), result.rangeSize(), RangeSizeProjection.DEFAULT_CANDIDATES)) {
          w.write(
              "| "
                  + row.candidateRangeSize()
                  + " | "
                  + (row.projectedRows() == null ? "—" : row.projectedRows().toString())
                  + " | "
                  + row.note()
                  + " |");
          w.newLine();
        }
      }
    }
  }
}
