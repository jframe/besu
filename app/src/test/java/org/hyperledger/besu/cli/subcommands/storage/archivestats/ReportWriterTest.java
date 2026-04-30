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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReportWriterTest {

  @Test
  void writesExpectedFiles(@TempDir final Path output) throws IOException {
    final ScanResult result = newSyntheticScanResult();

    new ReportWriter(output).write(result);

    assertThat(Files.exists(output.resolve("stats.json"))).isTrue();
    assertThat(Files.exists(output.resolve("summary.md"))).isTrue();
    assertThat(Files.exists(output.resolve("account-rows-per-key.csv"))).isTrue();
    assertThat(Files.exists(output.resolve("account-entries-per-row.csv"))).isTrue();
    assertThat(Files.exists(output.resolve("account-modifications-per-key.csv"))).isTrue();
    assertThat(Files.exists(output.resolve("account-class-distribution.csv"))).isTrue();
    assertThat(Files.exists(output.resolve("account-per-range.csv"))).isTrue();
    assertThat(Files.exists(output.resolve("account-fp-grid.csv"))).isTrue();
  }

  @Test
  void classDistributionCsvHasExpectedHeaderAndRows(@TempDir final Path output) throws IOException {
    new ReportWriter(output).write(newSyntheticScanResult());
    final List<String> lines = Files.readAllLines(output.resolve("account-class-distribution.csv"));
    assertThat(lines.get(0)).isEqualTo("classLabel,rangeMods,count,percentage");
    assertThat(lines.size()).isEqualTo(6); // header + 5 classes
  }

  @Test
  void summaryMdContainsHeadlineNumbers(@TempDir final Path output) throws IOException {
    new ReportWriter(output).write(newSyntheticScanResult());
    final String summary = Files.readString(output.resolve("summary.md"));
    assertThat(summary).contains("ACCOUNT");
    assertThat(summary).contains("RANGE_SIZE");
  }

  private static ScanResult newSyntheticScanResult() {
    final HistogramCollector rowsPerKey = HistogramCollector.linear(5);
    rowsPerKey.record(1L);
    rowsPerKey.record(2L);

    final HistogramCollector entriesPerRow = HistogramCollector.log(20);
    entriesPerRow.record(1L);
    entriesPerRow.record(5L);

    final HistogramCollector modsPerKey = HistogramCollector.log(20);
    modsPerKey.record(2L);
    modsPerKey.record(20L);

    final ClassBinner cb =
        new ClassBinner(
            List.of(3L, 50L, 10_000L, 1_000_000L),
            List.of("dormant", "active", "long-lived", "hot", "mega-hot"));
    cb.record(2L);
    cb.record(20L);

    final RangeStatsCollector rs = new RangeStatsCollector();
    rs.observe(0L, new byte[] {1});
    rs.observe(0L, new byte[] {2});
    rs.observe(1L, new byte[] {3});

    final List<FpRateProjector.GridPoint> grid =
        List.of(new FpRateProjector.GridPoint(7, 1_048_576L));
    final FpRateProjector.Result fp = FpRateProjector.project(grid, Map.of(0L, 2L, 1L, 1L));

    final CfResult acc =
        new CfResult(
            3L,
            3L,
            2L,
            rowsPerKey,
            entriesPerRow,
            modsPerKey,
            cb.snapshot(),
            List.of(3L, 50L, 10_000L, 1_000_000L),
            rs,
            fp);

    final EnumMap<ArchiveCf, CfResult> map = new EnumMap<>(ArchiveCf.class);
    map.put(ArchiveCf.ACCOUNT, acc);

    return new ScanResult(
        "/tmp/test",
        1_000_001L,
        1_000_000L,
        2L,
        Instant.parse("2026-04-25T10:00:00Z"),
        Instant.parse("2026-04-25T10:05:00Z"),
        Map.of(ArchiveCf.ACCOUNT, 1024L),
        grid,
        map,
        new EnumMap<>(ArchiveCf.class));
  }
}
