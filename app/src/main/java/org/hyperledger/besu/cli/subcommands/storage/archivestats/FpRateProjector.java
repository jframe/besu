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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

/** Pure-arithmetic projection of bloom false-positive rate over a {@code (k, m)} sweep grid. */
public final class FpRateProjector {

  private FpRateProjector() {}

  /**
   * A point in the sweep grid: {@code k} hash functions over {@code m} bits.
   *
   * @param k number of hash functions
   * @param m bloom filter size in bits
   */
  public record GridPoint(int k, long m) {
    /**
     * Stable label for output keys.
     *
     * @return label like "k=7,m=1048576"
     */
    public String label() {
      return "k=" + k + ",m=" + m;
    }
  }

  /**
   * All FP rates and per-grid-point summary stats for one CF.
   *
   * @param perRange per-range FP rates keyed by rangeId then grid point
   * @param summaries per-grid-point summary stats
   */
  public record Result(
      Map<Long, Map<GridPoint, Double>> perRange, Map<GridPoint, GridSummary> summaries) {}

  /**
   * Median, p95, max, and the range ID where max occurred for one grid point.
   *
   * @param median median FP rate across ranges
   * @param p95 95th-percentile FP rate
   * @param max maximum FP rate observed
   * @param worstRangeId ID of the range that produced the max
   */
  public record GridSummary(double median, double p95, double max, long worstRangeId) {}

  /**
   * Parse {@code "k:m,k:m,..."} CLI strings into grid points.
   *
   * @param rawPairs the raw "k:m" strings
   * @return parsed grid points
   */
  public static List<GridPoint> parseSweep(final List<String> rawPairs) {
    final List<GridPoint> out = new ArrayList<>(rawPairs.size());
    for (final String raw : rawPairs) {
      final List<String> parts = Splitter.on(':').splitToList(raw);
      if (parts.size() != 2) {
        throw new IllegalArgumentException("Bad sweep entry, expected k:m, got: " + raw);
      }
      final int k;
      final long m;
      try {
        k = Integer.parseInt(parts.get(0).trim());
        m = Long.parseLong(parts.get(1).trim());
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("Bad sweep entry numbers: " + raw, e);
      }
      if (k <= 0 || m <= 0) {
        throw new IllegalArgumentException("k and m must be positive: " + raw);
      }
      out.add(new GridPoint(k, m));
    }
    return out;
  }

  /**
   * Evaluate {@code FP = (1 - e^(-kn/m))^k}. Returns 0.0 for {@code n == 0}.
   *
   * @param gp grid point
   * @param n unique-key cardinality for the range
   * @return projected FP rate in [0, 1]
   */
  public static double fpRate(final GridPoint gp, final long n) {
    if (n == 0) {
      return 0.0;
    }
    final double x = (double) gp.k() * n / gp.m();
    final double bitFill = 1.0 - Math.exp(-x);
    return Math.pow(bitFill, gp.k());
  }

  /**
   * Project FP rates and summary stats for the given grid against per-range cardinalities.
   *
   * @param grid sweep grid
   * @param uniqueKeysByRange per-range cardinalities
   * @return projected results
   */
  public static Result project(
      final List<GridPoint> grid, final Map<Long, Long> uniqueKeysByRange) {
    final Map<Long, Map<GridPoint, Double>> perRange = new HashMap<>();
    for (final var entry : uniqueKeysByRange.entrySet()) {
      final long rangeId = entry.getKey();
      final long n = entry.getValue();
      final Map<GridPoint, Double> rangeFps = new HashMap<>();
      for (final GridPoint gp : grid) {
        rangeFps.put(gp, fpRate(gp, n));
      }
      perRange.put(rangeId, rangeFps);
    }

    final Map<GridPoint, GridSummary> summaries = new HashMap<>();
    for (final GridPoint gp : grid) {
      final List<Double> fps = new ArrayList<>(perRange.size());
      double max = 0.0;
      long worstRange = -1L;
      for (final var e : perRange.entrySet()) {
        final double v = e.getValue().get(gp);
        fps.add(v);
        if (v >= max) {
          max = v;
          worstRange = e.getKey();
        }
      }
      Collections.sort(fps);
      summaries.put(
          gp, new GridSummary(percentile(fps, 0.5), percentile(fps, 0.95), max, worstRange));
    }
    return new Result(perRange, summaries);
  }

  private static double percentile(final List<Double> sorted, final double p) {
    if (sorted.isEmpty()) {
      return 0.0;
    }
    final int idx = (int) Math.min((double) (sorted.size() - 1L), Math.ceil(p * sorted.size()) - 1);
    return sorted.get(Math.max(idx, 0));
  }
}
