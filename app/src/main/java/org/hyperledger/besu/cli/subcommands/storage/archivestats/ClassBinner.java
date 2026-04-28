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
import java.util.List;

/**
 * Bins per-key total-modification counts into N+1 named classes given N ascending boundaries.
 *
 * <p>Boundary convention: value {@code v} lands in class {@code i} where {@code i} is the smallest
 * index with {@code v <= boundaries[i]}; values strictly greater than the last boundary land in the
 * final class.
 */
public final class ClassBinner {

  private final long[] boundaries;
  private final String[] labels;
  private final long[] counts;
  private long total;

  /**
   * Construct a class binner.
   *
   * @param boundaries N strictly ascending positive thresholds.
   * @param labels N+1 class labels in order from lowest to highest.
   */
  public ClassBinner(final List<Long> boundaries, final List<String> labels) {
    if (labels.size() != boundaries.size() + 1) {
      throw new IllegalArgumentException(
          "labels must be one more than boundaries: got "
              + labels.size()
              + " labels, "
              + boundaries.size()
              + " boundaries");
    }
    for (int i = 1; i < boundaries.size(); i++) {
      if (boundaries.get(i) <= boundaries.get(i - 1)) {
        throw new IllegalArgumentException("boundaries must be strictly ascending");
      }
    }
    this.boundaries = boundaries.stream().mapToLong(Long::longValue).toArray();
    this.labels = labels.toArray(new String[0]);
    this.counts = new long[labels.size()];
  }

  /**
   * Record one observation.
   *
   * @param value modification count to bin
   */
  public void record(final long value) {
    counts[binIndex(value)]++;
    total++;
  }

  /**
   * Immutable snapshot of all bins.
   *
   * @return bins in label order
   */
  public List<Bin> snapshot() {
    final List<Bin> out = new ArrayList<>(labels.length);
    for (int i = 0; i < labels.length; i++) {
      out.add(new Bin(labels[i], rangeModsFor(i), counts[i], percentageFor(counts[i])));
    }
    return List.copyOf(out);
  }

  private int binIndex(final long value) {
    for (int i = 0; i < boundaries.length; i++) {
      if (value <= boundaries[i]) {
        return i;
      }
    }
    return labels.length - 1;
  }

  private String rangeModsFor(final int i) {
    final long lower = (i == 0) ? 1L : boundaries[i - 1] + 1L;
    if (i == labels.length - 1) {
      return lower + "+";
    }
    final long upper = boundaries[i];
    return lower + "–" + upper; // U+2013 EN DASH
  }

  private double percentageFor(final long count) {
    return total == 0 ? 0.0 : 100.0 * count / total;
  }

  /**
   * A single class bin.
   *
   * @param label class label
   * @param rangeMods printable range, e.g. "1–3" or "1000001+"
   * @param count number of keys in this bin
   * @param percentage percentage of total keys
   */
  public record Bin(String label, String rangeMods, long count, double percentage) {}
}
