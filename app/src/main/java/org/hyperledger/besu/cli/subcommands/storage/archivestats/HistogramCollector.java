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

/**
 * Counts values into either log-spaced (powers-of-2) or linear (one-bucket-per-integer) buckets.
 *
 * <p>Use {@link #log(int)} for skewed distributions where most values are small (entries-per-row,
 * modifications-per-key). Use {@link #linear(int)} for bounded enumerations (rows-per-key, where
 * the value cannot exceed the number of ranges).
 *
 * <p><b>Special bucket-0 behavior for log histograms:</b> in log mode, bucket 0 initially
 * advertises a lower bound of {@code 1} (so it covers {@code [1, 2)}). The first call to {@code
 * record(0L)} dynamically rewrites {@code bucketLowerBounds()[0]} to {@code 0} to reflect that
 * bucket 0 has absorbed a value below its initial lower bound. As a result, the value returned by
 * {@link #bucketLowerBounds()} for index 0 is path-dependent on input order: callers that have
 * already retrieved the array before the first {@code record(0L)} will see {@code 1}; callers
 * retrieving it afterwards will see {@code 0}.
 */
public final class HistogramCollector {

  /**
   * Construct a log-bucketed collector.
   *
   * <p>Bucket {@code i} covers {@code [2^i, 2^(i+1))} for {@code i >= 1}. Bucket 0 covers {@code
   * [1, 2)} until {@code record(0L)} is called, after which {@code bucketLowerBounds()[0]} is
   * reported as 0 to reflect that bucket 0 has absorbed values below its initial lower bound.
   *
   * @param numBuckets number of buckets.
   * @return the new collector
   */
  public static HistogramCollector log(final int numBuckets) {
    return new HistogramCollector(true, numBuckets);
  }

  /**
   * Construct a linear-bucketed collector.
   *
   * @param numBuckets number of buckets, one per integer in {@code [0, numBuckets)}. Values &ge;
   *     {@code numBuckets - 1} are clamped to the last bucket.
   * @return the new collector
   */
  public static HistogramCollector linear(final int numBuckets) {
    return new HistogramCollector(false, numBuckets);
  }

  private final boolean log;
  private final long[] counts;
  private final long[] lowerBounds;
  private long total;
  private long max;

  private HistogramCollector(final boolean log, final int numBuckets) {
    if (numBuckets <= 0) {
      throw new IllegalArgumentException("numBuckets must be positive");
    }
    this.log = log;
    this.counts = new long[numBuckets];
    this.lowerBounds = new long[numBuckets];
    if (log) {
      // Bucket i covers [2^i, 2^(i+1)).
      for (int i = 0; i < numBuckets; i++) {
        lowerBounds[i] = 1L << i;
      }
    } else {
      for (int i = 0; i < numBuckets; i++) {
        lowerBounds[i] = i;
      }
    }
  }

  /**
   * Record one value.
   *
   * @param value non-negative value to record.
   */
  public void record(final long value) {
    if (value < 0) {
      throw new IllegalArgumentException("value must be non-negative");
    }
    counts[bucketIndex(value)]++;
    total++;
    if (value > max) {
      max = value;
    }
    if (log && value == 0L) {
      // Document that bucket 0 now covers values starting at 0.
      lowerBounds[0] = 0L;
    }
  }

  /**
   * Bucket lower bounds.
   *
   * <p>For log histograms, the lower bound at index 0 is dynamically updated by {@code record(0L)}
   * &mdash; see {@link #log(int)}.
   *
   * @return parallel array of inclusive lower bounds, same length as {@link #bucketCounts()}.
   */
  public long[] bucketLowerBounds() {
    return lowerBounds.clone();
  }

  /**
   * Per-bucket counts.
   *
   * @return per-bucket counts array
   */
  public long[] bucketCounts() {
    return counts.clone();
  }

  /**
   * Total recorded samples.
   *
   * @return total number of recorded samples
   */
  public long total() {
    return total;
  }

  /**
   * Largest value seen.
   *
   * @return largest value seen, or 0 if no samples recorded.
   */
  public long max() {
    return max;
  }

  /**
   * Approximate percentile from bucket counts.
   *
   * <p>Returns 0 if no samples were recorded. For log buckets, returns the lower bound of the
   * bucket containing the percentile (a coarse but monotone estimate).
   *
   * @param p target percentile in {@code [0, 1]}
   * @return percentile value
   */
  public long percentile(final double p) {
    if (total == 0) {
      return 0L;
    }
    final long target = (long) Math.ceil(p * total);
    long running = 0;
    for (int i = 0; i < counts.length; i++) {
      running += counts[i];
      if (running >= target) {
        return lowerBounds[i];
      }
    }
    return lowerBounds[counts.length - 1];
  }

  private int bucketIndex(final long value) {
    if (log) {
      if (value < 2) {
        return 0;
      }
      // floor(log2(value)) gives the bucket: 2 -> 1, 4 -> 2, 8 -> 3, ...
      final int idx = 63 - Long.numberOfLeadingZeros(value);
      return Math.min(idx, counts.length - 1);
    } else {
      return (int) Math.min(value, counts.length - 1);
    }
  }
}
