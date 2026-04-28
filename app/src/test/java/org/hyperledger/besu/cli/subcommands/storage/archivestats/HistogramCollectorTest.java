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

import org.junit.jupiter.api.Test;

class HistogramCollectorTest {

  @Test
  void logHistogramBucketsAreInclusiveLowerExclusiveUpper() {
    final HistogramCollector h = HistogramCollector.log(10);
    h.record(1L);
    h.record(2L);
    h.record(3L);
    h.record(4L);
    // bucket 0: [1, 2)  -> {1}             count 1
    // bucket 1: [2, 4)  -> {2, 3}          count 2
    // bucket 2: [4, 8)  -> {4}             count 1
    assertThat(h.bucketLowerBounds()).startsWith(1L, 2L, 4L, 8L);
    assertThat(h.bucketCounts()).startsWith(1L, 2L, 1L, 0L);
    assertThat(h.total()).isEqualTo(4);
  }

  @Test
  void logHistogramHandlesZero() {
    final HistogramCollector h = HistogramCollector.log(10);
    h.record(0L);
    h.record(0L);
    // bucket 0 covers [1, 2); zero collapses to bucket 0 with lower bound 0 documented.
    assertThat(h.bucketCounts()[0]).isEqualTo(2L);
    assertThat(h.bucketLowerBounds()[0]).isZero();
  }

  @Test
  void linearHistogramOneBucketPerInteger() {
    final HistogramCollector h = HistogramCollector.linear(5);
    h.record(0L);
    h.record(2L);
    h.record(2L);
    h.record(2L);
    h.record(4L);
    assertThat(h.bucketLowerBounds()).containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(h.bucketCounts()).containsExactly(1L, 0L, 3L, 0L, 1L);
  }

  @Test
  void linearHistogramClampsValuesAtMaxBucket() {
    final HistogramCollector h = HistogramCollector.linear(3);
    h.record(0L);
    h.record(2L);
    h.record(99L); // out of range, clamps to last bucket (2)
    assertThat(h.bucketCounts()).containsExactly(1L, 0L, 2L);
  }

  @Test
  void percentilesAreCorrectOnSortedInput() {
    final HistogramCollector h = HistogramCollector.linear(101);
    for (long v = 1; v <= 100; v++) {
      h.record(v);
    }
    assertThat(h.percentile(0.5)).isEqualTo(50L);
    assertThat(h.percentile(0.9)).isEqualTo(90L);
    assertThat(h.percentile(0.99)).isEqualTo(99L);
    assertThat(h.max()).isEqualTo(100L);
  }

  @Test
  void percentileOfEmptyHistogramIsZero() {
    final HistogramCollector h = HistogramCollector.log(10);
    assertThat(h.percentile(0.5)).isZero();
    assertThat(h.max()).isZero();
    assertThat(h.total()).isZero();
  }

  @Test
  void recordingMaxIntDoesNotThrow() {
    final HistogramCollector h = HistogramCollector.log(64);
    h.record(Integer.MAX_VALUE);
    assertThat(h.total()).isEqualTo(1);
    assertThat(h.max()).isEqualTo(Integer.MAX_VALUE);
  }
}
