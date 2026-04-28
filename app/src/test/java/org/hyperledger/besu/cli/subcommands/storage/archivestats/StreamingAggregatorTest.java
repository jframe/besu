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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class StreamingAggregatorTest {

  private static final long RANGE_SIZE = 1_000_000L;

  private final List<RowRecord> rows = new ArrayList<>();
  private final List<KeyRecord> keys = new ArrayList<>();

  private StreamingAggregator newAggregator() {
    return new StreamingAggregator(RANGE_SIZE, rows::add, keys::add);
  }

  @Test
  void emptyStreamEmitsNothing() {
    final StreamingAggregator agg = newAggregator();
    agg.flush();
    assertThat(rows).isEmpty();
    assertThat(keys).isEmpty();
  }

  @Test
  void singleEntryEmitsOneRowAndOneKey() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 5L);
    agg.flush();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).count()).isEqualTo(1);
    assertThat(rows.get(0).rangeId()).isZero();
    assertThat(keys).hasSize(1);
    assertThat(keys.get(0).distinctRanges()).isEqualTo(1);
    assertThat(keys.get(0).totalModifications()).isEqualTo(1L);
  }

  @Test
  void multipleEntriesSameRangeMergeIntoOneRow() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 5L);
    agg.observe(prefix(0xaa), 6L);
    agg.observe(prefix(0xaa), 999_999L); // same rangeId = 0
    agg.flush();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).count()).isEqualTo(3);
    assertThat(keys).hasSize(1);
    assertThat(keys.get(0).distinctRanges()).isEqualTo(1);
    assertThat(keys.get(0).totalModifications()).isEqualTo(3L);
  }

  @Test
  void singleKeySpanningMultipleRangesEmitsOneRowPerRange() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 5L); // range 0
    agg.observe(prefix(0xaa), 1_000_001L); // range 1
    agg.observe(prefix(0xaa), 2_500_000L); // range 2
    agg.observe(prefix(0xaa), 2_500_001L); // range 2
    agg.flush();

    assertThat(rows).hasSize(3);
    assertThat(rows).extracting(RowRecord::rangeId).containsExactly(0L, 1L, 2L);
    assertThat(rows).extracting(RowRecord::count).containsExactly(1, 1, 2);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0).distinctRanges()).isEqualTo(3);
    assertThat(keys.get(0).totalModifications()).isEqualTo(4L);
  }

  @Test
  void prefixTransitionFlushesPriorKey() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 5L);
    agg.observe(prefix(0xbb), 5L);
    agg.flush();

    assertThat(rows).hasSize(2);
    assertThat(keys).hasSize(2);
    assertThat(keys)
        .extracting(KeyRecord::prefix)
        .extracting(b -> b[0])
        .containsExactly((byte) 0xaa, (byte) 0xbb);
  }

  @Test
  void rangeBoundaryAtBlockZero() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 0L);
    agg.flush();
    assertThat(rows.get(0).rangeId()).isZero();
  }

  @Test
  void totalModificationsEqualsSumOfRowCounts() {
    final StreamingAggregator agg = newAggregator();
    agg.observe(prefix(0xaa), 5L);
    agg.observe(prefix(0xaa), 1_500_000L);
    agg.observe(prefix(0xaa), 1_500_001L);
    agg.observe(prefix(0xaa), 3_500_000L);
    agg.flush();

    final long sumOfRows = rows.stream().mapToLong(r -> (long) r.count()).sum();
    assertThat(keys).hasSize(1);
    assertThat(keys.get(0).totalModifications()).isEqualTo(sumOfRows);
  }

  @Test
  void emittedPrefixIsDefensivelyCopiedFromObserveBuffer() {
    final StreamingAggregator agg = newAggregator();
    final byte[] buffer = prefix(0xaa);
    agg.observe(buffer, 5L);
    buffer[0] = (byte) 0xff; // mutate caller-owned buffer
    agg.flush();
    assertThat(rows.get(0).prefix()[0]).isEqualTo((byte) 0xaa);
    assertThat(keys.get(0).prefix()[0]).isEqualTo((byte) 0xaa);
  }

  private static byte[] prefix(final int firstByte) {
    final byte[] p = new byte[32];
    p[0] = (byte) firstByte;
    return p;
  }
}
