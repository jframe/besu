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

class RangeStatsCollectorTest {

  @Test
  void entriesAndUniqueKeysAreZeroForUnseenRange() {
    final RangeStatsCollector c = new RangeStatsCollector();
    assertThat(c.entries(7L)).isZero();
    assertThat(c.uniqueKeys(7L)).isZero();
    assertThat(c.rangeIds()).isEmpty();
  }

  @Test
  void countsExactEntriesPerRange() {
    final RangeStatsCollector c = new RangeStatsCollector();
    c.observe(0L, prefix(0xaa));
    c.observe(0L, prefix(0xbb));
    c.observe(1L, prefix(0xcc));
    assertThat(c.entries(0L)).isEqualTo(2);
    assertThat(c.entries(1L)).isEqualTo(1);
    assertThat(c.rangeIds()).containsExactlyInAnyOrder(0L, 1L);
  }

  @Test
  void uniqueKeysWithinSmallSetIsExact() {
    final RangeStatsCollector c = new RangeStatsCollector();
    c.observe(0L, prefix(0xaa));
    c.observe(0L, prefix(0xaa)); // dup
    c.observe(0L, prefix(0xbb));
    // HLL is ~exact at small cardinalities thanks to HIP / sparse mode.
    assertThat(c.uniqueKeys(0L)).isEqualTo(2L);
  }

  @Test
  void uniqueKeysApproximatesGroundTruthAtTenThousand() {
    final RangeStatsCollector c = new RangeStatsCollector();
    final int n = 10_000;
    for (int i = 0; i < n; i++) {
      final byte[] p = new byte[32];
      p[0] = (byte) (i & 0xff);
      p[1] = (byte) ((i >>> 8) & 0xff);
      p[2] = (byte) ((i >>> 16) & 0xff);
      c.observe(0L, p);
    }
    final long est = c.uniqueKeys(0L);
    final double err = Math.abs(est - n) / (double) n;
    assertThat(err).isLessThan(0.02); // within 2%
  }

  private static byte[] prefix(final int firstByte) {
    final byte[] p = new byte[32];
    p[0] = (byte) firstByte;
    return p;
  }
}
