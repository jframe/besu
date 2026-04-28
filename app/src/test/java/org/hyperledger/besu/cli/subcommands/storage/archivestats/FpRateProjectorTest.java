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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class FpRateProjectorTest {

  @Test
  void parsesSweepStrings() {
    final List<FpRateProjector.GridPoint> grid =
        FpRateProjector.parseSweep(List.of("7:1048576", "10:2097152"));
    assertThat(grid)
        .containsExactly(
            new FpRateProjector.GridPoint(7, 1_048_576L),
            new FpRateProjector.GridPoint(10, 2_097_152L));
  }

  @Test
  void rejectsMalformedSweepEntry() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> FpRateProjector.parseSweep(List.of("seven:foo")));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> FpRateProjector.parseSweep(List.of("7-1048576")));
  }

  @Test
  void analyticalFormulaMatchesHandComputation() {
    // FP(k=7, m=1048576, n=10000) = (1 - e^(-7*10000/1048576))^7
    // x = 7*10000/1048576 ≈ 0.06676
    // 1 - e^-x ≈ 0.06458
    // (...)^7 ≈ 4.78e-9
    final double fp = FpRateProjector.fpRate(new FpRateProjector.GridPoint(7, 1_048_576L), 10_000L);
    assertThat(fp).isCloseTo(4.78e-9, org.assertj.core.data.Offset.offset(1e-10));
  }

  @Test
  void zeroCardinalityProducesZeroFp() {
    assertThat(FpRateProjector.fpRate(new FpRateProjector.GridPoint(7, 1_048_576L), 0L)).isZero();
  }

  @Test
  void fpIsNonDecreasingAsMShrinks() {
    final long n = 100_000L;
    final double fpBig = FpRateProjector.fpRate(new FpRateProjector.GridPoint(7, 4_194_304L), n);
    final double fpMid = FpRateProjector.fpRate(new FpRateProjector.GridPoint(7, 1_048_576L), n);
    final double fpSmall = FpRateProjector.fpRate(new FpRateProjector.GridPoint(7, 524_288L), n);
    assertThat(fpBig).isLessThanOrEqualTo(fpMid);
    assertThat(fpMid).isLessThanOrEqualTo(fpSmall);
  }

  @Test
  void summaryStatsComputeMedianP95MaxWorstRange() {
    final List<FpRateProjector.GridPoint> grid =
        List.of(new FpRateProjector.GridPoint(7, 1_048_576L));
    // 5 ranges with cardinalities 100, 200, 500, 1000, 5000
    final Map<Long, Long> cardByRange = Map.of(0L, 100L, 1L, 200L, 2L, 500L, 3L, 1000L, 4L, 5000L);

    final FpRateProjector.Result r = FpRateProjector.project(grid, cardByRange);

    final FpRateProjector.GridSummary s = r.summaries().get(grid.get(0));
    // Worst range is the one with the highest cardinality.
    assertThat(s.worstRangeId()).isEqualTo(4L);
    assertThat(s.max()).isGreaterThanOrEqualTo(s.p95());
    assertThat(s.p95()).isGreaterThanOrEqualTo(s.median());
  }
}
