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

import java.util.List;

import org.junit.jupiter.api.Test;

class RangeSizeProjectionTest {

  @Test
  void scaledRowCountInverselyProportionalToRangeSize() {
    final long observedRows = 96_000_000L;
    final long observedRangeSize = 1_000_000L;
    final List<RangeSizeProjection.Row> projections =
        RangeSizeProjection.project(
            observedRows, observedRangeSize, List.of(1_000_000L, 2_000_000L, 4_000_000L));
    assertThat(projections).hasSize(3);
    assertThat(projections.get(0).projectedRows()).isEqualTo(96_000_000L);
    assertThat(projections.get(1).projectedRows()).isEqualTo(48_000_000L);
    assertThat(projections.get(2).projectedRows()).isEqualTo(24_000_000L);
  }

  @Test
  void smallerThanObservedRangeReportedAsUnknown() {
    final List<RangeSizeProjection.Row> projections =
        RangeSizeProjection.project(100_000L, 1_000_000L, List.of(500_000L, 1_000_000L));
    assertThat(projections.get(0).projectedRows()).isNull();
    assertThat(projections.get(0).note()).contains("not derivable");
    assertThat(projections.get(1).projectedRows()).isEqualTo(100_000L);
  }
}
