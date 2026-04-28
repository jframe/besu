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

/** Project total row count for alternative RANGE_SIZE values. */
public final class RangeSizeProjection {

  /** Default candidate range sizes from the spec. */
  public static final List<Long> DEFAULT_CANDIDATES =
      List.of(
          100_000L,
          250_000L,
          500_000L,
          1_000_000L,
          2_000_000L,
          4_000_000L,
          8_000_000L,
          16_000_000L);

  private RangeSizeProjection() {}

  /**
   * Project row counts at alternative range sizes.
   *
   * <p>Sizes smaller than {@code observedRangeSize} cannot be derived post-hoc and are reported
   * with a {@code null} count and an explanatory note.
   *
   * @param observedRows total rows produced at scan time
   * @param observedRangeSize range size used at scan time
   * @param candidates candidate range sizes to project
   * @return one row per candidate, in input order
   */
  public static List<Row> project(
      final long observedRows, final long observedRangeSize, final List<Long> candidates) {
    final List<Row> out = new ArrayList<>(candidates.size());
    for (final long candidate : candidates) {
      if (candidate < observedRangeSize) {
        out.add(new Row(candidate, null, "not derivable from a coarser scan"));
      } else if (candidate == observedRangeSize) {
        out.add(new Row(candidate, observedRows, "observed"));
      } else {
        // Approximate: rows scale roughly inversely with rangeSize. Round to nearest.
        final long scaled = Math.round(observedRows * ((double) observedRangeSize / candidate));
        out.add(new Row(candidate, scaled, "scaled estimate"));
      }
    }
    return List.copyOf(out);
  }

  /**
   * One row of the projection table.
   *
   * @param candidateRangeSize candidate range size
   * @param projectedRows projected row count, null if not derivable
   * @param note short explanation of how this row was produced
   */
  public record Row(long candidateRangeSize, Long projectedRows, String note) {}
}
