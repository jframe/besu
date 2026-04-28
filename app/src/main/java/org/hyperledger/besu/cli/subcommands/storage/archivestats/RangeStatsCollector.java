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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.datasketches.hll.HllSketch;

/** Per-range entry counts plus DataSketches HyperLogLog cardinality estimates. */
public final class RangeStatsCollector {

  /** Log-base-2 of HLL register count. lgK=14 produces 16384 registers, ~0.8% standard error. */
  static final int LG_K = 14;

  private final Map<Long, PerRange> byRange = new HashMap<>();

  /**
   * Observe one entry for a range.
   *
   * @param rangeId block-range index
   * @param prefix natural key bytes
   */
  public void observe(final long rangeId, final byte[] prefix) {
    final PerRange r = byRange.computeIfAbsent(rangeId, id -> new PerRange());
    r.entries++;
    r.hll.update(prefix);
  }

  /**
   * Number of entries observed for a range.
   *
   * @param rangeId block-range index
   * @return number of entries observed for {@code rangeId}, or 0 if unseen
   */
  public long entries(final long rangeId) {
    final PerRange r = byRange.get(rangeId);
    return r == null ? 0L : r.entries;
  }

  /**
   * HLL-estimated number of unique prefixes observed for a range.
   *
   * @param rangeId block-range index
   * @return HLL-estimated unique-prefix count, or 0 if unseen
   */
  public long uniqueKeys(final long rangeId) {
    final PerRange r = byRange.get(rangeId);
    return r == null ? 0L : Math.round(r.hll.getEstimate());
  }

  /**
   * All range IDs we have stats for.
   *
   * <p>The returned set is an unmodifiable live view: it reflects subsequent {@code observe(...)}
   * calls but cannot be mutated by callers. Iteration concurrent with {@code observe} is
   * unsupported.
   *
   * @return unmodifiable view of observed range IDs
   */
  public Set<Long> rangeIds() {
    return Collections.unmodifiableSet(byRange.keySet());
  }

  private static final class PerRange {
    long entries;
    final HllSketch hll = new HllSketch(LG_K);
  }
}
