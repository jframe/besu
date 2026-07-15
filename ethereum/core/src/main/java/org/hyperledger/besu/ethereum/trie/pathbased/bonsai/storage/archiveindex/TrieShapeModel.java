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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

/**
 * Analytic shape of a random {@code radix}-ary Merkle-Patricia trie whose keys are uniform hashes
 * (keccak account/slot hashes). Used by the size estimator to bound how deep each changed leaf's
 * node path runs, and to split branch vs short nodes per depth, without walking the real trie.
 *
 * <p>With {@code N} uniform keys, the expected number of keys sharing a given length-{@code d}
 * nibble prefix is {@code occupancy(d) = N * radix^-d}. A leaf is "alone" (unique) at depth {@code
 * d} with probability {@code exp(-occupancy(d))} (Poisson-0 approximation), so it terminates at
 * exactly depth {@code d} with probability {@code exp(-occupancy(d)) - exp(-occupancy(d-1))}.
 */
public final class TrieShapeModel {

  private final double logRadix;

  public TrieShapeModel(final int radix) {
    this.logRadix = Math.log(radix);
  }

  private double occupancy(final long leafCount, final int depth) {
    // N * radix^-depth
    return leafCount * Math.exp(-depth * logRadix);
  }

  private double probAloneAt(final long leafCount, final int depth) {
    if (depth < 0) {
      return 0.0; // cannot be unique before the root
    }
    return Math.exp(-occupancy(leafCount, depth));
  }

  public double expectedLeafDepth(final long leafCount) {
    if (leafCount <= 1) {
      return 0.0;
    }
    final double[] pmf = terminationDepthPmf(leafCount, 64);
    double expected = 0.0;
    for (int d = 0; d < pmf.length; d++) {
      expected += d * pmf[d];
    }
    return expected;
  }

  public double[] terminationDepthPmf(final long leafCount, final int maxDepth) {
    final double[] pmf = new double[maxDepth + 1];
    if (leafCount <= 1) {
      pmf[0] = 1.0;
      return pmf;
    }
    for (int d = 0; d <= maxDepth; d++) {
      pmf[d] = Math.max(0.0, probAloneAt(leafCount, d) - probAloneAt(leafCount, d - 1));
    }
    return pmf;
  }

  /**
   * Fraction of nodes at {@code depth} expected to be branch nodes (vs short/extension nodes). A
   * node at depth {@code d} is a branch when its subtree still holds ≥2 keys diverging at that
   * level, i.e. roughly when {@code occupancy(d) >= 2}. We map occupancy smoothly to [0,1].
   */
  public double branchFraction(final int depth, final long leafCount) {
    final double occ = occupancy(leafCount, depth);
    // occ >> 2 → all branches; occ << 1 → all short. Logistic in log-occupancy centred at occ≈1.5.
    return 1.0 / (1.0 + Math.exp(-(Math.log(occ + 1e-9) - Math.log(1.5))));
  }
}
