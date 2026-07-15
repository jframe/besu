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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class TrieShapeModelTest {

  private final TrieShapeModel model = new TrieShapeModel(16);

  @Test
  void expectedLeafDepth_isAboutLog16OfLeafCount() {
    // 16^7 ≈ 2.7e8 accounts → expected unique-prefix depth ≈ 7 nibbles.
    assertThat(model.expectedLeafDepth(268_435_456L)).isCloseTo(7.0, within(1.0));
    // Tiny trie: a handful of keys become unique almost immediately.
    assertThat(model.expectedLeafDepth(16L)).isCloseTo(1.0, within(1.0));
  }

  @Test
  void terminationDepthPmf_sumsToApproximatelyOne() {
    final double[] pmf = model.terminationDepthPmf(1_000_000L, 64);
    double sum = 0;
    for (final double p : pmf) {
      sum += p;
    }
    assertThat(sum).isCloseTo(1.0, within(0.02));
  }

  @Test
  void terminationDepthPmf_peaksNearExpectedDepth() {
    final long n = 1_000_000L; // log16(1e6) ≈ 4.98
    final double[] pmf = model.terminationDepthPmf(n, 64);
    int argmax = 0;
    for (int d = 1; d < pmf.length; d++) {
      if (pmf[d] > pmf[argmax]) {
        argmax = d;
      }
    }
    assertThat(argmax).isBetween(4, 6);
  }

  @Test
  void branchFraction_isHighAboveLeafDepthAndLowBelow() {
    final long n = 1_000_000L; // expected leaf depth ≈ 5
    assertThat(model.branchFraction(2, n)).isGreaterThan(0.9); // shallow: dense, all branches
    assertThat(model.branchFraction(10, n)).isLessThan(0.5); // deep: sparse, mostly short nodes
  }
}
