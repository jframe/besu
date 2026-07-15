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

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class HistorySizeEstimateTest {

  private ChangeCountResult countsWithUpperChurn() {
    final ChangeCountResult r = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    // 1000 writes at depth 1, all sampled with lifetime spread so ~1/16 are checkpoints.
    for (int i = 0; i < 1000; i++) {
      r.recordMutation(1, false);
    }
    // 20 sampled distinct keys at depth 1, each mutated 50 times → ceil(50/16)=4 FULLs of 50 → 8%.
    for (int k = 0; k < 20; k++) {
      final Bytes key = Bytes.ofUnsignedInt(k);
      for (int w = 0; w < 50; w++) {
        r.recordSampledWrite(key, 1);
      }
    }
    return r;
  }

  @Test
  void loweringFullAboveDepthReducesEstimatedSize() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);

    final long atDepth2 = est.estimatedOnDiskBytes(2, 16); // depth 1 is upper-FULL
    final long atDepth0 = est.estimatedOnDiskBytes(0, 16); // depth 1 becomes checkpoint+diff

    assertThat(atDepth0).isLessThan(atDepth2);
  }

  @Test
  void sampledFullFractionMatchesCeilRatio() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);
    // 20 keys × ceil(50/16)=4 FULLs = 80 FULLs / (20×50=1000 writes) = 0.08.
    assertThat(est.sampledFullFraction(1, 16))
        .isCloseTo(0.08, org.assertj.core.api.Assertions.within(0.001));
  }

  @Test
  void leverTableHasOneRowPerDepthSetting() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);
    final long[][] table = est.leverTable(new int[] {0, 1, 2}, new int[] {8, 16, 32});
    assertThat(table).hasNumberOfRows(3);
    assertThat(table[0]).hasSize(3);
  }
}
