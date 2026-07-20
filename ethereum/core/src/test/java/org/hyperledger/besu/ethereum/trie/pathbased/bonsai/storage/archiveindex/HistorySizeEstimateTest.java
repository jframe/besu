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

import com.fasterxml.jackson.databind.JsonNode;
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
  void leafCountAtLatestEraUsesLastCumulativeEntryNotReSummed() {
    // leafCountByRange is ALREADY cumulative (produced by prefixSum in the subcommand): each entry
    // is the running distinct-leaf total as of that era. The latest era's count is simply the last
    // entry (12000), NOT the re-sum 1000+6000+18000 the pre-fix code produced.
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1000, 5000, 12000},
            1.93,
            1.44);
    assertThat(est.leafCountAtLatestEra()).isEqualTo(12000L);
  }

  @Test
  void leafCountAtLatestEraTakesLastNonZeroByPositionUnderNetDeletions() {
    // Net deletions make a later cumulative value SMALLER than an earlier one. The correct answer
    // is
    // the last non-zero entry BY POSITION (5000), not the maximum (8000) nor any re-sum.
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1000, 8000, 5000},
            1.93,
            1.44);
    assertThat(est.leafCountAtLatestEra()).isEqualTo(5000L);
  }

  @Test
  void renderTextHeadlineReflectsRequestedLevers() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);
    // Non-default levers: F=0 (depth 1 becomes checkpoint+diff), K=64. Headline must match these,
    // not the 2/16 defaults.
    final long expected = est.estimatedOnDiskBytes(0, 64);
    final long atDefaults = est.estimatedOnDiskBytes(2, 16);
    assertThat(expected).isNotEqualTo(atDefaults);

    final String text = est.renderText(0, 64);
    assertThat(text).contains("FULL_ABOVE_DEPTH=0");
    assertThat(text).contains("CHECKPOINT_INTERVAL=64");
    assertThat(text).contains(Long.toString(expected));
    assertThat(text).doesNotContain("headline (FULL_ABOVE_DEPTH=2, CHECKPOINT_INTERVAL=16)");
  }

  @Test
  void renderJsonHeadlineReflectsRequestedLevers() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);
    final long expected = est.estimatedOnDiskBytes(0, 64);
    final long atDefaults = est.estimatedOnDiskBytes(2, 16);
    assertThat(expected).isNotEqualTo(atDefaults);

    final JsonNode json = est.renderJson(0, 64);
    assertThat(json.get("headline").asLong()).isEqualTo(expected);
  }

  @Test
  void storageCorrectionScalesStorageWritesButNotAccountWrites() {
    // Split counts: 100 account and 100 storage node writes at depth 1.
    final ChangeCountResult counts = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    for (int i = 0; i < 100; i++) {
      counts.recordMutation(1, false);
      counts.recordCategoryMutation(1, true); // account
      counts.recordMutation(1, false);
      counts.recordCategoryMutation(1, false); // storage
    }

    // Correction 0.5 at depth 1: storage counts halve, account counts unchanged.
    final double[] correction = new double[ChangeCountResult.MAX_DEPTH];
    java.util.Arrays.fill(correction, 1.0);
    correction[1] = 0.5;

    final HistorySizeEstimate corrected =
        new HistorySizeEstimate(
            counts,
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44,
            correction);
    final HistorySizeEstimate uncorrected =
        new HistorySizeEstimate(
            counts,
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);

    // Corrected total at depth 1 = 100 account + 100 storage×0.5 = 150; uncorrected = 200.
    assertThat(corrected.correctedTotalWrites(1)).isEqualTo(150.0);
    assertThat(uncorrected.correctedTotalWrites(1)).isEqualTo(200.0);
    assertThat(corrected.estimatedOnDiskBytes(2, 16))
        .isLessThan(uncorrected.estimatedOnDiskBytes(2, 16));
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
