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

  private static EntrySizeTable tableWithUniformFullSizeAtDepth0(final double size) {
    final double[] fullBranch = new double[ChangeCountResult.MAX_DEPTH];
    final double[] fullShort = new double[ChangeCountResult.MAX_DEPTH];
    // Both shapes the same size at depth 0 so the routed bytes are independent of branchFraction.
    fullBranch[0] = size;
    fullShort[0] = size;
    return new EntrySizeTable(
        fullBranch,
        fullShort,
        new double[ChangeCountResult.MAX_DEPTH],
        new double[ChangeCountResult.MAX_DEPTH],
        0.0 /* keyBytes: isolate value bytes */);
  }

  @Test
  void blobOverheadAppliesOnlyToBlobEligibleValuesSubMinBlobUsesSstPath() {
    final int n = 1000;
    final ChangeCountResult counts = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    for (int i = 0; i < n; i++) {
      counts.recordMutation(0, false);
    }
    final double sst = 1.93;
    final double blob = 1.44;

    // 500B value (>= MIN_BLOB_SIZE 100) → blob file; 50B value (< 100) → inline SST.
    final HistorySizeEstimate big =
        new HistorySizeEstimate(
            counts,
            tableWithUniformFullSizeAtDepth0(500.0),
            new TrieShapeModel(16),
            new long[] {1_000_000_000L},
            sst,
            blob);
    final HistorySizeEstimate small =
        new HistorySizeEstimate(
            counts,
            tableWithUniformFullSizeAtDepth0(50.0),
            new TrieShapeModel(16),
            new long[] {1_000_000_000L},
            sst,
            blob);

    // fullAboveDepth=0 forces depth-0 writes FULL, so all n writes are FULL value bytes.
    assertThat(big.estimatedOnDiskBytes(0, 16)).isEqualTo(Math.round(n * 500.0 * blob));
    assertThat(small.estimatedOnDiskBytes(0, 16)).isEqualTo(Math.round(n * 50.0 / sst));
    // The fix matters: the sub-100B value is far cheaper than if blob overhead were applied to it.
    assertThat(small.estimatedOnDiskBytes(0, 16)).isLessThan(Math.round(n * 50.0 * blob));
  }

  @Test
  void depthTieredEquivalencesAndRootStaysFull() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);

    // A single tier of 1 clamps every depth to always-FULL == fullAboveDepth beyond every depth.
    assertThat(est.estimatedOnDiskBytesTiered(new int[] {1}))
        .isEqualTo(est.estimatedOnDiskBytes(ChangeCountResult.MAX_DEPTH, 16));
    // A single tier of 16 clamps every depth to interval 16 with no forced-FULL tier (F=-1, K=16).
    assertThat(est.estimatedOnDiskBytesTiered(new int[] {16}))
        .isEqualTo(est.estimatedOnDiskBytes(-1, 16));
    // The 2026-07-20 design tiers: depth-1 churn moves from forced-FULL (F=2) to interval 32, so
    // the
    // tiered estimate is strictly smaller than the current FULL_ABOVE_DEPTH=2, K=16 default.
    assertThat(est.estimatedOnDiskBytesTiered(new int[] {1, 32, 32, 16}))
        .isLessThan(est.estimatedOnDiskBytes(2, 16));
  }

  @Test
  void renderJsonIncludesDepthTieredSectionWhenRequested() {
    final HistorySizeEstimate est =
        new HistorySizeEstimate(
            countsWithUpperChurn(),
            EntrySizeTable.hoodiDefaults(),
            new TrieShapeModel(16),
            new long[] {1_000_000L},
            1.93,
            1.44);
    final int[] tiers = {1, 32, 32, 16};
    final JsonNode json = est.renderJson(2, 16, tiers);
    assertThat(json.get("depthTiered").get("onDiskBytes").asLong())
        .isEqualTo(est.estimatedOnDiskBytesTiered(tiers));
    assertThat(json.get("depthTiered").get("intervalByDepth").get(1).asInt()).isEqualTo(32);
    // Omitting the tiers (2-arg overload) leaves the section out.
    assertThat(est.renderJson(2, 16).has("depthTiered")).isFalse();
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
