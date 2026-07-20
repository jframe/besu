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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Joins {@link ChangeCountResult} write counts with {@link EntrySizeTable} byte sizes and {@link
 * TrieShapeModel} branch/short shape to estimate the on-disk size of {@code
 * TRIE_NODE_HISTORY_ARCHIVE}, and to price the {@code FULL_ABOVE_DEPTH} / {@code
 * CHECKPOINT_INTERVAL} levers against it.
 *
 * <p>Per depth {@code d}, given {@code fullAboveDepth=F} and {@code checkpointInterval=K}:
 *
 * <pre>
 * totalWrites  = counts.mutationsByDepth()[d]
 * branchFrac   = shape.branchFraction(d, leafCountAtLatestEra)
 * fullFrac     = (d <= F) ? 1.0 : sampledFullFraction(d, K)
 * fullWrites   = totalWrites * fullFrac
 * diffWrites   = totalWrites - fullWrites
 * valueBytes   = fullWrites * sizes.fullBytes(d, branchFrac) + diffWrites * sizes.diffBytes(d, branchFrac)
 * keyBytes     = totalWrites * sizes.keyBytes()
 * onDisk       = keyBytes / sstCompressionRatio + valueBytes * blobOverheadRatio
 * </pre>
 *
 * summed over all depths.
 */
public final class HistorySizeEstimate {

  // Representative lever sweep for the text/JSON renderers, informed by analysis.md §5 Lever 1
  // (FULL_ABOVE_DEPTH candidates 0/1/2) and §8.1 (CHECKPOINT_INTERVAL candidates 16/64/128).
  private static final int[] DEFAULT_SWEEP_FULL_ABOVE_DEPTHS = {0, 1, 2};
  private static final int[] DEFAULT_SWEEP_CHECKPOINT_INTERVALS = {16, 64, 128};

  private final ChangeCountResult counts;
  private final EntrySizeTable sizes;
  private final TrieShapeModel shape;
  private final long[] leafCountByRange;
  private final double sstCompressionRatio;
  private final double blobOverheadRatio;

  public HistorySizeEstimate(
      final ChangeCountResult counts,
      final EntrySizeTable sizes,
      final TrieShapeModel shape,
      final long[] leafCountByRange,
      final double sstCompressionRatio,
      final double blobOverheadRatio) {
    this.counts = counts;
    this.sizes = sizes;
    this.shape = shape;
    this.leafCountByRange = leafCountByRange;
    this.sstCompressionRatio = sstCompressionRatio;
    this.blobOverheadRatio = blobOverheadRatio;
  }

  public long estimatedOnDiskBytes(final int fullAboveDepth, final int checkpointInterval) {
    final long leafCount = leafCountAtLatestEra();
    final long[] mutationsByDepth = counts.mutationsByDepth();
    double totalKeyBytes = 0;
    double totalValueBytes = 0;
    for (int d = 0; d < mutationsByDepth.length; d++) {
      final long totalWrites = mutationsByDepth[d];
      if (totalWrites == 0) {
        continue;
      }
      final double branchFraction = shape.branchFraction(d, leafCount);
      final double fullFraction =
          d <= fullAboveDepth ? 1.0 : sampledFullFraction(d, checkpointInterval);
      final double fullWrites = totalWrites * fullFraction;
      final double diffWrites = totalWrites - fullWrites;
      totalValueBytes +=
          fullWrites * sizes.fullBytes(d, branchFraction)
              + diffWrites * sizes.diffBytes(d, branchFraction);
      totalKeyBytes += totalWrites * sizes.keyBytes();
    }
    return Math.round(totalKeyBytes / sstCompressionRatio + totalValueBytes * blobOverheadRatio);
  }

  public long[][] leverTable(final int[] fullAboveDepths, final int[] checkpointIntervals) {
    final long[][] table = new long[fullAboveDepths.length][checkpointIntervals.length];
    for (int i = 0; i < fullAboveDepths.length; i++) {
      for (int j = 0; j < checkpointIntervals.length; j++) {
        table[i][j] = estimatedOnDiskBytes(fullAboveDepths[i], checkpointIntervals[j]);
      }
    }
    return table;
  }

  /**
   * Ratio estimator {@code Σ ceil(m/K) / Σ m} over sampled keys at {@code depth}, where {@code m}
   * is each key's lifetime write count. This is unbiased for a genuine hash sample; for
   * account-trie depths <= 2 the sample is actually exhaustive (see {@code TrieLogChangeCounter}'s
   * {@code EXACT_ACCOUNT_TRACKING_MAX_DEPTH}), so at those depths the same formula yields an exact
   * answer rather than an estimate.
   *
   * <p>Depths {@code <= fullAboveDepth} never reach the fallback below: the caller ({@link
   * #estimatedOnDiskBytes}) treats them as fullFrac=1.0 directly, without calling this method.
   */
  public double sampledFullFraction(final int depth, final int checkpointInterval) {
    long sumWrites = 0;
    long sumFulls = 0;
    for (final int[] lifetime : counts.sampledLifetime().values()) {
      if (lifetime[0] == depth) {
        final int writes = lifetime[1];
        sumWrites += writes;
        sumFulls += (writes + checkpointInterval - 1) / checkpointInterval;
      }
    }
    if (sumWrites == 0) {
      return (1.0 / checkpointInterval) + globalCreationRate();
    }
    return sumFulls / (double) sumWrites;
  }

  /**
   * Rough proxy for the fraction of writes that are first-appearances (which must be FULL
   * regardless of checkpoint schedule), used only when a depth has zero samples. Approximated as
   * net new leaves (positive {@code accountDeltaByRange} entries) over total trie-node writes
   * across all depths.
   */
  private double globalCreationRate() {
    long totalMutations = 0;
    for (final long m : counts.mutationsByDepth()) {
      totalMutations += m;
    }
    if (totalMutations == 0) {
      return 0.0;
    }
    long netNewLeaves = 0;
    for (final long delta : counts.accountDeltaByRange()) {
      if (delta > 0) {
        netNewLeaves += delta;
      }
    }
    return Math.min(1.0, netNewLeaves / (double) totalMutations);
  }

  long leafCountAtLatestEra() {
    // leafCountByRange is already cumulative (per-era running totals), so the latest era's leaf
    // count is the last non-zero entry BY POSITION — not a re-sum, and not the maximum (net
    // deletions can make a later cumulative value smaller than an earlier one).
    long lastNonZero = 0;
    for (final long cumulative : leafCountByRange) {
      if (cumulative != 0) {
        lastNonZero = cumulative;
      }
    }
    return lastNonZero;
  }

  public String renderText(final int fullAboveDepth, final int checkpointInterval) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Estimated on-disk TRIE_NODE_HISTORY_ARCHIVE size\n");
    sb.append(
        String.format(
            "  headline (FULL_ABOVE_DEPTH=%d, CHECKPOINT_INTERVAL=%d): %d bytes%n",
            fullAboveDepth,
            checkpointInterval,
            estimatedOnDiskBytes(fullAboveDepth, checkpointInterval)));
    sb.append("  lever sweep (fullAboveDepth x checkpointInterval), on-disk bytes:\n");
    final long[][] table =
        leverTable(DEFAULT_SWEEP_FULL_ABOVE_DEPTHS, DEFAULT_SWEEP_CHECKPOINT_INTERVALS);
    for (int i = 0; i < DEFAULT_SWEEP_FULL_ABOVE_DEPTHS.length; i++) {
      sb.append(String.format("    F=%d: ", DEFAULT_SWEEP_FULL_ABOVE_DEPTHS[i]));
      for (int j = 0; j < DEFAULT_SWEEP_CHECKPOINT_INTERVALS.length; j++) {
        sb.append(
            String.format("K=%d -> %d  ", DEFAULT_SWEEP_CHECKPOINT_INTERVALS[j], table[i][j]));
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  public JsonNode renderJson(final int fullAboveDepth, final int checkpointInterval) {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode root = mapper.createObjectNode();

    final long leafCount = leafCountAtLatestEra();
    final long[] mutationsByDepth = counts.mutationsByDepth();
    final ObjectNode perDepth = root.putObject("perDepth");
    double totalFullBytes = 0;
    double totalDiffBytes = 0;
    double totalKeyBytes = 0;
    for (int d = 0; d < mutationsByDepth.length; d++) {
      final long totalWrites = mutationsByDepth[d];
      if (totalWrites == 0) {
        continue;
      }
      final double branchFraction = shape.branchFraction(d, leafCount);
      final double fullFraction =
          d <= fullAboveDepth ? 1.0 : sampledFullFraction(d, checkpointInterval);
      final double fullWrites = totalWrites * fullFraction;
      final double diffWrites = totalWrites - fullWrites;
      final double fullBytes = fullWrites * sizes.fullBytes(d, branchFraction);
      final double diffBytes = diffWrites * sizes.diffBytes(d, branchFraction);
      final double keyBytes = totalWrites * sizes.keyBytes();

      final ObjectNode depthNode = perDepth.putObject(Integer.toString(d));
      depthNode.put("totalWrites", totalWrites);
      depthNode.put("accountWrites", counts.accountMutationsByDepth()[d]);
      depthNode.put("storageWrites", counts.storageMutationsByDepth()[d]);
      depthNode.put("branchFraction", branchFraction);
      depthNode.put("fullFraction", fullFraction);
      depthNode.put("logicalBytes", fullBytes + diffBytes + keyBytes);

      totalFullBytes += fullBytes;
      totalDiffBytes += diffBytes;
      totalKeyBytes += keyBytes;
    }

    final ObjectNode byCategory = root.putObject("byCategory");
    byCategory.put("fullBytes", totalFullBytes);
    byCategory.put("diffBytes", totalDiffBytes);
    byCategory.put("keyBytes", totalKeyBytes);

    final long[][] table =
        leverTable(DEFAULT_SWEEP_FULL_ABOVE_DEPTHS, DEFAULT_SWEEP_CHECKPOINT_INTERVALS);
    final ArrayNode leverTableNode = root.putArray("leverTable");
    for (int i = 0; i < DEFAULT_SWEEP_FULL_ABOVE_DEPTHS.length; i++) {
      final ObjectNode rowNode = leverTableNode.addObject();
      rowNode.put("fullAboveDepth", DEFAULT_SWEEP_FULL_ABOVE_DEPTHS[i]);
      final ObjectNode byInterval = rowNode.putObject("onDiskBytesByCheckpointInterval");
      for (int j = 0; j < DEFAULT_SWEEP_CHECKPOINT_INTERVALS.length; j++) {
        byInterval.put(Integer.toString(DEFAULT_SWEEP_CHECKPOINT_INTERVALS[j]), table[i][j]);
      }
    }

    addEraDiagnostics(root.putObject("diagnostics"));

    root.put("headline", estimatedOnDiskBytes(fullAboveDepth, checkpointInterval));
    return root;
  }

  /**
   * Per-era (100k-block) attribution: node writes split account vs storage, and the mean
   * per-contract storage-trie leaf count actually used to price storage depth that era. If the
   * storage over-counting is concentrated in early eras whose mean assumed leaf count is
   * nonetheless head-scale, that confirms the live-trie probe is pricing early history at
   * head-state contract sizes (contracts that grew over time).
   */
  private void addEraDiagnostics(final ObjectNode diagnostics) {
    final long[] accountByEra = counts.accountWritesByRange();
    final long[] storageByEra = counts.storageWritesByRange();
    final long[] leafSumByEra = counts.assumedStorageLeafCountSumByRange();
    final long[] groupsByEra = counts.storageContractGroupsByRange();
    final int eras =
        Math.max(
            Math.max(accountByEra.length, storageByEra.length),
            Math.max(leafSumByEra.length, groupsByEra.length));
    final ArrayNode byEra = diagnostics.putArray("byEra");
    for (int e = 0; e < eras; e++) {
      final long account = e < accountByEra.length ? accountByEra[e] : 0;
      final long storage = e < storageByEra.length ? storageByEra[e] : 0;
      if (account == 0 && storage == 0) {
        continue;
      }
      final long groups = e < groupsByEra.length ? groupsByEra[e] : 0;
      final long leafSum = e < leafSumByEra.length ? leafSumByEra[e] : 0;
      final ObjectNode eraNode = byEra.addObject();
      eraNode.put("era", e);
      eraNode.put("firstBlock", e * ChangeCountResult.RANGE_BLOCKS);
      eraNode.put("accountWrites", account);
      eraNode.put("storageWrites", storage);
      eraNode.put(
          "meanAssumedStorageLeafCount", groups == 0 ? 0.0 : (double) leafSum / (double) groups);
    }
  }
}
