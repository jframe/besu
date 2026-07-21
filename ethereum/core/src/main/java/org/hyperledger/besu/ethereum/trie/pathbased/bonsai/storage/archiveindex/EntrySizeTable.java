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
 * Per-depth, per-shape byte-size constants used to convert {@link ChangeCountResult} write counts
 * into estimated bytes. A FULL entry's size depends on whether the node is a branch (many children,
 * larger) or a short/extension node (few, smaller); {@link #fullBytes} and {@link #diffBytes} blend
 * the two using the caller-supplied branch fraction from {@link TrieShapeModel#branchFraction}.
 */
public final class EntrySizeTable {

  private final double[] fullBranchBytesByDepth;
  private final double[] fullShortBytesByDepth;
  private final double[] diffBranchBytesByDepth;
  private final double[] diffShortBytesByDepth;
  private final double keyBytesPerEntry;

  public EntrySizeTable(
      final double[] fullBranchBytesByDepth,
      final double[] fullShortBytesByDepth,
      final double[] diffBranchBytesByDepth,
      final double[] diffShortBytesByDepth,
      final double keyBytesPerEntry) {
    this.fullBranchBytesByDepth = fullBranchBytesByDepth;
    this.fullShortBytesByDepth = fullShortBytesByDepth;
    this.diffBranchBytesByDepth = diffBranchBytesByDepth;
    this.diffShortBytesByDepth = diffShortBytesByDepth;
    this.keyBytesPerEntry = keyBytesPerEntry;
  }

  // Depths <= 2 are the "upper trie" (location.size() <= FULL_ABOVE_DEPTH's current default of 2),
  // forced FULL every write today; depths > 2 are the "checkpoint" bucket, FULL only once per
  // CHECKPOINT_INTERVAL. The hoodi composition scan (2026-07-14-trie-node-history-storage-
  // analysis.md §2) only distinguishes these two buckets, not individual depths within them, so
  // hoodiDefaults() applies one constant across each bucket.
  private static final int UPPER_TRIE_MAX_DEPTH = 2;

  // 434 GiB LogicalValue / 914,375,407 entries (analysis.md §2, UPPER_TRIE_BRANCH row).
  private static final double UPPER_TRIE_BRANCH_FULL_BYTES = 509.64;
  // 1 GiB LogicalValue / 34,572,307 entries (analysis.md §2, UPPER_TRIE_SHORT row).
  private static final double UPPER_TRIE_SHORT_FULL_BYTES = 31.06;
  // 71 GiB LogicalValue / 341,960,724 entries (analysis.md §2, CHECKPOINT_BRANCH row; also cited
  // as "the ~223 B average" in §8.3).
  private static final double CHECKPOINT_BRANCH_FULL_BYTES = 222.94;
  // 25 GiB LogicalValue / 468,917,527 entries (analysis.md §2, CHECKPOINT_SHORT row).
  private static final double CHECKPOINT_SHORT_FULL_BYTES = 57.25;
  // 49 GiB LogicalValue / 1,445,765,010 entries (analysis.md §2, DIFF_BRANCH row; ~36 B/entry is
  // also the figure used in §3.2 and §8.4). Diff bucket has no depth breakdown in the scan, so the
  // same constant is used at every depth.
  private static final double DIFF_BRANCH_BYTES = 36.39;
  // 13 GiB LogicalValue / 318,796,822 entries (analysis.md §2, DIFF_SHORT row).
  private static final double DIFF_SHORT_BYTES = 43.79;
  // 108 GiB total key bytes / 3,524,387,797 entries (analysis.md §3.3 "Logical (scanned): Keys 108
  // GiB"; also "33 B avg x 3.5B entries" in §8.0).
  private static final double KEY_BYTES_PER_ENTRY = 32.90;

  /**
   * Embedded constants derived from the hoodi composition scan documented in
   * 2026-07-14-trie-node-history-storage-analysis.md. Precision isn't critical here: this is a
   * fallback default, superseded by measured constants from calibration (a later task) whenever
   * they're available.
   */
  public static EntrySizeTable hoodiDefaults() {
    final int maxDepth = ChangeCountResult.MAX_DEPTH;
    final double[] fullBranch = new double[maxDepth];
    final double[] fullShort = new double[maxDepth];
    final double[] diffBranch = new double[maxDepth];
    final double[] diffShort = new double[maxDepth];
    for (int d = 0; d < maxDepth; d++) {
      final boolean upperTrie = d <= UPPER_TRIE_MAX_DEPTH;
      fullBranch[d] = upperTrie ? UPPER_TRIE_BRANCH_FULL_BYTES : CHECKPOINT_BRANCH_FULL_BYTES;
      fullShort[d] = upperTrie ? UPPER_TRIE_SHORT_FULL_BYTES : CHECKPOINT_SHORT_FULL_BYTES;
      diffBranch[d] = DIFF_BRANCH_BYTES;
      diffShort[d] = DIFF_SHORT_BYTES;
    }
    return new EntrySizeTable(fullBranch, fullShort, diffBranch, diffShort, KEY_BYTES_PER_ENTRY);
  }

  public double fullBytes(final int depth, final double branchFraction) {
    final int d = clampDepth(depth, fullBranchBytesByDepth.length);
    return branchFraction * fullBranchBytesByDepth[d]
        + (1.0 - branchFraction) * fullShortBytesByDepth[d];
  }

  public double diffBytes(final int depth, final double branchFraction) {
    final int d = clampDepth(depth, diffBranchBytesByDepth.length);
    return branchFraction * diffBranchBytesByDepth[d]
        + (1.0 - branchFraction) * diffShortBytesByDepth[d];
  }

  /** Mean FULL <em>branch</em>-node value size at {@code depth}. */
  public double fullBranchBytes(final int depth) {
    return fullBranchBytesByDepth[clampDepth(depth, fullBranchBytesByDepth.length)];
  }

  /** Mean FULL <em>short</em>-node value size at {@code depth}. */
  public double fullShortBytes(final int depth) {
    return fullShortBytesByDepth[clampDepth(depth, fullShortBytesByDepth.length)];
  }

  /** Mean DIFF <em>branch</em>-node value size at {@code depth}. */
  public double diffBranchBytes(final int depth) {
    return diffBranchBytesByDepth[clampDepth(depth, diffBranchBytesByDepth.length)];
  }

  /** Mean DIFF <em>short</em>-node value size at {@code depth}. */
  public double diffShortBytes(final int depth) {
    return diffShortBytesByDepth[clampDepth(depth, diffShortBytesByDepth.length)];
  }

  public double keyBytes() {
    return keyBytesPerEntry;
  }

  private static int clampDepth(final int depth, final int arrayLength) {
    return Math.min(Math.max(depth, 0), arrayLength - 1);
  }
}
