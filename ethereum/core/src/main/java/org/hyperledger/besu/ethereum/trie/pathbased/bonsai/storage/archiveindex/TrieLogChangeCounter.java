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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.TrieNodePathEnumerator;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decodes a single {@link TrieLog} into per-depth trie-node write counts for the archive history
 * size estimator. Mirrors the migrator's {@code persist()} walk: every changed leaf touches the
 * nodes on its root→leaf path, deduped per block. See the design doc for the counting model.
 */
public final class TrieLogChangeCounter {

  private static final int ACCOUNT_HASH_BYTES = 32;
  private static final int SLACK = 3;

  // Account-trie paths at depth <= 2 have only 1 + 16 + 256 = 273 distinct possible values, so
  // it's cheap to track every one of them exactly instead of hash-sampling. This prices the
  // dominant upper-trie bucket exactly for any FULL_ABOVE_DEPTH / interval combination. Storage
  // trie shallow nodes are per-contract and unbounded, so they stay on the hash sample.
  private static final int EXACT_ACCOUNT_TRACKING_MAX_DEPTH = 2;

  private final int fullAboveDepth;
  private final int sampleShift;
  private final TrieShapeModel shapeModel;

  public TrieLogChangeCounter(
      final int fullAboveDepth, final int sampleShift, final TrieShapeModel shapeModel) {
    this.fullAboveDepth = fullAboveDepth;
    this.sampleShift = sampleShift;
    this.shapeModel = shapeModel;
  }

  int terminationCap(final long leafCount) {
    final int cap = (int) Math.ceil(shapeModel.expectedLeafDepth(leafCount)) + SLACK;
    return Math.min(64, Math.max(1, cap));
  }

  /**
   * Derives a deterministic value in {@code [0,1)} from a key's own hash bytes: no actual
   * randomness, just a stable per-key seed (same technique {@link java.util.Random#nextDouble()}
   * uses to turn 53 bits into a uniform double). Since account/slot hashes are already
   * keccak-uniform, any consistent byte slice of the hash preserves that uniformity.
   */
  static double uniformFromHash(final Bytes hash) {
    final int size = hash.size();
    long bits = 0L;
    for (int i = size - 8; i < size; i++) {
      bits = (bits << 8) | (hash.get(i) & 0xFFL);
    }
    return (bits >>> 11) / (double) (1L << 53);
  }

  /**
   * Draws the depth at which a real trie would terminate this key's path, per {@link
   * TrieShapeModel#terminationDepthPmf}. The draw is seeded by the key's own full hash (not a path
   * prefix) so every prefix along the key's path agrees on the same termination depth — a key's
   * real termination depth is one fixed property of that key, not of a truncated view of it.
   */
  int sampledTerminationDepth(final Bytes fullKeyHash, final long leafCount, final int maxDepth) {
    final double[] pmf = shapeModel.terminationDepthPmf(leafCount, maxDepth);
    final double u = uniformFromHash(fullKeyHash);
    double cumulative = 0.0;
    for (int d = 0; d <= maxDepth; d++) {
      cumulative += pmf[d];
      if (cumulative > u) {
        return d;
      }
    }
    return maxDepth; // rounding-residue fallback: pmf mass didn't fully reach 1 by maxDepth.
  }

  boolean isSampled(final Bytes naturalKey) {
    if (sampleShift <= 0) {
      return true;
    }
    final int mask = (1 << sampleShift) - 1;
    // Use the trailing bytes of the natural key (uniform hash) as the sampling hash. Shallow
    // paths (root, depth 1) are shorter than two bytes, so missing bytes contribute zero.
    final int size = naturalKey.size();
    final int last = size >= 1 ? naturalKey.get(size - 1) & 0xFF : 0;
    final int secondLast = size >= 2 ? naturalKey.get(size - 2) & 0xFF : 0;
    final int h = last | (secondLast << 8);
    return (h & mask) == 0;
  }

  /**
   * Convenience overload that prices storage-trie paths against the same {@code leafCountForEra} as
   * the account trie. Retained for tests and callers that don't model per-contract storage sizes;
   * production estimation should use {@link #countBlock(TrieLog, long, long,
   * StorageTrieLeafCountProvider, ChangeCountResult)} with a real provider, since a global account
   * leaf count drives storage-slot paths far too deep.
   */
  public void countBlock(
      final TrieLog trieLog,
      final long blockNumber,
      final long leafCountForEra,
      final ChangeCountResult out) {
    countBlock(trieLog, blockNumber, leafCountForEra, accountHash -> leafCountForEra, out);
  }

  /**
   * Counts a block's per-depth trie-node writes, drawing account-trie path depths from {@code
   * accountLeafCountForEra} (the global account-trie leaf count) and each storage-trie's path
   * depths from that contract's own leaf count supplied by {@code storageLeafCounts}. Storage tries
   * are per-contract and typically far smaller than the account trie, so using the global count
   * here is the dominant source of over-counting; a per-contract count makes each slot's path
   * terminate at the depth its real (compacted) storage trie would.
   */
  public void countBlock(
      final TrieLog trieLog,
      final long blockNumber,
      final long accountLeafCountForEra,
      final StorageTrieLeafCountProvider storageLeafCounts,
      final ChangeCountResult out) {
    // Per-trie safety ceilings: how far a path could possibly be expanded. The actual per-key limit
    // is drawn per-key below, since a real trie only has a node at each key's own termination depth
    // (path compaction), not at every depth up to a shared cap.
    final int accountCap = terminationCap(accountLeafCountForEra);
    final Set<Bytes> accountPaths = new LinkedHashSet<>();
    final Set<Bytes> storagePaths = new LinkedHashSet<>();

    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              final int keyDepthLimit =
                  Math.min(
                      accountCap,
                      sampledTerminationDepth(accountHash, accountLeafCountForEra, accountCap));
              TrieNodePathEnumerator.addLocationPrefixes(
                  TrieNodePathEnumerator.toNibbles(accountHash), keyDepthLimit, null, accountPaths);
              if (change.getPrior() == null && change.getUpdated() != null) {
                out.recordAccountDelta(blockNumber, 1);
              } else if (change.getUpdated() == null && change.getPrior() != null) {
                out.recordAccountDelta(blockNumber, -1);
              }
            });

    trieLog
        .getStorageChanges()
        .forEach(
            (address, slotMap) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              // Floor the probed head-state slot count at the number of slots this block touches: a
              // block cannot change more distinct slots than the trie holds, so this guards against
              // under-counting depth for accounts whose slots were pruned/destructed before head.
              final long storageLeafCount =
                  Math.max(slotMap.size(), storageLeafCounts.leafCount(accountHash));
              final int storageCap = terminationCap(storageLeafCount);
              slotMap.forEach(
                  (slotKey, change) -> {
                    final Bytes slotHash = slotKey.getSlotHash().getBytes();
                    final int keyDepthLimit =
                        Math.min(
                            storageCap,
                            sampledTerminationDepth(slotHash, storageLeafCount, storageCap));
                    TrieNodePathEnumerator.addLocationPrefixes(
                        TrieNodePathEnumerator.toNibbles(slotHash),
                        keyDepthLimit,
                        accountHash,
                        storagePaths);
                  });
            });

    for (final Bytes path : accountPaths) {
      recordPath(path, path.size(), true, out);
    }
    for (final Bytes path : storagePaths) {
      recordPath(path, path.size() - ACCOUNT_HASH_BYTES, false, out);
    }
  }

  private void recordPath(
      final Bytes naturalKey,
      final int depth,
      final boolean isAccountPath,
      final ChangeCountResult out) {
    out.recordMutation(depth, false);
    if (depth <= fullAboveDepth) {
      out.recordUpperFull(depth);
    }
    if ((isAccountPath && depth <= EXACT_ACCOUNT_TRACKING_MAX_DEPTH) || isSampled(naturalKey)) {
      out.recordSampledWrite(naturalKey, depth);
    }
  }
}
