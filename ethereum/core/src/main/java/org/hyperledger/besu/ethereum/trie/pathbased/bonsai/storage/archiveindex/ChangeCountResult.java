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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;

/**
 * Mergeable accumulator of trie-node write counts produced by {@link TrieLogChangeCounter}. One
 * instance per parallel worker; {@link #merge(ChangeCountResult)} combines them exactly (workers
 * count disjoint block ranges, so per-key lifetime write counts sum).
 */
public final class ChangeCountResult {

  public static final int MAX_DEPTH = 65; // depths 0..64, index 64 = overflow bucket
  public static final long RANGE_BLOCKS = 100_000L;

  private final long[] mutationsByDepth;
  private final long[] upperFullByDepth;
  private final long[] deletionsByDepth;
  // Diagnostic-only: the combined mutationsByDepth split by which trie the node lives in, to
  // attribute per-depth over/under-counting between the account trie and per-contract storage
  // tries.
  private final long[] accountMutationsByDepth;
  private final long[] storageMutationsByDepth;
  private final Map<Bytes, int[]> sampledLifetime = new HashMap<>();
  private long[] accountDeltaByRange = new long[1];
  // Diagnostic-only per-era (100k-block) totals: node writes attributed to each trie, plus the sum
  // and population of the per-contract storage leaf counts actually used to price storage depth.
  // The average assumed storage-trie size per era (sum/population) reveals whether early history is
  // being priced at head-state contract sizes (the head-vs-historical over-counting hypothesis).
  private long[] accountWritesByRange = new long[1];
  private long[] storageWritesByRange = new long[1];
  private long[] assumedStorageLeafCountSumByRange = new long[1];
  private long[] storageContractGroupsByRange = new long[1];

  public ChangeCountResult(final int maxDepth) {
    this.mutationsByDepth = new long[maxDepth];
    this.upperFullByDepth = new long[maxDepth];
    this.deletionsByDepth = new long[maxDepth];
    this.accountMutationsByDepth = new long[maxDepth];
    this.storageMutationsByDepth = new long[maxDepth];
  }

  public void recordMutation(final int depth, final boolean deletion) {
    final int d = Math.min(depth, mutationsByDepth.length - 1);
    mutationsByDepth[d]++;
    if (deletion) {
      deletionsByDepth[d]++;
    }
  }

  /**
   * Diagnostic split of {@link #recordMutation} by owning trie (account vs per-contract storage).
   */
  public void recordCategoryMutation(final int depth, final boolean isAccountPath) {
    final int d = Math.min(depth, mutationsByDepth.length - 1);
    if (isAccountPath) {
      accountMutationsByDepth[d]++;
    } else {
      storageMutationsByDepth[d]++;
    }
  }

  /** Diagnostic per-era tally of total node writes attributed to each trie for a block. */
  public void recordCategoryWritesForEra(
      final long blockNumber, final long accountWrites, final long storageWrites) {
    final int range = (int) (blockNumber / RANGE_BLOCKS);
    accountWritesByRange = ensureRange(accountWritesByRange, range);
    storageWritesByRange = ensureRange(storageWritesByRange, range);
    accountWritesByRange[range] += accountWrites;
    storageWritesByRange[range] += storageWrites;
  }

  /**
   * Diagnostic: record that one contract's storage trie was priced at {@code leafCount} slots in a
   * block within the block's era. Accumulates the sum and population so a per-era mean assumed
   * storage-trie size can be reported.
   */
  public void recordAssumedStorageLeafCount(final long blockNumber, final long leafCount) {
    final int range = (int) (blockNumber / RANGE_BLOCKS);
    assumedStorageLeafCountSumByRange = ensureRange(assumedStorageLeafCountSumByRange, range);
    storageContractGroupsByRange = ensureRange(storageContractGroupsByRange, range);
    assumedStorageLeafCountSumByRange[range] += leafCount;
    storageContractGroupsByRange[range]++;
  }

  private static long[] ensureRange(final long[] array, final int range) {
    return range < array.length ? array : Arrays.copyOf(array, range + 1);
  }

  public void recordUpperFull(final int depth) {
    upperFullByDepth[Math.min(depth, upperFullByDepth.length - 1)]++;
  }

  public void recordSampledWrite(final Bytes naturalKey, final int depth) {
    sampledLifetime.compute(
        naturalKey,
        (k, v) -> {
          if (v == null) {
            return new int[] {depth, 1};
          }
          v[1]++;
          return v;
        });
  }

  public void recordAccountDelta(final long blockNumber, final int delta) {
    final int range = (int) (blockNumber / RANGE_BLOCKS);
    if (range >= accountDeltaByRange.length) {
      accountDeltaByRange = Arrays.copyOf(accountDeltaByRange, range + 1);
    }
    accountDeltaByRange[range] += delta;
  }

  public void merge(final ChangeCountResult other) {
    for (int i = 0; i < mutationsByDepth.length; i++) {
      mutationsByDepth[i] += other.mutationsByDepth[i];
      upperFullByDepth[i] += other.upperFullByDepth[i];
      deletionsByDepth[i] += other.deletionsByDepth[i];
      accountMutationsByDepth[i] += other.accountMutationsByDepth[i];
      storageMutationsByDepth[i] += other.storageMutationsByDepth[i];
    }
    other.sampledLifetime.forEach(
        (key, val) -> sampledLifetime.merge(key, val, (a, b) -> new int[] {a[0], a[1] + b[1]}));
    accountDeltaByRange = mergeRange(accountDeltaByRange, other.accountDeltaByRange);
    accountWritesByRange = mergeRange(accountWritesByRange, other.accountWritesByRange);
    storageWritesByRange = mergeRange(storageWritesByRange, other.storageWritesByRange);
    assumedStorageLeafCountSumByRange =
        mergeRange(assumedStorageLeafCountSumByRange, other.assumedStorageLeafCountSumByRange);
    storageContractGroupsByRange =
        mergeRange(storageContractGroupsByRange, other.storageContractGroupsByRange);
  }

  private static long[] mergeRange(final long[] into, final long[] from) {
    final long[] target = into.length >= from.length ? into : Arrays.copyOf(into, from.length);
    for (int i = 0; i < from.length; i++) {
      target[i] += from[i];
    }
    return target;
  }

  public long[] mutationsByDepth() {
    return mutationsByDepth;
  }

  public long[] upperFullByDepth() {
    return upperFullByDepth;
  }

  public long[] deletionsByDepth() {
    return deletionsByDepth;
  }

  public Map<Bytes, int[]> sampledLifetime() {
    return sampledLifetime;
  }

  public long[] accountDeltaByRange() {
    return accountDeltaByRange;
  }

  public long[] accountMutationsByDepth() {
    return accountMutationsByDepth;
  }

  public long[] storageMutationsByDepth() {
    return storageMutationsByDepth;
  }

  public long[] accountWritesByRange() {
    return accountWritesByRange;
  }

  public long[] storageWritesByRange() {
    return storageWritesByRange;
  }

  public long[] assumedStorageLeafCountSumByRange() {
    return assumedStorageLeafCountSumByRange;
  }

  public long[] storageContractGroupsByRange() {
    return storageContractGroupsByRange;
  }
}
