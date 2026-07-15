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
  private final Map<Bytes, int[]> sampledLifetime = new HashMap<>();
  private long[] accountDeltaByRange = new long[1];

  public ChangeCountResult(final int maxDepth) {
    this.mutationsByDepth = new long[maxDepth];
    this.upperFullByDepth = new long[maxDepth];
    this.deletionsByDepth = new long[maxDepth];
  }

  public void recordMutation(final int depth, final boolean deletion) {
    final int d = Math.min(depth, mutationsByDepth.length - 1);
    mutationsByDepth[d]++;
    if (deletion) {
      deletionsByDepth[d]++;
    }
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
    }
    other.sampledLifetime.forEach(
        (key, val) -> sampledLifetime.merge(key, val, (a, b) -> new int[] {a[0], a[1] + b[1]}));
    if (other.accountDeltaByRange.length > accountDeltaByRange.length) {
      accountDeltaByRange = Arrays.copyOf(accountDeltaByRange, other.accountDeltaByRange.length);
    }
    for (int i = 0; i < other.accountDeltaByRange.length; i++) {
      accountDeltaByRange[i] += other.accountDeltaByRange[i];
    }
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
}
