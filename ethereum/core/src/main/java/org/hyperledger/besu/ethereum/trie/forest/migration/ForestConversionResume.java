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
package org.hyperledger.besu.ethereum.trie.forest.migration;

import org.hyperledger.besu.datatypes.Hash;

import java.util.function.LongFunction;
import java.util.function.Predicate;

/**
 * Determines where a Forest conversion should resume by probing which blocks' account-state root
 * nodes are already present in the Forest storage. Because Forest nodes are content-addressed,
 * presence of a block's state-root node is a reliable marker that the block was committed.
 */
public final class ForestConversionResume {

  private ForestConversionResume() {}

  /**
   * Returns the largest block number {@code K} in {@code [0, head]} whose state root is present in
   * Forest storage. {@code 0} means only genesis is present, so replay should start at block 1.
   *
   * <p>A binary search locates the boundary assuming presence is monotonic (blocks 1..K committed,
   * the rest not). A forward scan then extends past any coincidental state-root reuse so the result
   * is never below the true highest committed block.
   *
   * @param head the chain head block number
   * @param stateRootByBlock maps a block number to its canonical state root; block 0 is genesis
   * @param rootPresent tests whether a given state root's account node exists in Forest storage
   * @return the resume block number K (start replay at K+1)
   */
  public static long findResumeBlock(
      final long head,
      final LongFunction<Hash> stateRootByBlock,
      final Predicate<Hash> rootPresent) {
    long lo = 0;
    long hi = head;
    long boundary = 0;
    while (lo <= hi) {
      final long mid = lo + (hi - lo) / 2;
      if (rootPresent.test(stateRootByBlock.apply(mid))) {
        boundary = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    // Extend past any coincidental root reuse beyond the binary-search boundary.
    while (boundary < head && rootPresent.test(stateRootByBlock.apply(boundary + 1))) {
      boundary++;
    }
    return boundary;
  }
}
