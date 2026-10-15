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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.List;

/**
 * The blocks one snap/2 pivot move covers (cf. snap-new2 SyncAttempt.Reorg).
 *
 * @param ancestor the newest block shared by the old and new branch; the old pivot itself when no
 *     reorg happened
 * @param orphaned blocks (ancestor, oldPivot] of the old branch, ascending; empty unless reorg
 * @param canonical blocks (ancestor, newPivot] of the new branch, ascending; never empty
 */
public record SnapV2PivotMove(
    BlockHeader ancestor, List<BlockHeader> orphaned, List<BlockHeader> canonical) {

  public SnapV2PivotMove(
      final BlockHeader ancestor,
      final List<BlockHeader> orphaned,
      final List<BlockHeader> canonical) {
    if (canonical.isEmpty()) {
      throw new IllegalArgumentException("a snap/2 pivot move needs at least one canonical block");
    }
    this.ancestor = ancestor;
    this.orphaned = List.copyOf(orphaned);
    this.canonical = List.copyOf(canonical);
  }

  public boolean isReorg() {
    return !orphaned.isEmpty();
  }

  public BlockHeader oldPivot() {
    return isReorg() ? orphaned.getLast() : ancestor;
  }

  public BlockHeader newPivot() {
    return canonical.getLast();
  }
}
