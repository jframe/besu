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
 * Verified header chain for one snap/2 pivot catch-up.
 *
 * @param commonAncestor W: the newest block shared by the old and new chain (the old pivot itself
 *     when the old pivot is still canonical)
 * @param canonicalHeaders blocks (W, newPivot] in ascending order; never empty
 * @param orphanedHeaders blocks (W, oldPivot] of the old chain in ascending order; empty unless the
 *     old pivot was reorged out
 */
public record SnapV2ChainSegment(
    BlockHeader commonAncestor,
    List<BlockHeader> canonicalHeaders,
    List<BlockHeader> orphanedHeaders) {

  public SnapV2ChainSegment(
      final BlockHeader commonAncestor,
      final List<BlockHeader> canonicalHeaders,
      final List<BlockHeader> orphanedHeaders) {
    if (canonicalHeaders.isEmpty()) {
      throw new IllegalArgumentException("snap/2 catch-up segment has no canonical headers");
    }
    this.commonAncestor = commonAncestor;
    this.canonicalHeaders = List.copyOf(canonicalHeaders);
    this.orphanedHeaders = List.copyOf(orphanedHeaders);
  }

  public boolean isReorg() {
    return !orphanedHeaders.isEmpty();
  }

  public BlockHeader newPivot() {
    return canonicalHeaders.getLast();
  }

  public BlockHeader oldPivot() {
    return isReorg() ? orphanedHeaders.getLast() : commonAncestor;
  }
}
