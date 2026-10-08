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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Builds a {@link SnapV2ChainSegment} from new-chain headers fetched downward from the (trusted)
 * new pivot. Pure logic: no I/O.
 */
final class SnapV2SegmentResolver {

  /** Deepest reorg below the old pivot that a catch-up can recover from. */
  static final int MAX_ANCESTOR_WALK = 95;

  private SnapV2SegmentResolver() {}

  /**
   * Finds the common ancestor of the old and new chain.
   *
   * @param oldPivot the current (possibly orphaned) pivot
   * @param newPivot the trusted new pivot
   * @param canonicalDescending new-chain headers starting at {@code newPivot}, descending,
   *     contiguous
   * @param oldChainLookup old-chain header lookup by hash (local ancestry)
   * @return the segment, or empty if {@code canonicalDescending} does not yet reach the ancestor
   * @throws InvalidCatchupHeadersException if the headers do not form a chain from {@code
   *     newPivot}, or {@code newPivot} is the old pivot or one of its ancestors (not above the old
   *     chain)
   * @throws ReorgUnrecoverableException if no ancestor lies within {@link #MAX_ANCESTOR_WALK}
   *     blocks of the old pivot, or an old-chain parent is unknown
   */
  static Optional<SnapV2ChainSegment> resolve(
      final BlockHeader oldPivot,
      final BlockHeader newPivot,
      final List<BlockHeader> canonicalDescending,
      final Function<Hash, Optional<BlockHeader>> oldChainLookup) {
    verifyLinkage(newPivot, canonicalDescending);

    final List<BlockHeader> orphanedDescending = new ArrayList<>();
    BlockHeader oldCursor = oldPivot;
    for (int i = 0; i < canonicalDescending.size(); i++) {
      final BlockHeader canonical = canonicalDescending.get(i);
      if (canonical.getNumber() > oldPivot.getNumber()) {
        continue;
      }
      while (oldCursor.getNumber() > canonical.getNumber()) {
        orphanedDescending.add(oldCursor);
        oldCursor = parentOf(oldCursor, oldChainLookup);
      }
      if (oldCursor.getHash().equals(canonical.getHash())) {
        if (i == 0) {
          throw new InvalidCatchupHeadersException(
              "snap/2 catch-up new pivot "
                  + newPivot.getNumber()
                  + " is not above the old chain (old pivot "
                  + oldPivot.getNumber()
                  + ")");
        }
        final List<BlockHeader> canonicalAscending =
            new ArrayList<>(canonicalDescending.subList(0, i));
        Collections.reverse(canonicalAscending);
        final List<BlockHeader> orphanedAscending = new ArrayList<>(orphanedDescending);
        Collections.reverse(orphanedAscending);
        return Optional.of(
            new SnapV2ChainSegment(canonical, canonicalAscending, orphanedAscending));
      }
      if (oldPivot.getNumber() - canonical.getNumber() >= MAX_ANCESTOR_WALK) {
        throw new ReorgUnrecoverableException(
            "Cannot recover reorg: no common ancestor within "
                + MAX_ANCESTOR_WALK
                + " blocks of old pivot "
                + oldPivot.getNumber()
                + " ("
                + oldPivot.getHash()
                + ")");
      }
    }
    return Optional.empty();
  }

  /**
   * Checks that {@code descending} starts at {@code top} and that every header is the parent of the
   * one before it.
   */
  static void verifyLinkage(final BlockHeader top, final List<BlockHeader> descending) {
    if (descending.isEmpty() || !descending.getFirst().getHash().equals(top.getHash())) {
      throw new InvalidCatchupHeadersException(
          "snap/2 catch-up headers do not start at new pivot " + top.getNumber());
    }
    for (int i = 1; i < descending.size(); i++) {
      final BlockHeader child = descending.get(i - 1);
      final BlockHeader parent = descending.get(i);
      if (parent.getNumber() != child.getNumber() - 1
          || !parent.getHash().equals(child.getParentHash())) {
        throw new InvalidCatchupHeadersException(
            "snap/2 catch-up header "
                + parent.getNumber()
                + " is not the parent of "
                + child.getNumber());
      }
    }
  }

  private static BlockHeader parentOf(
      final BlockHeader header, final Function<Hash, Optional<BlockHeader>> lookup) {
    return lookup
        .apply(header.getParentHash())
        .orElseThrow(
            () ->
                new ReorgUnrecoverableException(
                    "Cannot recover reorg: orphaned parent of block "
                        + header.getNumber()
                        + " missing"));
  }

  /** Peer-supplied catch-up headers do not form a chain from the new pivot. */
  static final class InvalidCatchupHeadersException extends RuntimeException {
    InvalidCatchupHeadersException(final String message) {
      super(message);
    }
  }
}
