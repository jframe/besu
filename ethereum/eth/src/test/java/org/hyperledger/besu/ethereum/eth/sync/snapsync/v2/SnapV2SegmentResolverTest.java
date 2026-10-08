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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class SnapV2SegmentResolverTest {

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();

  /** Headers from {@code top} down to genesis, following parent hashes. */
  private List<BlockHeader> descendingFrom(final BlockHeader top) {
    final List<BlockHeader> headers = new ArrayList<>();
    BlockHeader h = top;
    while (true) {
      headers.add(h);
      if (h.getNumber() == 0) {
        return headers;
      }
      h = b.blockchain().getBlockHeader(h.getParentHash()).orElseThrow();
    }
  }

  private Optional<SnapV2ChainSegment> resolve(
      final BlockHeader oldPivot, final BlockHeader newPivot, final List<BlockHeader> desc) {
    return SnapV2SegmentResolver.resolve(oldPivot, newPivot, desc, b.blockchain()::getBlockHeader);
  }

  @Test
  void sameChainUsesOldPivotAsCommonAncestor() {
    final BlockHeader block5 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final BlockHeader block3 = b.header(3);

    final SnapV2ChainSegment segment =
        resolve(block3, block5, descendingFrom(block5)).orElseThrow();

    assertThat(segment.commonAncestor()).isEqualTo(block3);
    assertThat(segment.canonicalHeaders()).containsExactly(b.header(4), block5);
    assertThat(segment.orphanedHeaders()).isEmpty();
    assertThat(segment.isReorg()).isFalse();
    assertThat(segment.oldPivot()).isEqualTo(block3);
    assertThat(segment.newPivot()).isEqualTo(block5);
  }

  @Test
  void reorgFindsCommonAncestorAndOrphanedHeaders() {
    final BlockHeader stale3 = b.appendStaleChain(b.header(0), 1L, 3);
    final BlockHeader stale2 = b.blockchain().getBlockHeader(stale3.getParentHash()).orElseThrow();
    final BlockHeader block1 = b.header(1);
    final Block c2 = b.appendCanonical(block1, b.emptyBal(), 2L);
    final Block c3 = b.appendCanonical(c2.getHeader(), b.emptyBal(), 3L);
    final Block c4 = b.appendCanonical(c3.getHeader(), b.emptyBal(), 4L);

    final SnapV2ChainSegment segment =
        resolve(stale3, c4.getHeader(), descendingFrom(c4.getHeader())).orElseThrow();

    assertThat(segment.commonAncestor()).isEqualTo(block1);
    assertThat(segment.canonicalHeaders())
        .containsExactly(c2.getHeader(), c3.getHeader(), c4.getHeader());
    assertThat(segment.orphanedHeaders()).containsExactly(stale2, stale3);
    assertThat(segment.isReorg()).isTrue();
    assertThat(segment.oldPivot()).isEqualTo(stale3);
  }

  @Test
  void returnsEmptyWhenMoreHeadersAreNeeded() {
    final BlockHeader stale3 = b.appendStaleChain(b.header(0), 1L, 3);
    final Block c2 = b.appendCanonical(b.header(1), b.emptyBal(), 2L);
    final Block c3 = b.appendCanonical(c2.getHeader(), b.emptyBal(), 3L);
    final Block c4 = b.appendCanonical(c3.getHeader(), b.emptyBal(), 4L);

    // Only 4c and 3c fetched so far: the ancestor (block 1) is not yet in the list.
    final List<BlockHeader> partial = List.of(c4.getHeader(), c3.getHeader());

    assertThat(resolve(stale3, c4.getHeader(), partial)).isEmpty();
  }

  @Test
  void throwsWhenNoAncestorWithinWalkBound() {
    final BlockHeader stale97 = b.appendStaleChain(b.header(0), 1L, 97);
    final BlockHeader canonical98 = b.appendCanonicalChain(b.header(0), 1L, 98);

    assertThatThrownBy(() -> resolve(stale97, canonical98, descendingFrom(canonical98)))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }

  @Test
  void rejectsHeadersThatDoNotLink() {
    final BlockHeader block5 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final List<BlockHeader> gap = List.of(block5, b.header(3)); // 4 missing

    assertThatThrownBy(() -> resolve(b.header(2), block5, gap))
        .isInstanceOf(SnapV2SegmentResolver.InvalidCatchupHeadersException.class);
  }

  @Test
  void rejectsNewPivotEqualToOldPivot() {
    final BlockHeader block5 = b.appendCanonicalChain(b.header(0), 1L, 5);

    assertThatThrownBy(() -> resolve(block5, block5, descendingFrom(block5)))
        .isInstanceOf(SnapV2SegmentResolver.InvalidCatchupHeadersException.class);
  }

  @Test
  void rejectsNewPivotThatIsAncestorOfOldPivot() {
    final BlockHeader block5 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final BlockHeader block3 = b.header(3);

    assertThatThrownBy(() -> resolve(block5, block3, descendingFrom(block3)))
        .isInstanceOf(SnapV2SegmentResolver.InvalidCatchupHeadersException.class);
  }

  @Test
  void rejectsListNotStartingAtNewPivot() {
    final BlockHeader block5 = b.appendCanonicalChain(b.header(0), 1L, 5);
    final List<BlockHeader> wrongStart = List.of(b.header(4), b.header(3));

    assertThatThrownBy(() -> resolve(b.header(2), block5, wrongStart))
        .isInstanceOf(SnapV2SegmentResolver.InvalidCatchupHeadersException.class);
  }

  @Test
  void throwsWhenOldChainParentIsMissing() {
    final BlockHeader stale3 = b.appendStaleChain(b.header(0), 1L, 3);
    final Block c2 = b.appendCanonical(b.header(1), b.emptyBal(), 2L);
    final Block c3 = b.appendCanonical(c2.getHeader(), b.emptyBal(), 3L);
    final Block c4 = b.appendCanonical(c3.getHeader(), b.emptyBal(), 4L);

    assertThatThrownBy(
            () ->
                SnapV2SegmentResolver.resolve(
                    stale3,
                    c4.getHeader(),
                    descendingFrom(c4.getHeader()),
                    hash -> Optional.empty()))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }
}
