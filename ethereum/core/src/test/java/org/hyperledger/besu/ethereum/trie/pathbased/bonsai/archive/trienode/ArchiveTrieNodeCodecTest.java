/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCodecTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Bytes node(final int... bytes) {
    final byte[] b = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      b[i] = (byte) bytes[i];
    }
    return Bytes.wrap(b);
  }

  private static Bytes fill(final int len, final int value) {
    final byte[] b = new byte[len];
    java.util.Arrays.fill(b, (byte) value);
    return Bytes.wrap(b);
  }

  // ---------------------------------------------------------------------------
  // encodeFull
  // ---------------------------------------------------------------------------

  @Test
  void encodeFullRoundTrips() {
    final Bytes n = node(0xAA, 0xBB, 0xCC);
    final ArchiveTrieNodeEntry e = ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(n));
    assertThat(e.isFull()).isTrue();
    assertThat(e.isDeletion()).isFalse();
    assertThat(e.fullNode()).isEqualTo(n);
  }

  // ---------------------------------------------------------------------------
  // encodeDiff — lifecycle cases
  // ---------------------------------------------------------------------------

  @Test
  void encodeDiffNullOldIsCreationFull() {
    final Bytes n = node(0x01, 0x02);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(null, n));
    assertThat(e.isFull()).isTrue();
    assertThat(e.fullNode()).isEqualTo(n);
  }

  @Test
  void encodeDiffNullNewIsDeletionTombstoneWithNoBody() {
    final Bytes n = node(0x01, 0x02);
    final Bytes entry = ArchiveTrieNodeCodec.encodeDiff(n, null);
    assertThat(entry.size()).isEqualTo(1);
    assertThat(ArchiveTrieNodeCodec.decode(entry).isDeletion()).isTrue();
  }

  @Test
  void encodeDiffBothNullThrows() {
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.encodeDiff(null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // encodeDiff — binary patch properties
  // ---------------------------------------------------------------------------

  @Test
  void encodeDiffIdenticalNodesProducesNonFullDiff() {
    // A no-op diff: same bytes in and out, but NOT a FULL entry (it encodes as a zero-op patch).
    final Bytes n = fill(40, 0xAB);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(n, n));
    assertThat(e.isFull()).isFalse();
    assertThat(e.isDeletion()).isFalse();
  }

  @Test
  void encodeDiffProducesEntrySmallerthanNodeForSingleByteChange() {
    // Dominant trie-node case: one byte changes in a 36-byte node.
    final byte[] old = new byte[36];
    final byte[] newBytes = old.clone();
    newBytes[18] = (byte) 0xFF;
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(Bytes.wrap(old), Bytes.wrap(newBytes));
    assertThat(diffEntry.size()).isLessThan(36 + 1); // must beat FULL
  }

  @Test
  void encodeDiffFallsBackToFullWhenPatchExceedsNodeSize() {
    // Every byte differs → patch body (INSERT + SKIP) exceeds new node size → FULL.
    final Bytes old = fill(8, 0x11);
    final Bytes newNode = fill(8, 0x22);
    // INSERT(8 bytes) = 2+8=10 bytes, SKIP(8) = 2 bytes, total patch = 12 > 8 → FULL
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(old, newNode));
    assertThat(e.isFull()).isTrue();
    assertThat(e.fullNode()).isEqualTo(newNode);
  }

  // ---------------------------------------------------------------------------
  // reconstruct — round-trip correctness
  // ---------------------------------------------------------------------------

  @Test
  void reconstructWithNoDiffsReturnsFullNodeByteExact() {
    final Bytes n = node(0x01, 0x02, 0x03);
    assertThat(ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(n), List.of()))
        .isEqualTo(n);
  }

  @Test
  void roundTripSingleByteChange() {
    final Bytes old = node(0xAA, 0xBB, 0xCC);
    final Bytes newNode = node(0xAA, 0xFF, 0xCC);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripHashSizedBlockChange() {
    // Simulates a 32-byte hash ref changing inside a larger node (dominant trie change pattern).
    final Bytes prefix = fill(3, 0x01);
    final Bytes suffix = fill(5, 0x02);
    final Bytes oldHash = fill(32, 0x00);
    final Bytes newHash = fill(32, 0xAA);
    final Bytes old = Bytes.concatenate(prefix, oldHash, suffix);
    final Bytes newNode = Bytes.concatenate(prefix, newHash, suffix);

    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(diff.size()).isLessThan(newNode.size() + 1); // patch beats FULL
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripPrefixOnlyChange() {
    // First byte changes; suffix is long and unchanged.
    final Bytes old = Bytes.concatenate(node(0x01), fill(50, 0xBB));
    final Bytes newNode = Bytes.concatenate(node(0x02), fill(50, 0xBB));
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripSuffixOnlyChange() {
    // Last byte changes; prefix is long and unchanged.
    final Bytes old = Bytes.concatenate(fill(50, 0xBB), node(0x01));
    final Bytes newNode = Bytes.concatenate(fill(50, 0xBB), node(0x02));
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripSizeIncreaseChange() {
    // new node is longer than old (e.g. an extension node's path grows).
    final Bytes old = node(0xAA, 0xBB);
    final Bytes newNode = node(0xAA, 0xCC, 0xDD, 0xEE);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry entry = ArchiveTrieNodeCodec.decode(diff);
    if (entry.isFull()) {
      assertThat(entry.fullNode()).isEqualTo(newNode);
    } else {
      assertThat(
              ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
          .isEqualTo(newNode);
    }
  }

  @Test
  void roundTripSizeDecreaseChange() {
    // new node is shorter than old.
    final Bytes old = node(0xAA, 0xBB, 0xCC, 0xDD);
    final Bytes newNode = node(0xAA, 0xEE);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry entry = ArchiveTrieNodeCodec.decode(diff);
    if (entry.isFull()) {
      assertThat(entry.fullNode()).isEqualTo(newNode);
    } else {
      assertThat(
              ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
          .isEqualTo(newNode);
    }
  }

  @Test
  void reconstructAppliesMultipleDiffsInAscendingOrder() {
    final Bytes v1 = node(0xAA, 0xBB, 0xCC);
    final Bytes v2 = node(0xAA, 0xFF, 0xCC);
    final Bytes v3 = node(0xDD, 0xFF, 0xCC);
    final Bytes diff1 = ArchiveTrieNodeCodec.encodeDiff(v1, v2);
    final Bytes diff2 = ArchiveTrieNodeCodec.encodeDiff(v2, v3);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(v1), List.of(diff1, diff2)))
        .isEqualTo(v3);
  }

  @Test
  void reconstructChainOfHashBlockChanges() {
    // Three successive mutations each changing the same 32-byte field.
    final Bytes prefix = fill(4, 0x01);
    final Bytes suffix = fill(4, 0x02);
    final Bytes v1 = Bytes.concatenate(prefix, fill(32, 0x00), suffix);
    final Bytes v2 = Bytes.concatenate(prefix, fill(32, 0x11), suffix);
    final Bytes v3 = Bytes.concatenate(prefix, fill(32, 0x22), suffix);
    final Bytes diff1 = ArchiveTrieNodeCodec.encodeDiff(v1, v2);
    final Bytes diff2 = ArchiveTrieNodeCodec.encodeDiff(v2, v3);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(v1), List.of(diff1, diff2)))
        .isEqualTo(v3);
  }

  // ---------------------------------------------------------------------------
  // reconstruct — input validation
  // ---------------------------------------------------------------------------

  @Test
  void reconstructRejectsNonFullBaseEntry() {
    final Bytes n = node(0x01, 0x02, 0x03);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(n, node(0x04, 0x05, 0x06));
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(diff, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsFullEntryInDiffList() {
    final Bytes n = node(0x01, 0x02, 0x03);
    final Bytes full1 = ArchiveTrieNodeCodec.encodeFull(n);
    final Bytes full2 = ArchiveTrieNodeCodec.encodeFull(node(0x04, 0x05, 0x06));
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(full1, List.of(full2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsDeletionTombstoneInDiffList() {
    final Bytes n = node(0x01, 0x02, 0x03);
    final Bytes full = ArchiveTrieNodeCodec.encodeFull(n);
    final Bytes tombstone = ArchiveTrieNodeCodec.encodeDiff(n, null);
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(full, List.of(tombstone)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // ArchiveTrieNodeEntry.patchBody()
  // ---------------------------------------------------------------------------

  @Test
  void patchBodyThrowsOnFullEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(node(0x01)));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patchBodyThrowsOnDeletionEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(node(0x01), null));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patchBodyReturnsNonNullForDiffEntry() {
    final Bytes old = fill(40, 0x01);
    final Bytes newNode = fill(40, 0x01);
    newNode.toArray()[20] = (byte) 0xFF; // mutate a copy — need to use array
    final byte[] arr = old.toArray();
    arr[20] = (byte) 0xFF;
    final Bytes mutated = Bytes.wrap(arr);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(old, mutated));
    if (!e.isFull()) {
      assertThat(e.patchBody()).isNotNull();
    }
  }
}
