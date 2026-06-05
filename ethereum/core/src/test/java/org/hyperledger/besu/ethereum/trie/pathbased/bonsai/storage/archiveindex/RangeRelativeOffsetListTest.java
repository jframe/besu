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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.OptionalInt;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class RangeRelativeOffsetListTest {

  // -------------------------------------------------------------------------
  // Plan-required test
  // -------------------------------------------------------------------------

  @Test
  void appendAndFindLatestLeq() {
    var list = RangeRelativeOffsetList.empty();
    list = list.append(10).append(2_000).append(900_000); // offsets within a 1M range
    assertThat(list.latestLeq(1_500)).hasValue(10);
    assertThat(list.latestLeq(2_000)).hasValue(2_000);
    assertThat(list.latestLeq(5)).isEmpty();
    assertThat(list.size()).isEqualTo(3);
  }

  // -------------------------------------------------------------------------
  // Idempotent re-append
  // -------------------------------------------------------------------------

  @Test
  void appendSameTailOffsetIsNoOp() {
    var list = RangeRelativeOffsetList.empty().append(100).append(200);
    var list2 = list.append(200); // re-append of tail
    assertThat(list2.size()).isEqualTo(2);
    assertThat(list2.toBytes()).isEqualTo(list.toBytes());
    assertThat(list2).isSameAs(list);
  }

  // -------------------------------------------------------------------------
  // Ascending contract
  // -------------------------------------------------------------------------

  @Test
  void appendSmallerThanTailThrows() {
    var list = RangeRelativeOffsetList.empty().append(500);
    assertThatThrownBy(() -> list.append(499))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-decreasing");
  }

  @Test
  void fromBytesRejectsOutOfOrderAppend() {
    // lastOffset must be primed from the serialized form, not just from in-memory appends.
    Bytes packed = RangeRelativeOffsetList.empty().append(500).toBytes();
    var restored = RangeRelativeOffsetList.fromBytes(packed);
    assertThatThrownBy(() -> restored.append(499))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-decreasing");
  }

  @Test
  void appendDoesNotMutateOriginal() {
    var original = RangeRelativeOffsetList.empty().append(100).append(200);
    Bytes originalBytes = original.toBytes();
    var ignored = original.append(300); // discard the result
    assertThat(ignored.size()).isEqualTo(3); // sanity: append did produce a longer list
    assertThat(original.size()).isEqualTo(2);
    assertThat(original.toBytes()).isEqualTo(originalBytes);
    assertThat(original.latestLeq(999_999)).hasValue(200);
  }

  // -------------------------------------------------------------------------
  // 3-byte range validation
  // -------------------------------------------------------------------------

  @Test
  void appendNegativeOffsetThrows() {
    var list = RangeRelativeOffsetList.empty();
    assertThatThrownBy(() -> list.append(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void appendOffsetExceedingThreeByteLimitThrows() {
    var list = RangeRelativeOffsetList.empty();
    assertThatThrownBy(() -> list.append(0x1000000)) // 16_777_216 — one more than 0xFFFFFF
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void appendMaxValidOffsetSucceeds() {
    var list = RangeRelativeOffsetList.empty().append(0xFFFFFF);
    assertThat(list.size()).isEqualTo(1);
    assertThat(list.latestLeq(0xFFFFFF)).hasValue(0xFFFFFF);
  }

  // -------------------------------------------------------------------------
  // toBytes / fromBytes round-trip
  // -------------------------------------------------------------------------

  @Test
  void toBytesFromBytesRoundTrip() {
    var list = RangeRelativeOffsetList.empty().append(0).append(999).append(999_999);
    Bytes packed = list.toBytes();
    assertThat(packed.size()).isEqualTo(9); // 3 entries × 3 bytes
    var restored = RangeRelativeOffsetList.fromBytes(packed);
    assertThat(restored.size()).isEqualTo(3);
    assertThat(restored.latestLeq(0)).hasValue(0);
    assertThat(restored.latestLeq(999)).hasValue(999);
    assertThat(restored.latestLeq(999_999)).hasValue(999_999);
    assertThat(restored.toBytes()).isEqualTo(packed);
  }

  @Test
  void emptyListToBytesIsEmpty() {
    assertThat(RangeRelativeOffsetList.empty().toBytes()).isEqualTo(Bytes.EMPTY);
  }

  @Test
  void fromBytesEmptyIsEmpty() {
    var list = RangeRelativeOffsetList.fromBytes(Bytes.EMPTY);
    assertThat(list.size()).isEqualTo(0);
    assertThat(list.latestLeq(999_999)).isEqualTo(OptionalInt.empty());
  }

  @Test
  void fromBytesRejectsNonMultipleOfThreeLength() {
    assertThatThrownBy(() -> RangeRelativeOffsetList.fromBytes(Bytes.of(0x01, 0x02)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("multiple of 3");
  }

  // -------------------------------------------------------------------------
  // latestLeq boundary conditions
  // -------------------------------------------------------------------------

  @Test
  void latestLeqExactMatchReturnsOffset() {
    var list = RangeRelativeOffsetList.empty().append(100).append(200).append(300);
    assertThat(list.latestLeq(200)).hasValue(200);
  }

  @Test
  void latestLeqTargetLargerThanAllReturnsLast() {
    var list = RangeRelativeOffsetList.empty().append(10).append(20).append(30);
    assertThat(list.latestLeq(999_999)).hasValue(30);
  }

  @Test
  void latestLeqTargetSmallerThanAllReturnsEmpty() {
    var list = RangeRelativeOffsetList.empty().append(100).append(200);
    assertThat(list.latestLeq(50)).isEmpty();
  }

  @Test
  void latestLeqOnEmptyListReturnsEmpty() {
    assertThat(RangeRelativeOffsetList.empty().latestLeq(0)).isEmpty();
  }

  @Test
  void latestLeqSingleElement() {
    var list = RangeRelativeOffsetList.empty().append(500);
    assertThat(list.latestLeq(500)).hasValue(500);
    assertThat(list.latestLeq(501)).hasValue(500);
    assertThat(list.latestLeq(499)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Binary search correctness with larger list
  // -------------------------------------------------------------------------

  @Test
  void binarySearchCorrectnessLargeList() {
    // Build a list of 50 ascending offsets: 0, 1000, 2000, ..., 49000
    var list = RangeRelativeOffsetList.empty();
    for (int i = 0; i < 50; i++) {
      list = list.append(i * 1000);
    }
    assertThat(list.size()).isEqualTo(50);

    // Exact matches
    assertThat(list.latestLeq(0)).hasValue(0);
    assertThat(list.latestLeq(25_000)).hasValue(25_000);
    assertThat(list.latestLeq(49_000)).hasValue(49_000);

    // Between-value lookups: target falls between two entries
    assertThat(list.latestLeq(1_500)).hasValue(1_000);
    assertThat(list.latestLeq(24_999)).hasValue(24_000);
    assertThat(list.latestLeq(48_999)).hasValue(48_000);

    // Below first entry
    assertThat(list.latestLeq(-1 + 1)).hasValue(0); // target==0 is exact match
    assertThat(list.latestLeq(500)).hasValue(0); // between 0 and 1000

    // Above last entry
    assertThat(list.latestLeq(100_000)).hasValue(49_000);
  }

  // -------------------------------------------------------------------------
  // Packing correctness (3-byte big-endian, no sign extension)
  // -------------------------------------------------------------------------

  @Test
  void packingNoBitCorruptionForLargeOffset() {
    // 0x800000 = 8_388_608, high bit of byte 0 set — must not sign-extend
    int offset = 0x800000;
    var list = RangeRelativeOffsetList.empty().append(offset);
    assertThat(list.latestLeq(offset)).hasValue(offset);
    // Verify raw bytes: [0x80, 0x00, 0x00]
    Bytes packed = list.toBytes();
    assertThat(packed.get(0) & 0xFF).isEqualTo(0x80);
    assertThat(packed.get(1) & 0xFF).isEqualTo(0x00);
    assertThat(packed.get(2) & 0xFF).isEqualTo(0x00);
  }

  @Test
  void packingNoSignExtensionForHighBitInByte1() {
    // 0x008000 = 32_768, high bit of byte 1 set — must not sign-extend
    int offset = 0x008000;
    var list = RangeRelativeOffsetList.empty().append(offset);
    assertThat(list.latestLeq(offset)).hasValue(offset);
    Bytes packed = list.toBytes();
    assertThat(packed.get(0) & 0xFF).isEqualTo(0x00);
    assertThat(packed.get(1) & 0xFF).isEqualTo(0x80);
    assertThat(packed.get(2) & 0xFF).isEqualTo(0x00);
  }

  @Test
  void packingNoSignExtensionForHighBitInByte2() {
    // 0x000080 = 128, high bit of byte 2 set — must not sign-extend
    int offset = 0x000080;
    var list = RangeRelativeOffsetList.empty().append(offset);
    assertThat(list.latestLeq(offset)).hasValue(offset);
    Bytes packed = list.toBytes();
    assertThat(packed.get(0) & 0xFF).isEqualTo(0x00);
    assertThat(packed.get(1) & 0xFF).isEqualTo(0x00);
    assertThat(packed.get(2) & 0xFF).isEqualTo(0x80);
  }

  // -------------------------------------------------------------------------
  // last() — returns the largest offset or empty
  // -------------------------------------------------------------------------

  @Test
  void lastOnEmptyListReturnsEmpty() {
    assertThat(RangeRelativeOffsetList.empty().last()).isEqualTo(OptionalInt.empty());
  }

  @Test
  void lastOnSingleElementReturnsThatElement() {
    var list = RangeRelativeOffsetList.empty().append(42);
    assertThat(list.last()).hasValue(42);
  }

  @Test
  void lastReturnsLargestOffset() {
    var list = RangeRelativeOffsetList.empty().append(10).append(200).append(999_999);
    assertThat(list.last()).hasValue(999_999);
  }

  @Test
  void lastMatchesLatestLeqMaxOffset() {
    // last() and latestLeq(MAX) should agree
    var list = RangeRelativeOffsetList.empty().append(100).append(500).append(700);
    assertThat(list.last()).hasValue(700);
    assertThat(list.latestLeq(0xFFFFFF)).hasValue(700);
  }

  @Test
  void lastAfterFromBytesRoundTrip() {
    var list = RangeRelativeOffsetList.empty().append(1).append(999).append(50_000);
    var restored = RangeRelativeOffsetList.fromBytes(list.toBytes());
    assertThat(restored.last()).hasValue(50_000);
  }

  // -------------------------------------------------------------------------
  // equals / hashCode
  // -------------------------------------------------------------------------

  @Test
  void equalsAndHashCodeBasedOnPackedBytes() {
    var a = RangeRelativeOffsetList.empty().append(1).append(2).append(3);
    var b = RangeRelativeOffsetList.fromBytes(a.toBytes());
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void emptyListsAreEqual() {
    assertThat(RangeRelativeOffsetList.empty()).isEqualTo(RangeRelativeOffsetList.empty());
  }
}
