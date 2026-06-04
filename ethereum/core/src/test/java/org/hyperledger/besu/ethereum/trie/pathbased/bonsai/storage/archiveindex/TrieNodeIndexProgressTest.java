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

import org.junit.jupiter.api.Test;

class TrieNodeIndexProgressTest {

  // -------------------------------------------------------------------------
  // Coverage gate (from the plan)
  // -------------------------------------------------------------------------

  @Test
  void coverageGate() {
    var p = new TrieNodeIndexProgress(/* rangeSize */ 1_000_000);
    assertThat(p.covers(5_000)).isFalse();
    p.markRangeComplete(0); // range [0, 1_000_000)
    assertThat(p.covers(5_000)).isTrue();
    assertThat(p.covers(1_500_000)).isFalse();
  }

  @Test
  void coversMultipleRanges() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.markRangeComplete(0);
    p.markRangeComplete(2); // range [2_000_000, 3_000_000)
    assertThat(p.covers(0)).isTrue();
    assertThat(p.covers(999_999)).isTrue();
    assertThat(p.covers(1_000_000)).isFalse(); // range 1 not marked
    assertThat(p.covers(2_000_000)).isTrue();
    assertThat(p.covers(2_999_999)).isTrue();
    assertThat(p.covers(3_000_000)).isFalse(); // range 3 not marked
  }

  @Test
  void coversReturnsFalseForBlockExceedingIntRangeIds() {
    // With rangeSize 1, rangeId == block. Pick a block whose rangeId exceeds Integer.MAX_VALUE.
    var p = new TrieNodeIndexProgress(1);
    long hugeBlock = (long) Integer.MAX_VALUE + 1_000L;
    assertThat(hugeBlock).isGreaterThan(Integer.MAX_VALUE);
    // Must return false (not throw, not narrow to a wrong/negative int).
    assertThat(p.covers(hugeBlock)).isFalse();
  }

  // -------------------------------------------------------------------------
  // lastIndexedBlock — monotonic UP
  // -------------------------------------------------------------------------

  @Test
  void lastIndexedBlockStartsAtMinusOne() {
    var p = new TrieNodeIndexProgress(1_000_000);
    assertThat(p.lastIndexedBlock()).isEqualTo(-1L);
  }

  @Test
  void lastIndexedBlockAdvancesMonotonically() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.setLastIndexedBlock(100);
    assertThat(p.lastIndexedBlock()).isEqualTo(100L);
    p.setLastIndexedBlock(200);
    assertThat(p.lastIndexedBlock()).isEqualTo(200L);
  }

  @Test
  void lastIndexedBlockIgnoresDecreasingValue() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.setLastIndexedBlock(500);
    p.setLastIndexedBlock(100); // should be a no-op
    assertThat(p.lastIndexedBlock()).isEqualTo(500L);
  }

  // -------------------------------------------------------------------------
  // indexStartBlock — monotonic DOWN
  // -------------------------------------------------------------------------

  @Test
  void indexStartBlockStartsAtMaxValue() {
    var p = new TrieNodeIndexProgress(1_000_000);
    assertThat(p.indexStartBlock()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void indexStartBlockDecreasesMonotonically() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.setIndexStartBlock(5_000_000);
    assertThat(p.indexStartBlock()).isEqualTo(5_000_000L);
    p.setIndexStartBlock(3_000_000);
    assertThat(p.indexStartBlock()).isEqualTo(3_000_000L);
  }

  @Test
  void indexStartBlockIgnoresIncreasingValue() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.setIndexStartBlock(3_000_000);
    p.setIndexStartBlock(7_000_000); // should be a no-op
    assertThat(p.indexStartBlock()).isEqualTo(3_000_000L);
  }

  // -------------------------------------------------------------------------
  // toBytes / fromBytes round-trip
  // -------------------------------------------------------------------------

  @Test
  void toBytesFromBytesRoundTrip() {
    var p = new TrieNodeIndexProgress(1_000_000);
    p.markRangeComplete(0);
    p.markRangeComplete(5);
    p.markRangeComplete(20);
    p.setLastIndexedBlock(19_999_999);
    p.setIndexStartBlock(1_000_000);

    byte[] serialized = p.toBytes();
    var restored = TrieNodeIndexProgress.fromBytes(1_000_000, serialized);

    assertThat(restored.covers(5_000)).isTrue();
    assertThat(restored.covers(5_500_000)).isTrue();
    assertThat(restored.covers(20_500_000)).isTrue();
    assertThat(restored.covers(1_000_000)).isFalse(); // range 1 not marked
    assertThat(restored.lastIndexedBlock()).isEqualTo(19_999_999L);
    assertThat(restored.indexStartBlock()).isEqualTo(1_000_000L);
  }

  @Test
  void toBytesFromBytesEmptyProgress() {
    var p = new TrieNodeIndexProgress(500_000);
    byte[] serialized = p.toBytes();
    var restored = TrieNodeIndexProgress.fromBytes(500_000, serialized);

    assertThat(restored.covers(0)).isFalse();
    assertThat(restored.lastIndexedBlock()).isEqualTo(-1L);
    assertThat(restored.indexStartBlock()).isEqualTo(Long.MAX_VALUE);
  }
}
