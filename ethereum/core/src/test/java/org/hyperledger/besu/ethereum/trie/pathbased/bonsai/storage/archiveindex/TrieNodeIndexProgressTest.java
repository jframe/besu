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
  // Coverage gate — window-check semantics: block in [indexStartBlock, lastIndexedBlock]
  // -------------------------------------------------------------------------

  @Test
  void covers_returnsFalseWhenUninitialized() {
    final TrieNodeIndexProgress p = new TrieNodeIndexProgress(1_000_000L);
    assertThat(p.covers(0)).isFalse();
    assertThat(p.covers(5_000)).isFalse();
  }

  @Test
  void covers_returnsTrueForBlockWithinWindow() {
    final TrieNodeIndexProgress p = new TrieNodeIndexProgress(1_000_000L);
    p.setIndexStartBlock(0L);
    p.setLastIndexedBlock(5_000L);
    assertThat(p.covers(0)).isTrue();
    assertThat(p.covers(5_000)).isTrue();
    assertThat(p.covers(2_500)).isTrue();
  }

  @Test
  void covers_returnsFalseOutsideWindow() {
    final TrieNodeIndexProgress p = new TrieNodeIndexProgress(1_000_000L);
    p.setIndexStartBlock(1_000_000L);
    p.setLastIndexedBlock(1_500_000L);
    assertThat(p.covers(999_999)).isFalse(); // below indexStartBlock
    assertThat(p.covers(1_500_001)).isFalse(); // above lastIndexedBlock
    assertThat(p.covers(1_200_000)).isTrue(); // within window
  }

  @Test
  void covers_returnsNegativeBlockFalse() {
    final TrieNodeIndexProgress p = new TrieNodeIndexProgress(1_000_000L);
    assertThat(p.covers(-1)).isFalse();
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
  void serialisationRoundTrip() {
    final TrieNodeIndexProgress p = new TrieNodeIndexProgress(1_000_000L);
    p.setLastIndexedBlock(42L);
    p.setIndexStartBlock(0L);

    final byte[] bytes = p.toBytes();
    assertThat(bytes).hasSize(16); // two longs, 8 bytes each

    final TrieNodeIndexProgress restored = TrieNodeIndexProgress.fromBytes(1_000_000L, bytes);
    assertThat(restored.lastIndexedBlock()).isEqualTo(42L);
    assertThat(restored.indexStartBlock()).isEqualTo(0L);
    assertThat(restored.covers(42L)).isTrue();
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
