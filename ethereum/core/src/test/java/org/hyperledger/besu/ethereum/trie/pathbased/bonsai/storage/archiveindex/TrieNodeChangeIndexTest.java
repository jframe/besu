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

import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class TrieNodeChangeIndexTest {

  // A plausible account location (compact nibble path, variable length)
  private static final Bytes KEY =
      Bytes.fromHexString("0xdeadbeefcafe1234567890abcdef0102030405060708090a0b0c0d0e0f101112");

  // A different key, used to verify per-key isolation
  private static final Bytes OTHER_KEY =
      Bytes.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");

  // -------------------------------------------------------------------------
  // Plan-required test (verbatim from task spec)
  // -------------------------------------------------------------------------

  @Test
  void appendSetsMarkerBloomAndList() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 1_234);
    tx.commit();
    assertThat(idx.rangeMarkerPresent(KEY, 0)).isTrue();
    assertThat(idx.bloomMaybeContains(0, KEY)).isTrue();
    assertThat(idx.latestChangeBlock(KEY, 2_000)).hasValue(1_234L);
  }

  // -------------------------------------------------------------------------
  // Negative block → clear IllegalArgumentException naming "block"
  // -------------------------------------------------------------------------

  @Test
  void appendNegativeBlockThrows() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    assertThatThrownBy(() -> idx.append(tx, KEY, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("block");
  }

  // -------------------------------------------------------------------------
  // Bloom-negative: empty range returns false for any key
  // -------------------------------------------------------------------------

  @Test
  void bloomNegativeEmptyRange() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // Range 0 has never been written — bloom must be absent → false
    assertThat(idx.bloomMaybeContains(0, KEY)).isFalse();
    assertThat(idx.bloomMaybeContains(0, OTHER_KEY)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Range marker absent for an uninvolved range
  // -------------------------------------------------------------------------

  @Test
  void rangeMarkerAbsentForOtherRange() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 1_234); // range 0
    tx.commit();
    // Range 1 has no entry for KEY
    assertThat(idx.rangeMarkerPresent(KEY, 1)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Two appends to same key/range — list has both offsets
  // -------------------------------------------------------------------------

  @Test
  void twoAppendsInSameRange() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx1 = kv.startTransaction();
    idx.append(tx1, KEY, 100_000); // offset 100_000 in range 0
    tx1.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 500_000); // offset 500_000 in range 0
    tx2.commit();

    // latestChangeBlock at 500_000 → 500_000
    assertThat(idx.latestChangeBlock(KEY, 500_000)).hasValue(500_000L);
    // latestChangeBlock at 200_000 → 100_000 (the first one)
    assertThat(idx.latestChangeBlock(KEY, 200_000)).hasValue(100_000L);

    // Marker and bloom must still report present after the second append — guards the bloom
    // fromBytes/toBytes clone/copy round-trip against regressions.
    assertThat(idx.rangeMarkerPresent(KEY, 0)).isTrue();
    assertThat(idx.bloomMaybeContains(0, KEY)).isTrue();
  }

  // -------------------------------------------------------------------------
  // latestChangeBlock: t before first change → empty
  // -------------------------------------------------------------------------

  @Test
  void latestChangeBlockBeforeFirstChangeIsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500_000);
    tx.commit();
    // t = 499_999 is before the only change
    assertThat(idx.latestChangeBlock(KEY, 499_999)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // latestChangeBlock: bloom-negative range → empty (short-circuit)
  // -------------------------------------------------------------------------

  @Test
  void latestChangeBlockBloomNegativeShortCircuits() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // Nothing appended — all ranges empty
    assertThat(idx.latestChangeBlock(KEY, 999_999)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Two keys in same range: each gets its own list/marker; bloom shared
  // -------------------------------------------------------------------------

  @Test
  void twoKeysInSameRangeShareBloomButHaveOwnMarkerAndList() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx1 = kv.startTransaction();
    idx.append(tx1, KEY, 100);
    tx1.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, OTHER_KEY, 200);
    tx2.commit();

    // Bloom for range 0 should report maybe-present for both keys
    assertThat(idx.bloomMaybeContains(0, KEY)).isTrue();
    assertThat(idx.bloomMaybeContains(0, OTHER_KEY)).isTrue();

    // Each key has its own marker in range 0
    assertThat(idx.rangeMarkerPresent(KEY, 0)).isTrue();
    assertThat(idx.rangeMarkerPresent(OTHER_KEY, 0)).isTrue();

    // Each key's list is independent
    assertThat(idx.latestChangeBlock(KEY, 999_999)).hasValue(100L);
    assertThat(idx.latestChangeBlock(OTHER_KEY, 999_999)).hasValue(200L);
  }

  // -------------------------------------------------------------------------
  // Storage-trie key (32-byte accountHash || location)
  // -------------------------------------------------------------------------

  @Test
  void storageTrieKeyWorks() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    Bytes accountHash =
        Bytes.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Bytes location = Bytes.fromHexString("0xbb");
    Bytes storageKey = ArchiveNodeKey.storage(accountHash, location);

    var tx = kv.startTransaction();
    idx.append(tx, storageKey, 42_000);
    tx.commit();

    assertThat(idx.rangeMarkerPresent(storageKey, 0)).isTrue();
    assertThat(idx.bloomMaybeContains(0, storageKey)).isTrue();
    assertThat(idx.latestChangeBlock(storageKey, 999_999)).hasValue(42_000L);
  }
}
