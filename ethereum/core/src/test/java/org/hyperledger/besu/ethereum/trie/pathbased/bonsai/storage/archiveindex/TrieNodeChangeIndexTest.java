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

  // ===========================================================================
  // Task 2.4: cross-range descending walk tests
  // ===========================================================================

  // -------------------------------------------------------------------------
  // Cross-range: change in range 0 and range 2, query at T in range 1
  // -------------------------------------------------------------------------

  @Test
  void crossRangeQueryInMiddleRangeReturnsRange0Block() {
    // rangeSize = 1_000_000 → range 0 = [0, 999_999], range 1 = [1_000_000, 1_999_999],
    //                          range 2 = [2_000_000, 2_999_999]
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    // Append in separate transactions to avoid the bloom same-tx hazard.
    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000); // range 0, offset 500_000
    tx0.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000); // range 2, offset 500_000
    tx2.commit();

    // T = 1_500_000 → in range 1; range 2 is above T so skipped; range 1 empty; range 0 → 500_000
    assertThat(idx.latestChangeBlock(KEY, 1_500_000)).hasValue(500_000L);
  }

  @Test
  void crossRangeQueryAtRange2AfterChangeReturnsRange2Block() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000);
    tx0.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000);
    tx2.commit();

    // T = 2_700_000 → in range 2, after the range-2 change → returns 2_500_000
    assertThat(idx.latestChangeBlock(KEY, 2_700_000)).hasValue(2_500_000L);
  }

  @Test
  void crossRangeQueryAtRange2BeforeChangeReturnsRange0Block() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000);
    tx0.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000);
    tx2.commit();

    // T = 2_400_000 → in range 2, before the range-2 change (2_500_000); falls back to range 0
    assertThat(idx.latestChangeBlock(KEY, 2_400_000)).hasValue(500_000L);
  }

  @Test
  void crossRangeQueryBeforeAllChangesIsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000);
    tx0.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000);
    tx2.commit();

    // T = 400_000 → in range 0, before the range-0 change (500_000) → empty
    assertThat(idx.latestChangeBlock(KEY, 400_000)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Never-changed key → empty
  // -------------------------------------------------------------------------

  @Test
  void neverChangedKeyIsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // No appends at all for KEY
    assertThat(idx.latestChangeBlock(KEY, 5_000_000)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Bloom-negative short-circuit: a key never appended returns empty across ranges
  // -------------------------------------------------------------------------

  @Test
  void bloomNegativeShortCircuitsAcrossRanges() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    // Append OTHER_KEY to ranges 0, 1, 2 — blooms have bits, but NOT for KEY
    var tx0 = kv.startTransaction();
    idx.append(tx0, OTHER_KEY, 100_000);
    tx0.commit();

    var tx1 = kv.startTransaction();
    idx.append(tx1, OTHER_KEY, 1_100_000);
    tx1.commit();

    var tx2 = kv.startTransaction();
    idx.append(tx2, OTHER_KEY, 2_200_000);
    tx2.commit();

    // KEY was never appended; bloom for each range is negative for KEY → empty
    assertThat(idx.latestChangeBlock(KEY, 2_500_000)).isEmpty();
    assertThat(idx.bloomMaybeContains(0, KEY)).isFalse();
    assertThat(idx.bloomMaybeContains(1, KEY)).isFalse();
    assertThat(idx.bloomMaybeContains(2, KEY)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Multiple changes in range 0 + one in range 2; verify latestLeq + cross-range
  // -------------------------------------------------------------------------

  @Test
  void multipleChangesInRange0PlusRange2CrossRangeInterplay() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    // Two changes in range 0
    var tx0a = kv.startTransaction();
    idx.append(tx0a, KEY, 200_000); // range 0, offset 200_000
    tx0a.commit();

    var tx0b = kv.startTransaction();
    idx.append(tx0b, KEY, 700_000); // range 0, offset 700_000
    tx0b.commit();

    // One change in range 2
    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_300_000); // range 2, offset 300_000
    tx2.commit();

    // T in range 0, between the two range-0 changes → picks 200_000 (700_000 is above T)
    assertThat(idx.latestChangeBlock(KEY, 500_000)).hasValue(200_000L);

    // T at range-0 upper bound → picks 700_000
    assertThat(idx.latestChangeBlock(KEY, 999_999)).hasValue(700_000L);

    // T in range 1 → range 1 empty, falls back to range 0 → picks 700_000 (latest in range 0)
    assertThat(idx.latestChangeBlock(KEY, 1_500_000)).hasValue(700_000L);

    // T in range 2 after range-2 change → picks 2_300_000
    assertThat(idx.latestChangeBlock(KEY, 2_999_999)).hasValue(2_300_000L);

    // T in range 2 before range-2 change → falls back to range 0 → picks 700_000
    assertThat(idx.latestChangeBlock(KEY, 2_100_000)).hasValue(700_000L);
  }

  // -------------------------------------------------------------------------
  // Inclusive ceiling in the cross-range start range: change exactly at T in a
  // higher range is returned
  // -------------------------------------------------------------------------

  @Test
  void changeExactlyAtTInHigherRangeIsReturned() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    var tx = kv.startTransaction();
    idx.append(tx, KEY, 2_500_000); // range 2, offset 500_000
    tx.commit();

    // T == the change block, in range 2 (the start range). The within-range ceiling equals T's
    // offset (inclusive), so latestLeq must include the offset exactly at T.
    assertThat(idx.latestChangeBlock(KEY, 2_500_000)).hasValue(2_500_000L);
  }

  // -------------------------------------------------------------------------
  // Negative T → clear IllegalArgumentException naming "t"
  // -------------------------------------------------------------------------

  @Test
  void latestChangeBlockNegativeTThrows() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    assertThatThrownBy(() -> idx.latestChangeBlock(KEY, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("t");
  }

  // ===========================================================================
  // Task 2.5: modifiedAfter(naturalKey, t, headBlock) ascending range walk
  // ===========================================================================

  // -------------------------------------------------------------------------
  // Basic: change AFTER T → true
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterReturnsTrueWhenChangeExistsAfterT() {
    // Change at block 1_000 (range 0, offset 1_000). T=500, headBlock=2_000_000.
    // The change is strictly after T → should return true.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 1_000);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 500, 2_000_000)).isTrue();
  }

  // -------------------------------------------------------------------------
  // Basic: change BEFORE T → false
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterReturnsFalseWhenChangeIsBeforeT() {
    // Change at block 500 (range 0, offset 500). T=1_000, headBlock=2_000_000.
    // The only change is before T → should return false.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 1_000, 2_000_000)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Boundary: change exactly at T is NOT "after T" → false
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterReturnsFalseWhenChangeExactlyAtT() {
    // Change at T=1_000. "After T" is strictly > T, so T itself does not count.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 1_000);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 1_000, 2_000_000)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Boundary: change at T+1 IS "after T" → true
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterReturnsTrueWhenChangeAtTPlus1() {
    // Change at T+1=1_001. Strictly after T=1_000 → true.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 1_001);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 1_000, 2_000_000)).isTrue();
  }

  // -------------------------------------------------------------------------
  // Cross-range: change in range 0 and range 2; query T in range 1
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterCrossRangeReturnsTrueWhenLaterRangeHasChange() {
    // rangeSize=1_000_000: range 0=[0,999_999], range 1=[1_000_000,1_999_999],
    //                       range 2=[2_000_000,2_999_999]
    // Changes at 500_000 (range 0) and 2_500_000 (range 2).
    // T=1_000_000 (in range 1), headBlock=3_000_000.
    // range 1 is empty; range 2 has a change (2_500_000 > T) → true.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000);
    tx0.commit();
    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000);
    tx2.commit();
    assertThat(idx.modifiedAfter(KEY, 1_000_000, 3_000_000)).isTrue();
  }

  @Test
  void modifiedAfterCrossRangeReturnsFalseWhenAllChangesBeforeT() {
    // Same setup: changes at 500_000 and 2_500_000.
    // T=2_600_000 (in range 2 after the range-2 change), headBlock=3_000_000.
    // Range 2 has a change, but it's at 2_500_000 which is < T → false.
    // No ranges above 2 have changes → false.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx0 = kv.startTransaction();
    idx.append(tx0, KEY, 500_000);
    tx0.commit();
    var tx2 = kv.startTransaction();
    idx.append(tx2, KEY, 2_500_000);
    tx2.commit();
    assertThat(idx.modifiedAfter(KEY, 2_600_000, 3_000_000)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Bloom-negative skips range: key never indexed → false
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterBloomNegativeReturnsFalse() {
    // KEY is never indexed; bloom is empty → false.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // Only OTHER_KEY is indexed, so bloom for range 0 is positive for OTHER_KEY but negative KEY.
    var tx = kv.startTransaction();
    idx.append(tx, OTHER_KEY, 500_000);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 0, 2_000_000)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Validation: negative T → IllegalArgumentException mentioning "t"
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterNegativeTThrows() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    assertThatThrownBy(() -> idx.modifiedAfter(KEY, -1, 1_000_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("t");
  }

  // -------------------------------------------------------------------------
  // Validation: negative headBlock → IllegalArgumentException
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterNegativeHeadBlockThrows() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    assertThatThrownBy(() -> idx.modifiedAfter(KEY, 0, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("headBlock");
  }

  // -------------------------------------------------------------------------
  // Validation: headBlock < t → IllegalArgumentException
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterHeadBlockLessThanTThrows() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    assertThatThrownBy(() -> idx.modifiedAfter(KEY, 1_000, 500))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("headBlock");
  }

  // -------------------------------------------------------------------------
  // headBlock == t → no range above startRange; nothing after T → false
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterHeadBlockEqualsTReturnsFalse() {
    // headBlock == t means the search window (T, T] is empty; floor = T's within-range offset,
    // and the change at T has offset == floor, so floor > floor is false.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 500, 500)).isFalse();
  }

  // -------------------------------------------------------------------------
  // Known false-positive: t and headBlock in same range; change after headBlock but
  // before range boundary → modifiedAfter returns true (intentional conservative behaviour)
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterInRangeFalsePositiveWhenChangeExceedsHeadBlockButNotRangeBoundary() {
    // rangeSize=1_000_000. Change at block 700 (range 0, offset 700).
    // t=500, headBlock=600 — both in range 0, same startRange=headRange=0.
    // hasChangeAboveFloor uses latestLeq(999_999) (full-range max), not headBlock's offset (600).
    // The last entry is 700 which > floor (500) → returns true, even though 700 > headBlock (600).
    // This is a known, acceptable false positive: Stage 4 handles it by falling back to
    // latestChangeBlock which finds the actual latest change ≤ T.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 700);
    tx.commit();
    assertThat(idx.modifiedAfter(KEY, 500, 600)).isTrue();
  }

  // -------------------------------------------------------------------------
  // headBlock bounds the walk: change in range beyond headBlock is NOT seen → false
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterDoesNotWalkBeyondHeadBlock() {
    // Change at 2_500_000 (range 2). headBlock=1_999_999 (range 1) → walk stops at range 1;
    // range 2 never consulted even though it has a change.
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 2_500_000);
    tx.commit();
    // T=0, headBlock=1_999_999 → only ranges 0 and 1 are checked; range 2 not reached.
    assertThat(idx.modifiedAfter(KEY, 0, 1_999_999)).isFalse();
  }

  // ===========================================================================
  // Task 2.6: Sub-block split for hot keys
  // ===========================================================================

  // Use a small rangeSize that fits offsets in 3 bytes (max 0xFFFFFF = 16,777,215).
  // threshold=10, splitAt=5: after appending 11 entries, a split should occur.
  private static final long SMALL_RANGE = 100_000L;
  private static final int TEST_THRESHOLD = 10;
  private static final int TEST_SPLIT_AT = 5;

  /**
   * Helper: build an index with small threshold/splitAt and a rangeSize small enough for
   * single-range testing (all block numbers < SMALL_RANGE → rangeId 0).
   */
  private TrieNodeChangeIndex smallThresholdIndex(
      final org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage kv) {
    return new TrieNodeChangeIndex(kv, SMALL_RANGE, TEST_THRESHOLD, TEST_SPLIT_AT);
  }

  // -------------------------------------------------------------------------
  // Split triggers at threshold+1 appends
  // -------------------------------------------------------------------------

  @Test
  void subBlockSplitOccursAfterThresholdExceeded() {
    // With threshold=10 and splitAt=5: after 11 distinct appends (each in its own tx),
    // a split should occur. The first 5 entries (lowest offsets) move to sub-block 0.
    // The remaining 6 entries stay in the main list (tail).
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    // Append 11 distinct offsets in separate transactions so each append reads committed state.
    for (int i = 0; i < 11; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 100); // blocks 0, 100, 200, ..., 1000
      tx.commit();
    }

    // After 11 appends: sub-block 0 holds offsets {0, 100, 200, 300, 400} (the first 5);
    // tail holds offsets {500, 600, 700, 800, 900, 1000} (the remaining 6).

    // latestChangeBlock for an offset in the sub-block: T=250 → latest ≤ 250 is 200.
    assertThat(idx.latestChangeBlock(KEY, 250)).hasValue(200L);

    // latestChangeBlock for an offset in the tail: T=750 → latest ≤ 750 is 700.
    assertThat(idx.latestChangeBlock(KEY, 750)).hasValue(700L);

    // latestChangeBlock at the exact boundary between sub-block and tail: T=500 → 500.
    assertThat(idx.latestChangeBlock(KEY, 500)).hasValue(500L);

    // latestChangeBlock before any entry: T < 0 would be illegal; use T=50 which is between 0 and
    // 100.
    assertThat(idx.latestChangeBlock(KEY, 50)).hasValue(0L);

    // latestChangeBlock at the very last entry: T=1000 → 1000.
    assertThat(idx.latestChangeBlock(KEY, 1000)).hasValue(1000L);
  }

  // -------------------------------------------------------------------------
  // No split below threshold
  // -------------------------------------------------------------------------

  @Test
  void noSplitBelowThreshold() {
    // Append exactly threshold (10) entries; no split should occur.
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    for (int i = 0; i < TEST_THRESHOLD; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 10);
      tx.commit();
    }

    // All 10 entries are in the tail; latestChangeBlock should resolve correctly.
    assertThat(idx.latestChangeBlock(KEY, 50)).hasValue(50L);
    assertThat(idx.latestChangeBlock(KEY, 90)).hasValue(90L);
    assertThat(idx.latestChangeBlock(KEY, 5)).hasValue(0L);
  }

  // -------------------------------------------------------------------------
  // modifiedAfter still correct after split (tail check sufficient)
  // -------------------------------------------------------------------------

  @Test
  void modifiedAfterStillCorrectAfterSplit() {
    // After a split, the tail holds the newest entries. modifiedAfter uses hasChangeAboveFloor
    // which only needs the tail (tail has the largest offsets).
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    // Append 11 entries to trigger split: blocks 0, 100, ..., 1000.
    for (int i = 0; i < 11; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 100);
      tx.commit();
    }

    // T=400, headBlock=50000: change at 500 is after T → true.
    assertThat(idx.modifiedAfter(KEY, 400, 50_000)).isTrue();

    // T=1000, headBlock=50000: no change after 1000 → false.
    assertThat(idx.modifiedAfter(KEY, 1000, 50_000)).isFalse();

    // T=0, headBlock=50000: changes at 100, 200, ..., 1000 are all after T → true.
    assertThat(idx.modifiedAfter(KEY, 0, 50_000)).isTrue();
  }

  // -------------------------------------------------------------------------
  // Multiple splits: second split creates sub-block 1
  // -------------------------------------------------------------------------

  @Test
  void multipleSubBlockSplits() {
    // threshold=10, splitAt=5: first split at entry 11 creates sub-block 0 (entries 0–4).
    // After split, tail has entries 5–10 (6 entries). The second split occurs when tail
    // exceeds threshold again: after appending entries 11–15 (tail reaches 11 entries:
    // entries 5–15), a second split creates sub-block 1 (entries 5–9, the oldest 5 in tail).
    // Tail then has entries 10–15.
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    // Append 16 distinct offsets: blocks 0, 10, 20, ..., 150.
    for (int i = 0; i < 16; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 10);
      tx.commit();
    }

    // Sub-block 0: entries {0, 10, 20, 30, 40} (first split, first 5 entries).
    // Sub-block 1: entries {50, 60, 70, 80, 90} (second split, next 5 entries).
    // Tail: entries {100, 110, 120, 130, 140, 150}.

    // Query in sub-block 0 range: T=25 → 20.
    assertThat(idx.latestChangeBlock(KEY, 25)).hasValue(20L);

    // Query in sub-block 1 range: T=75 → 70.
    assertThat(idx.latestChangeBlock(KEY, 75)).hasValue(70L);

    // Query in tail range: T=135 → 130.
    assertThat(idx.latestChangeBlock(KEY, 135)).hasValue(130L);

    // Query at the very end: T=150 → 150.
    assertThat(idx.latestChangeBlock(KEY, 150)).hasValue(150L);

    // Query before all entries: T=5 → 0.
    assertThat(idx.latestChangeBlock(KEY, 5)).hasValue(0L);
  }

  // -------------------------------------------------------------------------
  // Other keys are not affected by split of KEY
  // -------------------------------------------------------------------------

  @Test
  void otherKeyUnaffectedBySplit() {
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    // Trigger a split on KEY.
    for (int i = 0; i < 11; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 100);
      tx.commit();
    }

    // Also append one entry for OTHER_KEY (no split).
    var otherTx = kv.startTransaction();
    idx.append(otherTx, OTHER_KEY, 500);
    otherTx.commit();

    // OTHER_KEY resolves correctly with no sub-blocks.
    assertThat(idx.latestChangeBlock(OTHER_KEY, 600)).hasValue(500L);
    assertThat(idx.latestChangeBlock(OTHER_KEY, 400)).isEmpty();
  }

  // ===========================================================================
  // getChangeBlocksUpTo: new method for optimised backward walk
  // ===========================================================================

  // -------------------------------------------------------------------------
  // Basic: single entry in range 0
  // -------------------------------------------------------------------------

  @Test
  void getChangeBlocksUpToSingleEntry() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500_000);
    tx.commit();

    // T = 500_000: should return [500_000].
    assertThat(idx.getChangeBlocksUpTo(KEY, 500_000))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(500_000L));

    // T = 999_999: same entry (only one exists).
    assertThat(idx.getChangeBlocksUpTo(KEY, 999_999))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(500_000L));

    // T = 499_999: the only entry is above T → empty.
    assertThat(idx.getChangeBlocksUpTo(KEY, 499_999)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Multiple entries in range 0, all <= T
  // -------------------------------------------------------------------------

  @Test
  void getChangeBlocksUpToMultipleEntriesAllIncluded() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    for (long block : new long[] {100_000L, 300_000L, 700_000L}) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, block);
      tx.commit();
    }

    // T = 700_000: all three included.
    assertThat(idx.getChangeBlocksUpTo(KEY, 700_000))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(100_000L, 300_000L, 700_000L));

    // T = 400_000: first two included.
    assertThat(idx.getChangeBlocksUpTo(KEY, 400_000))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(100_000L, 300_000L));

    // T = 200_000: only first included.
    assertThat(idx.getChangeBlocksUpTo(KEY, 200_000))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(100_000L));
  }

  // -------------------------------------------------------------------------
  // Key not present in this range → empty (bloom negative)
  // -------------------------------------------------------------------------

  @Test
  void getChangeBlocksUpToUnknownKeyReturnsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // No appends at all.
    assertThat(idx.getChangeBlocksUpTo(KEY, 500_000)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Cross-range: T in range 1, entries in range 0 only → empty (different range)
  // -------------------------------------------------------------------------

  @Test
  void getChangeBlocksUpToCrossRangeReturnsEmptyForQueryRangeWithNoEntries() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500_000); // range 0
    tx.commit();

    // T = 1_500_000 in range 1: range 1 has no entries for KEY → empty.
    assertThat(idx.getChangeBlocksUpTo(KEY, 1_500_000)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // With sub-block split: entries span sub-blocks and tail, all <= T
  // -------------------------------------------------------------------------

  @Test
  void getChangeBlocksUpToAfterSplitIncludesSubBlockAndTailEntries() {
    // threshold=10, splitAt=5: after 11 appends sub-block 0 holds offsets 0–4 (×100),
    // tail holds 5–10 (×100).
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    for (int i = 0; i < 11; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 100);
      tx.commit();
    }

    // T = 1000 (last entry): all 11 entries should appear.
    assertThat(idx.getChangeBlocksUpTo(KEY, 1000))
        .hasValueSatisfying(
            arr ->
                assertThat(arr)
                    .containsExactly(
                        0L, 100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L));

    // T = 450: first 5 sub-block entries (0–400) + first tail entry (500 > 450 → excluded) → 5.
    assertThat(idx.getChangeBlocksUpTo(KEY, 450))
        .hasValueSatisfying(arr -> assertThat(arr).containsExactly(0L, 100L, 200L, 300L, 400L));

    // T = 600: first 5 sub-block + 2 tail (500, 600) → 7 entries.
    assertThat(idx.getChangeBlocksUpTo(KEY, 600))
        .hasValueSatisfying(
            arr -> assertThat(arr).containsExactly(0L, 100L, 200L, 300L, 400L, 500L, 600L));
  }

  // ===========================================================================
  // countMutationsInEarlierRanges: new method for cross-range mutation counting
  // ===========================================================================

  // -------------------------------------------------------------------------
  // Range 0 only: countMutationsInEarlierRanges(key, rangeId=0) → 0
  // -------------------------------------------------------------------------

  @Test
  void countMutationsInEarlierRangesRangeZeroIsZero() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 100_000);
    tx.commit();

    // rangeId=0: no earlier ranges → 0.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 0)).isEqualTo(0);
  }

  // -------------------------------------------------------------------------
  // Entries in range 0, query for range 1: count = entries in range 0
  // -------------------------------------------------------------------------

  @Test
  void countMutationsInEarlierRangesCountsRange0EntriesForRange1() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    // Append 3 entries to range 0.
    for (long block : new long[] {100_000L, 400_000L, 700_000L}) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, block);
      tx.commit();
    }

    // rangeId=1: range 0 has 3 entries → 3.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 1)).isEqualTo(3);

    // rangeId=2: still only range 0 has entries → 3.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 2)).isEqualTo(3);
  }

  // -------------------------------------------------------------------------
  // Entries in ranges 0 and 1, query for range 2
  // -------------------------------------------------------------------------

  @Test
  void countMutationsInEarlierRangesAcrossMultipleRanges() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    // 2 entries in range 0.
    var tx0a = kv.startTransaction();
    idx.append(tx0a, KEY, 200_000);
    tx0a.commit();
    var tx0b = kv.startTransaction();
    idx.append(tx0b, KEY, 800_000);
    tx0b.commit();

    // 4 entries in range 1.
    for (long block : new long[] {1_100_000L, 1_300_000L, 1_600_000L, 1_900_000L}) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, block);
      tx.commit();
    }

    // rangeId=1: only range 0 counted → 2.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 1)).isEqualTo(2);

    // rangeId=2: ranges 0 and 1 counted → 2 + 4 = 6.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 2)).isEqualTo(6);
  }

  // -------------------------------------------------------------------------
  // Key absent from earlier ranges → 0
  // -------------------------------------------------------------------------

  @Test
  void countMutationsInEarlierRangesUnknownKeyReturnsZero() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // Append OTHER_KEY to range 0, not KEY.
    var tx = kv.startTransaction();
    idx.append(tx, OTHER_KEY, 100_000);
    tx.commit();

    // KEY has no entries in any range → 0.
    assertThat(idx.countMutationsInEarlierRanges(KEY, 1)).isEqualTo(0);
  }

  // ===========================================================================
  // readRangeList: returns the full (unfiltered) offset list for (naturalKey, rangeId)
  // ===========================================================================

  // -------------------------------------------------------------------------
  // Unknown key → empty (bloom negative)
  // -------------------------------------------------------------------------

  @Test
  void readRangeListUnknownKeyReturnsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    // No appends at all.
    assertThat(idx.readRangeList(KEY, 0)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Key in different range → empty (range-marker negative)
  // -------------------------------------------------------------------------

  @Test
  void readRangeListWrongRangeReturnsEmpty() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 500_000); // range 0
    tx.commit();
    // Querying range 1 where KEY has no entries.
    assertThat(idx.readRangeList(KEY, 1)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Single entry: returns list containing just that offset
  // -------------------------------------------------------------------------

  @Test
  void readRangeListSingleEntryReturnsListWithOneOffset() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);
    var tx = kv.startTransaction();
    idx.append(tx, KEY, 300_000); // range 0, offset 300_000
    tx.commit();

    var listOpt = idx.readRangeList(KEY, 0);
    assertThat(listOpt).isPresent();
    var list = listOpt.get();
    assertThat(list.size()).isEqualTo(1);
    assertThat(list.latestLeq(999_999)).hasValue(300_000);
    assertThat(list.last()).hasValue(300_000);
  }

  // -------------------------------------------------------------------------
  // Multiple entries: returns ALL offsets (unfiltered, no ceiling)
  // -------------------------------------------------------------------------

  @Test
  void readRangeListReturnsAllEntriesUnceiledForRange() {
    var kv = new SegmentedInMemoryKeyValueStorage();
    var idx = new TrieNodeChangeIndex(kv, 1_000_000);

    for (long block : new long[] {100_000L, 400_000L, 700_000L}) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, block);
      tx.commit();
    }

    var listOpt = idx.readRangeList(KEY, 0);
    assertThat(listOpt).isPresent();
    var list = listOpt.get();
    // All three entries returned — no ceiling filter applied.
    assertThat(list.size()).isEqualTo(3);
    assertThat(list.latestLeq(150_000)).hasValue(100_000);
    assertThat(list.latestLeq(500_000)).hasValue(400_000);
    assertThat(list.last()).hasValue(700_000);
  }

  // -------------------------------------------------------------------------
  // After split: readRangeList returns combined sub-blocks + tail (all entries)
  // -------------------------------------------------------------------------

  @Test
  void readRangeListAfterSplitReturnsCombinedList() {
    // threshold=10, splitAt=5: after 11 appends sub-block 0 holds offsets 0–4 (×100),
    // tail holds 5–10 (×100). readRangeList should return all 11 entries.
    var kv = new org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage();
    var idx = smallThresholdIndex(kv);

    for (int i = 0; i < 11; i++) {
      var tx = kv.startTransaction();
      idx.append(tx, KEY, i * 100);
      tx.commit();
    }

    var listOpt = idx.readRangeList(KEY, 0);
    assertThat(listOpt).isPresent();
    var list = listOpt.get();
    assertThat(list.size()).isEqualTo(11);

    // Verify all offsets are present and in order.
    for (int i = 0; i < 11; i++) {
      assertThat(list.get(i)).isEqualTo(i * 100);
    }
    assertThat(list.last()).hasValue(1000);
    assertThat(list.latestLeq(350)).hasValue(300);
  }
}
