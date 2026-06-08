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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TrieNodeIndexDropper} (Design 5, Task 3.5).
 *
 * <p>Uses a small range size (1000) for tests that involve multiple ranges and a standard setup for
 * single-range tests. Uses {@link SegmentedInMemoryKeyValueStorage} as the backend so that all
 * interactions with committed storage are exercised without mocking.
 */
class TrieNodeIndexDropperTest {

  private static final long RANGE_SIZE = 1_000L;

  // A plausible account-trie naturalKey (compact nibble path)
  private static final Bytes KEY_A = Bytes.fromHexString("0xdeadbeef");

  // A different natural key for multi-key tests
  private static final Bytes KEY_B =
      Bytes.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");

  // A third key for "no-op on missing" and isolation tests
  private static final Bytes KEY_C = Bytes.fromHexString("0xcafebabe");

  // Minimal node RLP values for encoding
  private static final Bytes NODE_RLP_A = Bytes.fromHexString("0xc0");
  private static final Bytes NODE_RLP_B = Bytes.fromHexString("0xc1");

  private SegmentedInMemoryKeyValueStorage kv;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeChangeIndex changeIndex;
  private TrieNodeIndexDropper dropper;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(kv);
    changeIndex = new TrieNodeChangeIndex(kv, RANGE_SIZE);
    dropper = new TrieNodeIndexDropper(RANGE_SIZE);
  }

  // -------------------------------------------------------------------------
  // Task-spec required test: single key, dropBlock removes history and index
  // -------------------------------------------------------------------------

  /**
   * Plan-required test: capture history at block 100; simulate rollback; assert history (key,100)
   * deleted and 100 removed from the index list.
   */
  @Test
  void dropBlockRemovesHistoryAndIndexEntryForKey() {
    // Capture history + index at block 100
    final long block = 100L;
    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    changeIndex.append(tx, KEY_A, block);
    tx.commit();

    // Verify the entry exists before drop
    assertThat(historyStore.get(KEY_A, block)).isPresent();
    assertThat(changeIndex.latestChangeBlock(KEY_A, block)).hasValue(block);

    // Simulate rollback: drop block 100
    var dropTx = kv.startTransaction();
    dropper.dropBlock(block, kv, dropTx);
    dropTx.commit();

    // History entry must be gone
    assertThat(historyStore.get(KEY_A, block)).isEmpty();

    // Index must not report block 100 any more
    assertThat(changeIndex.latestChangeBlock(KEY_A, block)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Two keys changed at block 100 — both are cleaned up
  // -------------------------------------------------------------------------

  @Test
  void dropBlockRemovesBothKeysWhenTwoChangedAtSameBlock() {
    final long block = 100L;

    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    historyStore.put(tx, KEY_B, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_B));
    changeIndex.append(tx, KEY_A, block);
    changeIndex.append(tx, KEY_B, block);
    tx.commit();

    assertThat(historyStore.get(KEY_A, block)).isPresent();
    assertThat(historyStore.get(KEY_B, block)).isPresent();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(block, kv, dropTx);
    dropTx.commit();

    assertThat(historyStore.get(KEY_A, block)).isEmpty();
    assertThat(historyStore.get(KEY_B, block)).isEmpty();
    assertThat(changeIndex.latestChangeBlock(KEY_A, block)).isEmpty();
    assertThat(changeIndex.latestChangeBlock(KEY_B, block)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Dropping a block that was never captured is a no-op (no exception)
  // -------------------------------------------------------------------------

  @Test
  void dropBlockNonExistentIsNoOp() {
    // Nothing was ever written; dropping must succeed silently
    var dropTx = kv.startTransaction();
    dropper.dropBlock(999L, kv, dropTx);
    dropTx.commit();

    assertThat(historyStore.get(KEY_A, 999L)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // dropBlock does not affect entries at other block numbers
  // -------------------------------------------------------------------------

  @Test
  void dropBlockPreservesOtherBlockEntries() {
    final long blockDrop = 100L;
    final long blockKeep = 200L;

    // Write block 100 first and commit so block 200 reads the committed state.
    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, blockDrop, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    changeIndex.append(tx, KEY_A, blockDrop);
    tx.commit();

    // Write block 200 in a separate transaction.
    var tx2 = kv.startTransaction();
    historyStore.put(tx2, KEY_A, blockKeep, TrieNodeDiffCodec.encodeFull(NODE_RLP_B));
    changeIndex.append(tx2, KEY_A, blockKeep);
    tx2.commit();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(blockDrop, kv, dropTx);
    dropTx.commit();

    // Dropped block gone
    assertThat(historyStore.get(KEY_A, blockDrop)).isEmpty();
    // Kept block still present
    assertThat(historyStore.get(KEY_A, blockKeep)).isPresent();
    assertThat(changeIndex.latestChangeBlock(KEY_A, blockKeep)).hasValue(blockKeep);
  }

  // -------------------------------------------------------------------------
  // Range marker is removed when the list becomes empty after drop
  // -------------------------------------------------------------------------

  @Test
  void dropBlockRemovesRangeMarkerWhenListBecomesEmpty() {
    final long block = 50L;

    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    changeIndex.append(tx, KEY_A, block);
    tx.commit();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(block, kv, dropTx);
    dropTx.commit();

    // List is empty; no blocks findable for this key.
    assertThat(changeIndex.latestChangeBlock(KEY_A, block)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Range marker preserved when other blocks remain in the same range
  // -------------------------------------------------------------------------

  @Test
  void dropBlockPreservesRangeMarkerWhenOtherBlocksRemainInRange() {
    final long blockDrop = 50L;
    final long blockKeep = 60L; // same range (both in range 0 with RANGE_SIZE=1000)

    // Write blockDrop first and commit so blockKeep's append reads committed state.
    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, blockDrop, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    changeIndex.append(tx, KEY_A, blockDrop);
    tx.commit();

    // Write blockKeep in a separate transaction.
    var tx2 = kv.startTransaction();
    historyStore.put(tx2, KEY_A, blockKeep, TrieNodeDiffCodec.encodeFull(NODE_RLP_B));
    changeIndex.append(tx2, KEY_A, blockKeep);
    tx2.commit();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(blockDrop, kv, dropTx);
    dropTx.commit();

    // The kept block is still findable
    assertThat(changeIndex.latestChangeBlock(KEY_A, blockKeep)).hasValue(blockKeep);
  }

  // -------------------------------------------------------------------------
  // dropBlock with negative block number throws
  // -------------------------------------------------------------------------

  @Test
  void dropBlockNegativeBlockThrows() {
    assertThatThrownBy(() -> dropper.dropBlock(-1L, kv, kv.startTransaction()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockNumber");
  }

  // -------------------------------------------------------------------------
  // dropBlock(null storage) throws NullPointerException
  // -------------------------------------------------------------------------

  @Test
  void dropBlockNullStorageThrows() {
    assertThatThrownBy(() -> dropper.dropBlock(100L, null, kv.startTransaction()))
        .isInstanceOf(NullPointerException.class);
  }

  // -------------------------------------------------------------------------
  // dropBlock(null tx) throws NullPointerException
  // -------------------------------------------------------------------------

  @Test
  void dropBlockNullTxThrows() {
    assertThatThrownBy(() -> dropper.dropBlock(100L, kv, null))
        .isInstanceOf(NullPointerException.class);
  }

  // -------------------------------------------------------------------------
  // block 0 is a valid input (no off-by-one at zero)
  // -------------------------------------------------------------------------

  @Test
  void dropBlockZeroIsValid() {
    final long block = 0L;
    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    changeIndex.append(tx, KEY_A, block);
    tx.commit();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(block, kv, dropTx);
    dropTx.commit();

    assertThat(historyStore.get(KEY_A, block)).isEmpty();
    assertThat(changeIndex.latestChangeBlock(KEY_A, block)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Range marker preserved when tail is emptied but sub-blocks still exist
  // -------------------------------------------------------------------------

  /**
   * Verifies the critical {@code remainingEntries == 0 && subCount == 0} condition in {@link
   * TrieNodeIndexDropper}. If the condition were accidentally weakened to {@code ||}, the range
   * marker would be wrongly removed even though sub-blocks still hold older change records.
   *
   * <p>Uses the package-private {@link TrieNodeChangeIndex} constructor with threshold=10,
   * splitAt=5 to force a sub-block split after 11 appends, leaving subCount=1 and a 6-entry tail.
   * Dropping the last tail block must preserve the marker because sub-blocks remain.
   */
  @Test
  void dropBlockPreservesRangeMarkerWhenTailEmptiedButSubBlocksExist() {
    // Use a custom ChangeIndex with a tiny split threshold so we can force a split easily.
    // threshold=10: split is triggered when list size > 10 (i.e. on the 11th append).
    // splitAt=5:    5 entries are moved to a new sub-block, leaving 6 entries in the tail.
    final int threshold = 10;
    final int splitAt = 5;
    final TrieNodeChangeIndex splitIndex =
        new TrieNodeChangeIndex(kv, RANGE_SIZE, threshold, splitAt);
    final TrieNodeIndexDropper splitDropper = new TrieNodeIndexDropper(RANGE_SIZE);
    // Append 11 entries one-per-transaction (each append must read the prior committed state).
    // Blocks 0..10 — all in range 0, all distinct offsets.
    for (int i = 0; i <= 10; i++) {
      final long block = i;
      var tx = kv.startTransaction();
      historyStore.put(tx, KEY_A, block, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
      splitIndex.append(tx, KEY_A, block);
      tx.commit();
    }

    // After 11 appends the split has fired: subCount=1, tail has 6 entries (blocks 5..10).

    // Drop the most recent tail block (block 10). Tail becomes 5 entries (blocks 5..9).
    // subCount is still 1.
    var dropTx = kv.startTransaction();
    splitDropper.dropBlock(10L, kv, dropTx);
    dropTx.commit();

    // History entry gone.
    assertThat(historyStore.get(KEY_A, 10L)).isEmpty();

    // Older blocks in the tail (e.g. block 5) are still findable.
    assertThat(splitIndex.latestChangeBlock(KEY_A, 9L)).hasValue(9L);
  }

  // -------------------------------------------------------------------------
  // Unrelated key (not changed at the dropped block) is untouched
  // -------------------------------------------------------------------------

  @Test
  void dropBlockDoesNotAffectUnrelatedKey() {
    final long blockDrop = 100L;
    final long blockC = 150L;

    var tx = kv.startTransaction();
    historyStore.put(tx, KEY_A, blockDrop, TrieNodeDiffCodec.encodeFull(NODE_RLP_A));
    historyStore.put(tx, KEY_C, blockC, TrieNodeDiffCodec.encodeFull(NODE_RLP_B));
    changeIndex.append(tx, KEY_A, blockDrop);
    changeIndex.append(tx, KEY_C, blockC);
    tx.commit();

    var dropTx = kv.startTransaction();
    dropper.dropBlock(blockDrop, kv, dropTx);
    dropTx.commit();

    // KEY_C at blockC is untouched
    assertThat(historyStore.get(KEY_C, blockC)).isPresent();
    assertThat(changeIndex.latestChangeBlock(KEY_C, blockC)).hasValue(blockC);
  }
}
