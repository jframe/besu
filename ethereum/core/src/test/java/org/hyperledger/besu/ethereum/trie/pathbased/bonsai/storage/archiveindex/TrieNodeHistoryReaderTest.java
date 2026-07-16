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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrieNodeHistoryReaderTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryReader reader;
  private final Bytes naturalKey = Bytes.fromHexString("0x0a0b");

  /** A compact-encoded leaf path shared by every fixture node in this test. */
  private static final Bytes LEAF_PATH = Bytes.fromHexString("0x20ab");

  /**
   * Builds a minimal valid 2-item short (leaf) node RLP so real {@link TrieNodeDiffCodec} arity
   * checks (which require an actual 2-item or 17-item RLP list, not arbitrary bytes) succeed. Only
   * the value byte varies between fixtures so distinct "versions" of the same node are
   * distinguishable.
   */
  private static Bytes leafNode(final int valueByte) {
    final Bytes value = Bytes.of((byte) valueByte);
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(LEAF_PATH);
          out.writeRaw(RLP.encodeOne(value));
          out.endList();
        });
  }

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_NODE_HISTORY_ARCHIVE_V2));
    reader = new TrieNodeHistoryReader(storage);
  }

  private void writeFull(final long block, final Bytes nodeRlp) {
    final Bytes entry =
        HistoryEntryCodec.encode(
            HistoryEntryCodec.EntryType.FULL, 0, TrieNodeDiffCodec.encodeFull(nodeRlp));
    put(block, entry);
  }

  private void writeDiff(
      final long block, final int countSinceFull, final Bytes oldRlp, final Bytes newRlp) {
    final Bytes entry =
        HistoryEntryCodec.encode(
            HistoryEntryCodec.EntryType.DIFF,
            countSinceFull,
            TrieNodeDiffCodec.encodeDiff(oldRlp, newRlp));
    put(block, entry);
  }

  /**
   * Writes a DELETION tombstone entry directly (bypassing the normal writer, since no writer exists
   * yet — per the design, ArchiveTrieBuilder never emits these; this simulates corrupt/legacy data
   * for the defensive-guard test).
   */
  private void writeDeletionTombstone(
      final long block, final int countSinceFull, final Bytes oldRlp) {
    final Bytes entry =
        HistoryEntryCodec.encode(
            HistoryEntryCodec.EntryType.DIFF,
            countSinceFull,
            TrieNodeDiffCodec.encodeDiff(oldRlp, null));
    put(block, entry);
  }

  private void put(final long block, final Bytes entry) {
    final var tx = storage.startTransaction();
    tx.put(
        TRIE_NODE_HISTORY_ARCHIVE_V2,
        HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, naturalKey, block).toArrayUnsafe(),
        entry.toArrayUnsafe());
    tx.commit();
  }

  @Test
  void returnsEmptyWhenNoHistoryExistsForNode() {
    assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 100L)).isEmpty();
  }

  @Test
  void returnsDirectFullHitWithNoWalk() {
    final Bytes nodeRlp = leafNode(0x00);
    writeFull(10L, nodeRlp);
    final Optional<TrieNodeHistoryReader.Hit> hit =
        reader.nodeAtWithMeta(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 50L);
    assertThat(hit).isPresent();
    assertThat(hit.get().nodeRlp()).isEqualTo(nodeRlp);
    assertThat(hit.get().countSinceFull()).isEqualTo(0);
  }

  @Test
  void reconstructsThroughASingleDiff() {
    final Bytes v0 = leafNode(0xaa);
    final Bytes v1 = leafNode(0xbb);
    writeFull(0L, v0);
    writeDiff(1L, 1, v0, v1);

    final Optional<Bytes> result = reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 5L);
    assertThat(result).contains(v1);
  }

  @Test
  void reconstructsThroughFifteenChainedDiffs() {
    Bytes prior = leafNode(0x80);
    writeFull(0L, prior);
    for (int i = 1; i <= 15; i++) {
      final Bytes next = leafNode(i);
      writeDiff(i, i, prior, next);
      prior = next;
    }
    assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 15L)).contains(prior);
  }

  @Test
  void throwsWhenChainExceedsMaxBackwardWalkStepsWithoutAFull() {
    // Simulate corrupt/missing history: 70 chained diffs, no FULL anywhere.
    Bytes prior = leafNode(0x80);
    // First entry is a DIFF (not FULL) against an unwritten predecessor — deliberately broken.
    writeDiff(0L, 1, prior, prior);
    for (int i = 1; i <= 70; i++) {
      final Bytes next = leafNode(i % 256);
      writeDiff(i, i + 1, prior, next);
      prior = next;
    }
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 70L))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void throwsIllegalStateNotIllegalArgumentWhenChainRunsPastBlockZeroWithoutAFull() {
    // Corrupt/missing history: the earliest entry (block 0) was mis-typed as DIFF instead of
    // FULL/FULL_CREATION. Chain depth here (6 entries) is well within MAX_BACKWARD_WALK_STEPS
    // (64), so this must NOT be caught by the step-count guard -- it exercises the separate
    // block-underflow guard when the walk steps past block 0 with no FULL ever found. Before the
    // fix, walkBlock would go negative and HistoryKey.encode's "block must be >= 0" check would
    // throw IllegalArgumentException instead of the IllegalStateException the corruption-guard
    // contract requires.
    Bytes prior = leafNode(0x80);
    writeDiff(0L, 1, prior, prior); // block 0 deliberately mis-typed as DIFF, not FULL
    for (int i = 1; i <= 5; i++) {
      final Bytes next = leafNode(i);
      writeDiff(i, i + 1, prior, next);
      prior = next;
    }
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 5L))
        .isInstanceOf(IllegalStateException.class)
        .isNotInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void throwsIllegalStateWhenChainContainsADeletionTombstone() {
    // No writer exists yet (ArchiveTrieBuilder is a later task) and the design says the writer
    // never emits DELETION-tagged entries, but TrieNodeDiffCodec#decode can still produce a
    // DELETION-shaped Decoded if ever handed corrupt/legacy data. The reader must fail closed with
    // a clear, specific error rather than let reconstruct() throw an unrelated
    // IllegalArgumentException three calls away.
    final Bytes v0 = leafNode(0xaa);
    writeFull(0L, v0);
    writeDeletionTombstone(1L, 1, v0);

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 5L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("DELETION");
  }

  @Test
  void picksTheNewestEntryAtOrBeforeTargetBlockIgnoringLaterOnes() {
    final Bytes v0 = leafNode(0xaa);
    final Bytes v1 = leafNode(0xbb);
    writeFull(0L, v0);
    writeFull(100L, v1); // written "later" — must not be visible at block 50
    assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 50L)).contains(v0);
    assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 100L)).contains(v1);
  }
}
