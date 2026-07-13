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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryComposition.Category;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class TrieNodeHistoryCompositionTest {

  private static final int MIN_BLOB_SIZE = 100;
  private static final int FULL_ABOVE_DEPTH = 2;
  private static final byte[] ACCOUNT_HASH = new byte[32];

  // -------------------------------------------------------------------------
  // Helpers to build node RLPs and history-CF entries/keys.
  // -------------------------------------------------------------------------

  /** A branch (17-item) RLP big enough (>100 bytes) to land in a blob file. */
  private static Bytes bigBranchRlp() {
    return RLP.encode(
        out -> {
          out.startList();
          for (int i = 0; i < 16; i++) {
            out.writeBytes(Bytes32.leftPad(Bytes.of(i + 1))); // 16 x 33-byte hash refs
          }
          out.writeNull();
          out.endList();
        });
  }

  /** A short (2-item) RLP small enough (<100 bytes) to stay inline in the SST. */
  private static Bytes smallShortRlp() {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(Bytes.fromHexString("0x1234"));
          out.writeBytes(Bytes.fromHexString("0xdeadbeef"));
          out.endList();
        });
  }

  private static byte[] accountKey(final int locationBytes, final long block) {
    final Bytes location = Bytes.repeat((byte) 0x11, locationBytes);
    return ArchiveNodeKey.historyKey(location, block).toArrayUnsafe();
  }

  private static byte[] storageKey(final int locationBytes, final long block) {
    final Bytes location = Bytes.repeat((byte) 0x11, locationBytes);
    final Bytes natural = ArchiveNodeKey.storage(Bytes.wrap(ACCOUNT_HASH), location);
    return ArchiveNodeKey.historyKey(natural, block).toArrayUnsafe();
  }

  // =========================================================================
  // classify() — pure category logic
  // =========================================================================

  @Test
  void classifiesFullCreationBranch() {
    final byte[] value = TrieNodeDiffCodec.encodeDiff(null, bigBranchRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.CREATION_BRANCH);
  }

  @Test
  void classifiesFullCheckpointBranchAtDeepLocation() {
    // Non-creation FULL at a deep location => checkpoint, not upper-trie.
    final byte[] value = TrieNodeDiffCodec.encodeFull(bigBranchRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.CHECKPOINT_BRANCH);
  }

  @Test
  void classifiesUpperTrieFullForShallowAccountLocation() {
    final byte[] value = TrieNodeDiffCodec.encodeFull(bigBranchRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(2, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.UPPER_TRIE_BRANCH);
  }

  @Test
  void classifiesUpperTrieFullForShallowStorageLocation() {
    // storage natural key = 32-byte account hash + 1-byte location => location depth 1 <= 2.
    final byte[] value = TrieNodeDiffCodec.encodeFull(bigBranchRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(storageKey(1, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.UPPER_TRIE_BRANCH);
  }

  @Test
  void classifiesDeepStorageFullAsCheckpoint() {
    // storage natural key = 32 + 6 location bytes => depth 6 > 2 => checkpoint.
    final byte[] value = TrieNodeDiffCodec.encodeFull(bigBranchRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(storageKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.CHECKPOINT_BRANCH);
  }

  @Test
  void classifiesDiffShort() {
    final byte[] value =
        TrieNodeDiffCodec.encodeDiff(smallShortRlp(), smallShortRlp()).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.DIFF_SHORT);
  }

  @Test
  void classifiesDeletionTombstone() {
    final byte[] value = TrieNodeDiffCodec.encodeDiff(smallShortRlp(), null).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.DELETION);
  }

  @Test
  void classifiesUnparseableFullAsUnknownShape() {
    // ENTRY_FULL metadata but a body that is not a valid RLP list => arity unknown.
    final byte[] value = TrieNodeDiffCodec.encodeFull(Bytes.fromHexString("0xff")).toArrayUnsafe();
    assertThat(TrieNodeHistoryComposition.classify(accountKey(6, 100), value, FULL_ABOVE_DEPTH))
        .isEqualTo(Category.CHECKPOINT_UNKNOWN);
  }

  // =========================================================================
  // record() — accounting into buckets
  // =========================================================================

  @Test
  void recordsBlobBytesForLargeFullEntry() {
    final TrieNodeHistoryComposition comp =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    final byte[] key = accountKey(6, 100);
    final byte[] value = TrieNodeDiffCodec.encodeDiff(null, bigBranchRlp()).toArrayUnsafe();
    assertThat(value.length).isGreaterThanOrEqualTo(MIN_BLOB_SIZE);

    comp.record(key, value);

    final TrieNodeHistoryComposition.Bucket b = comp.bucket(Category.CREATION_BRANCH);
    assertThat(b.count()).isEqualTo(1);
    assertThat(b.valueBytes()).isEqualTo(value.length);
    assertThat(b.keyBytes()).isEqualTo(key.length);
    assertThat(b.blobCount()).isEqualTo(1);
    assertThat(b.blobValueBytes()).isEqualTo(value.length);
    assertThat(comp.totalEntries()).isEqualTo(1);
  }

  @Test
  void recordsInlineBytesForSmallDiffEntry() {
    final TrieNodeHistoryComposition comp =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    final byte[] value =
        TrieNodeDiffCodec.encodeDiff(smallShortRlp(), smallShortRlp()).toArrayUnsafe();
    assertThat(value.length).isLessThan(MIN_BLOB_SIZE);

    comp.record(accountKey(6, 100), value);

    final TrieNodeHistoryComposition.Bucket b = comp.bucket(Category.DIFF_SHORT);
    assertThat(b.count()).isEqualTo(1);
    assertThat(b.blobCount()).isEqualTo(0);
    assertThat(b.blobValueBytes()).isEqualTo(0);
  }

  @Test
  void blobThresholdIsInclusiveAtMinBlobSize() {
    final TrieNodeHistoryComposition comp =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    // DIFF short metadata byte (0x00): no RLP parse needed; category is stable.
    final byte[] inline = new byte[MIN_BLOB_SIZE - 1];
    final byte[] blob = new byte[MIN_BLOB_SIZE];

    comp.record(accountKey(6, 1), inline);
    comp.record(accountKey(6, 2), blob);

    final TrieNodeHistoryComposition.Bucket b = comp.bucket(Category.DIFF_SHORT);
    assertThat(b.count()).isEqualTo(2);
    assertThat(b.blobCount()).isEqualTo(1);
    assertThat(b.blobValueBytes()).isEqualTo(MIN_BLOB_SIZE);
  }

  @Test
  void mergeAddsBucketTotalsAndHistogram() {
    final TrieNodeHistoryComposition a =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    final TrieNodeHistoryComposition b =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    final byte[] creationBranch =
        TrieNodeDiffCodec.encodeDiff(null, bigBranchRlp()).toArrayUnsafe();
    final byte[] diffShort =
        TrieNodeDiffCodec.encodeDiff(smallShortRlp(), smallShortRlp()).toArrayUnsafe();

    a.record(accountKey(6, 1), creationBranch); // depth 6, creation branch, blob
    b.record(accountKey(6, 2), diffShort); // depth 6, diff short, inline
    b.record(storageKey(3, 3), creationBranch); // depth 3, creation branch, blob

    a.merge(b);

    assertThat(a.totalEntries()).isEqualTo(3);
    assertThat(a.bucket(Category.CREATION_BRANCH).count()).isEqualTo(2);
    assertThat(a.bucket(Category.CREATION_BRANCH).blobValueBytes())
        .isEqualTo(2L * creationBranch.length);
    assertThat(a.bucket(Category.DIFF_SHORT).count()).isEqualTo(1);
    final long[] hist = a.locationDepthHistogram();
    assertThat(hist[6]).isEqualTo(2);
    assertThat(hist[3]).isEqualTo(1);
  }

  @Test
  void tracksLocationDepthHistogramInBytes() {
    final TrieNodeHistoryComposition comp =
        new TrieNodeHistoryComposition(MIN_BLOB_SIZE, FULL_ABOVE_DEPTH);
    final byte[] value = TrieNodeDiffCodec.encodeFull(bigBranchRlp()).toArrayUnsafe();

    comp.record(accountKey(0, 1), value); // depth 0
    comp.record(storageKey(3, 2), value); // depth 3 (32 + 3 - 32)

    final long[] hist = comp.locationDepthHistogram();
    assertThat(hist[0]).isEqualTo(1);
    assertThat(hist[3]).isEqualTo(1);
  }
}
