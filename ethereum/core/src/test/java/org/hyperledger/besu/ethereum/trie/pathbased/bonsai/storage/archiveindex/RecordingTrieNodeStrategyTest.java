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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class RecordingTrieNodeStrategyTest {

  // A 2-item RLP list (extension/leaf shape = "short").
  private static final Bytes SHORT_NODE = shortNodeWith(Bytes.of(0x12), Bytes.of(0x34));

  // Helper: builds a 17-item branch-node RLP with one changed child slot (index 0) so DIFF
  // encodings are non-trivial; other slots and the terminal value are empty.
  private static Bytes branchWith(final Bytes child0) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(child0 == null || child0.isEmpty() ? Bytes.EMPTY : child0);
          for (int i = 1; i < 16; i++) {
            out.writeNull();
          }
          out.writeNull(); // terminal value
          out.endList();
        });
  }

  private static Bytes shortNodeWith(final Bytes path, final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(path);
          out.writeBytes(value);
          out.endList();
        });
  }

  @Test
  void creationRecordsFullSizeAtLocationDepthAndDelegates() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    when(storage.get(any(), any())).thenReturn(Optional.empty()); // no prior -> creation

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Bytes location = Bytes.of(0x01, 0x02);
    final Bytes32 hash = Bytes32.ZERO;
    final Bytes node = SHORT_NODE;

    rec.putFlatAccountTrieNode(storage, tx, location, hash, node);

    verify(delegate).putFlatAccountTrieNode(storage, tx, location, hash, node);
    assertThat(rec.result().writesByDepth()[2]).isEqualTo(1L);
  }

  @Test
  void creationRecordsEqualFullAndDiffSizes() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    when(storage.get(any(), any())).thenReturn(Optional.empty());

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Bytes location = Bytes.of(0x01, 0x02);
    rec.putFlatAccountTrieNode(storage, tx, location, Bytes32.ZERO, SHORT_NODE);

    final CalibrationResult result = rec.result();
    // Creation: diffSize must equal fullSize (encodeDiff(null, node) == encodeFull(node) size).
    assertThat(result.diffShortBytesByDepth()[2]).isEqualTo(result.fullShortBytesByDepth()[2]);
  }

  @Test
  void subsequentWriteRecordsSmallerDiffThanFull() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    final Bytes priorBranch = branchWith(Bytes.EMPTY);
    final Bytes newBranch = branchWith(Bytes.repeat((byte) 0xab, 32));
    when(storage.get(any(), any())).thenReturn(Optional.of(priorBranch.toArrayUnsafe()));

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Bytes location = Bytes.of(0x03, 0x04, 0x05);
    rec.putFlatAccountTrieNode(storage, tx, location, Bytes32.ZERO, newBranch);

    final CalibrationResult result = rec.result();
    assertThat(result.writesByDepth()[3]).isEqualTo(1L);
    assertThat(result.diffBranchBytesByDepth()[3]).isLessThan(result.fullBranchBytesByDepth()[3]);
  }

  @Test
  void storageWriteUsesLocationSizeAsDepthAndDelegates() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    when(storage.get(any(), any())).thenReturn(Optional.empty());

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Hash accountHash = Hash.ZERO;
    final Bytes location = Bytes.of(0x0a);
    rec.putFlatStorageTrieNode(storage, tx, accountHash, location, Bytes32.ZERO, SHORT_NODE);

    verify(delegate)
        .putFlatStorageTrieNode(storage, tx, accountHash, location, Bytes32.ZERO, SHORT_NODE);
    assertThat(rec.result().writesByDepth()[1]).isEqualTo(1L);
  }

  @Test
  void readsAndRemovesDelegateWithoutRecording() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    final Bytes location = Bytes.of(0x01);
    when(delegate.getFlatAccountTrieNode(location, Bytes32.ZERO, storage))
        .thenReturn(Optional.of(SHORT_NODE));

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Optional<Bytes> result = rec.getFlatAccountTrieNode(location, Bytes32.ZERO, storage);

    assertThat(result).contains(SHORT_NODE);
    rec.removeFlatAccountStateTrieNode(storage, tx, location);
    verify(delegate).removeFlatAccountStateTrieNode(storage, tx, location);
    // No writes should have been recorded by reads/removals.
    assertThat(rec.result().writesByDepth()[1]).isEqualTo(0L);
  }
}
