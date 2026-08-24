/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveHistoryReaderTest {

  /** Minimal valid branch-node RLP: 16 empty children + empty terminal value. */
  private static Bytes emptyBranchRlp() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < 16; i++) {
      out.writeNull();
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  /** Branch node with one child slot replaced by a 33-byte raw RLP hash ref. */
  private static Bytes branchRlpWithChild(final int slot, final Bytes childRlp) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < 16; i++) {
      if (i == slot) {
        out.writeRaw(childRlp);
      } else {
        out.writeNull();
      }
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  private static Bytes dummyChildRlp() {
    // 0xa0 = RLP string of length 32, followed by 32 sequential bytes
    final byte[] raw = new byte[33];
    raw[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) {
      raw[i] = (byte) i;
    }
    return Bytes.wrap(raw);
  }

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore store;
  private ArchiveHistoryReader reader;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    store = new ArchiveNodeHistoryStore(storage);
    reader = new ArchiveHistoryReader(store);
  }

  private void putFull(final Bytes nk, final long block, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(nk, block),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node)));
    tx.commit();
  }

  private void putDiff(
      final Bytes nk,
      final long block,
      final int counter,
      final Bytes oldNode,
      final Bytes newNode) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(nk, block),
        ArchiveNodeHistoryStore.encodeStoredValue(
            counter, NodeLogCodec.encodeDiff(MptNodeCodecAdapter.INSTANCE, oldNode, newNode)));
    tx.commit();
  }

  @Test
  void returnsFullNodeDirectly() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0e));
    final Bytes node = emptyBranchRlp();
    putFull(nk, 5L, node);
    assertThat(reader.nodeAt(nk, 9L)).contains(node);
    assertThat(reader.nodeAt(nk, 4L)).isEmpty();
  }

  @Test
  void reconstructsNodeFromDiff() {
    final Bytes nodeV1 = emptyBranchRlp();
    final Bytes nodeV2 = branchRlpWithChild(0, dummyChildRlp());
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x01));

    putFull(nk, 100L, nodeV1);
    putDiff(nk, 101L, 1, nodeV1, nodeV2);

    assertThat(reader.nodeAt(nk, 100L)).contains(nodeV1);
    assertThat(reader.nodeAt(nk, 101L)).contains(nodeV2);
    assertThat(reader.nodeAt(nk, 200L)).contains(nodeV2);
  }

  @Test
  void reconstructsAccountRootDiffChain() {
    // Account root natural key is [0x00] (length prefix 0). Prove a root FULL + DIFF reconstructs.
    final Bytes nk = ArchiveNodeKey.account(Bytes.EMPTY);
    final Bytes v1 = emptyBranchRlp();
    final Bytes v2 = branchRlpWithChild(0, dummyChildRlp());

    putFull(nk, 100L, v1);
    putDiff(nk, 101L, 1, v1, v2);

    assertThat(reader.nodeAt(nk, 100L)).contains(v1);
    assertThat(reader.nodeAt(nk, 101L)).contains(v2);
    assertThat(reader.nodeAt(nk, 200L)).contains(v2);
  }

  @Test
  void reconstructsStorageRootDiffChain() {
    // Storage root natural key is accountHash(32) ‖ [0x00]. Prove it reconstructs independently.
    final Bytes accountHash = Bytes32.leftPad(Bytes.of(0x09));
    final Bytes nk = ArchiveNodeKey.storage(accountHash, Bytes.EMPTY);
    final Bytes v1 = emptyBranchRlp();
    final Bytes v2 = branchRlpWithChild(1, dummyChildRlp());

    putFull(nk, 50L, v1);
    putDiff(nk, 51L, 1, v1, v2);

    assertThat(reader.nodeAt(nk, 50L)).contains(v1);
    assertThat(reader.nodeAt(nk, 51L)).contains(v2);
  }

  @Test
  void reconstructsNodeAtMaxDiffChainLength() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x03));
    Bytes prev = emptyBranchRlp();
    putFull(nk, 0L, prev);
    final int chainLength = ArchiveHistoryReader.CHECKPOINT_INTERVAL - 1;
    for (int i = 1; i <= chainLength; i++) {
      final Bytes next = branchRlpWithChild(i % 16, dummyChildRlp());
      putDiff(nk, i, i, prev, next);
      prev = next;
    }
    assertThat(reader.nodeAt(nk, chainLength)).contains(prev);
  }

  @Test
  void returnsDeletionAsEmpty() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x02));
    final Bytes node = emptyBranchRlp();
    putFull(nk, 10L, node);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(nk, 11L),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeDiff(node, null)));
    tx.commit();

    assertThat(reader.nodeAt(nk, 11L)).isEmpty();
    assertThat(reader.nodeAt(nk, 15L)).isEmpty();
  }

  @Test
  void rejectsNegativeBlock() {
    assertThatThrownBy(() -> reader.nodeAt(Bytes.of(0x01, 0x0e), -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructsNodeAtMaxBackwardWalkBoundary() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x06));
    Bytes prev = emptyBranchRlp();
    putFull(nk, 0L, prev);
    for (int i = 1; i <= ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS; i++) {
      final Bytes next = branchRlpWithChild(i % 16, dummyChildRlp());
      putDiff(nk, i, i, prev, next);
      prev = next;
    }
    assertThat(reader.nodeAt(nk, ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS)).contains(prev);
  }

  @Test
  void reconstructsNodeAtShallowIntervalBoundary() {
    // Shallow nodes use SHALLOW_CHECKPOINT_INTERVAL (32): write a FULL then 31 DIFFs and confirm
    // the reader can reconstruct any block in the chain — proving the window is wide enough.
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x05));
    Bytes prev = emptyBranchRlp();
    putFull(nk, 0L, prev);
    final int shallowInterval = ArchiveTrieNodeCapture.SHALLOW_CHECKPOINT_INTERVAL;
    for (int i = 1; i < shallowInterval; i++) {
      final Bytes next = branchRlpWithChild(i % 16, dummyChildRlp());
      putDiff(nk, i, i, prev, next);
      prev = next;
    }
    // The last DIFF in the shallow run (block shallowInterval - 1) must be reconstructible.
    assertThat(reader.nodeAt(nk, shallowInterval - 1)).contains(prev);
  }

  @Test
  void reconstructsEveryVersionOfASemanticDiffChain() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0c));
    final java.util.List<Bytes> versions = new java.util.ArrayList<>();
    Bytes prev = branchRlpWithChild(0, dummyChildRlp());
    putFull(nk, 0L, prev);
    versions.add(prev);
    for (int i = 1; i < ArchiveTrieNodeCapture.DEEP_CHECKPOINT_INTERVAL; i++) {
      final Bytes next = branchRlpWithChild(i % 16, dummyChildRlp());
      final Bytes entry = NodeLogCodec.encodeDiff(MptNodeCodecAdapter.INSTANCE, prev, next);
      final org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction tx =
          storage.startTransaction();
      store.putEncoded(
          tx,
          ArchiveNodeKey.historyKey(nk, i),
          ArchiveNodeHistoryStore.encodeStoredValue(i, entry));
      tx.commit();
      versions.add(next);
      prev = next;
    }
    for (int i = 0; i < versions.size(); i++) {
      assertThat(reader.nodeAt(nk, i)).contains(versions.get(i));
    }
  }

  @Test
  void givesUpWhenDiffChainExceedsMaxBackwardWalkSteps() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x07));
    // Use valid branch RLP nodes where each transition changes exactly one child slot — a DIFF
    // that is always smaller than the node — guaranteeing no FULL fallback mid-chain.
    Bytes prev = branchRlpWithChild(0, dummyChildRlp());
    putFull(nk, 0L, prev);
    final int chainLength = ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS + 1;
    for (int i = 1; i <= chainLength; i++) {
      final Bytes next = branchRlpWithChild(i % 16, dummyChildRlp());
      putDiff(nk, i, i, prev, next);
      prev = next;
    }
    assertThat(reader.nodeAt(nk, chainLength)).isEmpty();
  }
}
