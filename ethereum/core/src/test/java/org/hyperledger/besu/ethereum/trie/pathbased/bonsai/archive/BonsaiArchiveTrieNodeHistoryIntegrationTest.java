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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link BonsaiArchiveTrieNodeStrategy} through a sequence of block-by-block writes
 * (Updater-level, not full EVM execution — see the investigation note above for why) and asserts
 * that {@link TrieNodeHistoryReader} reconstructs the correct historical version at every
 * intermediate block, across a creation/mutation/checkpoint/deletion lifecycle.
 */
class BonsaiArchiveTrieNodeHistoryIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
  }

  /**
   * Builds a valid 2-item short-node RLP list ({@code [path, value]}) whose value byte
   * distinguishes it from other calls. {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec#encodeDiff}
   * requires genuine RLP-list-shaped trie nodes whenever both the old and new node are present
   * (real trie nodes are always RLP lists); a bare single byte is not valid input there. Same
   * fixture-construction pattern as {@code ArchiveTrieNodeCodecTest#shortNode}.
   */
  private static Bytes leafNode(final int value) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(Bytes.fromHexString("0x0102"));
          out.writeBytes(Bytes.of((byte) value));
          out.endList();
        });
  }

  /** Writes {@code node} at the given {@code blockNumber} via a fresh strategy instance. */
  private void writeAtBlock(final long blockNumber, final Bytes location, final Bytes node) {
    final BonsaiArchiveTrieNodeStrategy s =
        new BonsaiArchiveTrieNodeStrategy(reader, historyStore, blockNumber);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    tx.commit();
  }

  @Test
  void fullLifecycleAcrossCreationMutationCheckpointAndDeletionReconstructsCorrectlyAtEveryBlock() {
    final Bytes location = Bytes.fromHexString("0x030405"); // non-root, deterministic interval
    final Bytes naturalKey = ArchiveNodeKey.account(location);

    // Block 0: creation.
    writeAtBlock(0L, location, leafNode(0x00));
    // Blocks 1..CHECKPOINT_INTERVAL: enough mutations to cross exactly one checkpoint boundary.
    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      writeAtBlock((long) i, location, leafNode(i));
    }
    // One block after the checkpoint: deletion.
    final long deletionBlock = TrieNodeHistoryReader.CHECKPOINT_INTERVAL + 1L;
    final BonsaiArchiveTrieNodeStrategy deleteStrategy =
        new BonsaiArchiveTrieNodeStrategy(reader, historyStore, deletionBlock);
    final SegmentedKeyValueStorageTransaction deleteTx = storage.startTransaction();
    deleteStrategy.removeFlatAccountStateTrieNode(storage, deleteTx, location);
    deleteTx.commit();

    // Reconstruction at every intermediate block must match what was actually written then.
    assertThat(reader.nodeAt(naturalKey, 0L)).contains(leafNode(0x00));
    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      assertThat(reader.nodeAt(naturalKey, (long) i)).as("block %d", i).contains(leafNode(i));
    }
    // After the deletion, the node no longer exists.
    assertThat(reader.nodeAt(naturalKey, deletionBlock)).isEmpty();
    // But its state immediately before deletion is still reconstructable.
    assertThat(reader.nodeAt(naturalKey, deletionBlock - 1L))
        .contains(leafNode(TrieNodeHistoryReader.CHECKPOINT_INTERVAL));
  }

  @Test
  void storageTrieNodeLifecycleReconstructsCorrectlyAtEveryBlock() {
    final org.hyperledger.besu.datatypes.Hash accountHash =
        org.hyperledger.besu.datatypes.Hash.hash(Bytes.fromHexString("0xaa"));
    final Bytes location = Bytes.fromHexString("0x0203");
    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);

    final Bytes node11 = leafNode(0x11);
    final Bytes node22 = leafNode(0x22);

    // Block 0: creation.
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    new BonsaiArchiveTrieNodeStrategy(reader, historyStore, 0L)
        .putFlatStorageTrieNode(
            storage,
            tx,
            accountHash,
            location,
            org.hyperledger.besu.crypto.Hash.keccak256(node11),
            node11);
    tx.commit();

    // Block 1: update.
    tx = storage.startTransaction();
    new BonsaiArchiveTrieNodeStrategy(reader, historyStore, 1L)
        .putFlatStorageTrieNode(
            storage,
            tx,
            accountHash,
            location,
            org.hyperledger.besu.crypto.Hash.keccak256(node22),
            node22);
    tx.commit();

    assertThat(reader.nodeAt(naturalKey, 0L)).contains(node11);
    assertThat(reader.nodeAt(naturalKey, 1L)).contains(node22);
  }
}
