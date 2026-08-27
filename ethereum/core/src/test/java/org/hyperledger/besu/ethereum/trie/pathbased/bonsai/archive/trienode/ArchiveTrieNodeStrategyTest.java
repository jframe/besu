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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private static final Bytes LOCATION = Bytes.of(0x01);
  private static final Bytes ROOT_LOCATION = Bytes.EMPTY;

  /** Branch node whose slot 0 holds a 33-byte hash ref derived from {@code seed}. */
  private static Bytes branchNode(final int seed) {
    final byte[] childRef = new byte[33];
    childRef[0] = (byte) 0xa0; // RLP string, 32 bytes
    for (int i = 1; i < 33; i++) {
      childRef[i] = (byte) (i + seed);
    }
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(Bytes.wrap(childRef));
    for (int i = 1; i < 16; i++) {
      out.writeNull();
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  private static Bytes32 hashOf(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveHistoryReader reader;
  private ArchiveTrieNodeStrategy strategy;
  private final AtomicBoolean gateOpen = new AtomicBoolean(true);

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    reader = new ArchiveHistoryReader(historyStore);
    final ArchiveTrieNodeWriter capture =
        new ArchiveTrieNodeWriter(historyStore, coverageTracker, Executors.newFixedThreadPool(2));
    strategy = new ArchiveTrieNodeStrategy(new BonsaiTrieNodeStrategy(), capture, gateOpen::get);
  }

  /**
   * Writes {@code node} at {@code location} as block {@code block}, then advances the stored world
   * block number to {@code block} exactly as a real block commit would.
   */
  private void writeBlock(final long block, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hashOf(node), node);
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    strategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  private void setStoredBlockNumber(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
  }

  private ArchiveNodeHistoryStore.HistoryEntry entryAt(final Bytes location, final long block) {
    return historyStore.getLatestBefore(ArchiveNodeKey.account(location), block).orElseThrow();
  }

  @Test
  void firstWriteAtALocationIsACheckpoint() {
    final Bytes node = branchNode(0);
    writeBlock(0L, LOCATION, node);

    // No prior flat node, so this is a CREATION entry, which the reader treats as a checkpoint.
    assertThat(entryAt(LOCATION, 0L).codecEntry().isFull()).isTrue();
    assertThat(entryAt(LOCATION, 0L).counter()).isZero();
    assertThat(reader.nodeAt(ArchiveNodeKey.account(LOCATION), 0L)).contains(node);
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void doesNotArchiveWhenGateClosedAndNotGenesis() {
    // Gate closed (at-head sync): node writes go live but must NOT be archived.
    // Store block 5 as the last committed block, making the current block 6.
    setStoredBlockNumber(5L);
    gateOpen.set(false);

    final Bytes node = branchNode(0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION, hashOf(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, LOCATION.toArrayUnsafe())).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(LOCATION), 6L)).isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }

  @Test
  void secondWriteAtALocationIsADiff() {
    final Bytes nodeV1 = branchNode(0);
    final Bytes nodeV2 = branchNode(1);
    writeBlock(0L, LOCATION, nodeV1);
    writeBlock(1L, LOCATION, nodeV2);

    final var diff = entryAt(LOCATION, 1L);
    assertThat(diff.block()).isEqualTo(1L);
    assertThat(diff.counter()).isEqualTo(1);
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.codecEntry().isDeletion()).isFalse();

    // The diff is smaller than the node it encodes — that is the point of the format.
    assertThat(diff.rawEntryBytes().size()).isLessThan(nodeV2.size());

    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    assertThat(reader.nodeAt(nk, 0L)).contains(nodeV1);
    assertThat(reader.nodeAt(nk, 1L)).contains(nodeV2);
  }

  @Test
  void deepNode_writesFullCheckpointAtDeepInterval() {
    // Deep node (location size 3 → DEEP_CHECKPOINT_INTERVAL = 16).
    final Bytes deepLocation = Bytes.of(0x01, 0x02, 0x03);
    final Bytes nk = ArchiveNodeKey.account(deepLocation);
    final int interval = ArchiveTrieNodeWriter.DEEP_CHECKPOINT_INTERVAL;
    for (int block = 0; block <= interval; block++) {
      writeBlock(block, deepLocation, branchNode(block));
    }

    // Blocks 1..interval-1 are diffs with a rising counter; the interval block resets.
    assertThat(entryAt(deepLocation, interval - 1L).counter()).isEqualTo(interval - 1);

    final var checkpoint = entryAt(deepLocation, interval);
    assertThat(checkpoint.codecEntry().isFull()).isTrue();
    assertThat(checkpoint.counter()).isZero();

    // Every version is still reconstructable.
    for (int block = 0; block <= interval; block++) {
      assertThat(reader.nodeAt(nk, block)).contains(branchNode(block));
    }
  }

  @Test
  void rootSparseChange_producesDiff() {
    // Root participates in checkpoint+diff: creation FULL, a small subsequent delta DIFF, and the
    // whole chain still reconstructs.
    writeBlock(0L, ROOT_LOCATION, branchNode(0));
    writeBlock(1L, ROOT_LOCATION, branchNode(1));

    assertThat(entryAt(ROOT_LOCATION, 0L).codecEntry().isFull()).isTrue();
    assertThat(entryAt(ROOT_LOCATION, 0L).counter()).isZero();

    final var diff = entryAt(ROOT_LOCATION, 1L);
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.counter()).isEqualTo(1);

    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    assertThat(reader.nodeAt(nk, 0L)).contains(branchNode(0));
    assertThat(reader.nodeAt(nk, 1L)).contains(branchNode(1));
  }

  @Test
  void removeWritesDeletionTombstone() {
    final Bytes node = branchNode(0);
    writeBlock(0L, LOCATION, node);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, LOCATION);
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(1L).toArrayUnsafe());
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    assertThat(entryAt(LOCATION, 1L).codecEntry().isDeletion()).isTrue();
    assertThat(reader.nodeAt(nk, 0L)).contains(node); // history before the delete is intact
    assertThat(reader.nodeAt(nk, 1L)).isEmpty();
    assertThat(reader.nodeAt(nk, 99L)).isEmpty();
  }

  @Test
  void removeOfAbsentNodeWritesNothing() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, LOCATION);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(LOCATION), 0L)).isEmpty();
  }

  @Test
  void writesFullAfterAnArchiveGap() {
    writeBlock(0L, LOCATION, branchNode(0));
    writeBlock(1L, LOCATION, branchNode(1));
    assertThat(entryAt(LOCATION, 1L).codecEntry().isFull()).isFalse();

    // Gate closes: blocks 2 and 3 update the flat DB but are not archived.
    gateOpen.set(false);
    writeBlock(2L, LOCATION, branchNode(2));
    writeBlock(3L, LOCATION, branchNode(3));
    assertThat(entryAt(LOCATION, 3L).block()).isEqualTo(1L); // nothing new archived

    // Gate reopens: the chain is no longer contiguous, so block 4 must be a FULL checkpoint.
    gateOpen.set(true);
    writeBlock(4L, LOCATION, branchNode(4));

    final var resumed = entryAt(LOCATION, 4L);
    assertThat(resumed.block()).isEqualTo(4L);
    assertThat(resumed.codecEntry().isFull()).isTrue();
    assertThat(resumed.counter()).isZero();
    assertThat(reader.nodeAt(ArchiveNodeKey.account(LOCATION), 4L)).contains(branchNode(4));
  }

  @Test
  void archivesStorageTrieNodesUnderTheirAccount() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes nodeV1 = branchNode(0);
    final Bytes nodeV2 = branchNode(1);

    final SegmentedKeyValueStorageTransaction tx0 = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx0, accountHash, LOCATION, hashOf(nodeV1), nodeV1);
    tx0.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(0L).toArrayUnsafe());
    strategy.onBeforeCommit(storage, tx0);
    tx0.commit();

    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx1, accountHash, LOCATION, hashOf(nodeV2), nodeV2);
    tx1.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(1L).toArrayUnsafe());
    strategy.onBeforeCommit(storage, tx1);
    tx1.commit();

    final Bytes nk = ArchiveNodeKey.storage(accountHash.getBytes(), LOCATION);
    assertThat(historyStore.getLatestBefore(nk, 1L).orElseThrow().counter()).isEqualTo(1);
    assertThat(reader.nodeAt(nk, 0L)).contains(nodeV1);
    assertThat(reader.nodeAt(nk, 1L)).contains(nodeV2);
  }
}
