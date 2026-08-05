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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;
  private TrieNodeHistoryProgress progress;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
    progress = new TrieNodeHistoryProgress();
  }

  /** Distinct valid 2-item short-node RLP so ArchiveTrieNodeCodec's arity check accepts it. */
  private static Bytes shortNodeRlp(final int i) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.of(0x01));
    out.writeBytes(Bytes.of(i));
    out.endList();
    return out.encoded();
  }

  /** Set the committed world block number so the strategy derives currentBlock = n + 1. */
  private void setWorldBlockNumber(final long n) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(n).toArrayUnsafe());
    tx.commit();
  }

  private BonsaiArchiveTrieNodeStrategy strategy(final LongSupplier highestSafeBlock) {
    return new BonsaiArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), historyStore, progress, highestSafeBlock);
  }

  private void putAccount(
      final BonsaiArchiveTrieNodeStrategy strategy, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    tx.commit();
  }

  @Test
  void readDelegatesToLiveBaseValueNotHistory() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = Bytes.fromHexString("0xaa");
    // Write straight into the live segment via the base strategy.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    new BonsaiTrieNodeStrategy().putFlatAccountTrieNode(storage, tx, location, null, node);
    tx.commit();

    assertThat(strategy(() -> Long.MAX_VALUE).getFlatAccountTrieNode(location, null, storage))
        .contains(node);
  }

  @Test
  void creationWritesFullEntryWithCounterZeroAtGenesis() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(0);
    // No WORLD_BLOCK_NUMBER_KEY => block 0.
    putAccount(strategy(() -> Long.MAX_VALUE), location, node);

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.get(ArchiveNodeKey.account(location), 0L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.codecEntry().isCreation()).isTrue();
    assertThat(entry.counter()).isEqualTo(0);
  }

  @Test
  void diffBaseComesFromLiveValueAndChecksInFullEveryCheckpointInterval() {
    final Bytes location = Bytes.fromHexString("0x030405"); // depth 3, non-root
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    // Block 0 creation (FULL).
    putAccount(strategy, location, shortNodeRlp(0));
    // Blocks 1..CHECKPOINT_INTERVAL: each reads prior live value as diff base.
    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      setWorldBlockNumber(i - 1L);
      putAccount(strategy, location, shortNodeRlp(i));
    }
    assertThat(
            historyStore
                .get(ArchiveNodeKey.account(location), 1L)
                .orElseThrow()
                .codecEntry()
                .isFull())
        .isFalse();
    assertThat(
            historyStore
                .get(
                    ArchiveNodeKey.account(location),
                    (long) TrieNodeHistoryReader.CHECKPOINT_INTERVAL)
                .orElseThrow()
                .codecEntry()
                .isFull())
        .isTrue();
    // Reconstruction at every block matches the node written at that block.
    for (int i = 0; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      assertThat(reader.nodeAt(ArchiveNodeKey.account(location), i)).contains(shortNodeRlp(i));
    }
  }

  @Test
  void gateSkipsCaptureButStillWritesLiveNodeWhenBlockAboveThreshold() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = shortNodeRlp(7);
    setWorldBlockNumber(9L); // currentBlock = 10
    putAccount(strategy(() -> 5L), location, node); // 10 > 5 => gated out

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 10L)).isEmpty();
    // But the live node was still written (block import must not be blocked).
    assertThat(new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, null, storage))
        .contains(node);
  }

  @Test
  void genesisCapturedEvenWhenThresholdGateIsClosed() {
    final Bytes location = Bytes.fromHexString("0x0102");
    // No WORLD_BLOCK_NUMBER_KEY => block 0; supplier far below 0.
    putAccount(strategy(() -> Long.MIN_VALUE), location, shortNodeRlp(0));

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isPresent();
  }

  @Test
  void removeCapturesTombstoneSoNodeAtReturnsEmpty() {
    final Bytes location = Bytes.fromHexString("0x0708");
    final BonsaiArchiveTrieNodeStrategy strategy = strategy(() -> Long.MAX_VALUE);
    putAccount(strategy, location, shortNodeRlp(1)); // block 0 creation

    setWorldBlockNumber(0L); // currentBlock = 1
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.commit();

    assertThat(reader.nodeAt(ArchiveNodeKey.account(location), 1L)).isEmpty();
    assertThat(new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, null, storage))
        .isEmpty();
  }

  @Test
  void progressAdvancesToCapturedBlockOncePerBlock() {
    final Bytes location = Bytes.fromHexString("0x0102");
    setWorldBlockNumber(2L); // currentBlock = 3
    putAccount(strategy(() -> Long.MAX_VALUE), location, shortNodeRlp(1));

    assertThat(progress.lastIndexedBlock()).isEqualTo(3L);
    assertThat(progress.indexStartBlock()).isLessThanOrEqualTo(3L);
  }

  @Test
  void storageWriteCapturesHistoryAndWritesLiveNode() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    final Bytes location = Bytes.fromHexString("0x0304");
    final Bytes node = shortNodeRlp(3);
    // No WORLD_BLOCK_NUMBER_KEY => block 0.
    final BonsaiArchiveTrieNodeStrategy strat = strategy(() -> Long.MAX_VALUE);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strat.putFlatStorageTrieNode(storage, tx, accountHash, location, null, node);
    tx.commit();

    // History captured under the storage natural key.
    assertThat(historyStore.get(ArchiveNodeKey.storage(accountHash.getBytes(), location), 0L))
        .isPresent();
    // Live node written to flat DB.
    assertThat(
            new BonsaiTrieNodeStrategy()
                .getFlatStorageTrieNode(accountHash, location, null, storage))
        .contains(node);
  }
}
