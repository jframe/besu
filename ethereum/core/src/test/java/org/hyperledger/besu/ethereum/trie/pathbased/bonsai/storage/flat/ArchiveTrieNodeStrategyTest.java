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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveNodeHistoryProgress historyProgress;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    historyProgress = new ArchiveNodeHistoryProgress();
  }

  private ArchiveTrieNodeStrategy strategyWithGate(final long highestSafeBlock) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), historyStore, historyProgress, () -> highestSafeBlock);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void setStoredBlockNumber(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void capturesFullNodeWhenGateOpen() {
    // Gate wide open (initial sync): block 0 (no prior stored block) must be captured.
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(Long.MAX_VALUE);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).contains(node);
    assertThat(historyProgress.covers(0L)).isTrue();
  }

  @Test
  void doesNotCaptureWhenGateClosedAndNotGenesis() {
    // Gate closed (at-head sync): node writes go live but must NOT be archived.
    // Store block 5 as the last committed block, making the current block 6.
    setStoredBlockNumber(5L);

    final ArchiveTrieNodeStrategy strategy = strategyWithGate(Long.MIN_VALUE);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    // Live trie write happened
    assertThat(storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())).isPresent();
    // Archive must be empty
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 6L)).isEmpty();
    // Progress must be unset
    assertThat(historyProgress.covers(6L)).isFalse();
  }

  @Test
  void foreignTransactionDoesNotWipeCaptureState() {
    // Regression guard for fix 657cf447d9.
    // TrieLogManager.saveTrieLog() opens a second updater mid-block and calls commitTrieLogOnly(),
    // which triggers onDiscard(tx2). Without the ownership guard that call would wipe the capture
    // state built up by tx1, losing the block from the archive entirely.
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(Long.MAX_VALUE);
    final Bytes location = Bytes.EMPTY;
    final Bytes node = Bytes.fromHexString("0x1234");

    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction(); // import tx
    strategy.putFlatAccountTrieNode(storage, tx1, location, hash(node), node);

    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction(); // trie-log tx
    strategy.onDiscard(tx2); // must be a no-op (657cf447d9)

    strategy.onBeforeCommit(storage, tx1);
    tx1.commit();

    // If onDiscard(tx2) had wiped state, progress would be unset and the archive empty.
    assertThat(historyProgress.covers(0L)).isTrue();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).isPresent();
  }
}
