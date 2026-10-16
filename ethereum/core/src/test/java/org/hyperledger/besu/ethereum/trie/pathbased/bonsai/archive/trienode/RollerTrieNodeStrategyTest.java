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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RollerTrieNodeStrategyTest {

  private static final Bytes KEY = Bytes.of(0x01);
  private static final Bytes NODE_AT_3 = Bytes.fromHexString("0xc3010203");
  private static final Bytes NODE_AT_7 = Bytes.fromHexString("0xc3070809");

  private SegmentedKeyValueStorage live;
  private ArchiveTrieNodeStrategy capture;

  @BeforeEach
  void setUp() {
    live =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    capture =
        ArchiveTrieNodeStrategy.createArchiveStrategy(
            live,
            Executors.newSingleThreadExecutor(),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
    final ArchiveNodeHistoryStore history = new ArchiveNodeHistoryStore(live);
    final SegmentedKeyValueStorageTransaction tx = live.startTransaction();
    history.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.of(KEY), 3L),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(NODE_AT_3)));
    history.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.of(KEY), 7L),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(NODE_AT_7)));
    tx.commit();
  }

  @AfterEach
  void tearDown() {
    capture.close();
  }

  private static SegmentedKeyValueStorage scratchAtBlock(final long block) {
    final SegmentedKeyValueStorage scratch =
        new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE));
    final SegmentedKeyValueStorageTransaction tx = scratch.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
    return scratch;
  }

  @Test
  void readsTheArchivedNodeAtTheStoragesCurrentBlock() {
    final RollerTrieNodeStrategy strategy = new RollerTrieNodeStrategy(capture);
    final Bytes32 anyHash = Bytes32.wrap(Hash.EMPTY.getBytes());

    assertThat(strategy.getTrieNode(scratchAtBlock(5L), KEY, anyHash)).contains(NODE_AT_3);
    assertThat(strategy.getTrieNode(scratchAtBlock(7L), KEY, anyHash)).contains(NODE_AT_7);
    assertThat(strategy.getTrieNode(scratchAtBlock(2L), KEY, anyHash)).isEmpty();
  }

  @Test
  void writesAndCommitHooksGoToTheCapturingStrategy() {
    final ArchiveTrieNodeStrategy mockCapture = mock(ArchiveTrieNodeStrategy.class);
    when(mockCapture.historyReader()).thenReturn(capture.historyReader());
    final RollerTrieNodeStrategy strategy = new RollerTrieNodeStrategy(mockCapture);
    final SegmentedKeyValueStorage scratch = scratchAtBlock(5L);
    final SegmentedKeyValueStorageTransaction tx = scratch.startTransaction();
    final Bytes32 hash = Bytes32.wrap(Hash.hash(NODE_AT_7).getBytes());

    strategy.putTrieNode(scratch, tx, KEY, hash, NODE_AT_7);
    strategy.removeTrieNode(scratch, tx, KEY);
    strategy.onBeforeCommit(scratch, tx);
    strategy.onBeforeBlockCommit(scratch, tx, 6L);
    strategy.onRollback(tx);

    verify(mockCapture).putTrieNode(scratch, tx, KEY, hash, NODE_AT_7);
    verify(mockCapture).removeTrieNode(scratch, tx, KEY);
    verify(mockCapture).onBeforeCommit(scratch, tx);
    verify(mockCapture).onBeforeBlockCommit(scratch, tx, 6L);
    verify(mockCapture).onRollback(tx);
  }
}
