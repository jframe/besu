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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveProofNodeLoaderTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveHistoryReader historyReader;
  private BonsaiTrieNodeStrategy trieStrategy;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    historyReader = new ArchiveHistoryReader(historyStore);
    trieStrategy = new BonsaiTrieNodeStrategy();
  }

  private static Bytes32 keccak(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void putLive(final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    trieStrategy.putFlatAccountTrieNode(storage, tx, location, keccak(node), node);
    tx.commit();
  }

  private void putArchive(final Bytes location, final long block, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.put(tx, ArchiveNodeKey.account(location), block, node);
    tx.commit();
  }

  @Test
  void returnsEmptyTrieNodeForEmptyHash() {
    final NodeLoader loader =
        ArchiveProofNodeLoader.forAccount(trieStrategy, storage, historyReader, 5L);
    assertThat(loader.getNode(Bytes.EMPTY, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @Test
  void returnsLiveNodeWhenHashMatches() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");
    putLive(location, node);

    final NodeLoader loader =
        ArchiveProofNodeLoader.forAccount(trieStrategy, storage, historyReader, 5L);
    assertThat(loader.getNode(location, keccak(node))).contains(node);
  }

  @Test
  void fallsBackToArchiveWhenLiveHashDoesNotMatch() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes oldNode = Bytes.fromHexString("0xaabb");
    final Bytes newNode = Bytes.fromHexString("0xccdd");

    // Live has the NEW node (state advanced past targetBlock).
    putLive(location, newNode);
    // Archive has the old node at block 3.
    putArchive(location, 3L, oldNode);

    // Proof for block 3 must return oldNode.
    final NodeLoader loader =
        ArchiveProofNodeLoader.forAccount(trieStrategy, storage, historyReader, 3L);
    assertThat(loader.getNode(location, keccak(oldNode))).contains(oldNode);
  }

  @Test
  void returnsEmptyWhenNeitherLiveNorArchiveMatchesHash() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes archiveNode = Bytes.fromHexString("0x1122");
    final Bytes32 unknownHash = keccak(Bytes.fromHexString("0xfeed"));
    putArchive(location, 5L, archiveNode);

    final NodeLoader loader =
        ArchiveProofNodeLoader.forAccount(trieStrategy, storage, historyReader, 5L);
    // Fail-closed: unknown hash → empty
    assertThat(loader.getNode(location, unknownHash)).isEmpty();
  }

  @Test
  void returnsEmptyWhenNotInArchiveAndLiveMismatches() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes liveNode = Bytes.fromHexString("0xabcd");
    final Bytes32 differentHash = keccak(Bytes.fromHexString("0x9999"));
    putLive(location, liveNode);

    final NodeLoader loader =
        ArchiveProofNodeLoader.forAccount(trieStrategy, storage, historyReader, 2L);
    assertThat(loader.getNode(location, differentHash)).isEmpty();
  }
}
