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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.ArchiveTrieBuilder.CHECKPOINT_INTERVAL;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReaderV2;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieBuilderTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveTrieBuilder builder;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_NODE_HISTORY_ARCHIVE_V2, TRIE_BRANCH_STORAGE));
    builder = new ArchiveTrieBuilder(storage, 0L);
  }

  @Test
  void newAccountsStorageTrieRootMatchesASingleSlotWrite() {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addAccountChange(
        address, null, new PmtStateTrieAccountValue(0, Wei.ZERO, Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    trieLog.addStorageChange(address, slotKey, null, UInt256.valueOf(42));
    trieLog.freeze();

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    final Hash computedRoot =
        builder.applyStorageChanges(
            address, trieLog.getStorageChanges().get(address), trieLog, 1L, tx);
    tx.commit();

    assertThat(computedRoot).isNotEqualTo(Hash.wrap(MerkleTrie.EMPTY_TRIE_NODE_HASH));
  }

  @Test
  void removingTheOnlySlotReturnsToTheEmptyTrieRoot() {
    final Address address = Address.fromHexString("0x2222222222222222222222222222222222222222");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer writeLog = new TrieLogLayer();
    writeLog.addStorageChange(address, slotKey, null, UInt256.valueOf(42));
    writeLog.freeze();
    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
    builder.applyStorageChanges(
        address, writeLog.getStorageChanges().get(address), writeLog, 1L, tx1);
    tx1.commit();

    final TrieLogLayer removeLog = new TrieLogLayer();
    removeLog.addStorageChange(address, slotKey, UInt256.valueOf(42), UInt256.ZERO);
    removeLog.freeze();
    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
    final Hash rootAfterRemoval =
        builder.applyStorageChanges(
            address, removeLog.getStorageChanges().get(address), removeLog, 2L, tx2);
    tx2.commit();

    assertThat(rootAfterRemoval).isEqualTo(Hash.wrap(MerkleTrie.EMPTY_TRIE_NODE_HASH));
  }

  @Test
  void accountTrieRootChangesWhenAnAccountIsAdded() {
    final Address address = Address.fromHexString("0x3333333333333333333333333333333333333333");
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addAccountChange(
        address,
        null,
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    trieLog.freeze();

    final var tx = storage.startTransaction();
    final Hash root = builder.applyAccountChanges(trieLog, Hash.EMPTY_TRIE_HASH, 1L, tx);
    tx.commit();

    assertThat(root).isNotEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @Test
  void accountTrieRootReturnsToEmptyAfterTheOnlyAccountIsDeleted() {
    final Address address = Address.fromHexString("0x4444444444444444444444444444444444444444");
    final TrieLogLayer addLog = new TrieLogLayer();
    addLog.addAccountChange(
        address,
        null,
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    addLog.freeze();
    final var tx1 = storage.startTransaction();
    final Hash rootAfterAdd = builder.applyAccountChanges(addLog, Hash.EMPTY_TRIE_HASH, 1L, tx1);
    tx1.commit();

    final TrieLogLayer removeLog = new TrieLogLayer();
    removeLog.addAccountChange(
        address,
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY_TRIE_HASH, Hash.EMPTY),
        null);
    removeLog.freeze();
    final var tx2 = storage.startTransaction();
    final Hash rootAfterRemove = builder.applyAccountChanges(removeLog, rootAfterAdd, 2L, tx2);
    tx2.commit();

    assertThat(rootAfterRemove).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @Test
  void capturesAFullEntryOnFirstWriteOfAShallowNode() {
    final Address address = Address.fromHexString("0x5555555555555555555555555555555555555555");
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addAccountChange(
        address,
        null,
        new PmtStateTrieAccountValue(1, Wei.of(1), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    trieLog.freeze();

    final var tx = storage.startTransaction();
    builder.applyAccountChanges(trieLog, Hash.EMPTY_TRIE_HASH, 1L, tx);
    tx.commit();

    final long entryCount = storage.stream(TRIE_NODE_HISTORY_ARCHIVE_V2).count();
    assertThat(entryCount).isGreaterThan(0); // at minimum, the root leaf/branch entry
  }

  @Test
  void writesReconstructableEntriesForRepeatedMutationsOfASingleAccount() {
    // A single account mutated CHECKPOINT_INTERVAL+2 times: entries are written for every block
    // and the root node (at trie location Bytes.EMPTY) must be reconstructable at each block
    // via TrieNodeHistoryReaderV2.
    final Address address = Address.fromHexString("0x6666666666666666666666666666666666666666");
    Hash root = Hash.EMPTY_TRIE_HASH;
    for (long block = 1; block <= CHECKPOINT_INTERVAL + 2; block++) {
      final TrieLogLayer trieLog = new TrieLogLayer();
      final var prior =
          block == 1
              ? null
              : new PmtStateTrieAccountValue(
                  block - 1, Wei.of(block - 1), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
      trieLog.addAccountChange(
          address,
          prior,
          new PmtStateTrieAccountValue(block, Wei.of(block), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
      trieLog.freeze();
      final var tx = storage.startTransaction();
      root = builder.applyAccountChanges(trieLog, root, block, tx);
      tx.commit();
    }

    final var reader = new TrieNodeHistoryReaderV2(storage);
    for (long block = 1; block <= CHECKPOINT_INTERVAL + 2; block++) {
      // The account trie has a single leaf at the root (location = Bytes.EMPTY); the reader must
      // reconstruct it successfully at every block in the sequence.
      assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, Bytes.EMPTY, block)).isPresent();
    }
  }
}
