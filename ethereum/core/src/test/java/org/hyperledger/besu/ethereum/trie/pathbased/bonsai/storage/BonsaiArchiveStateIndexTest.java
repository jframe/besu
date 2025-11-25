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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STATE_INDEX;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveStateIndexTest {

  private SegmentedKeyValueStorage storage;
  private BonsaiArchiveStateIndex index;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
    index = new BonsaiArchiveStateIndex(storage);
  }

  @Test
  public void testAddAccountModification() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 200L);
    index.addAccountModification(tx, accountHash, 300L);

    tx.commit();

    // Verify the index contains the modifications
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 250L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(200L);
  }

  @Test
  public void testAddStorageModification() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(12345));
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addStorageModification(tx, accountHash, slotKey, 100L);
    index.addStorageModification(tx, accountHash, slotKey, 200L);
    index.addStorageModification(tx, accountHash, slotKey, 300L);

    tx.commit();

    // Verify the index contains the modifications
    Optional<Long> result =
        index.findStorageModificationBlockNumber(accountHash, slotKey, 250L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(200L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_exactMatch() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 200L);
    index.addAccountModification(tx, accountHash, 300L);

    tx.commit();

    // Query for exact block number
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 200L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(200L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_beforeFirst() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 200L);

    tx.commit();

    // Query for block before first modification
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 50L);
    assertThat(result).isEmpty();
  }

  @Test
  public void testFindAccountModificationBlockNumber_afterLast() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 200L);
    index.addAccountModification(tx, accountHash, 300L);

    tx.commit();

    // Query for block after last modification
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 500L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(300L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_betweenModifications() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 300L);

    tx.commit();

    // Query for block between modifications
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 200L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(100L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_notFound() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());

    // Query for account that was never modified
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 200L);
    assertThat(result).isEmpty();
  }

  @Test
  public void testMultipleAccountsIndependent() {
    Hash account1 = Hash.hash(Address.fromHexString("0x1111").addressHash());
    Hash account2 = Hash.hash(Address.fromHexString("0x2222").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, account1, 100L);
    index.addAccountModification(tx, account2, 200L);

    tx.commit();

    // Verify each account has independent history
    Optional<Long> result1 = index.findAccountModificationBlockNumber(account1, 150L);
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(100L);

    Optional<Long> result2 = index.findAccountModificationBlockNumber(account2, 150L);
    assertThat(result2).isEmpty();

    Optional<Long> result3 = index.findAccountModificationBlockNumber(account2, 250L);
    assertThat(result3).isPresent();
    assertThat(result3.get()).isEqualTo(200L);
  }

  @Test
  public void testMarkIndexBuilt() {
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    index.markIndexBuilt(tx, 1000L);
    tx.commit();

    assertThat(index.isIndexBuilt()).isTrue();
    assertThat(index.getLatestIndexedBlock()).isPresent();
    assertThat(index.getLatestIndexedBlock().get()).isEqualTo(1000L);
  }

  @Test
  public void testClear() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    index.addAccountModification(tx, accountHash, 100L);
    tx.commit();

    // Verify data exists
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 200L);
    assertThat(result).isPresent();

    // Clear the index
    index.clear();

    // Verify data is gone
    result = index.findAccountModificationBlockNumber(accountHash, 200L);
    assertThat(result).isEmpty();
  }

  @Test
  public void testDeduplication() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    // Add same block number multiple times
    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 100L);
    index.addAccountModification(tx, accountHash, 100L);

    tx.commit();

    // Should only have one entry
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 100L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(100L);
  }

  @Test
  public void testLargeNumberOfModifications() {
    Hash accountHash = Hash.hash(Address.fromHexString("0x1234").addressHash());
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    // Add many modifications
    for (long i = 0; i < 10000; i += 100) {
      index.addAccountModification(tx, accountHash, i);
    }

    tx.commit();

    // Test binary search efficiency with large dataset
    Optional<Long> result = index.findAccountModificationBlockNumber(accountHash, 5555L);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(5500L);
  }
}
