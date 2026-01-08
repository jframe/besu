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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveFlatDbStrategyTest {

  private BonsaiArchiveFlatDbStrategy archiveFlatDbStrategy;
  private SegmentedKeyValueStorage storage;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
    archiveFlatDbStrategy =
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy());
  }

  @Test
  public void genesisBlockUsesZeroSuffixWhenWorldBlockNumberKeyNotSet() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000001").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xAABBCC");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void block1UsesOneSuffixWhenWorldBlockNumberKeyIsZero() {
    setWorldBlockNumber(0);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000002").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xDDEEFF");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);

    final byte[] genesisKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    assertThat(storage.get(ACCOUNT_INFO_STATE, genesisKey)).isEmpty();
  }

  @Test
  public void block2UsesTwoSuffixWhenWorldBlockNumberKeyIsOne() {
    setWorldBlockNumber(1);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000003").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0x112233");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(2)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void genesisAndBlock1AccountsDoNotOverwrite() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000004").addressHash();
    final Bytes genesisAccountValue = Bytes.fromHexString("0xAABBCCDDEEFF00");
    final Bytes block1AccountValue = Bytes.fromHexString("0x112233445566FF");

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, genesisAccountValue);
    tx.commit();

    setWorldBlockNumber(0);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, block1AccountValue);
    tx.commit();

    final byte[] genesisKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final byte[] block1Key =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(1)).toArrayUnsafe();

    final Optional<byte[]> genesisValue = storage.get(ACCOUNT_INFO_STATE, genesisKey);
    final Optional<byte[]> block1Value = storage.get(ACCOUNT_INFO_STATE, block1Key);

    assertThat(genesisValue).isPresent();
    assertThat(Bytes.wrap(genesisValue.get())).isEqualTo(genesisAccountValue);

    assertThat(block1Value).isPresent();
    assertThat(Bytes.wrap(block1Value.get())).isEqualTo(block1AccountValue);
  }

  @Test
  public void sequentialBlocksUseIncrementingSuffixes() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000005").addressHash();

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA00"));
    tx.commit();

    setWorldBlockNumber(0);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA01"));
    tx.commit();

    setWorldBlockNumber(1);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA02"));
    tx.commit();

    setWorldBlockNumber(2);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA03"));
    tx.commit();

    final Bytes[] expectedValues = {
      Bytes.fromHexString("0xAA00"),
      Bytes.fromHexString("0xAA01"),
      Bytes.fromHexString("0xAA02"),
      Bytes.fromHexString("0xAA03")
    };

    for (long blockNum = 0; blockNum <= 3; blockNum++) {
      final byte[] key =
          Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(blockNum)).toArrayUnsafe();
      final Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE, key);
      assertThat(value).as("Block " + blockNum + " should have stored value").isPresent();
      assertThat(Bytes.wrap(value.get())).isEqualTo(expectedValues[(int) blockNum]);
    }
  }

  private void setWorldBlockNumber(final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  @Test
  public void getFlatAccountFallsBackToNonArchiveWhenArchiveNotFound() {
    // Set world block number so archive strategy looks for suffixed keys
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000006").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xDEADBEEF");

    // Store value using non-archive format (simple key without suffix)
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE, accountHash.toArrayUnsafe(), accountValue.toArrayUnsafe());
    tx.commit();

    // Verify no archive-formatted key exists
    final byte[] archiveKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(5)).toArrayUnsafe();
    assertThat(storage.get(ACCOUNT_INFO_STATE, archiveKey)).isEmpty();

    // getFlatAccount should fallback to non-archive lookup and find the value
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatAccount(
            Optional::empty, (location, hash) -> Optional.empty(), accountHash, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(accountValue);
  }

  @Test
  public void getFlatAccountReturnsArchiveValueWhenBothExist() {
    // Set world block number so archive strategy looks for suffixed keys
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000007").addressHash();
    final Bytes nonArchiveValue = Bytes.fromHexString("0x01020304");
    final Bytes archiveValue = Bytes.fromHexString("0x05060708");

    // Store value using non-archive format
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE, accountHash.toArrayUnsafe(), nonArchiveValue.toArrayUnsafe());
    tx.commit();

    // Store value using archive format with block 5 suffix
    final byte[] archiveKey =
        Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(5)).toArrayUnsafe();
    tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE, archiveKey, archiveValue.toArrayUnsafe());
    tx.commit();

    // getFlatAccount should return the archive value, not the non-archive fallback
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatAccount(
            Optional::empty, (location, hash) -> Optional.empty(), accountHash, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(archiveValue);
  }

  @Test
  public void getFlatStorageValueFallsBackToNonArchiveWhenArchiveNotFound() {
    // Set world block number so archive strategy looks for suffixed keys
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000008").addressHash();
    final Hash slotHash = Hash.hash(Bytes.of(1, 2, 3));
    final StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());
    final Bytes storageValue = Bytes.fromHexString("0xCAFEBABE");

    // Store value using non-archive format (account hash + slot hash, no suffix)
    final byte[] nonArchiveKey = Bytes.concatenate(accountHash, slotHash).toArrayUnsafe();
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_STORAGE_STORAGE, nonArchiveKey, storageValue.toArrayUnsafe());
    tx.commit();

    // Verify no archive-formatted key exists
    final byte[] archiveKey =
        Bytes.concatenate(accountHash, slotHash, Bytes.ofUnsignedLong(5)).toArrayUnsafe();
    assertThat(storage.get(ACCOUNT_STORAGE_STORAGE, archiveKey)).isEmpty();

    // getFlatStorageValueByStorageSlotKey should fallback to non-archive lookup
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatStorageValueByStorageSlotKey(
            Optional::empty,
            Optional::empty,
            (location, hash) -> Optional.empty(),
            accountHash,
            slotKey,
            storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(storageValue);
  }

  @Test
  public void getFlatStorageValueReturnsArchiveValueWhenBothExist() {
    // Set world block number so archive strategy looks for suffixed keys
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000009").addressHash();
    final Hash slotHash = Hash.hash(Bytes.of(4, 5, 6));
    final StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());
    final Bytes nonArchiveValue = Bytes.fromHexString("0xAA11BB22");
    final Bytes archiveValue = Bytes.fromHexString("0xCC33DD44");

    // Store value using non-archive format
    final byte[] nonArchiveKey = Bytes.concatenate(accountHash, slotHash).toArrayUnsafe();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_STORAGE_STORAGE, nonArchiveKey, nonArchiveValue.toArrayUnsafe());
    tx.commit();

    // Store value using archive format with block 5 suffix
    final byte[] archiveKey =
        Bytes.concatenate(accountHash, slotHash, Bytes.ofUnsignedLong(5)).toArrayUnsafe();
    tx = storage.startTransaction();
    tx.put(ACCOUNT_STORAGE_STORAGE, archiveKey, archiveValue.toArrayUnsafe());
    tx.commit();

    // getFlatStorageValueByStorageSlotKey should return archive value, not fallback
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatStorageValueByStorageSlotKey(
            Optional::empty,
            Optional::empty,
            (location, hash) -> Optional.empty(),
            accountHash,
            slotKey,
            storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(archiveValue);
  }

  @Test
  public void getFlatAccountReturnsEmptyWhenNeitherArchiveNorNonArchiveExists() {
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x000000000000000000000000000000000000000A").addressHash();

    // Don't store any value
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatAccount(
            Optional::empty, (location, hash) -> Optional.empty(), accountHash, storage);

    assertThat(result).isEmpty();
  }

  @Test
  public void getFlatStorageValueReturnsEmptyWhenNeitherArchiveNorNonArchiveExists() {
    setWorldBlockNumber(5);

    final Hash accountHash =
        Address.fromHexString("0x000000000000000000000000000000000000000B").addressHash();
    final Hash slotHash = Hash.hash(Bytes.of(7, 8, 9));
    final StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());

    // Don't store any value
    final Optional<Bytes> result =
        archiveFlatDbStrategy.getFlatStorageValueByStorageSlotKey(
            Optional::empty,
            Optional::empty,
            (location, hash) -> Optional.empty(),
            accountHash,
            slotKey,
            storage);

    assertThat(result).isEmpty();
  }
}
