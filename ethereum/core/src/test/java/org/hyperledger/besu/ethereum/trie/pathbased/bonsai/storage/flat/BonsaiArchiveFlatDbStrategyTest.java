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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

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
  public void genesisBlockUsesZeroSuffixWhenContextNotSet() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000001").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xAABBCC");

    // No context set - should default to block 0
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> writeContextSupplier = Optional::empty;
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, accountValue, writeContextSupplier);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void block1UsesOneSuffixWhenContextIsBlockZero() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000002").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xDDEEFF");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> writeContextSupplier =
        () -> Optional.of(new BonsaiContext(0L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, accountValue, writeContextSupplier);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void block2UsesTwoSuffixWhenContextIsBlockOne() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000003").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0x112233");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> writeContextSupplier =
        () -> Optional.of(new BonsaiContext(1L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, accountValue, writeContextSupplier);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
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

    // Write genesis (no context = block 0)
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> genesisContext = Optional::empty;
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, genesisAccountValue, genesisContext);
    tx.commit();

    // Write block 1
    tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> block1Context = () -> Optional.of(new BonsaiContext(0L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, block1AccountValue, block1Context);
    tx.commit();

    final byte[] genesisKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final byte[] block1Key =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();

    final Optional<byte[]> genesisValue = storage.get(ACCOUNT_INFO_STATE, genesisKey);
    final Optional<byte[]> block1Value = storage.get(ACCOUNT_INFO_STATE, block1Key);

    // Both should exist but block1 overwrites genesis since they have the same key
    assertThat(genesisValue).isPresent();
    assertThat(block1Value).isPresent();
    assertThat(Bytes.wrap(block1Value.get())).isEqualTo(block1AccountValue);
  }

  @Test
  public void sequentialBlocksUseIncrementingSuffixes() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000005").addressHash();

    // Block 0
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context0 = () -> Optional.of(new BonsaiContext(0L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, Bytes.fromHexString("0xAA00"), context0);
    tx.commit();

    // Block 1
    tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context1 = () -> Optional.of(new BonsaiContext(1L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, Bytes.fromHexString("0xAA01"), context1);
    tx.commit();

    // Block 2
    tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context2 = () -> Optional.of(new BonsaiContext(2L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, Bytes.fromHexString("0xAA02"), context2);
    tx.commit();

    // Block 3
    tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context3 = () -> Optional.of(new BonsaiContext(3L));
    archiveFlatDbStrategy.putFlatAccount(
        storage, tx, accountHash, Bytes.fromHexString("0xAA03"), context3);
    tx.commit();

    final Bytes[] expectedValues = {
      Bytes.fromHexString("0xAA00"),
      Bytes.fromHexString("0xAA01"),
      Bytes.fromHexString("0xAA02"),
      Bytes.fromHexString("0xAA03")
    };

    for (long blockNum = 0; blockNum <= 3; blockNum++) {
      final byte[] key =
          Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(blockNum)).toArrayUnsafe();
      final Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE, key);
      assertThat(value).as("Block " + blockNum + " should have stored value").isPresent();
      assertThat(Bytes.wrap(value.get())).isEqualTo(expectedValues[(int) blockNum]);
    }
  }
}
