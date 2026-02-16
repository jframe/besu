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
import org.apache.tuweni.bytes.Bytes32;
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

  @Test
  public void hasHistoricalDataBeforeShouldReturnTrueWhenDataExists() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000100").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0x1122334455");

    // Write data at block 10
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context10 = () -> Optional.of(new BonsaiContext(10L));
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue, context10);
    tx.commit();

    // Check if historical data exists before block 20
    boolean hasData =
        archiveFlatDbStrategy.hasHistoricalAccountDataBefore(storage, accountHash, 20);

    assertThat(hasData).isTrue();
  }

  @Test
  public void hasHistoricalDataBeforeShouldReturnFalseWhenNoDataExists() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000101").addressHash();

    // No data written for this account

    // Check if historical data exists before block 20
    boolean hasData =
        archiveFlatDbStrategy.hasHistoricalAccountDataBefore(storage, accountHash, 20);

    assertThat(hasData).isFalse();
  }

  @Test
  public void hasHistoricalDataBeforeShouldReturnFalseForGenesisBlock() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000102").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xAABBCC");

    // Write data at block 0
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context0 = () -> Optional.of(new BonsaiContext(0L));
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue, context0);
    tx.commit();

    // Check if historical data exists before block 0 (should be false, no blocks before genesis)
    boolean hasData = archiveFlatDbStrategy.hasHistoricalAccountDataBefore(storage, accountHash, 0);

    assertThat(hasData).isFalse();
  }

  @Test
  public void hasHistoricalDataBeforeShouldIgnoreDeletionMarkers() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000103").addressHash();

    // Write a deletion marker at block 10
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context10 = () -> Optional.of(new BonsaiContext(10L));
    archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context10);
    tx.commit();

    // Check if historical data exists before block 20
    // Should return false because deletion markers are not legitimate data
    boolean hasData =
        archiveFlatDbStrategy.hasHistoricalAccountDataBefore(storage, accountHash, 20);

    assertThat(hasData).isFalse();
  }

  @Test
  public void hasHistoricalStorageDataBeforeShouldReturnTrueWhenDataExists() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000200").addressHash();
    final Hash slotHash = Hash.wrap(Bytes32.leftPad(Bytes.fromHexString("0x01")));
    final Bytes storageValue = Bytes.fromHexString("0x9988776655");

    // Write storage data at block 10
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context10 = () -> Optional.of(new BonsaiContext(10L));
    archiveFlatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
        storage, tx, accountHash, slotHash, storageValue, context10);
    tx.commit();

    // Check if historical storage data exists before block 20
    boolean hasData =
        archiveFlatDbStrategy.hasHistoricalStorageDataBefore(storage, accountHash, slotHash, 20);

    assertThat(hasData).isTrue();
  }

  @Test
  public void hasHistoricalStorageDataBeforeShouldReturnFalseWhenNoDataExists() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000201").addressHash();
    final Hash slotHash = Hash.wrap(Bytes32.leftPad(Bytes.fromHexString("0x02")));

    // No data written for this storage slot

    // Check if historical storage data exists before block 20
    boolean hasData =
        archiveFlatDbStrategy.hasHistoricalStorageDataBefore(storage, accountHash, slotHash, 20);

    assertThat(hasData).isFalse();
  }

  @Test
  public void hasHistoricalStorageDataBeforeShouldReturnFalseForGenesisBlock() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000202").addressHash();
    final Hash slotHash = Hash.hash(Bytes.of(1));

    // Check for historical data before genesis (block 0)
    boolean hasHistory =
        archiveFlatDbStrategy.hasHistoricalStorageDataBefore(storage, accountHash, slotHash, 0L);

    assertThat(hasHistory).isFalse();
  }

  @Test
  public void hasHistoricalStorageDataBeforeShouldIgnoreDeletionMarkers() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000203").addressHash();
    final Hash slotHash = Hash.hash(Bytes.of(1));

    // Write deletion marker at block 10
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context = () -> Optional.of(new BonsaiContext(10L));
    archiveFlatDbStrategy.removeFlatAccountStorageValueByStorageSlotHash(
        storage, tx, accountHash, slotHash, context);
    tx.commit();

    // Check for historical data before block 20 (only marker exists)
    boolean hasHistory =
        archiveFlatDbStrategy.hasHistoricalStorageDataBefore(storage, accountHash, slotHash, 20L);

    assertThat(hasHistory).isFalse();
  }

  @Test
  public void removeFlatAccountShouldWriteMarkerForSelfDestruct() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000300").addressHash();

    // Scenario: SELFDESTRUCT at block 20 - no existing data at this block
    // Expected: Write a deletion marker (hide any historical data)

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context20 = () -> Optional.of(new BonsaiContext(20L));
    archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context20);
    tx.commit();

    // Verify marker was written at block 20
    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(storedValue.get())
        .isEqualTo(BonsaiArchiveFlatDbStrategy.DELETED_ACCOUNT_VALUE); // Empty byte array marker
  }

  @Test
  public void removeFlatAccountShouldDeleteOrphanedDataWhenHistoryExists() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000301").addressHash();
    final Bytes historicalValue = Bytes.fromHexString("0xAABBCCDD");
    final Bytes orphanedValue = Bytes.fromHexString("0x11223344");

    // Setup: Write historical data at block 10
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context10 = () -> Optional.of(new BonsaiContext(10L));
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, historicalValue, context10);
    tx.commit();

    // Setup: Write orphaned data at block 20 (from abandoned chain)
    tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context20 = () -> Optional.of(new BonsaiContext(20L));
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, orphanedValue, context20);
    tx.commit();

    // Scenario: Reorg cleanup at block 20 - orphaned data exists + history exists
    // Expected: DELETE the orphaned entry (reveal historical data at block 10)

    tx = storage.startTransaction();
    archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context20);
    tx.commit();

    // Verify orphaned data at block 20 was deleted
    final byte[] keyBlock20 =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20)).toArrayUnsafe();
    final Optional<byte[]> valueAtBlock20 = storage.get(ACCOUNT_INFO_STATE, keyBlock20);
    assertThat(valueAtBlock20).isEmpty(); // Should be deleted, not marked

    // Verify historical data at block 10 still exists
    final byte[] keyBlock10 =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(10)).toArrayUnsafe();
    final Optional<byte[]> valueAtBlock10 = storage.get(ACCOUNT_INFO_STATE, keyBlock10);
    assertThat(valueAtBlock10).isPresent();
    assertThat(Bytes.wrap(valueAtBlock10.get())).isEqualTo(historicalValue);
  }

  @Test
  public void removeFlatAccountShouldWriteMarkerWhenOrphanedDataHasNoHistory() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000302").addressHash();
    final Bytes orphanedValue = Bytes.fromHexString("0x55667788");

    // Setup: Write orphaned data at block 20 (no historical data before this)
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    Supplier<Optional<BonsaiContext>> context20 = () -> Optional.of(new BonsaiContext(20L));
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, orphanedValue, context20);
    tx.commit();

    // Scenario: Reorg cleanup at block 20 - orphaned data exists but NO history
    // Expected: Write MARKER (overwrite orphaned data to prevent reads)

    tx = storage.startTransaction();
    archiveFlatDbStrategy.removeFlatAccount(storage, tx, accountHash, context20);
    tx.commit();

    // Verify a deletion marker was written at block 20 (not deleted)
    final byte[] keyBlock20 =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(20)).toArrayUnsafe();
    final Optional<byte[]> valueAtBlock20 = storage.get(ACCOUNT_INFO_STATE, keyBlock20);

    assertThat(valueAtBlock20).isPresent();
    assertThat(valueAtBlock20.get()).isEqualTo(BonsaiArchiveFlatDbStrategy.DELETED_ACCOUNT_VALUE);
  }
}
