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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiHybridFlatDbStrategyTest {

  private static final int ARCHIVE_BOUNDARY = 512;

  private BonsaiHybridFlatDbStrategy hybridStrategy;
  private SegmentedKeyValueStorage storage;
  private final AtomicLong headBlockNumber = new AtomicLong(Long.MAX_VALUE);

  @BeforeEach
  public void setup() {
    final CodeHashCodeStorageStrategy codeStorageStrategy = new CodeHashCodeStorageStrategy();
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    storage = new SegmentedInMemoryKeyValueStorage();
    headBlockNumber.set(Long.MAX_VALUE);
    hybridStrategy =
        new BonsaiHybridFlatDbStrategy(
            new BonsaiFullFlatDbStrategy(metricsSystem, codeStorageStrategy),
            new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy),
            headBlockNumber::get,
            ARCHIVE_BOUNDARY,
            codeStorageStrategy);
  }

  // ======================== isRecentBlock routing logic ========================

  @Test
  public void isRecentBlockReturnsTrueForBlocksWithinBoundary() {
    long headBlock = 1000;
    // boundary: 1000 - 512 = 488, blocks > 488 are recent
    assertThat(hybridStrategy.isRecentBlock(489, headBlock, ARCHIVE_BOUNDARY)).isTrue();
    assertThat(hybridStrategy.isRecentBlock(600, headBlock, ARCHIVE_BOUNDARY)).isTrue();
    assertThat(hybridStrategy.isRecentBlock(1000, headBlock, ARCHIVE_BOUNDARY)).isTrue();
  }

  @Test
  public void isRecentBlockReturnsFalseForBlocksBeyondBoundary() {
    long headBlock = 1000;
    // 488 > 488 is false → historical
    assertThat(hybridStrategy.isRecentBlock(488, headBlock, ARCHIVE_BOUNDARY)).isFalse();
    assertThat(hybridStrategy.isRecentBlock(100, headBlock, ARCHIVE_BOUNDARY)).isFalse();
    assertThat(hybridStrategy.isRecentBlock(0, headBlock, ARCHIVE_BOUNDARY)).isFalse();
  }

  // ======================== Bonsai layer writes (simple [hash] key) ========================

  @Test
  public void normalWriteUsesSimpleHashKey() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000001").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xAABBCC");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    // Should be stored with simple [hash] key (not suffixed with block number)
    final Optional<byte[]> storedValue =
        storage.get(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void normalWriteOverwritesPreviousValue() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000002").addressHash();
    final Bytes firstValue = Bytes.fromHexString("0xAABBCC");
    final Bytes secondValue = Bytes.fromHexString("0xDDEEFF");

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, firstValue);
    tx.commit();

    tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, secondValue);
    tx.commit();

    final Optional<byte[]> storedValue =
        storage.get(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(secondValue);
  }

  // ======================== Archive layer context writes (inherited from parent)
  // ========================

  @Test
  public void contextWriteUsesBlockNumberSuffixedKey() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000003").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0x112233");
    final long blockNumber = 100L;

    final BonsaiContext context = new BonsaiContext(blockNumber);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(context, tx, accountHash, accountValue);
    tx.commit();

    // Should be stored with [hash+blockNumber] key
    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(blockNumber))
            .toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE, expectedKey);
    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void contextWritesForDifferentBlocksDontOverwrite() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000004").addressHash();
    final Bytes block0Value = Bytes.fromHexString("0xAA00");
    final Bytes block1Value = Bytes.fromHexString("0xAA01");

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(new BonsaiContext(0L), tx, accountHash, block0Value);
    tx.commit();

    tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(new BonsaiContext(1L), tx, accountHash, block1Value);
    tx.commit();

    final byte[] key0 =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0L)).toArrayUnsafe();
    final byte[] key1 =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(1L)).toArrayUnsafe();

    assertThat(storage.get(ACCOUNT_INFO_STATE, key0)).isPresent();
    assertThat(Bytes.wrap(storage.get(ACCOUNT_INFO_STATE, key0).get())).isEqualTo(block0Value);
    assertThat(storage.get(ACCOUNT_INFO_STATE, key1)).isPresent();
    assertThat(Bytes.wrap(storage.get(ACCOUNT_INFO_STATE, key1).get())).isEqualTo(block1Value);
  }

  // ======================== Read routing ========================

  @Test
  public void recentBlockReadsFromBonsaiLayerViaDirectLookup() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000010").addressHash();
    final Bytes bonsaiValue = Bytes.fromHexString("0xBBCCDD");
    final long headBlock = 1000L;

    // Write to Bonsai layer (simple [hash] key)
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, bonsaiValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    // Set storage context to head block (always recent)
    setWorldBlockNumber(headBlock);

    final Optional<Bytes> result =
        hybridStrategy.getFlatAccount(
            Optional::empty, (loc, hash) -> Optional.empty(), accountHash, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(bonsaiValue);
  }

  @Test
  public void historicalBlockRoutesToArchiveLayerNotBonsaiLayer() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000011").addressHash();
    final Bytes currentValue = Bytes.fromHexString("0xBBCCDD"); // current HEAD state
    final Bytes historicalValue = Bytes.fromHexString("0x112233"); // historical state
    final long headBlock = 1000L;
    final long historicalBlock = headBlock - ARCHIVE_BOUNDARY; // = 488, not recent

    // Write current state to Bonsai layer (simple [hash] key — new block import)
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, currentValue);
    tx.commit();

    // Write historical state to archive layer ([hash+488] key — migrator context write)
    tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(
        new BonsaiContext(historicalBlock), tx, accountHash, historicalValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(historicalBlock); // query context = historical block

    // Historical query must route to seekForPrev and return the archive value, not the Bonsai value
    final Optional<Bytes> result =
        hybridStrategy.getFlatAccount(
            Optional::empty, (loc, hash) -> Optional.empty(), accountHash, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(historicalValue);
  }

  // ======================== Storage slot read routing ========================

  @Test
  public void recentBlockStorageReadsFromBonsaiLayer() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000020").addressHash();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final Bytes bonsaiSlotValue = Bytes.fromHexString("0xAABBCC");
    final long headBlock = 1000L;

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccountStorageValueByStorageSlotHash(
        storage, tx, accountHash, slotKey.getSlotHash(), bonsaiSlotValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(headBlock);

    final Optional<Bytes> result =
        hybridStrategy.getFlatStorageValueByStorageSlotKey(
            Optional::empty, Optional::empty, (loc, hash) -> Optional.empty(),
            accountHash, slotKey, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(bonsaiSlotValue);
  }

  @Test
  public void historicalBlockStorageRoutesToArchiveLayer() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000021").addressHash();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final Bytes currentSlotValue = Bytes.fromHexString("0xCCDDEE");
    final Bytes historicalSlotValue = Bytes.fromHexString("0x112233");
    final long headBlock = 1000L;
    final long historicalBlock = headBlock - ARCHIVE_BOUNDARY; // = 488, not recent

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccountStorageValueByStorageSlotHash(
        storage, tx, accountHash, slotKey.getSlotHash(), currentSlotValue);
    tx.commit();

    tx = storage.startTransaction();
    hybridStrategy.putFlatAccountStorageValueByStorageSlotHash(
        new BonsaiContext(historicalBlock), tx, accountHash, slotKey.getSlotHash(),
        historicalSlotValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(historicalBlock);

    final Optional<Bytes> result =
        hybridStrategy.getFlatStorageValueByStorageSlotKey(
            Optional::empty, Optional::empty, (loc, hash) -> Optional.empty(),
            accountHash, slotKey, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(historicalSlotValue);
  }

  // ======================== Account stream routing ========================

  @Test
  public void recentBlockAccountStreamUsesBonsaiLayer() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000030").addressHash();
    final Bytes bonsaiValue = Bytes.fromHexString("0xAABBCC");
    final long headBlock = 1000L;

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(storage, tx, accountHash, bonsaiValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(headBlock);

    final NavigableMap<Bytes32, Bytes> accounts =
        hybridStrategy.streamAccountFlatDatabase(storage, Bytes32.ZERO, Bytes32.wrap(Bytes.repeat((byte) 0xFF, 32)), Long.MAX_VALUE);

    assertThat(accounts).containsKey(Bytes32.wrap(accountHash.getBytes()));
    assertThat(accounts.get(Bytes32.wrap(accountHash.getBytes()))).isEqualTo(bonsaiValue);
  }

  @Test
  public void historicalBlockAccountStreamUsesArchiveLayer() {
    // Use a distinct account that has only an archive entry (no competing bonsai key).
    // SegmentedInMemoryKeyValueStorage's getNearestBefore uses a prefix-match predicate that
    // differs from RocksDB's SeekForPrev: a 32-byte bonsai key is treated as a "prefix" of a
    // 40-byte archive search target and takes precedence. Testing routing correctness here
    // (archive strategy is invoked) requires isolating the archive key from bonsai interference.
    final Hash archiveOnlyAccount =
        Address.fromHexString("0x0000000000000000000000000000000000000031").addressHash();
    final Bytes archiveValue = Bytes.fromHexString("0x112233");
    final long headBlock = 1000L;
    final long historicalBlock = headBlock - ARCHIVE_BOUNDARY; // = 488, not recent

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccount(new BonsaiContext(historicalBlock), tx, archiveOnlyAccount, archiveValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(historicalBlock);

    final NavigableMap<Bytes32, Bytes> accounts =
        hybridStrategy.streamAccountFlatDatabase(
            storage, Bytes32.ZERO, Bytes32.wrap(Bytes.repeat((byte) 0xFF, 32)), Long.MAX_VALUE);

    assertThat(accounts).containsKey(Bytes32.wrap(archiveOnlyAccount.getBytes()));
    assertThat(accounts.get(Bytes32.wrap(archiveOnlyAccount.getBytes()))).isEqualTo(archiveValue);
  }

  // ======================== Storage stream routing ========================

  @Test
  public void recentBlockStorageStreamUsesBonsaiLayer() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000040").addressHash();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final Bytes bonsaiSlotValue = Bytes.fromHexString("0xAABB");
    final long headBlock = 1000L;

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccountStorageValueByStorageSlotHash(
        storage, tx, accountHash, slotKey.getSlotHash(), bonsaiSlotValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(headBlock);

    final NavigableMap<Bytes32, Bytes> slots =
        hybridStrategy.streamStorageFlatDatabase(
            storage, accountHash, Bytes32.ZERO, Bytes32.wrap(Bytes.repeat((byte) 0xFF, 32)), Long.MAX_VALUE);

    assertThat(slots).containsKey(Bytes32.wrap(slotKey.getSlotHash().getBytes()));
  }

  @Test
  public void historicalBlockStorageStreamUsesArchiveLayer() {
    // Use a distinct account/slot that has only an archive entry (no competing bonsai key).
    // See comment in historicalBlockAccountStreamUsesArchiveLayer for why.
    final Hash archiveOnlyAccount =
        Address.fromHexString("0x0000000000000000000000000000000000000041").addressHash();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final Bytes archiveSlotValue = Bytes.fromHexString("0xAABB");
    final long headBlock = 1000L;
    final long historicalBlock = headBlock - ARCHIVE_BOUNDARY; // = 488, not recent

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    hybridStrategy.putFlatAccountStorageValueByStorageSlotHash(
        new BonsaiContext(historicalBlock), tx, archiveOnlyAccount, slotKey.getSlotHash(), archiveSlotValue);
    tx.commit();

    headBlockNumber.set(headBlock);
    setWorldBlockNumber(historicalBlock);

    final NavigableMap<Bytes32, Bytes> slots =
        hybridStrategy.streamStorageFlatDatabase(
            storage, archiveOnlyAccount, Bytes32.ZERO, Bytes32.wrap(Bytes.repeat((byte) 0xFF, 32)), Long.MAX_VALUE);

    assertThat(slots).containsKey(Bytes32.wrap(slotKey.getSlotHash().getBytes()));
  }

  // ======================== Helpers ========================

  private void setWorldBlockNumber(final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }
}
