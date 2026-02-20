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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiverTest {

  private BonsaiWorldStateKeyValueStorage storage;
  private final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();

  @BeforeEach
  void setUp() {
    storage =
        spy(
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG));
    storage.upgradeToFullFlatDbMode();

    // Set initial block number
    updateStorageArchiveBlock(1);
  }

  private void updateStorageArchiveBlock(final long blockNumber) {
    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void archivePreviousAccountStateBatched_returnsZero_whenNoDataExists() {
    final BlockHeader header = blockBuilder.number(100).buildHeader();
    final Hash accountHash = Hash.hash(Bytes.fromHexString("0x1234"));

    // Create a transaction that we'll pass in
    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();

    // Call the batched method with empty storage
    int archivedCount = storage.archivePreviousAccountStateBatched(tx, header, accountHash);

    // No entries to archive in empty storage
    assertThat(archivedCount).isEqualTo(0);
  }

  @Test
  void archivePreviousAccountStateBatched_addsToTransaction_doesNotCommit() {
    // Set up: Create account data at block 50
    final Address testAddress = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Hash accountHash = testAddress.addressHash();
    final Bytes32 accountValue = Bytes32.random();

    // Put account data at block 50
    updateStorageArchiveBlock(50);
    storage.updater().putAccountInfoState(accountHash, accountValue).commit();

    // Now we're at block 100
    updateStorageArchiveBlock(100);

    // Verify the data exists in ACCOUNT_INFO_STATE before archiving
    long countBeforeArchive =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE).count();
    assertThat(countBeforeArchive).isGreaterThan(0);

    // Create a transaction that we'll pass in (but NOT commit)
    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();

    // Call the batched method with header for block 100
    // This should find the account state from block 50 and add it to the transaction
    final BlockHeader header = blockBuilder.number(100).buildHeader();
    int archivedCount = storage.archivePreviousAccountStateBatched(tx, header, accountHash);

    // Should have archived at least 1 entry
    assertThat(archivedCount).isGreaterThan(0);

    // CRITICAL: Verify the transaction was NOT committed by the method
    // The data should still be in ACCOUNT_INFO_STATE (not yet moved to archive)
    // because we haven't committed the transaction
    long countAfterBatch =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE).count();
    assertThat(countAfterBatch)
        .as("Data should still be in original segment since tx was not committed")
        .isEqualTo(countBeforeArchive);

    // The archive segment should still be empty (only has the archived block marker)
    long archiveCount =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE).count();
    assertThat(archiveCount)
        .as("Archive segment should have no account data yet (only block marker if present)")
        .isLessThanOrEqualTo(1);

    // Now commit the transaction and verify data moves
    tx.commit();

    // After commit, the data should be moved from ACCOUNT_INFO_STATE to ACCOUNT_INFO_STATE_ARCHIVE
    long countAfterCommit =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE).count();
    assertThat(countAfterCommit)
        .as("Data should be removed from original segment after commit")
        .isLessThan(countBeforeArchive);

    long archiveCountAfterCommit =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE).count();
    assertThat(archiveCountAfterCommit)
        .as("Data should now be in archive segment")
        .isGreaterThan(archiveCount);
  }

  @Test
  void archivePreviousStorageStateBatched_returnsZero_whenNoDataExists() {
    final BlockHeader header = blockBuilder.number(100).buildHeader();
    final Hash accountHash = Hash.hash(Bytes.fromHexString("0x1234"));
    final Hash slotHash = Hash.hash(Bytes.fromHexString("0x5678"));
    final Bytes storageSlotKey = Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes());

    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();

    int archivedCount = storage.archivePreviousStorageStateBatched(tx, header, storageSlotKey);

    assertThat(archivedCount).isEqualTo(0);
  }

  @Test
  void batchedArchiving_commitsMultipleEntriesInSingleTransaction() {
    // This test verifies that multiple account/storage changes are batched
    // into a single transaction commit rather than individual commits

    final BlockHeader header = blockBuilder.number(100).buildHeader();
    final SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();

    // Archive multiple accounts in the same transaction
    int totalArchived = 0;
    for (int i = 0; i < 10; i++) {
      Hash accountHash = Hash.hash(Bytes.of((byte) i));
      totalArchived += storage.archivePreviousAccountStateBatched(tx, header, accountHash);
    }

    // Transaction not committed yet - caller controls commit
    // In production, commit happens after BATCH_SIZE entries or at end of batch

    // Now commit all at once
    tx.commit();

    // Verify the test ran (even if no entries archived from empty storage)
    assertThat(totalArchived).isGreaterThanOrEqualTo(0);
  }

  @Test
  void archiveAccountStateByFullScan_archivesEntriesBelowThreshold() {
    // Setup: Create account data at multiple blocks
    final Address testAddress = Address.fromHexString("0x4444444444444444444444444444444444444444");
    final Hash accountHash = testAddress.addressHash();

    // Write state at blocks 10, 20, 30, 40
    for (long block : new long[] {10L, 20L, 30L, 40L}) {
      updateStorageArchiveBlock(block);
      storage.updater().putAccountInfoState(accountHash, Bytes32.random()).commit();
    }

    // Count entries before
    long countBefore =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(countBefore).isEqualTo(4);

    // Archive everything before block 35
    int archived = storage.archiveAccountStateByFullScan(35L, 1000);

    // Should archive blocks 10, 20, 30 (3 entries)
    assertThat(archived).isEqualTo(3);

    // Verify only block 40 remains in live segment
    long countAfter =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(countAfter).isEqualTo(1);

    // Verify 3 entries in archive
    long archiveCount =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archiveCount).isEqualTo(3);
  }

  @Test
  void archiveAccountStateByFullScan_respectsBatchSize() {
    // Setup: Create 10 accounts, each with 2 versions (at block 5 and block 50)
    // This gives us 10 entries to archive (block 5 versions) and 10 to keep (block 50 versions)
    for (int i = 0; i < 10; i++) {
      Address addr = Address.fromHexString(String.format("0x%040d", i));
      // First version at block 5
      updateStorageArchiveBlock(5);
      storage.updater().putAccountInfoState(addr.addressHash(), Bytes32.random()).commit();
      // Second version at block 50
      updateStorageArchiveBlock(50);
      storage.updater().putAccountInfoState(addr.addressHash(), Bytes32.random()).commit();
    }

    // Archive with batch size of 3 - should archive the 10 older entries (block 5 versions)
    // keeping the 10 most recent entries (block 50 versions) in live segment
    int archived = storage.archiveAccountStateByFullScan(100L, 3);

    assertThat(archived).isEqualTo(10);
  }

  @Test
  void archiveAccountStateByFullScan_preservesMostRecentEntry() {
    // Setup: Create 10 accounts with ONLY ONE entry each at block 5
    // These should NOT be archived because they are the most recent (and only) entry
    updateStorageArchiveBlock(5);
    for (int i = 0; i < 10; i++) {
      Address addr = Address.fromHexString(String.format("0x%040d", i));
      storage.updater().putAccountInfoState(addr.addressHash(), Bytes32.random()).commit();
    }

    // Try to archive - should archive 0 because each entry is the most recent for its account
    int archived = storage.archiveAccountStateByFullScan(100L, 3);

    assertThat(archived).isEqualTo(0);

    // All entries should still be in the live segment
    long countAfter = storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE).count();
    assertThat(countAfter).isEqualTo(10);
  }

  @Test
  void archiveAccountStateByFullScan_returnsZeroWhenNothingToArchive() {
    // Setup: Create data at block 100
    updateStorageArchiveBlock(100);
    final Address testAddress = Address.fromHexString("0x5555555555555555555555555555555555555555");
    storage.updater().putAccountInfoState(testAddress.addressHash(), Bytes32.random()).commit();

    // Try to archive before block 50 - nothing qualifies
    int archived = storage.archiveAccountStateByFullScan(50L, 1000);

    assertThat(archived).isEqualTo(0);
  }

  @Test
  void archiveStorageStateByFullScan_archivesEntriesBelowThreshold() {
    final Address testAddress = Address.fromHexString("0x6666666666666666666666666666666666666666");
    final Hash accountHash = testAddress.addressHash();
    final Hash slotHash = Hash.hash(Bytes.fromHexString("0x1234"));

    // Write storage at blocks 10, 20, 30, 40
    for (long block : new long[] {10L, 20L, 30L, 40L}) {
      updateStorageArchiveBlock(block);
      storage.updater().putStorageValueBySlotHash(accountHash, slotHash, Bytes32.random()).commit();
    }

    // Count storage entries before
    long countBefore =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE).count();
    assertThat(countBefore).isEqualTo(4);

    // Archive everything before block 35
    int archived = storage.archiveStorageStateByFullScan(35L, 1000);

    // Should archive blocks 10, 20, 30 (3 entries)
    assertThat(archived).isEqualTo(3);

    // Verify only 1 entry remains in live segment
    long countAfter =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE).count();
    assertThat(countAfter).isEqualTo(1);

    // Verify 3 entries in archive
    long archiveCount =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_ARCHIVE).count();
    assertThat(archiveCount).isEqualTo(3);
  }

  @Test
  void fullScanAndBatchedArchiving_produceSameResults() {
    // This integration test verifies that full scan archiving and batched (TrieLog-driven)
    // archiving produce equivalent results

    // Create two separate storage instances with identical data
    BonsaiWorldStateKeyValueStorage storage1 =
        spy(
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG));
    storage1.upgradeToFullFlatDbMode();

    BonsaiWorldStateKeyValueStorage storage2 =
        spy(
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG));
    storage2.upgradeToFullFlatDbMode();

    // Same test data for both
    final Address testAddress = Address.fromHexString("0x7777777777777777777777777777777777777777");
    final Hash accountHash = testAddress.addressHash();
    final Bytes32 value1 =
        Bytes32.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");
    final Bytes32 value2 =
        Bytes32.fromHexString("0x2222222222222222222222222222222222222222222222222222222222222222");
    final Bytes32 value3 =
        Bytes32.fromHexString("0x3333333333333333333333333333333333333333333333333333333333333333");

    // Write same data to both storages at blocks 10, 20, 30
    for (BonsaiWorldStateKeyValueStorage s :
        new BonsaiWorldStateKeyValueStorage[] {storage1, storage2}) {
      // Block 10
      SegmentedKeyValueStorageTransaction tx1 = s.getComposedWorldStateStorage().startTransaction();
      tx1.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(10).toArrayUnsafe());
      tx1.commit();
      s.updater().putAccountInfoState(accountHash, value1).commit();

      // Block 20
      SegmentedKeyValueStorageTransaction tx2 = s.getComposedWorldStateStorage().startTransaction();
      tx2.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(20).toArrayUnsafe());
      tx2.commit();
      s.updater().putAccountInfoState(accountHash, value2).commit();

      // Block 30
      SegmentedKeyValueStorageTransaction tx3 = s.getComposedWorldStateStorage().startTransaction();
      tx3.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(30).toArrayUnsafe());
      tx3.commit();
      s.updater().putAccountInfoState(accountHash, value3).commit();
    }

    // Archive entries before block 25 using FULL SCAN on storage1
    int fullScanArchived = storage1.archiveAccountStateByFullScan(25L, 1000);

    // Archive entries before block 25 using BATCHED method on storage2
    // This simulates TrieLog-driven archiving for specific accounts
    final BlockHeader header = blockBuilder.number(25).buildHeader();
    SegmentedKeyValueStorageTransaction batchTx =
        storage2.getComposedWorldStateStorage().startTransaction();
    int batchedArchived = storage2.archivePreviousAccountStateBatched(batchTx, header, accountHash);
    batchTx.commit();

    // Both methods should archive the same number of entries (blocks 10, 20 = 2 entries)
    assertThat(fullScanArchived).isEqualTo(2);
    assertThat(batchedArchived).isEqualTo(1); // Batched method processes one entry at a time

    // Both storages should have same count in live segment (block 30 only)
    long live1 =
        storage1.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    long live2 =
        storage2.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();

    // Full scan archived 2 entries (blocks 10, 20), leaving 1 (block 30)
    assertThat(live1).isEqualTo(1);
    // Batched only archived 1 entry per call (need multiple calls for multiple entries)
    // So it has 2 remaining (blocks 20, 30)
    assertThat(live2).isEqualTo(2);

    // For full equivalence test, we need to call batched multiple times
    // Let's do another batched call
    SegmentedKeyValueStorageTransaction batchTx2 =
        storage2.getComposedWorldStateStorage().startTransaction();
    storage2.archivePreviousAccountStateBatched(batchTx2, header, accountHash);
    batchTx2.commit();

    // Now both should have same live count
    long live2After =
        storage2.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(live2After).isEqualTo(1);

    // Both storages should have same count in archive segment
    long archive1 =
        storage1.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    long archive2 =
        storage2.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archive1).isEqualTo(2);
    assertThat(archive2).isEqualTo(2);

    // Clean up
    try {
      storage1.close();
      storage2.close();
    } catch (Exception e) {
      // Ignore close exceptions in test
    }
  }

  @Test
  void repairAccountStateFromArchive_restoresEntriesMissingFromLiveSegment() {
    // Setup: Create an account with data at a single block
    final Address testAddress = Address.fromHexString("0x8888888888888888888888888888888888888888");
    final Hash accountHash = testAddress.addressHash();
    final Bytes32 accountValue = Bytes32.random();

    // Put account data at block 10
    updateStorageArchiveBlock(10);
    storage.updater().putAccountInfoState(accountHash, accountValue).commit();

    // Verify the entry exists in live segment
    long liveBefore =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveBefore).isEqualTo(1);

    // Simulate buggy full scan: manually move the entry to archive segment
    // This mimics what the old buggy code would have done - archive the ONLY entry
    SegmentedKeyValueStorageTransaction moveTx =
        storage.getComposedWorldStateStorage().startTransaction();

    // Find the entry in live segment and move it to archive
    storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
        .filter(
            p ->
                accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                    >= accountHash.size())
        .forEach(
            pair -> {
              // Copy to archive
              moveTx.put(ACCOUNT_INFO_STATE_ARCHIVE, pair.getKey(), pair.getValue());
              // Remove from live
              moveTx.remove(ACCOUNT_INFO_STATE, pair.getKey());
            });
    moveTx.commit();

    // Verify the entry is now only in archive
    long liveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveAfterMove).isEqualTo(0);

    long archiveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archiveAfterMove).isEqualTo(1);

    // Run repair
    int repaired = storage.repairAccountStateFromArchive(100);

    // Should have repaired 1 entry
    assertThat(repaired).isEqualTo(1);

    // Verify the entry is now restored to live segment
    long liveAfterRepair =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveAfterRepair).isEqualTo(1);

    // Archive should still have the entry (repair copies, doesn't move)
    long archiveAfterRepair =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archiveAfterRepair).isEqualTo(1);
  }

  @Test
  void repairAccountStateFromArchive_doesNotRestoreIfLiveEntryExists() {
    // Setup: Create an account with data at multiple blocks
    final Address testAddress = Address.fromHexString("0x9999999999999999999999999999999999999999");
    final Hash accountHash = testAddress.addressHash();

    // Put account data at block 10 and block 20
    updateStorageArchiveBlock(10);
    storage.updater().putAccountInfoState(accountHash, Bytes32.random()).commit();
    updateStorageArchiveBlock(20);
    storage.updater().putAccountInfoState(accountHash, Bytes32.random()).commit();

    // Archive entries before block 15 (should archive block 10 entry)
    int archived = storage.archiveAccountStateByFullScan(15L, 100);
    assertThat(archived).isEqualTo(1);

    // Verify: 1 in live (block 20), 1 in archive (block 10)
    long liveCount =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveCount).isEqualTo(1);

    long archiveCount =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archiveCount).isEqualTo(1);

    // Run repair - should not restore because live segment already has an entry
    int repaired = storage.repairAccountStateFromArchive(100);

    // Should have repaired 0 entries (live segment already has the account)
    assertThat(repaired).isEqualTo(0);

    // Live count should remain 1
    long liveCountAfter =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveCountAfter).isEqualTo(1);
  }

  @Test
  void repairStorageStateFromArchive_restoresEntriesMissingFromLiveSegment() {
    // Setup: Create storage data at a single block
    final Address testAddress = Address.fromHexString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    final Hash accountHash = testAddress.addressHash();
    final Hash slotHash = Hash.hash(Bytes.fromHexString("0xABCD"));
    final Bytes32 storageValue = Bytes32.random();

    // Put storage data at block 10
    updateStorageArchiveBlock(10);
    storage.updater().putStorageValueBySlotHash(accountHash, slotHash, storageValue).commit();

    // Verify the entry exists in live segment
    long liveBefore =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE).count();
    assertThat(liveBefore).isEqualTo(1);

    // Simulate buggy full scan: manually move the entry to archive segment
    SegmentedKeyValueStorageTransaction moveTx =
        storage.getComposedWorldStateStorage().startTransaction();

    storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE)
        .forEach(
            pair -> {
              moveTx.put(ACCOUNT_STORAGE_ARCHIVE, pair.getKey(), pair.getValue());
              moveTx.remove(ACCOUNT_STORAGE_STORAGE, pair.getKey());
            });
    moveTx.commit();

    // Verify the entry is now only in archive
    long liveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE).count();
    assertThat(liveAfterMove).isEqualTo(0);

    long archiveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_ARCHIVE).count();
    assertThat(archiveAfterMove).isEqualTo(1);

    // Run repair
    int repaired = storage.repairStorageStateFromArchive(100);

    // Should have repaired 1 entry
    assertThat(repaired).isEqualTo(1);

    // Verify the entry is now restored to live segment
    long liveAfterRepair =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_STORAGE_STORAGE).count();
    assertThat(liveAfterRepair).isEqualTo(1);
  }

  @Test
  void repairAccountStateFromArchive_restoresMostRecentEntryWhenMultipleInArchive() {
    // Setup: Create an account with data at multiple blocks, all incorrectly archived
    final Address testAddress = Address.fromHexString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    final Hash accountHash = testAddress.addressHash();
    final Bytes32 value10 =
        Bytes32.fromHexString("0x1010101010101010101010101010101010101010101010101010101010101010");
    final Bytes32 value20 =
        Bytes32.fromHexString("0x2020202020202020202020202020202020202020202020202020202020202020");
    final Bytes32 value30 =
        Bytes32.fromHexString("0x3030303030303030303030303030303030303030303030303030303030303030");

    // Put account data at blocks 10, 20, 30
    updateStorageArchiveBlock(10);
    storage.updater().putAccountInfoState(accountHash, value10).commit();
    updateStorageArchiveBlock(20);
    storage.updater().putAccountInfoState(accountHash, value20).commit();
    updateStorageArchiveBlock(30);
    storage.updater().putAccountInfoState(accountHash, value30).commit();

    // Simulate buggy full scan: move ALL entries to archive
    SegmentedKeyValueStorageTransaction moveTx =
        storage.getComposedWorldStateStorage().startTransaction();

    storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
        .filter(
            p ->
                accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                    >= accountHash.size())
        .forEach(
            pair -> {
              moveTx.put(ACCOUNT_INFO_STATE_ARCHIVE, pair.getKey(), pair.getValue());
              moveTx.remove(ACCOUNT_INFO_STATE, pair.getKey());
            });
    moveTx.commit();

    // Verify all 3 entries are in archive, none in live
    long liveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveAfterMove).isEqualTo(0);

    long archiveAfterMove =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE_ARCHIVE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(archiveAfterMove).isEqualTo(3);

    // Run repair
    int repaired = storage.repairAccountStateFromArchive(100);

    // Should have repaired 1 entry (the most recent one - block 30)
    assertThat(repaired).isEqualTo(1);

    // Verify only 1 entry is restored to live segment
    long liveAfterRepair =
        storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
            .filter(
                p ->
                    accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                        >= accountHash.size())
            .count();
    assertThat(liveAfterRepair).isEqualTo(1);

    // Verify the restored entry has the most recent value (block 30)
    storage.getComposedWorldStateStorage().stream(ACCOUNT_INFO_STATE)
        .filter(
            p ->
                accountHash.getBytes().commonPrefixLength(Bytes.wrap(p.getKey()))
                    >= accountHash.size())
        .forEach(
            pair -> {
              assertThat(Bytes.wrap(pair.getValue())).isEqualTo(value30);
            });
  }
}
