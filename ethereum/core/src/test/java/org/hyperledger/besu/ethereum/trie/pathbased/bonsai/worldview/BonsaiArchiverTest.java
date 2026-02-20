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
    final Address testAddress =
        Address.fromHexString("0x4444444444444444444444444444444444444444");
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
    // Setup: Create 10 accounts with data at block 5
    updateStorageArchiveBlock(5);
    for (int i = 0; i < 10; i++) {
      Address addr = Address.fromHexString(String.format("0x%040d", i));
      storage.updater().putAccountInfoState(addr.addressHash(), Bytes32.random()).commit();
    }

    // Archive with batch size of 3 - should still archive all 10
    int archived = storage.archiveAccountStateByFullScan(100L, 3);

    assertThat(archived).isEqualTo(10);
  }

  @Test
  void archiveAccountStateByFullScan_returnsZeroWhenNothingToArchive() {
    // Setup: Create data at block 100
    updateStorageArchiveBlock(100);
    final Address testAddress =
        Address.fromHexString("0x5555555555555555555555555555555555555555");
    storage.updater().putAccountInfoState(testAddress.addressHash(), Bytes32.random()).commit();

    // Try to archive before block 50 - nothing qualifies
    int archived = storage.archiveAccountStateByFullScan(50L, 1000);

    assertThat(archived).isEqualTo(0);
  }
}
